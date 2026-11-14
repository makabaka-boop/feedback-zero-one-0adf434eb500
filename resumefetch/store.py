"""SQLite 持久层：代（generation）、确认片段、终态与 URL 绑定。

设计要点
========
* 工作目录与 URL 一一绑定（``meta`` 单行）：同一工作目录不得换 URL，否则直接失败。
* 一个工作目录只保留**当前代**。HEAD 发现强 ETag/总长变化即清库换代，
  旧代确认片段全部删除——绝不允许把不同代的字节拼进同一份归档。
* 片段（segment）是已“整体校验并原子确认”的闭区间 [start, end]，
  入库时按起点排序并合并相邻/重叠段；重叠字节必须一致。
* 换代、片段确认、完成均在单事务内 ``commit``（FULL 同步），
  强退后重开库即可恢复，不存在半写代或半确认片段。
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass

DB_NAME = "resumefetch.sqlite3"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    url        TEXT NOT NULL,
    etag       TEXT NOT NULL,
    length     INTEGER NOT NULL CHECK (length >= 0),
    generation INTEGER NOT NULL CHECK (generation >= 1),
    state      TEXT NOT NULL CHECK (state IN ('running', 'complete'))
);
CREATE TABLE IF NOT EXISTS segment (
    generation INTEGER NOT NULL,
    start      INTEGER NOT NULL CHECK (start >= 0),
    end        INTEGER NOT NULL CHECK (end >= start),
    data       BLOB NOT NULL,
    PRIMARY KEY (generation, start)
);
"""


@dataclass(frozen=True)
class Meta:
    url: str
    etag: str
    length: int
    generation: int
    state: str


class StoreError(Exception):
    """持久层级错误（含 URL 绑定冲突、重叠不一致等）。"""


def _connect(workdir: str) -> sqlite3.Connection:
    # check_same_thread=False：客户端在锁保护下从 worker 线程访问连接，
    # 全部读写均由 client 的 lock 串行化，不会并发使用同一连接。
    conn = sqlite3.connect(os.path.join(workdir, DB_NAME), timeout=30,
                           check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def open_store(workdir: str) -> sqlite3.Connection:
    """打开（必要时初始化）工作目录下的库。"""
    os.makedirs(workdir, exist_ok=True)
    conn = _connect(workdir)
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def get_meta(conn: sqlite3.Connection) -> Meta | None:
    row = conn.execute(
        "SELECT url, etag, length, generation, state FROM meta WHERE id = 1"
    ).fetchone()
    if row is None:
        return None
    return Meta(url=row[0], etag=row[1], length=row[2],
                generation=row[3], state=row[4])


def initialize(conn: sqlite3.Connection, url: str, etag: str,
               length: int) -> int:
    """绑定 URL 并写入第一代。仅允许在空库上调用。返回代编号。"""
    with conn:
        conn.execute(
            "INSERT INTO meta (id, url, etag, length, generation, state) "
            "VALUES (1, ?, ?, ?, 1, 'running')",
            (url, etag, length),
        )
    return 1


def replace_generation(conn: sqlite3.Connection, etag: str,
                       length: int) -> int:
    """换代：清空全部已确认片段并写入新元数据（旧结果文件由调用方保留）。"""
    old = get_meta(conn)
    assert old is not None
    new_gen = old.generation + 1
    with conn:
        conn.execute("DELETE FROM segment")
        conn.execute(
            "UPDATE meta SET etag = ?, length = ?, generation = ?, "
            "state = 'running' WHERE id = 1",
            (etag, length, new_gen),
        )
    return new_gen


def replace_with_full_body(conn: sqlite3.Connection, etag: str,
                           length: int, body: bytes) -> int:
    """200 整份响应的原子换代：单事务内清段、写整段、换元数据。"""
    old = get_meta(conn)
    new_gen = 1 if old is None else old.generation + 1
    with conn:
        conn.execute("DELETE FROM segment")
        conn.execute(
            "INSERT INTO segment (generation, start, end, data) "
            "VALUES (?, 0, ?, ?)",
            (new_gen, length - 1, body),
        )
        if old is None:
            conn.execute(
                "INSERT INTO meta (id, url, etag, length, generation, state) "
                "VALUES (1, '', ?, ?, ?, 'running')",
                (etag, length, new_gen),
            )
        else:
            conn.execute(
                "UPDATE meta SET etag = ?, length = ?, generation = ?, "
                "state = 'running' WHERE id = 1",
                (etag, length, new_gen),
            )
    return new_gen


def replace_current_body_same_generation(conn: sqlite3.Connection,
                                         body: bytes) -> None:
    """同代 200：单事务内用整份正文覆盖该代全部局部片段。"""
    meta = get_meta(conn)
    assert meta is not None
    with conn:
        conn.execute("DELETE FROM segment WHERE generation = ?",
                     (meta.generation,))
        conn.execute(
            "INSERT INTO segment (generation, start, end, data) "
            "VALUES (?, 0, ?, ?)",
            (meta.generation, meta.length - 1, body),
        )


def load_segments(conn: sqlite3.Connection, generation: int) -> list[tuple[int, int, bytes]]:
    rows = conn.execute(
        "SELECT start, end, data FROM segment WHERE generation = ? "
        "ORDER BY start",
        (generation,),
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def missing_intervals(segments: list[tuple[int, int, bytes]],
                      total_length: int) -> list[tuple[int, int]]:
    """根据已确认片段计算缺失的闭区间集合。"""
    missing: list[tuple[int, int]] = []
    cursor = 0
    for start, end, _ in segments:
        if start > cursor:
            missing.append((cursor, start - 1))
        cursor = max(cursor, end + 1)
    if cursor < total_length:
        missing.append((cursor, total_length - 1))
    return missing


def merge_confirmed(
    conn: sqlite3.Connection,
    generation: int,
    incoming: list[tuple[int, int, bytes]],
) -> None:
    """把一批已整体校验的片段原子并入当前代。

    入库前在内存中与现有片段合并：重叠处逐字节比对，不一致即抛
    ``StoreError``（绝不写入）；相邻或重叠片段合并存储。
    """
    existing = load_segments(conn, generation)
    combined = list(existing)
    for start, end, data in incoming:
        if len(data) != end - start + 1:
            raise StoreError(
                f"片段载荷长度 {len(data)} 与范围 [{start},{end}] 不符")
        for s, e, blob in combined:
            ov_start = max(start, s)
            ov_end = min(end, e)
            if ov_start <= ov_end:
                new_part = data[ov_start - start: ov_end - start + 1]
                old_part = blob[ov_start - s: ov_end - s + 1]
                if new_part != old_part:
                    raise StoreError(
                        f"代 {generation} 重叠区间 [{ov_start},{ov_end}] "
                        "字节不一致（混版/损坏）")
        combined.append((start, end, data))

    combined.sort(key=lambda item: item[0])
    merged: list[tuple[int, int, bytearray]] = []
    for start, end, data in combined:
        if merged and start <= merged[-1][1] + 1:
            ps, pe, pbuf = merged[-1]
            if start <= pe:  # 重叠（已验证一致），只追加新尾部
                tail = data[pe - start + 1:]
            else:           # 严格相邻
                tail = data
            pbuf.extend(tail)
            merged[-1] = (ps, max(pe, end), pbuf)
        else:
            merged.append((start, end, bytearray(data)))

    with conn:
        conn.execute("DELETE FROM segment WHERE generation = ?",
                     (generation,))
        conn.executemany(
            "INSERT INTO segment (generation, start, end, data) "
            "VALUES (?, ?, ?, ?)",
            [(generation, s, e, bytes(buf)) for s, e, buf in merged],
        )


def mark_complete(conn: sqlite3.Connection) -> None:
    with conn:
        conn.execute(
            "UPDATE meta SET state = 'complete' WHERE id = 1")


def assemble(segments: list[tuple[int, int, bytes]],
             total_length: int) -> bytes:
    """把覆盖 [0, total_length) 的连续片段拼成完整正文。"""
    if total_length == 0:
        return b""
    if len(segments) != 1 or segments[0][0] != 0 \
            or segments[0][1] != total_length - 1:
        raise StoreError("片段未连续覆盖完整归档，拒绝拼装")
    body = segments[0][2]
    if len(body) != total_length:
        raise StoreError("拼装正文长度与总长不符")
    return body
