"""续采编排：HEAD 换代 → 并发交错 Range → 整批校验 → 原子确认 → 原子落盘。

代（generation）与终态规则
==========================
* HEAD 仅接受 200 + 强 ETag + 非负 Content-Length。
* 工作目录绑定 URL，换 URL 直接失败；元数据（ETag/总长）相同则复用已确认
  片段，不同则清空片段换代——不同代字节永不相混。
* 每个在途请求携带发起时代编号；响应回来先判代与终态：旧代响应、终止后的
  响应（含迟到错误）一律忽略，不影响本次结果。
* 206 各请求的整份响应（全部部件）先独立完整校验，但**不写库**；一批请求
  全部返回后，再做批内/与已确认片段的重叠字节一致性预检，通过后在单事务内
  整批原子确认。任一请求非法或重叠不一致，整批不确认、残体不落盘。
* 200 必须带强 ETag 且 Content-Length 与完整正文匹配，原子替换局部状态。
* 只有覆盖完整后才原子替换结果文件；确认、换代、完成均持久化。
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import threading

from . import http_source, store

# 测试用强退钩子（验收脚本通过环境变量打开；正常使用不设置）。
_KILL_BEFORE_CONFIRM = "RESUMEFETCH_KILL_BEFORE_CONFIRM"
_KILL_AFTER_CONFIRM = "RESUMEFETCH_KILL_AFTER_CONFIRM"


class FetchError(Exception):
    def __init__(self, message: str, *, field: str | None = None,
                 request: int | None = None, part: int | None = None):
        super().__init__(message)
        self.message = message
        self.field = field
        self.request = request
        self.part = part

    def to_dict(self) -> dict:
        return {"error": self.message, "field": self.field,
                "request": self.request, "part": self.part}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise FetchError(f"环境变量 {name} 不是整数: {raw}",
                         field=name) from exc
    if value <= 0:
        raise FetchError(f"环境变量 {name} 必须为正整数: {raw}",
                         field=name)
    return value


def _parse_plan(raw: str, total_length: int) -> list[list[tuple[int, int]]]:
    """解析 RESUMEFETCH_PLAN，如 ``2-3;0-2`` 或 ``0-1,3-3;2-2``。"""
    requests: list[list[tuple[int, int]]] = []
    for req_spec in raw.split(";"):
        req_spec = req_spec.strip()
        if not req_spec:
            continue
        ranges: list[tuple[int, int]] = []
        for piece in req_spec.split(","):
            piece = piece.strip()
            if "-" not in piece:
                raise FetchError(f"RESUMEFETCH_PLAN 区间非法: {piece}",
                                 field="RESUMEFETCH_PLAN")
            lo_s, hi_s = piece.split("-", 1)
            try:
                lo, hi = int(lo_s), int(hi_s)
            except ValueError as exc:
                raise FetchError(f"RESUMEFETCH_PLAN 区间非法: {piece}",
                                 field="RESUMEFETCH_PLAN") from exc
            if lo < 0 or hi >= total_length or lo > hi:
                raise FetchError(
                    f"RESUMEFETCH_PLAN 区间越界: [{lo},{hi}]，总长 {total_length}",
                    field="RESUMEFETCH_PLAN")
            ranges.append((lo, hi))
        if ranges:
            requests.append(ranges)
    if not requests:
        raise FetchError("RESUMEFETCH_PLAN 为空", field="RESUMEFETCH_PLAN")
    return requests


def _default_plan(missing: list[tuple[int, int]],
                  chunk: int) -> list[list[tuple[int, int]]]:
    """默认规划：把缺失区间按 chunk 切块，每块一个请求。"""
    plan: list[list[tuple[int, int]]] = []
    for s, e in missing:
        cursor = s
        while cursor <= e:
            end = min(cursor + chunk - 1, e)
            plan.append([(cursor, end)])
            cursor = end + 1
    return plan


def _atomic_write_result(result_path: str, payload: dict) -> None:
    directory = os.path.dirname(os.path.abspath(result_path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".resumefetch-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, result_path)
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _build_output(etag: str, body: bytes) -> dict:
    digest = hashlib.sha256(body).hexdigest()  # hexdigest 即小写
    return {
        "etag": etag,
        "length": len(body),
        "sha256": digest,
        "data": base64.b64encode(body).decode("ascii"),  # 带填充标准 Base64
    }


def run(url: str, workdir: str, result_path: str,
        *, timeout: float | None = None) -> dict:
    if not isinstance(url, str) or not url:
        raise FetchError("url 必须是非空字符串", field="url")
    if not isinstance(workdir, str) or not workdir:
        raise FetchError("workdir 必须是非空字符串", field="workdir")
    if not isinstance(result_path, str) or not result_path:
        raise FetchError("result_path 必须是非空字符串", field="result_path")
    if timeout is None:
        timeout = float(_env_int("RESUMEFETCH_TIMEOUT", 10))
    concurrency = _env_int("RESUMEFETCH_CONCURRENCY", 4)
    chunk = _env_int("RESUMEFETCH_CHUNK", 65536)

    conn = store.open_store(workdir)
    lock = threading.Lock()
    # terminal: 已完成或已被 200 终态接管；之后到达的任何响应都忽略。
    terminal = threading.Event()

    # ---------- 1. HEAD 与换代判定（同步，无并发） ----------
    try:
        head = http_source.head(url, timeout)
    except http_source.ProtocolError as exc:
        raise FetchError(str(exc), field="url") from exc

    meta = store.get_meta(conn)
    if meta is not None and meta.url != url:
        raise FetchError(
            f"工作目录已绑定 {meta.url}，禁止换 URL（请求 {url}）",
            field="url")
    if meta is None:
        generation = store.initialize(conn, url, head.etag, head.length)
        current_etag, current_length = head.etag, head.length
    elif meta.etag != head.etag or meta.length != head.length:
        # 元数据不同：清空覆盖换代（旧结果文件保留）。
        generation = store.replace_generation(conn, head.etag, head.length)
        current_etag, current_length = head.etag, head.length
    else:
        generation = meta.generation
        current_etag, current_length = meta.etag, meta.length
        if meta.state == "complete":
            return _resume_complete(conn, generation, current_etag,
                                    current_length, result_path)

    # ---------- 2. 零长归档立即完成 ----------
    if current_length == 0:
        return _finalize(conn, generation, current_etag, b"", result_path,
                         lock, terminal)

    plan_env = os.environ.get("RESUMEFETCH_PLAN")
    request_seq = 0  # 全局发起序编号
    round_no = 0

    # ---------- 3. 轮次：并发取 → 整批校验 → 原子确认 → 再规划 ----------
    while True:
        round_no += 1
        segments = store.load_segments(conn, generation)
        missing = store.missing_intervals(segments, current_length)
        if not missing:
            return _finalize(conn, generation, current_etag, None,
                             result_path, lock, terminal)

        # 显式计划只用于首轮（模拟验收请求形态）；后续轮次按缺口自动补采。
        if plan_env is not None and round_no == 1:
            requests_spec = _parse_plan(plan_env, current_length)
        else:
            requests_spec = _default_plan(missing, chunk)

        batch: list[tuple[int, list[tuple[int, int]]]] = []
        for ranges in requests_spec:
            batch.append((request_seq, ranges))
            request_seq += 1

        # 每个请求先独立完成“整份响应校验”（不碰库），产出带来源
        # （请求号/部件号）的候选；200 终态单独标记。校验失败入 batch_errors。
        candidates: dict[int, list[tuple]] = {}
        full200: list[tuple[int, object]] = []
        batch_errors: list[FetchError] = []
        batch_lock = threading.Lock()

        def worker(req_no: int, ranges: list[tuple[int, int]]) -> None:
            gen_token = generation
            try:
                resp = http_source.range_get(url, ranges, current_etag,
                                             timeout)
            except http_source.ProtocolError as exc:
                with batch_lock:
                    # 旧代/终止后的错误（迟到错误）忽略。
                    if terminal.is_set() or gen_token != generation:
                        return
                    batch_errors.append(
                        FetchError(str(exc), request=req_no, part=None))
                return

            with batch_lock:
                if terminal.is_set() or gen_token != generation:
                    return  # 旧代或终止后的响应，整体忽略
                try:
                    if resp.status == 206:
                        candidates[req_no] = _validate_206(
                            resp, ranges, req_no,
                            current_etag, current_length)
                    elif resp.status == 200:
                        # 立即置终态：此后同批到达的任何响应/错误均按迟到忽略。
                        full200.append((req_no, resp))
                        terminal.set()
                    else:
                        # 200 终态已建立后的迟到错误状态码直接丢弃。
                        if full200:
                            return
                        batch_errors.append(FetchError(
                            f"非法状态码 {resp.status}（仅 200/206 合法）",
                            request=req_no, part=None))
                except FetchError as exc:
                    batch_errors.append(exc)
                except http_source.ProtocolError as exc:
                    batch_errors.append(
                        FetchError(str(exc), request=req_no, part=None))
                except ValueError as exc:  # 非法响应头
                    batch_errors.append(
                        FetchError(f"非法响应头: {exc}",
                                   request=req_no, part=None))

        threads = []
        for i, (req_no, ranges) in enumerate(batch):
            t = threading.Thread(target=worker, args=(req_no, ranges),
                                 daemon=True)
            threads.append(t)
            t.start()
            if (i + 1) % concurrency == 0:
                t.join()
        for t in threads:
            t.join()

        if batch_errors:
            # 任一请求非法：整批不确认，保留此前已确认片段，按发起序上报。
            raise min(batch_errors,
                      key=lambda e: (e.request is None, e.request or 0))

        # 串行批处理（持锁）：200 终态优先；否则做 206 批级预检与整批确认。
        with lock:
            if full200:
                req_no, resp = min(full200, key=lambda item: item[0])
                _handle_200(resp, req_no, conn, generation,
                            current_length, current_etag)
                terminal.set()
                meta = store.get_meta(conn)
                generation = meta.generation
                current_etag, current_length = meta.etag, meta.length
                terminal.clear()
                continue

            ordered: list[tuple] = []
            for req_no in sorted(candidates):
                ordered.extend(candidates[req_no])

            # 批内/与已确认片段的重叠字节一致性预检，定位到后到部件。
            _check_batch_overlaps(conn, generation, ordered)

            # 强退钩子：整批校验通过、原子确认之前。
            if os.environ.get(_KILL_BEFORE_CONFIRM) == "1":
                os._exit(9)

            incoming = [(s, e, data) for s, e, data, _ in ordered]
            store.merge_confirmed(conn, generation, incoming)

            if os.environ.get(_KILL_AFTER_CONFIRM) == "1":
                os._exit(9)


def _validate_206(resp, ranges, req_no, current_etag: str,
                  current_length: int) -> list[tuple]:
    """校验单个 206 整份响应（不写库），返回带来源的候选部件列表。

    候选项结构：(start, end, data, (req_no, part_no))。
    """
    etag = http_source.parse_strong_etag(resp.headers.get("etag"),
                                         f"请求 {req_no} 的 206 响应")
    if etag != current_etag:
        raise FetchError(
            f"206 ETag {etag} 与本代 {current_etag} 不符（疑似混版）",
            request=req_no, part=None)
    declared_len = resp.headers.get("content-length")
    if declared_len is not None and int(declared_len) != len(resp.body):
        raise FetchError(
            f"Content-Length {declared_len} 与实际载荷 {len(resp.body)} 不符",
            request=req_no, part=None)
    parts = http_source.parse_206(resp)
    covered: list[tuple[int, int]] = []
    items: list[tuple] = []
    for part in parts:
        if part.total != current_length:
            raise FetchError(
                f"部件 {part.part_no} 声明总长 {part.total} "
                f"与本代总长 {current_length} 不符",
                request=req_no, part=part.part_no)
        if part.start < 0 or part.end >= current_length \
                or part.start > part.end:
            raise FetchError(
                f"部件 {part.part_no} 范围越界: "
                f"[{part.start},{part.end}]/{current_length}",
                request=req_no, part=part.part_no)
        if len(part.data) != part.end - part.start + 1:
            raise FetchError(
                f"部件 {part.part_no} 段长 {len(part.data)} 与载荷范围不符",
                request=req_no, part=part.part_no)
        covered.append((part.start, part.end))
        items.append((part.start, part.end, part.data,
                      (req_no, part.part_no)))

    # 并集必须覆盖请求的全部范围且不越界（越界已在上面拒绝）。
    for s, e in ranges:
        cursor = s
        for cs, ce in sorted(covered):
            if cs <= cursor <= ce:
                cursor = ce + 1
        if cursor <= e:
            raise FetchError(
                f"206 部件并集未覆盖请求范围 [{s},{e}]",
                request=req_no, part=None)
    return items


def _check_batch_overlaps(conn, generation,
                          ordered: list[tuple]) -> None:
    """按发起序/线序比对所有重叠字节（批内 + 与已确认片段）。

    ordered 已按 (请求号, 部件号) 排序；与已确认片段冲突时归因于新候选，
    批内冲突归因于后到部件，保证先发的损坏可被定位到正确来源。
    """
    placed: list[tuple[int, int, bytes, tuple[int, int]]] = [
        (s, e, data, (-1, -1))
        for s, e, data in store.load_segments(conn, generation)
    ]
    for start, end, data, source in ordered:
        req_no, part_no = source
        for ps, pe, pdata, psource in placed:
            ov_start, ov_end = max(start, ps), min(end, pe)
            if ov_start <= ov_end:
                new_part = data[ov_start - start: ov_end - start + 1]
                old_part = pdata[ov_start - ps: ov_end - ps + 1]
                if new_part != old_part:
                    if psource == (-1, -1):
                        # 已确认片段不可能损坏：责任在当前新候选。
                        bad_req, bad_part = req_no, part_no
                    else:
                        # 两条都是本批新候选：按发起序/线序归因先发者。
                        bad_req, bad_part = min(source, psource)
                    raise FetchError(
                        f"重叠区间 [{ov_start},{ov_end}] 字节不一致"
                        "（混版/损坏）",
                        request=bad_req, part=bad_part)
        placed.append((start, end, data, source))


def _handle_200(resp, req_no, conn, generation, current_length: int,
                current_etag: str) -> None:
    """调用时持锁。200 整份正文原子替换局部状态。"""
    etag = http_source.check_full_200(resp)
    length_header = resp.headers.get("content-length")
    if length_header is None:
        raise FetchError("200 响应缺少 Content-Length",
                         request=req_no, part=None)
    try:
        declared = int(length_header)
    except ValueError as exc:
        raise FetchError(f"200 Content-Length 非法: {length_header}",
                         request=req_no, part=None) from exc
    if declared != len(resp.body):
        raise FetchError(
            f"200 Content-Length {declared} 与正文长度 {len(resp.body)} 不符",
            request=req_no, part=None)

    if etag == current_etag:
        # 同 ETag 仍核对总长与已存字节：正文必须与本代总长一致。
        if len(resp.body) != current_length:
            raise FetchError(
                f"同 ETag 200 正文长度 {len(resp.body)} 与本代总长 "
                f"{current_length} 不符", request=req_no, part=None)
        if len(resp.body) == 0:
            return  # 零长正文无需片段
        store.replace_current_body_same_generation(conn, resp.body)
    else:
        # 新代：整份正文 + 新元数据单事务落盘，旧片段清空。
        store.replace_with_full_body(conn, etag, len(resp.body), resp.body)


def _finalize(conn, generation, etag, body, result_path, lock,
              terminal) -> dict:
    """覆盖完整后的收尾：拼装、校验、原子替换结果、标记完成。"""
    with lock:
        if body is None:
            meta = store.get_meta(conn)
            segments = store.load_segments(conn, generation)
            body = store.assemble(segments, meta.length)
            etag = meta.etag
        payload = _build_output(etag, body)
        _atomic_write_result(result_path, payload)
        store.mark_complete(conn)
        terminal.set()
    return payload


def _resume_complete(conn, generation, etag, length, result_path) -> dict:
    """已完成态重跑：同 ETag 核对总长及已存字节，必要时重建结果文件。"""
    segments = store.load_segments(conn, generation)
    body = store.assemble(segments, length)
    payload = _build_output(etag, body)
    if not os.path.exists(result_path) or \
            _read_json(result_path) != payload:
        _atomic_write_result(result_path, payload)
    return payload


def _read_json(path: str) -> dict | None:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
