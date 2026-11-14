#!/usr/bin/env python3
"""独立验收：``python3 tests/acceptance_test.py``

覆盖规格要求的全部验收点：
  1. 长度 4 并发交错（2-3 的 CD、0-2 的 ABC）→ 合并得 ABCD
  2. 前者把 C 改 X → 重叠字节不一致，必须失败，且旧结果/已确认片段保留
  3. 200 之后到达的迟到错误 → 忽略，照常成功
  4. 空归档 → 零长完成
  5. 确认前 / 确认后强退，续跑不混态、最终成功
另含：multipart 乱序、coalesce 合并空隙、弱 ETag、断连、坏总长、
200 换代、URL 绑定、完成态重跑等附加用例。
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from resumefetch import test_server  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def run_cli(url: str, workdir: str, result: str,
            env_extra: dict | None = None):
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, "-m", "resumefetch", url, workdir, result],
        capture_output=True, text=True, env=env, timeout=60)
    err = None
    if proc.stderr.strip():
        try:
            err = json.loads(proc.stderr.strip().splitlines()[-1])
        except json.JSONDecodeError:
            err = {"raw": proc.stderr}
    out = None
    if proc.stdout.strip():
        out = json.loads(proc.stdout)
    return proc.returncode, out, err


def db_segments(workdir: str):
    conn = sqlite3.connect(os.path.join(workdir, "resumefetch.sqlite3"))
    rows = conn.execute(
        "SELECT generation, start, end, length(data) FROM segment ORDER BY start"
    ).fetchall()
    meta = conn.execute(
        "SELECT url, etag, length, generation, state FROM meta").fetchone()
    conn.close()
    return meta, rows


def expect_archive(payload: dict, content: bytes, etag: str) -> bool:
    if payload is None:
        return False
    ok = (payload["etag"] == etag
          and payload["length"] == len(content)
          and payload["sha256"] == hashlib.sha256(content).hexdigest()
          and base64.b64decode(payload["data"], validate=True) == content)
    return ok


def fresh_dirs(base: str):
    work = tempfile.mkdtemp(dir=base)
    result = os.path.join(tempfile.mkdtemp(dir=base), "result.json")
    return work, result


def main() -> int:
    base = tempfile.mkdtemp(prefix="rf-accept-")

    # ---- 用例 1：并发交错 2-3 / 0-2 → ABCD --------------------------
    print("用例1 并发交错续采（2-3 得 CD，0-2 得 ABC，合并 ABCD）")
    srv = test_server.make_server(b"ABCD", '"v1"')
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out, err = run_cli(url, work, result, {
        "RESUMEFETCH_PLAN": "2-3;0-2",
        "RESUMEFETCH_CONCURRENCY": "4",
    })
    check("退出码 0", code == 0, f"code={code} err={err}")
    check("输出 {etag,length,sha256,data} 且正文为 ABCD",
          expect_archive(out, b"ABCD", '"v1"'))
    check("sha256 为小写十六进制",
          out is not None and out["sha256"] == out["sha256"].lower()
          and len(out["sha256"]) == 64)
    check("data 为带填充 Base64",
          out is not None and out["data"].endswith("=")
          and base64.b64decode(out["data"]) == b"ABCD")
    check("结果文件原子落盘且内容一致",
          os.path.exists(result) and json.load(open(result)) == out)
    check("两个 Range 请求按发起序编号 0、1",
          [r["get_index"] for r in srv.scenario.request_log] == [0, 1])
    check("均携带 If-Range 强 ETag",
          all(r["if_range"] == '"v1"' for r in srv.scenario.request_log))
    srv.shutdown()

    # ---- 用例 2：重叠字节被改（C→X）必须失败 ------------------------
    print("用例2 重叠损坏（请求0 的 C 改 X）必须失败，片段/旧结果保留")
    srv = test_server.make_server(
        b"ABCD", '"v1"', rules=[(0, {"action": "overlap_corrupt",
                                     "offset": 2, "byte": "X"})])
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    # 先放一份“旧结果”，验证失败时不被覆盖。
    old_payload = {"etag": "old", "length": 0, "sha256":
                   hashlib.sha256(b"").hexdigest(), "data": ""}
    os.makedirs(os.path.dirname(result), exist_ok=True)
    json.dump(old_payload, open(result, "w"))
    code, out, err = run_cli(url, work, result, {
        "RESUMEFETCH_PLAN": "2-3;0-2",
        "RESUMEFETCH_CONCURRENCY": "4",
    })
    check("非零退出", code != 0, f"code={code}")
    check("错误带请求号 0、部件号 0",
          err is not None and err.get("request") == 0
          and err.get("part") == 0, f"err={err}")
    check("旧结果文件保留未被覆盖",
          json.load(open(result)) == old_payload)
    meta, rows = db_segments(work)
    check("整批原子确认：损坏批次无任何片段落库（残体不确认）",
          rows == [], f"rows={rows}")
    check("库仍为 running 代，未标记完成", meta is not None and meta[4] == "running")
    srv.shutdown()

    # ---- 用例 3：200 后迟到错误被忽略 -------------------------------
    print("用例3 200 整份替换后，迟到的 500 错误必须忽略")
    srv = test_server.make_server(
        b"ABCD", '"v1"',
        rules=[(0, {"action": "full_200", "etag": '"v2"',
                    "content": b"XYZW"}),
               (1, {"action": "error", "status": 500, "delay": 1.5})])
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out, err = run_cli(url, work, result, {
        "RESUMEFETCH_PLAN": "0-1;2-3",
        "RESUMEFETCH_CONCURRENCY": "2",
    })
    check("退出码 0（迟到错误已忽略）", code == 0, f"code={code} err={err}")
    check("采用 200 的新代正文 XYZW 与新 ETag",
          expect_archive(out, b"XYZW", '"v2"'))
    srv.shutdown()

    # ---- 用例 4：空归档零长完成 -------------------------------------
    print("用例4 空归档零长完成")
    srv = test_server.make_server(b"", '"empty"')
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out, err = run_cli(url, work, result)
    check("退出码 0", code == 0, f"err={err}")
    check("零长摘要与空 Base64",
          out is not None and out["length"] == 0
          and out["sha256"] == hashlib.sha256(b"").hexdigest()
          and out["data"] == "" and out["etag"] == '"empty"')
    meta, rows = db_segments(work)
    check("状态 complete 且无片段", meta[4] == "complete" and rows == [])
    check("无 GET（仅 HEAD）", srv.scenario.get_count == 0)
    srv.shutdown()

    # ---- 用例 5a：确认前强退，续跑 ----------------------------------
    print("用例5a 确认前强退 → 无片段入库，续跑成功不混态")
    srv = test_server.make_server(b"ABCD", '"v1"')
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, _, _ = run_cli(url, work, result, {
        "RESUMEFETCH_PLAN": "0-1;2-3",
        "RESUMEFETCH_KILL_BEFORE_CONFIRM": "1",
    })
    check("强退退出码 9", code == 9, f"code={code}")
    meta, rows = db_segments(work)
    check("强退前无任何确认片段", rows == [], f"rows={rows}")
    check("元数据已持久化（第一代 running）",
          meta is not None and meta[2] == 4 and meta[3] == 1)
    code, out, err = run_cli(url, work, result)
    check("续跑成功", code == 0 and expect_archive(out, b"ABCD", '"v1"'),
          f"err={err}")
    srv.shutdown()

    # ---- 用例 5b：确认后强退，续跑复用片段 --------------------------
    print("用例5b 确认后强退 → 片段已持久化，续跑复用补齐")
    srv = test_server.make_server(b"ABCD", '"v1"')
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, _, _ = run_cli(url, work, result, {
        "RESUMEFETCH_PLAN": "0-1;2-3",
        "RESUMEFETCH_KILL_AFTER_CONFIRM": "1",
    })
    check("强退退出码 9", code == 9, f"code={code}")
    meta, rows = db_segments(work)
    check("强退时已有确认片段", len(rows) >= 1 and meta[4] == "running",
          f"rows={rows}")
    before = rows
    code, out, err = run_cli(url, work, result)
    check("续跑成功且结果为 ABCD",
          code == 0 and expect_archive(out, b"ABCD", '"v1"'), f"err={err}")
    meta, rows = db_segments(work)
    check("续跑复用已确认片段（同代号）", meta[3] == 1)
    check("最终单段连续覆盖 0-3",
          len(rows) == 1 and rows[0][1] == 0 and rows[0][2] == 3,
          f"rows={rows}")
    srv.shutdown()

    # ---- 用例 6：multipart 乱序 + coalesce 合并空隙 -----------------
    print("用例6 multipart/byteranges 乱序回送与合并空隙")
    srv = test_server.make_server(b"ABCD", '"v1"',
                                  rules=[(0, {"action": "multipart"}),
                                         (1, {"action": "coalesce"})])
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out, err = run_cli(url, work, result, {
        "RESUMEFETCH_PLAN": "0-1,3-3;2-2",
        "RESUMEFETCH_CONCURRENCY": "2",
    })
    check("乱序多部件 + 合并空隙后成功",
          code == 0 and expect_archive(out, b"ABCD", '"v1"'), f"err={err}")
    srv.shutdown()

    # ---- 用例 7：弱 ETag、断连、坏总长 ------------------------------
    print("用例7 非法响应：弱 ETag / 断连 / 伪造总长，均失败且 part 规则正确")
    for rule, expect_part in [
        ({"action": "weak_etag"}, None),
        ({"action": "drop"}, None),
        ({"action": "bad_total"}, 0),
    ]:
        # 持久规则对所有并发 GET 生效，避免一次性规则与取号线程竞态。
        srv = test_server.make_server(b"ABCD", '"v1"',
                                      persistent_rule=rule)
        url = test_server.server_url(srv)
        work, result = fresh_dirs(base)
        code, out, err = run_cli(url, work, result,
                                 {"RESUMEFETCH_PLAN": "0-1;2-3"})
        ok = code != 0 and err is not None \
            and err.get("part") == expect_part
        check(f"{rule['action']} 失败（part={expect_part}）", ok,
              f"err={err}")
        srv.shutdown()

    # ---- 用例 8：200 同 ETag 核对与异 ETag 换代 ---------------------
    print("用例8 200 响应：同 ETag 原子覆盖；异 ETag 清段换代")
    srv = test_server.make_server(
        b"ABCD", '"v1"', rules=[(0, {"action": "full_200", "etag": '"v2"',
                                     "content": b"HELLO"})])
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out, err = run_cli(url, work, result,
                             {"RESUMEFETCH_PLAN": "0-1;2-3"})
    check("异 ETag 200 换代成功为 HELLO",
          code == 0 and expect_archive(out, b"HELLO", '"v2"'), f"err={err}")
    meta, rows = db_segments(work)
    check("代号递增为 2 且只存新代整段",
          meta[3] == 2 and len(rows) == 1 and rows[0][0] == 2
          and rows[0][1] == 0 and rows[0][2] == 4, f"meta={meta} rows={rows}")
    srv.shutdown()

    # ---- 用例 9：目录禁换 URL ---------------------------------------
    print("用例9 同一工作目录换 URL 必须拒绝")
    srv1 = test_server.make_server(b"ABCD", '"v1"')
    srv2 = test_server.make_server(b"ABCD", '"v1"')
    work, result = fresh_dirs(base)
    code, _, _ = run_cli(test_server.server_url(srv1), work, result)
    check("首次成功", code == 0)
    code, _, err = run_cli(test_server.server_url(srv2, "/other"), work,
                           result)
    check("换 URL 非零退出且报 field=url",
          code != 0 and err is not None and err.get("field") == "url",
          f"err={err}")
    srv1.shutdown()
    srv2.shutdown()

    # ---- 用例 10：HEAD 换代（ETag 变化）不混版 ----------------------
    print("用例10 HEAD 发现 ETag 变化 → 清空旧片段换代")
    srv = test_server.make_server(b"ABCD", '"v1"')
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, _, _ = run_cli(url, work, result,
                         {"RESUMEFETCH_PLAN": "0-1;2-3"})
    check("第一代采集成功", code == 0)
    # 源端归档换代（内容与 ETag 同时变化，新内容长度 8）。
    srv.scenario.content = b"JKLMNOPQ"
    srv.scenario.etag = '"v3"'
    srv.scenario.rules = {}
    code, out, err = run_cli(url, work, result)
    check("换代后采集新内容成功",
          code == 0 and expect_archive(out, b"JKLMNOPQ", '"v3"'), f"err={err}")
    meta, rows = db_segments(work)
    check("旧代片段已清空、代号 2、总长 8",
          meta[3] == 2 and meta[2] == 8 and all(r[0] == 2 for r in rows),
          f"meta={meta} rows={rows}")
    srv.shutdown()

    # ---- 用例 11：完成态重跑幂等 -----------------------------------
    print("用例11 完成态重跑：核对总长与已存字节，结果幂等")
    srv = test_server.make_server(b"ABCD", '"v1"')
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out1, _ = run_cli(url, work, result)
    gets_after_first = srv.scenario.get_count
    mtime = os.path.getmtime(result)
    time.sleep(0.02)
    code, out2, err = run_cli(url, work, result)
    check("重跑成功且输出一致", code == 0 and out1 == out2, f"err={err}")
    check("完成态重跑无新增 GET（直接核对已存字节）",
          srv.scenario.get_count == gets_after_first)
    srv.shutdown()

    # ---- 用例 12：同 ETag 200 仍核对总长与已存字节 ------------------
    print("用例12 同 ETag 的 200：长度匹配则原子覆盖并完成")
    srv = test_server.make_server(
        b"ABCD", '"v1"', rules=[(0, {"action": "full_200", "etag": '"v1"',
                                     "content": b"ABCD"})])
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out, err = run_cli(url, work, result,
                             {"RESUMEFETCH_PLAN": "0-1;2-3"})
    check("同 ETag 200 成功且正文 ABCD",
          code == 0 and expect_archive(out, b"ABCD", '"v1"'), f"err={err}")
    meta, rows = db_segments(work)
    check("代号保持 1、整段覆盖 0-3",
          meta[3] == 1 and len(rows) == 1 and rows[0][1] == 0
          and rows[0][2] == 3, f"rows={rows}")
    srv.shutdown()

    # 同 ETag 但 200 长度与本代总长不符 → 失败
    srv = test_server.make_server(
        b"ABCD", '"v1"', rules=[(0, {"action": "full_200", "etag": '"v1"',
                                     "content": b"ABC"})])
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, _, err = run_cli(url, work, result,
                           {"RESUMEFETCH_PLAN": "0-1;2-3"})
    check("同 ETag 200 长度不符必须失败",
          code != 0 and err is not None and err.get("request") == 0
          and "总长" in err["error"], f"err={err}")
    srv.shutdown()

    # ---- 用例 13：206 重叠回送与空隙合并（并集覆盖请求范围） --------
    print("用例13 206 重叠/相邻部件：并集覆盖、重叠一致即可确认")
    # 单请求多范围，服务器以两个重叠的单段式响应不易构造；这里用多请求
    # 制造相邻+重叠：0-1、1-2（重叠字节 1）、3-3，覆盖 0-3。
    srv = test_server.make_server(b"ABCD", '"v1"')
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, out, err = run_cli(url, work, result, {
        "RESUMEFETCH_PLAN": "0-1;1-2;3-3",
        "RESUMEFETCH_CONCURRENCY": "1",
    })
    check("重叠一致 + 空隙合并后成功得 ABCD",
          code == 0 and expect_archive(out, b"ABCD", '"v1"'), f"err={err}")
    meta, rows = db_segments(work)
    check("合并为单段连续覆盖 0-3",
          len(rows) == 1 and rows[0][1] == 0 and rows[0][2] == 3,
          f"rows={rows}")
    srv.shutdown()

    # ---- 用例 14：并集不覆盖请求范围必须失败 ------------------------
    print("用例14 206 部件并集未覆盖请求范围 → 失败，part=null")
    srv = test_server.make_server(
        b"ABCD", '"v1"', rules=[(0, {"action": "short"})])
    url = test_server.server_url(srv)
    work, result = fresh_dirs(base)
    code, _, err = run_cli(url, work, result,
                           {"RESUMEFETCH_PLAN": "0-3"})
    check("短回（并集不覆盖）非零退出且 part=null",
          code != 0 and err is not None and err.get("request") == 0
          and err.get("part") is None and "未覆盖" in err["error"],
          f"err={err}")
    check("短回批次无片段确认", db_segments(work)[1] == [])
    srv.shutdown()

    shutil.rmtree(base, ignore_errors=True)
    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
