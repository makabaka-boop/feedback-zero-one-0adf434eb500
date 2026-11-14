"""可控 HTTP/1.1 源（测试/验收用）。

仅依赖标准库。维护一份归档内容与强 ETag，HEAD/GET 行为可按“第几个 GET”
注入脚本化故障，用于独立验收：

  scenario = Scenario(content=b"ABCD", etag='"v1"')
  scenario.rules = [(匹配序号, 动作), ...]

动作（dict）：
  {"action": "ok"}                 正常 206（单段/多段按请求决定）
  {"action": "multipart"}          强制 multipart/byteranges，部件乱序
  {"action": "coalesce"}           多范围时合并成一个覆盖部件
  {"action": "overlap_corrupt", "offset": 2, "byte": "X"}
                                   覆盖 offset 的部件把该字节改写
  {"action": "bad_total"}          Content-Range 总长伪造
  {"action": "weak_etag"}          206 回弱 ETag
  {"action": "drop"}               发头后断连（Content-Length 不符）
  {"action": "error", "status": 500}
  {"action": "full_200", "etag": '"v2"', "content": b"..."}
                                   If-Range 失配风格的整份 200（可换内容）
  {"action": "hang", "delay": 30}  延迟响应
  {"action": "multipart_corrupt", "part": 0, "byte": "X"}
                                   multipart 第 part 部件首字节改写
  {"action": "short"}               单段 206 只回请求范围的前半（并集不覆盖）
"""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


class Scenario:
    def __init__(self, content: bytes, etag: str,
                 rules: list[tuple[int, dict]] | None = None,
                 persistent_rule: dict | None = None):
        if not (etag.startswith('"') and etag.endswith('"')):
            raise ValueError("测试源 ETag 必须是强 ETag（带引号）")
        self.content = content
        self.etag = etag
        # 规则按 GET 序号匹配；命中即移除（一次性）。
        self.rules: dict[int, dict] = dict(rules or [])
        # 持久规则：无一次性规则命中时对所有 GET 生效（并发场景用）。
        self.persistent_rule = persistent_rule
        self.get_count = 0
        self.lock = threading.Lock()
        self.request_log: list[dict] = []

    def next_get_index(self) -> int:
        with self.lock:
            idx = self.get_count
            self.get_count += 1
            return idx

    def rule_for(self, idx: int) -> dict:
        return self.rules.get(idx, self.persistent_rule or {"action": "ok"})


def _parse_range_header(header: str | None, total: int):
    """解析 Range 头，返回闭区间列表；无法处理返回 None。"""
    if not header or not header.startswith("bytes="):
        return None
    intervals: list[tuple[int, int]] = []
    try:
        for spec in header[len("bytes="):].split(","):
            spec = spec.strip()
            if not spec or "-" not in spec:
                return None
            lo_s, hi_s = spec.split("-", 1)
            lo_s, hi_s = lo_s.strip(), hi_s.strip()
            if not lo_s or not hi_s:
                return None  # 不支持后缀/开放范围
            lo, hi = int(lo_s), int(hi_s)
            if lo > hi or lo < 0 or hi >= total:
                return None
            intervals.append((lo, hi))
    except ValueError:
        return None
    return intervals or None


def _build_multipart(boundary: bytes, pieces: list[tuple[int, int, bytes]],
                     total: int, weak: bool = False,
                     bad_total: bool = False) -> bytes:
    out = bytearray()
    for start, end, data in pieces:
        declared_total = total + 1 if bad_total else total
        out += b"--" + boundary + b"\r\n"
        out += b"Content-Type: application/octet-stream\r\n"
        out += (f"Content-Range: bytes {start}-{end}/{declared_total}\r\n").encode()
        out += b"\r\n"
        out += data
        out += b"\r\n"
    out += b"--" + boundary + b"--\r\n"
    return bytes(out)


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    scenario: Scenario = None  # 由工厂注入

    def log_message(self, *args):  # 静默
        pass

    def handle_one_request(self):
        # 故障注入（断连/客户端提前关闭）会产生连接级异常，静默吞掉。
        try:
            super().handle_one_request()
        except (ConnectionResetError, ConnectionAbortedError,
                BrokenPipeError):
            self.close_connection = True

    # ---- HEAD ----------------------------------------------------------
    def do_HEAD(self):
        sc = self.scenario
        self.send_response(200)
        self.send_header("ETag", sc.etag)
        self.send_header("Content-Length", str(len(sc.content)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()

    # ---- GET -----------------------------------------------------------
    def do_GET(self):
        sc = self.scenario
        idx = sc.next_get_index()
        rule = sc.rule_for(idx)
        action = rule["action"]
        range_header = self.headers.get("Range")
        if_range = self.headers.get("If-Range")
        sc.request_log.append({
            "get_index": idx, "range": range_header, "if_range": if_range,
            "action": action,
        })

        total = len(sc.content)
        if action == "hang":
            time.sleep(float(rule.get("delay", 30)))
            return
        if action == "error":
            delay = float(rule.get("delay", 0))
            if delay:
                time.sleep(delay)
            body = b"injected error"
            self.send_response(int(rule.get("status", 500)))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if action == "drop":
            # 声明的长度大于实际写出，随后直接关闭连接。
            self.send_response(206)
            self.send_header("ETag", sc.etag)
            self.send_header("Content-Range", f"bytes 0-0/{total}")
            self.send_header("Content-Length", "9999")
            self.end_headers()
            self.wfile.write(sc.content[:1])
            self.close_connection = True
            return
        if action == "full_200":
            body = rule.get("content", sc.content)
            etag = rule.get("etag", sc.etag)
            self.send_response(200)
            self.send_header("ETag", etag)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            self.wfile.write(body)
            return

        ranges = _parse_range_header(range_header, total)
        if ranges is None:
            # 无有效 Range：整份 200（当前内容）
            self.send_response(200)
            self.send_header("ETag", sc.etag)
            self.send_header("Content-Length", str(total))
            self.end_headers()
            self.wfile.write(sc.content)
            return

        etag_value = 'W/"weak"' if action == "weak_etag" else sc.etag
        declared_total = total + 1 if action == "bad_total" else total

        if action == "short":
            # 只回请求首段的前半区间，Content-Range 与载荷自洽，
            # 但部件并集覆盖不了请求范围（客户端必须判失败）。
            s, e = ranges[0]
            mid = s + (e - s) // 2
            self._send_single_206(s, mid, total,
                                  sc.content[s:mid + 1], etag_value)
            return

        if action == "multipart" or (len(ranges) > 1 and action != "coalesce"
                                     and action != "overlap_corrupt"):
            boundary = b"resumefetchboundary"
            pieces = []
            ordered = list(ranges)
            if action == "multipart":
                ordered.reverse()  # 乱序回送
            for part_no, (s, e) in enumerate(ordered):
                data = bytearray(sc.content[s:e + 1])
                if action == "multipart_corrupt" and part_no == int(rule.get("part", 0)):
                    data[0] = ord(rule.get("byte", "X"))
                pieces.append((s, e, bytes(data)))
            body = _build_multipart(boundary, pieces, total,
                                    bad_total=(action == "bad_total"))
            self.send_response(206)
            self.send_header("ETag", etag_value)
            self.send_header(
                "Content-Type",
                f'multipart/byteranges; boundary="{boundary.decode()}"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if action == "coalesce" and len(ranges) > 1:
            s = ranges[0][0]
            e = ranges[-1][1]
            data = sc.content[s:e + 1]
            self._send_single_206(s, e, declared_total, data, etag_value)
            return

        # 单段 206（多范围且无特殊动作时，逐段取并集后按每段回——这里
        # 简化为合并成覆盖最小并集的一个 206，也合法）
        s = ranges[0][0]
        e = ranges[-1][1]
        data = bytearray(sc.content[s:e + 1])
        if action == "overlap_corrupt":
            offset = int(rule["offset"])
            if s <= offset <= e:
                data[offset - s] = ord(rule.get("byte", "X"))
        self._send_single_206(s, e, declared_total, bytes(data), etag_value)

    def _send_single_206(self, start, end, total, data, etag):
        self.send_response(206)
        self.send_header("ETag", etag)
        self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()
        self.wfile.write(data)


def make_server(content: bytes, etag: str,
                rules: list[tuple[int, dict]] | None = None,
                host: str = "127.0.0.1", port: int = 0,
                persistent_rule: dict | None = None):
    """创建并返回已监听的测试服务器（线程化，守护线程）。"""
    scenario = Scenario(content, etag, rules,
                        persistent_rule=persistent_rule)

    class _BoundHandler(_Handler):
        pass

    _BoundHandler.scenario = scenario
    server = ThreadingHTTPServer((host, port), _BoundHandler)
    server.daemon_threads = True
    server.scenario = scenario
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    server.thread = thread
    return server


def server_url(server, path: str = "/archive") -> str:
    host, port = server.server_address
    return f"http://{host}:{port}{path}"
