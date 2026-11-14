"""仅用标准库实现的受控 HTTP 客户端（Range / If-Range，identity 编码）。

遵循 RFC 9110 §13.1.5（Range）与 §14（HTTP 校验器：ETag/If-Range）：

* 只发送 identity 可接受编码，收到 ``Content-Encoding``/``Transfer-Encoding``
  非 identity（含 chunked）一律按非法响应失败。
* Range 为总长内的零基闭区间，``If-Range`` 携带本代**强** ETag。
* 强 ETag 必须是带引号的 opaque-tag，弱 ETag（``W/`` 前缀）拒绝。
* 206 可以是单段（带 Content-Range）或 ``multipart/byteranges``，
  多段解析支持乱序、重叠、相邻（合并空隙由上层处理）。
"""

from __future__ import annotations

import http.client
import re
from dataclasses import dataclass
from urllib.parse import urlparse

_WEAK_PREFIX = "W/"
_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_BOUNDARY_RE = re.compile(r'boundary="?([^";]+)"?')


class ProtocolError(Exception):
    """非法 HTTP 响应/协议错误。"""


@dataclass
class HeadResult:
    status: int
    etag: str
    length: int


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes


@dataclass
class BodyPart:
    """multipart/byteranges 中的一个部件，part_no 按线上顺序从零编号。"""
    part_no: int
    start: int
    end: int
    total: int
    data: bytes


def parse_strong_etag(header_value: str | None, where: str) -> str:
    """解析并校验强 ETag，返回原始头值（含引号）。"""
    if header_value is None:
        raise ProtocolError(f"{where} 缺少 ETag")
    value = header_value.strip()
    if value.upper().startswith(_WEAK_PREFIX):
        raise ProtocolError(f"{where} 返回弱 ETag，仅接受强 ETag: {value}")
    if len(value) < 2 or not (value.startswith('"') and value.endswith('"')):
        raise ProtocolError(f"{where} ETag 非法（强 ETag 必须带引号）: {value}")
    return value


def _split_url(url: str):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ProtocolError(f"仅支持 http/https URL: {url}")
    if not parsed.hostname:
        raise ProtocolError(f"URL 缺少主机: {url}")
    return parsed


def _request(method: str, url: str, headers: dict[str, str],
             timeout: float) -> Response:
    parsed = _split_url(url)
    conn_cls = (http.client.HTTPSConnection
                if parsed.scheme == "https" else http.client.HTTPConnection)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    conn = conn_cls(parsed.hostname, port, timeout=timeout)
    try:
        full_headers = {"Accept-Encoding": "identity", **headers}
        try:
            conn.request(method, path, headers=full_headers)
            resp = conn.getresponse()
            body = resp.read()
        except (OSError, http.client.HTTPException) as exc:
            raise ProtocolError(f"{method} {url} 连接/传输失败: {exc}") from exc
        lower = {k.lower(): v for k, v in resp.getheaders()}
        return Response(status=resp.status, headers=lower, body=body)
    finally:
        conn.close()


def head(url: str, timeout: float) -> HeadResult:
    """HEAD：仅 200 合法；必须带强 ETag 与非负 Content-Length。"""
    resp = _request("HEAD", url, {}, timeout)
    if resp.status != 200:
        raise ProtocolError(f"HEAD 状态码 {resp.status}，仅 200 合法")
    etag = parse_strong_etag(resp.headers.get("etag"), "HEAD 响应")
    length_header = resp.headers.get("content-length")
    if length_header is None:
        raise ProtocolError("HEAD 响应缺少 Content-Length")
    try:
        length = int(length_header)
    except ValueError as exc:
        raise ProtocolError(f"Content-Length 非法: {length_header}") from exc
    if length < 0:
        raise ProtocolError(f"Content-Length 必须非负: {length}")
    if resp.body:
        raise ProtocolError("HEAD 响应不得携带正文")
    return HeadResult(status=200, etag=etag, length=length)


def _check_identity(resp: Response, where: str) -> None:
    encoding = resp.headers.get("content-encoding")
    if encoding is not None and encoding.lower() != "identity":
        raise ProtocolError(f"{where} 使用了非 identity 编码: {encoding}")
    transfer = resp.headers.get("transfer-encoding")
    if transfer is not None and transfer.lower() != "identity":
        raise ProtocolError(
            f"{where} 使用了非 identity 传输编码: {transfer}")


def _parse_content_range(header: str | None):
    if header is None:
        raise ProtocolError("206 单段响应缺少 Content-Range")
    match = _CONTENT_RANGE_RE.match(header.strip())
    if not match:
        raise ProtocolError(f"Content-Range 非法: {header}")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def _parse_multipart(content_type: str, body: bytes) -> list[BodyPart]:
    match = _BOUNDARY_RE.search(content_type)
    if not match:
        raise ProtocolError(f"multipart/byteranges 缺少 boundary: {content_type}")
    boundary = match.group(1).encode("ascii")
    delimiter = b"--" + boundary
    parts: list[BodyPart] = []
    # RFC 2046 分帧：--boundary CRLF headers CRLF CRLF payload CRLF
    #                 ... --boundary-- CRLF
    pos = body.find(delimiter)
    if pos < 0:
        raise ProtocolError("multipart 正文找不到起始 boundary")
    pos += len(delimiter)
    part_no = 0
    while True:
        if body[pos:pos + 2] == b"--":  # 结束 boundary
            break
        if body[pos:pos + 2] != b"\r\n":
            raise ProtocolError("multipart boundary 后缺少 CRLF")
        pos += 2
        header_end = body.find(b"\r\n\r\n", pos)
        if header_end < 0:
            raise ProtocolError("multipart 部件头不完整")
        raw_headers = body[pos:header_end].decode("iso-8859-1")
        pos = header_end + 4
        next_delim = body.find(b"\r\n" + delimiter, pos)
        if next_delim < 0:
            raise ProtocolError("multipart 部件缺少结束 boundary")
        payload = body[pos:next_delim]
        pos = next_delim + 2 + len(delimiter)

        part_headers: dict[str, str] = {}
        for line in raw_headers.split("\r\n"):
            if ":" in line:
                key, value = line.split(":", 1)
                part_headers[key.strip().lower()] = value.strip()
        start, end, total = _parse_content_range(
            part_headers.get("content-range"))
        parts.append(BodyPart(part_no, start, end, total, payload))
        part_no += 1
    if not parts:
        raise ProtocolError("multipart/byteranges 不含任何部件")
    return parts


def range_get(url: str, ranges: list[tuple[int, int]], etag: str,
              timeout: float) -> Response:
    """发起带 Range 与 If-Range（强 ETag）的 GET。

    ranges 为一个请求内的多个零基闭区间（多范围交错请求）。
    """
    if not ranges:
        raise ProtocolError("range_get 至少需要一个区间")
    specs = ", ".join(f"{s}-{e}" for s, e in ranges)
    headers = {"Range": f"bytes={specs}", "If-Range": etag}
    return _request("GET", url, headers, timeout)


def parse_206(resp: Response) -> list[BodyPart]:
    """解析 206 响应为部件列表（单段或 multipart），含 identity 校验。"""
    _check_identity(resp, "206 响应")
    content_type = resp.headers.get("content-type", "")
    if content_type.lower().startswith("multipart/byteranges"):
        return _parse_multipart(content_type, resp.body)
    start, end, total = _parse_content_range(
        resp.headers.get("content-range"))
    return [BodyPart(0, start, end, total, resp.body)]


def check_full_200(resp: Response) -> str:
    """校验 200 的强 ETag；返回 ETag。长度与正文由上层结合代信息核对。"""
    _check_identity(resp, "200 响应")
    return parse_strong_etag(resp.headers.get("etag"), "200 响应")
