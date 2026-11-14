"""仪器归档续采后端（防混版）。

模块：
  store       SQLite 持久化（代、确认片段、终态、URL 绑定）
  http_source 仅基于标准库的 Range/If-Range 客户端
  client      编排：HEAD 换代、并发交错 Range、206/200 处理、原子落盘
  test_server 可控故障注入的 HTTP/1.1 源
"""

__all__ = ["store", "http_source", "client", "test_server"]
