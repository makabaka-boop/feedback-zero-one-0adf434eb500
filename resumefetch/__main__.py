"""命令行入口。

用法::

    python3 -m resumefetch <url> <workdir> <result_path>

成功：结果 JSON 写到 result_path，并向 stdout 打印同内容；退出码 0。
失败：向 stderr 打印
``{"error": ..., "field": ..., "request": ..., "part": ...}``；
非零退出。请求号按发起序、部件号按线上顺序均从零编号，非部件错误 part=null。
"""

from __future__ import annotations

import json
import sys

from .client import FetchError, run


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 3:
        print(json.dumps({
            "error": "用法: python3 -m resumefetch <url> <workdir> <result_path>",
            "field": "arguments", "request": None, "part": None,
        }, ensure_ascii=False), file=sys.stderr)
        return 2
    url, workdir, result_path = argv
    try:
        payload = run(url, workdir, result_path)
    except FetchError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False), file=sys.stderr)
        return 1
    except Exception as exc:  # 未预期错误也按统一结构上报
        print(json.dumps({
            "error": f"内部错误: {exc}", "field": None,
            "request": None, "part": None,
        }, ensure_ascii=False), file=sys.stderr)
        return 3
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
