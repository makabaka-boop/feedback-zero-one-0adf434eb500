# 仪器归档续采后端（防混版）

从零实现的 HTTP 归档续采系统：纯 Python 标准库 + SQLite，无第三方依赖。
核心目标是**绝不混版**——不同代（ETag/总长不同）的字节永远不会拼进同一份
归档，崩溃/强退后恢复也不混态。

## 目录

```
resumefetch/
  __init__.py     包说明
  store.py        SQLite 持久层：代、确认片段、终态、URL 绑定
  http_source.py  标准库 HTTP 客户端：强 ETag、Range、If-Range、multipart 解析
  client.py       编排：HEAD 换代、并发交错 Range、整批校验、原子确认/落盘
  test_server.py  可控故障注入 HTTP/1.1 源（验收用）
  __main__.py     命令行入口
tests/
  acceptance_test.py  独立验收（47 项断言，无需外部服务）
```

## 入口与用法

```bash
python3 -m resumefetch <url> <workdir> <result_path>
```

- `url`：归档地址（http/https）
- `workdir`：工作目录，存放 SQLite 状态库 `resumefetch.sqlite3`；与 URL 绑定，
  同一目录换 URL 直接失败
- `result_path`：成功后原子写入的结果 JSON

成功退出码 0，stdout 打印结果；失败非零退出，stderr 打印：

```json
{"error": "...", "field": "url|workdir|result_path|...|null",
 "request": 0, "part": 0}
```

`request` 按**发起序**从零编号，`part` 按 multipart **线上顺序**从零编号，
非部件类错误 `part` 为 `null`。

### 输出结构

```json
{
  "etag": "\"v1\"",
  "length": 4,
  "sha256": "e3b0c4...（小写十六进制）",
  "data": "QUJDRA==（带填充的标准 Base64）"
}
```

### 可调环境变量

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `RESUMEFETCH_CONCURRENCY` | 4 | 并发请求上限 |
| `RESUMEFETCH_CHUNK` | 65536 | 自动规划的单请求字节块大小 |
| `RESUMEFETCH_TIMEOUT` | 10 | 单请求超时秒数 |
| `RESUMEFETCH_PLAN` | 无 | 显式首轮请求计划，如 `2-3;0-2`（分号分隔请求，逗号分隔请求内多范围） |
| `RESUMEFETCH_KILL_BEFORE_CONFIRM` | 无 | 置 1：整批校验后、原子确认前 `os._exit(9)`（强退演练） |
| `RESUMEFETCH_KILL_AFTER_CONFIRM` | 无 | 置 1：原子确认后强退（强退演练） |

## 协议要点（RFC 9110 §13.1.5、§14）

- 每次先 `HEAD`：仅 **200** 合法，必须带**强 ETag**（拒绝 `W/` 弱 ETag）与
  非负 `Content-Length`。
- Range 为总长内的**零基闭区间**；GET 携带 `Range: bytes=a-b, c-d` 与
  `If-Range: <强 ETag>`；只发 `Accept-Encoding: identity`，收到非 identity
  的 `Content-Encoding`/`Transfer-Encoding`（含 chunked）即失败。
- **206** 可以是单段（`Content-Range: bytes start-end/total`）或
  `multipart/byteranges`；部件允许乱序、重叠、相邻，按 `Content-Range` 定位，
  并集必须覆盖请求范围且不越界；ETag、各段声明总长匹配本代，段长匹配载荷，
  重叠字节必须逐字节一致。
- **200** 必须含强 ETag、`Content-Length` 与完整正文匹配；同 ETag 仍核对总长
  与已存字节，异 ETag 则整份正文单事务换代。
- 其他状态码、断连、残体（`IncompleteRead`）、非法响应一律失败。

## 防混版与崩溃安全模型

1. **代（generation）**：HEAD 发现 ETag 或总长变化即清空全部片段、代编号 +1；
   同代元数据相同才复用已确认片段。旧代字节物理上不可能进入新代结果。
2. **先判代号/终态**：每个请求记住发起时代号，响应到达先核对；旧代响应、
   终态（完成或 200 接管）之后到达的响应与错误（含迟到 500）整体忽略。
3. **整份响应校验后才确认，残体不确认**：单个 206 的全部部件先独立校验
   （不写库），一批请求全部返回后再做批内/与已确认片段的重叠一致性预检，
   全部通过才在**一个 SQLite 事务**内整批确认；任一失败整批不落盘。
4. **原子结果替换**：仅当片段连续覆盖 `[0, length)` 才拼装，经
   临时文件 + `fsync` + `os.replace` 原子替换结果文件，再标记 `complete`。
5. **持久化**：WAL + `synchronous=FULL`，确认/换代/完成均提交事务；
   强退后重开库即恢复，失败时已确认片段与旧结果文件原样保留。
6. **零长归档**：HEAD 即完成，输出空摘要与空 Base64。

## 可控 HTTP 源（test_server.py）

`make_server(content, etag, rules)` 起一个线程化 HTTP/1.1 服务，按 GET 序号
注入一次性故障动作：`ok / multipart（乱序）/ coalesce（合并空隙）/
overlap_corrupt（重叠改字节）/ multipart_corrupt / short（并集不覆盖）/
bad_total / weak_etag / drop（发头后断连）/ error（任意状态码，可 delay
模拟迟到错误）/ full_200（整份替换，可换 ETag 与内容）/ hang`。

## 独立验收

```bash
python3 tests/acceptance_test.py
```

覆盖（共 47 项断言）：

1. 长度 4、并发 4，交错请求 `2-3`（CD）与 `0-2`（ABC）→ 合并得 `ABCD`
2. 前者把重叠字节 C 改 X → 失败，报请求 0/部件 0，旧结果与库状态保留
3. 200 整份替换后迟到的 500 → 忽略，采用新代正文成功
4. 空归档零长完成
5. 确认前/确认后强退（退出码 9）后续跑：不混态、复用片段、最终成功
6. multipart 乱序 + coalesce 合并空隙
7. 弱 ETag、断连残体、伪造总长均失败且编号规则正确
8. 同 ETag 200 核对、异 ETag 200 清段换代
9. 同目录换 URL 拒绝
10. HEAD 换代清空旧片段
11. 完成态重跑幂等、无多余 GET
12-14. 同 ETag 200 长度不符失败、重叠/相邻合并、并集不覆盖失败
