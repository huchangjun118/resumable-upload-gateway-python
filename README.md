# Resumable Upload Gateway（本地大对象续传网关）

一个仅依赖**本地磁盘 + 进程内状态**的大文件分片上传与对象存储网关。它把
客户端的分片上传组织成**可续传、可校验、可并发处理**的上传会话，并在全部
分片确认、整体校验通过后，把对象**原子地**发布到本地存储。不依赖任何外部
对象存储或网络服务。

## 能力一览

* 会话生命周期：`init / in_progress / committing / completed / canceled /
  expired`，中断后凭 `session_id` 续传，创建支持幂等键。
* 分片：乱序提交、重复幂等、迟到拒绝；每片 SHA-256 校验，缺失分片明确报出。
* 原子可见性：对象在完整校验后经一次原子 rename 发布，读取方永不见半成品
  或混合版本。
* 并发覆盖：`always`（最后创建者胜，默认）/ `never`（先到先得）两种策略，
  竞争结果确定，败者无残留；提交期间的读写行为有明确定义。
* 归属与配额：按租户隔离，写入任何持久数据**之前**校验配额与会话数上限，
  拒绝不留孤儿数据。
* 崩溃恢复：重启后对账未完成提交、重建配额台账，已确认分片与已发布对象保持
  一致；TTL 回收只清理过期未完成会话。
* 有界内存：全流程固定缓冲流式处理，内存不随对象大小线性增长。

## 快速开始

```bash
uv sync
# 运行 HTTP 服务
UGW_DATA_DIR=./data .venv/bin/python main.py
```

## HTTP API

所有接口以 `X-Tenant-Id` 标识租户。

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/sessions` | 创建会话 |
| GET | `/v1/sessions/{sid}` | 查询状态与进度（含 `missing_chunks`） |
| DELETE | `/v1/sessions/{sid}` | 取消会话（幂等） |
| PUT | `/v1/sessions/{sid}/chunks/{n}` | 上传第 n 片（body 为原始字节） |
| POST | `/v1/sessions/{sid}/commit` | 提交，原子发布对象 |
| GET | `/v1/objects/{name}` | 流式读取当前可见版本 |
| HEAD | `/v1/objects/{name}` | 对象元信息 |
| GET | `/healthz` | 健康检查 |

分片请求头：`X-Content-Sha256`（必填，64 位 hex），`X-Chunk-Size`（可选）。

### 典型流程

```bash
T=tenant-1
# 1) 创建（可带 whole_sha256 在提交时整体校验；idempotency_key 防重复创建）
SID=$(curl -s -XPOST localhost:8080/v1/sessions -H "X-Tenant-Id: $T" \
  -H 'Content-Type: application/json' \
  -d '{"object_name":"videos/a.bin","total_size":12000,"chunk_size":4096,
       "idempotency_key":"client-uuid-1"}' | python -c 'import sys,json;print(json.load(sys.stdin)["session_id"])')

# 2) 分片可乱序、可重传（相同编号+相同摘要为幂等）
curl -s -XPUT "localhost:8080/v1/sessions/$SID/chunks/2" \
  -H "X-Tenant-Id: $T" -H "X-Content-Sha256: <sha256>" --data-binary @part2

# 3) 查询缺失分片
curl -s "localhost:8080/v1/sessions/$SID" -H "X-Tenant-Id: $T"

# 4) 提交（缺片时返回 409 与 missing_chunks 列表）
curl -s -XPOST "localhost:8080/v1/sessions/$SID/commit" -H "X-Tenant-Id: $T"

# 5) 读取
curl -s "localhost:8080/v1/objects/videos/a.bin" -H "X-Tenant-Id: $T" -o a.bin
```

### 错误码

`session_not_found(404)`、`session_expired(410)`、`session_canceled(409)`、
`session_completed(409)`、`session_closed(409)`、`object_already_exists(409)`、
`chunk_out_of_range(400)`、`chunk_checksum_mismatch(400)`、
`chunk_size_mismatch(400)`、`total_size_mismatch(400)`、
`whole_object_checksum_mismatch(400)`、`missing_chunks(409)`、
`forbidden(403)`、`quota_exceeded(413)`、`invalid_request(400)`。

## 嵌入式用法

```python
from gateway.config import Config
from gateway.manager import GatewayService
import io, hashlib

svc = GatewayService(Config(data_dir="./data"))
svc.start()  # 启动时自动崩溃恢复 + 可选后台回收
st = svc.create_session("tenant-a", "obj.bin", total_size=10_000,
                        chunk_size=4_096,
                        whole_sha256=hashlib.sha256(data).hexdigest())
svc.put_chunk("tenant-a", st.session_id, 0, io.BytesIO(chunk0),
              hashlib.sha256(chunk0).hexdigest())
svc.commit_session("tenant-a", st.session_id)
with svc.open_object("tenant-a", "obj.bin") as f:
    ...
svc.shutdown()
```

## 配置项

可用代码构造 `Config(...)`，或用环境变量（前缀 `UGW_`）覆盖：

| 配置 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `data_dir` | `UGW_DATA_DIR` | `./data` | 所有状态根目录 |
| `session_ttl_seconds` | `UGW_SESSION_TTL_SECONDS` | `86400` | 未完成会话不活跃保留秒数 |
| `reap_interval_seconds` | `UGW_REAP_INTERVAL_SECONDS` | `300` | 后台回收间隔，`0` 关闭 |
| `default_quota_bytes` | `UGW_DEFAULT_QUOTA_BYTES` | 无限制 | 每租户默认配额（已提交+预留） |
| `overwrite_policy` | `UGW_OVERWRITE_POLICY` | `always` | `always` 或 `never` |
| `read_buffer_size` | `UGW_READ_BUFFER_SIZE` | `65536` | 流式读写缓冲（内存上界相关） |
| `max_sessions_per_tenant` | `UGW_MAX_SESSIONS_PER_TENANT` | 无限制 | 每租户并发会话数上限 |

## 测试

```bash
.venv/bin/python -m pytest -q
```

覆盖：乱序/重复/迟到分片、缺失与校验失败、覆盖并发竞争（多线程）、
提交期间读取一致性、配额与归属拒绝（无孤儿）、崩溃恢复（提交前/中/后
各窗口）、TTL 回收、大对象有界内存流式处理，以及完整 HTTP 端到端流程。

## 进一步阅读

* [docs/architecture.md](docs/architecture.md)：会话状态机、原子可见性、
  并发裁决、配额与崩溃恢复语义。
