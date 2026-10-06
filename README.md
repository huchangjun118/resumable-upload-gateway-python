# Resumable Upload Gateway

面向大文件的对象上传与存储网关。把客户端的分片上传组织成**可续传、可校验、
可并发**的上传会话，并把对象**原子地**写入本地存储。整个流程只依赖本地磁盘
与进程内状态，不依赖外部对象存储或网络服务。

## 架构

```
gateway/
  config.py    配置项（GatewayConfig）
  errors.py    稳定错误码的业务异常体系
  models.py    会话状态机、会话/分片/对象指针数据模型
  storage.py   磁盘布局、原子写、崩溃恢复扫描
  sessions.py  会话生命周期与分片提交（乱序/重复/迟到）
  gateway.py   门面：配额、归属校验、原子提交、读取、回收
  api.py       FastAPI 薄封装（业务逻辑全在 UploadGateway）
```

### 磁盘布局

```
<root>/
  objects/<enc(name)>.CURRENT        可见性指针（JSON，原子替换）
  objects/<enc(name)>.data.<token>   某一版本的对象数据
  objects/.staging/<token>           提交中的装配临时文件
  sessions/<sid>/session.json        会话元数据（含已确认分片表）
  sessions/<sid>/chunks/<i>.part     已确认分片
  sessions/<sid>/chunks/<i>.tmp      写入中的分片（恢复时清除）
```

## 会话状态机

```
                create
                  │
                  ▼
        ┌──────────────────┐   complete 成功    ┌───────────┐
        │      ACTIVE      │ ─────────────────► │ COMPLETED │
        │                  │ ─────────────────► │ CANCELLED │
        │                  │ ─────────────────► │  EXPIRED  │
        └──────────────────┘   超过 TTL          └───────────┘
```

* `ACTIVE`：接受分片提交（任意顺序）、可取消、可完成。
* `COMPLETED` / `CANCELLED` / `EXPIRED`：终态。终态后一切写操作
  （包括迟到分片）返回 `session_state_error`。
* 过期有两条路径：后台 `sweep_expired()` 回收，以及访问时的惰性过期。
  过期会话的分片数据被回收，元数据保留以便客户端得到可解释的状态。

### 会话创建（幂等）

* 同一 `session_id` + 相同参数（对象名、属主、总大小、分片大小）重复创建：
  返回已有会话，`created=false`。
* 同一 `session_id` + 不同参数：`session_conflict`（409），原会话不受影响。
* 客户端中断后用同一 `session_id` 重新创建即可拿到断点状态，
  通过 `GET /sessions/{id}` 查看 `confirmed_chunks` 决定补传哪些分片。

## 分片语义

* **乱序**：任意顺序接受。
* **重复**：同 index 同摘要 → 幂等成功（`stored=false`），不重写数据；
  同 index 不同摘要 → `session_conflict`（409），已确认数据不被破坏。
* **校验**：每个分片携带 SHA-256 摘要；尺寸必须等于该 index 的期望尺寸
  （最后一片为余数），摘要必须匹配。校验失败的分片被删除，
  不留下任何已确认状态，绝不静默跳过。
* **缺失**：`complete` 时缺失分片以 `incomplete_upload`（409）返回，
  响应体带 `missing` 列表，会话保持 `ACTIVE` 可继续补传。

## 一致性语义

* **全部确认之前不可见**：分片只落在 `sessions/` 下；只有 `complete`
  校验通过、装配完成、指针原子切换后对象才对外可见。
* **原子可见性**：读取方先读 `CURRENT` 指针快照，再按 token 打开对应
  版本的数据文件。指针的 `os.replace` 是唯一的可见性切换点——
  要么发生要么不发生；提交进行中读取方看到旧版本（或不存在），
  绝不看到不完整对象或混合版本。已打开的旧版本文件描述符仍然可读。
* **并发覆盖**：同一对象名的并发提交在对象锁内串行化，
  **后完成者覆盖先完成者**（last-committer-wins），结果由获得锁的顺序
  确定。未胜出/失败的会话不在 `objects/` 下留任何残留
  （staging 文件由启动恢复清理）。
* **崩溃恢复**：重启时 `recover()` 清理 `.tmp` 分片、staging 残留与
  无指针引用的孤儿数据文件；已确认分片与已提交对象保持一致，
  未完成会话可继续上传。

## 配额与归属

* 每个写操作携带 `owner`；与会话属主不符 → `owner_mismatch`（403）。
* 配额按属主记账：已提交对象字节 + 进行中会话的已确认分片字节。
* 会话创建按声明的 `total_size` 预检，分片写入前再按实际尺寸预检；
  超配额 → `quota_exceeded`（413），**拒绝发生在任何持久写入之前**，
  不产生占用空间或元数据的孤儿。

## 流式与有界内存

* 分片写入、对象装配、对象读取全部按 `copy_buffer_size` 流式处理，
  单对象内存开销不随对象大小增长。
* 会话级锁保证不同会话的上传互不阻塞；对象锁只在提交瞬间持有。

## 配置项（GatewayConfig）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `root_dir` | `./data` | 存储根目录 |
| `default_quota_bytes` | 1 GiB | 每个属主的默认配额 |
| `per_owner_quota` | `{}` | 按属主覆盖配额 |
| `session_ttl_seconds` | 24 h | 会话存活时间，超时进入 EXPIRED |
| `copy_buffer_size` | 1 MiB | 流式拷贝缓冲区（内存上界） |
| `max_chunk_size` | 64 MiB | 单个分片最大尺寸 |

## HTTP API

| 方法与路径 | 说明 |
| --- | --- |
| `POST /sessions` | 创建会话（幂等），body: `object_name, owner, total_size, chunk_size, session_id?` |
| `GET /sessions/{id}` | 查询会话状态与已确认分片 |
| `PUT /sessions/{id}/chunks/{i}` | 提交分片，头：`X-Chunk-SHA256`, `X-Owner` |
| `POST /sessions/{id}/complete` | 完成并原子发布，body: `owner` |
| `POST /sessions/{id}/cancel` | 取消会话，body: `owner` |
| `GET /objects/{name}` | 流式下载对象 |
| `HEAD /objects/{name}` | 对象元信息（大小、SHA-256） |
| `POST /admin/sweep` | 触发过期会话回收 |

错误响应统一为 `{"error": <code>, "message": ..., ...context}`，
状态码映射：`session_not_found/object_not_found`→404，
`session_conflict/session_state_error/incomplete_upload`→409，
`chunk_digest_mismatch/chunk_index_out_of_range`→422，
`quota_exceeded`→413，`owner_mismatch`→403。

## 运行

```bash
uv sync
uv run python main.py        # 启动 HTTP 服务（127.0.0.1:8000）
uv run pytest tests/ -q      # 运行测试（42 个用例）
```

测试覆盖：乱序/重复/迟到分片、摘要与尺寸校验失败、并发竞争提交、
提交期间读取一致性、崩溃恢复（已确认分片/已提交对象/临时文件清理）、
配额与越权拒绝、过期回收。
