# 海洋数仓 · 浮标观测包接收服务

浮标站每日 Parquet 观测包入湖的接收 API。支持**双实例**共享同一存储、
**内容寻址去重**、**字节配额**、**崩溃自动回收**、**共享原件完整性核验与补传交接**，
以及完整的**接收进度查询/原件下载**。

技术栈：FastAPI + PyArrow + SQLite（WAL）+ uvicorn，全部阻塞 I/O 在线程池执行，
慢上传与慢核验不阻塞其他接收与查询。

---

## 1. 核心能力与需求对应

| 需求 | 实现 |
| --- | --- |
| 按接收号声明站号、UTC 日期、长度、SHA-256，再上传原包 | `PUT /receipts/{id}/declaration` → `POST /receipts/{id}/content` |
| 包内含站号、UTC 时刻、盐度三列，逐行站号/站日符合声明 | `app/validation.py` 用 PyArrow 逐行校验，保留全部原始行（只读不改写） |
| 每次接收独立留档，可查进度、下载原件 | `receipts` 表每接收一行；`GET /receipts`、`GET /receipts/{id}`、`.../download` |
| 同号同声明重试沿用原记录，异声明拒绝 | 声明幂等；四要素任一不同返回 `409 declaration_conflict` |
| 配置原包字节额度，暂存与成品共用 | `OBS_QUOTA_BYTES`；口径 = 成品 blob + 在途暂存 |
| 相同内容跨接收只存一份、不重复占额 | 内容寻址 `blobs/<sha 前2位>/<sha>` + `ref_count`，硬链接发布 |
| 空间不足明确拒收 | `507 quota_exceeded`，落盘/发布前按实际内容核算 |
| 长度、摘要、内容均通过才发布，失败无可下载成品 | 三关依次校验；失败保持 `UPLOADING`，下载返回 `409` |
| 中断后无用文件及占额自动回收，未完成可重传 | 启动 + 周期 + 手动 `reconcile`，清理孤儿暂存/成品、回退缺件记录 |
| 恢复不误删另一实例在途/已发布原包，不留缺件成功记录 | 全局 flock + 写事务对账；在途暂存按 DB 归属保留，缺件成品回退而非掩盖 |
| 慢上传期间其他包仍可接收、查询 | 网络读取异步、阻塞重活进 I/O 线程池；接收级 flock 只锁同一接收号 |
| **磁盘原件被等长改坏后不得再返回 200 与原 SHA-256** | 下载前惰性核验 + 对账全量重算 SHA-256；长度/摘要不符即隔离，下载 `409 artifact_quarantined`，绝不吐坏字节 |
| **坏原件的全部关联接收显示不可供数与补传原因，其余原件照常供数** | 引入 `QUARANTINED` 状态；隔离 blob 与全部关联接收，`ReceiptOut.available=false`、`last_error` 给原因 |
| **任一关联接收补传通过长度、摘要、站日三关后恢复全部供数** | 正确原包原子 `os.replace` 替换坏件，批量解除隔离；同内容物理一份、额度一份，不重复计额 |
| **双实例下载/对账/补传/删除交错、或更换文件与登记间重启，不得误标/误删/重复计额** | 状态变更全部在全局 flock + 写事务；取锁后复核文件指纹，ref_count 按真实关联重算 |
| **慢核验不拖住其他站日接收** | SHA-256 重算在全局锁之外、I/O 线程池执行，仅状态变更短暂持锁 |
| 可启动的双实例 Docker Compose 与 Swagger 入口 | `docker-compose.yml`（app1/app2 + nginx），`/docs` Swagger |

---

## 2. 目录结构

```
app/
  config.py      环境变量配置（数据目录、额度、对账周期等）
  db.py          SQLite 连接、WAL、BEGIN IMMEDIATE 写事务、建表与 v1→v2→v3 迁移
  errors.py      领域异常与 HTTP 状态码映射（含 artifact_quarantined）
  schemas.py     Pydantic 请求/响应模型（含 available、quarantined_bytes）
  storage.py     flock、内容寻址 blob、暂存、配额、字节核验、隔离/恢复、崩溃对账
  validation.py  PyArrow Parquet 三列与站号/站日逐行校验
  service.py     声明/上传/发布/隔离补传/中止/删除编排，I/O 线程池
  main.py        FastAPI 路由、Swagger、后台对账、异常处理
tests/           pytest 单元/并发/回收/完整性测试 + 自研同步 ASGI 客户端
scripts/
  run_local.sh       无 Docker 时本机双实例启动
  integration_test.py 真实双进程端到端自测（含等长改坏与补传交接）
Dockerfile  docker-compose.yml  nginx.conf  requirements.txt
```

数据卷布局（双实例共享）：

```
$OBS_DATA_DIR/
  obs.db                 SQLite（WAL：obs.db-wal / obs.db-shm）
  staging/<receipt>.part 在途暂存，按接收号命名
  blobs/<xx>/<sha256>    内容寻址成品
  locks/rcpt-<id>.lock   接收级互斥锁
  locks/global.lock      发布/对账全局锁
```

---

## 3. 快速开始

### 方式 A：Docker Compose（推荐，双实例）

```bash
docker compose up -d --build
# 统一入口（nginx 轮询到 app1/app2）
# Swagger:  http://localhost:8080/docs
# OpenAPI:  http://localhost:8080/openapi.json
# 健康检查:  http://localhost:8080/health
```

调整额度：`OBS_QUOTA_BYTES=10485760 docker compose up -d`（默认 1 GiB）。

### 方式 B：本机双进程（无 Docker 环境，自测用）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
./scripts/run_local.sh
# 实例1 http://127.0.0.1:8001/docs ；实例2 http://127.0.0.1:8002/docs
```

---

## 4. API 流程

### 第一步：声明

```bash
curl -X PUT http://localhost:8080/receipts/RCV-0001/declaration \
  -H 'Content-Type: application/json' \
  -d '{"receipt_id":"RCV-0001","station_id":"BUOY-001",
       "obs_date":"2026-11-15","size":1488,
       "sha256":"<64位十六进制>"}'
```

- 同号同声明重复调用 → 幂等返回同一记录；
- 同号异声明（站号/日期/长度/摘要任一不同）→ `409`；
- 声明长度超过总额度 → `507`。

### 第二步：上传原包（body 即 Parquet 原始字节）

```bash
curl -X POST http://localhost:8080/receipts/RCV-0001/content \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @BUOY-001_2026-11-15.parquet
```

依次通过 **长度 → SHA-256 → Parquet 三列与逐行站号/站日** 三关后发布为
`PUBLISHED`；任一失败返回 `422`，接收保持 `UPLOADING` 可重传，且**不产生成品**。

### 查询与下载

```bash
curl http://localhost:8080/receipts                         # 列表，可 ?state=&station_id=
curl http://localhost:8080/receipts/RCV-0001                # 单接收进度
curl -O -J http://localhost:8080/receipts/RCV-0001/download # 下载原件（未发布 409）
curl http://localhost:8080/quota                            # 额度占用
curl -X POST http://localhost:8080/admin/reconcile          # 手动触发回收对账
```

辅助接口：`POST /receipts/{id}/abort`（中止在途并释放暂存）、
`DELETE /receipts/{id}`（删除接收，唯一引用的成品一并回收）。

### 原件完整性与补传交接

接收有三种状态：

| 状态 | 含义 | 可否下载 |
| --- | --- | --- |
| `UPLOADING` | 待上传/可重传（含缺件回退） | 否（409） |
| `PUBLISHED` | 核验通过、可供数（`available=true`） | 是 |
| `QUARANTINED` | 共享原件磁盘字节与登记长度/SHA-256 不符，已隔离、等待补传 | 否（409 `artifact_quarantined`） |

- **检测**：① 每次下载前做惰性核验——若文件 `size + mtime` 与缓存一致直接放行，
  等长改坏必然改变 mtime，缓存失效后重算 SHA-256；② 启动与周期性 `reconcile`
  对全部 blob 重算真实字节摘要（慢哈希在全局锁外执行，不阻塞其他站日）。
- **隔离**：核验发现长度或摘要不符，即在**全局锁 + 写事务**内把该 blob 标记
  `quarantined=1` 并写入原因，同时把引用它的全部 `PUBLISHED` 接收置为
  `QUARANTINED`（引用 `blob_sha256` 保留、文件与额度保留）。下载返回
  `409 artifact_quarantined` 与补传原因，**绝不返回带原摘要的坏字节**；
  其他健康 blob 的接收照常供数。
- **补传交接**：对任一 `QUARANTINED` 关联接收按**原声明**重新 `POST .../content`，
  仍依次通过长度 → SHA-256 → Parquet 站号/站日三关；通过后用正确原包原子
  替换坏件（`.tmp` 硬链接 + `os.replace`），解除 blob 隔离并把**全部关联接收**
  一次性恢复为 `PUBLISHED`。同 SHA 物理只一份、`ref_count` 按真实关联重算，
  补传不新增物理占用、不重复计额。
- **删除交错**：隔离接收不能 `abort`（引用仍在），但可 `DELETE`；只有当 blob
  的引用（PUBLISHED + QUARANTINED）归零才回收文件，避免误删仍被引用的坏件、
  也避免漏删零引用坏件。
- **中断重启**：若“文件已替换为正确字节、DB 尚未登记”时崩溃，重启对账重算
  哈希通过后自动解除隔离并恢复全部关联接收；若补传仅落暂存未发布就崩溃，
  坏件保持 `QUARANTINED`，不会被重新标为成功，重新补传即可。

### Parquet 约束

- 必需三列：`station_id`（字符串）、`timestamp_utc`（timestamp/date/ISO8601 字符串）、
  `salinity`（数值）；允许携带额外列，原样保留。
- 每行 `station_id` 必须等于声明站号；每行时间戳的 **UTC 日期**必须等于声明日期。
- 空表拒绝；时区按 UTC 归一化处理。

---

## 5. 并发与崩溃安全设计

1. **SQLite 多进程共享**：WAL + `busy_timeout` + `BEGIN IMMEDIATE`，写事务跨实例串行化。
2. **接收级 flock**：同一接收号同时只允许一个上传会话，冲突返回 `409 receipt_busy`；
   不同接收号互不阻塞。进程崩溃时内核自动释放锁。
3. **全局 flock**：发布与对账互斥，保证回收不会与在途发布竞争。
4. **原子发布**：成品先写 `.tmp` 再 `rename`；内容已存在则只增 `ref_count`，不重复占额。
5. **发布临界标记 `publishing`**：接收在落盘前即于 DB 置 `publishing=1`，发布结束
   （成功或失败）清零。对账据此保护在途暂存——能非阻塞取得该接收 flock 且暂存缺失，
   才判定为崩溃残留并清标记；锁被活跃会话持有时一律保留。这样“更换文件与登记之间
   中断重启”或对账与补传交错，都不会把活跃暂存误删、把坏件误标成功或重复计额。
6. **对账（reconcile）**：启动即执行，之后每 `OBS_RECONCILE_INTERVAL` 秒一次，
   分两阶段——先在全局锁内快照磁盘/账本，**释放锁后逐文件重算 SHA-256（慢核验不
   持锁、不拖住其他站日）**，再重新持锁并复核指纹后应用结论：
   - 无在途接收归属的暂存（含他实例崩溃残留）→ 删除；活跃/临界态暂存保留；
   - `UPLOADING` 缺暂存 → 进度清零、标记可重传；
   - `PUBLISHED` 缺成品 → 回退 `UPLOADING` 并解除引用，杜绝缺件成功记录；
   - 成品字节长度/SHA-256 不符（含等长改坏）→ blob 与全部关联接收置 `QUARANTINED`；
   - 已隔离 blob 字节恢复正确（补传原子替换后）→ 解除隔离、恢复全部关联接收；
   - 无引用 blob → 删除并注销；账本与磁盘不符 → 以磁盘为准修正（磁盘访问容忍并发
     消失，不在遍历期抛 `FileNotFoundError` 中断整轮对账）。
   持锁后复核 `size+mtime` 指纹，使得对账与补传/删除交错时不会把刚替换的文件误判。
7. **发布自愈重试**：极端交错下若暂存在发布前一刻被回收，上传流程检测到
   `UploadNotComplete` 会用内存中已收齐的字节重新落盘并重试一次，不向调用方返回
   409/500；下载路径对核验期间接收被并发删除也返回明确的 404/409 而非 500。

---

## 6. 配额语义

- `GET /quota` 返回 `quota_bytes / used_bytes / blob_bytes / staging_bytes /
  available_bytes / quarantined_bytes`。
- 相同内容（同 SHA-256）无论被多少接收引用，物理只存一份，`blob_bytes` 只计一次。
- 落盘暂存前按「成品已用 + 本次暂存」核算；发布新内容前按「成品已用 + 本包」核算，
  超额均返回 `507` 且不留半成品。同内容重发因发布去重，不会因暂存而被误拒。
- 隔离坏件仍占额度（计入 `quarantined_bytes`，不释放、不重复计）；补传以同长度
  正确原件原子替换，物理占用不变，恢复后 `quarantined_bytes` 归零。

---

## 7. 测试

```bash
pip install pytest httpx pyarrow
python -m pytest tests/                  # 45 个用例（含并发、配额、崩溃回收、原件完整性）
python -W error -m pytest tests/         # 严格模式（资源告警也视为失败）

# 真实双进程端到端：先按方式 B 启动两实例，再执行
OBS_DATA_DIR=/tmp/obs-e2e python -I scripts/integration_test.py
```

测试覆盖：完整发布/下载往返、幂等与冲突、长度/摘要/Parquet/站号/站日五类拒绝、
去重占额、配额 507、慢上传不阻塞、同接收并发互斥、孤儿暂存/缺件成品回收、
双 Database 连接共享状态，以及**等长改坏检测与全关联隔离、下载惰性核验不吐坏字节、
健康原件照常供数、错误补传拒绝、任一关联补传恢复全部且不重复计额、隔离件删除交错、
v1→v2 schema 迁移、重启交接、慢核验不阻塞其他站日**。

---

## 8. 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OBS_DATA_DIR` | `/data` | 数据根目录（DB、暂存、成品、锁） |
| `OBS_QUOTA_BYTES` | `1073741824` | 原包字节总额度（暂存+成品共用） |
| `OBS_INSTANCE_ID` | `instance-0` | 实例标识，仅用于健康检查/排查 |
| `OBS_RECONCILE_INTERVAL` | `30` | 周期对账间隔秒 |
| `OBS_CHUNK_SIZE` | `1048576` | 流式分块大小 |
