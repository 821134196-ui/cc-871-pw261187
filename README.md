# 持久化协调服务（Pointwise Delivery Coordinator）

模拟下游会**乱序返回回调**、一次业务操作会经历**多次重试**的场景下，保证最终状态正确的持久化协调服务。纯 Python 标准库实现，持久化基于 SQLite（WAL 模式）。

## 核心保证

### 1. 操作-尝试绑定 + 单调栅栏：旧成功不能覆盖新失败

每个业务任务（`task`）持有单调递增的 `current_attempt` 栅栏，每次重试产生新的 `attempt_no`：

- 回调到达时先比栅栏：`attempt_no < current_attempt` 的回调一律记为 **stale**，写入 `callback_log` 审计表但**不允许移动任务状态**。
- 即使旧尝试迟到的是 SUCCESS、而新尝试已经 FAILED，任务终态仍为 FAILED（返回码 `STALE_SUCCESS_RECORDED`）。
- 同一 `(attempt_no, callback_id)` 的重投只生效一次（返回 `DUPLICATE`）。
- 所有写操作在 `BEGIN IMMEDIATE` 事务内完成，配合任务行的 `version` 乐观版本号，并发回调不会交错破坏栅栏。

### 2. 本地超时 ≠ 结论：可重入对账 + 幂等补偿

本地判定超时只把尝试置为 `TIMEOUT` 并产生对账作业，**绝不直接判成败**：

```
DISPATCHED --(本地超时)--> TIMEOUT --(对账作业 reconcile_job)
                               ├─ query_status = APPLIED      → COMPENSATING → 幂等逆向补偿 → COMPENSATED
                               ├─ query_status = NOT_APPLIED  → 新尝试（栅栏 +1）
                               └─ query_status = UNKNOWN      → 指数退避重试，超上限 → SUSPENDED 人工介入
```

- 补偿以确定性幂等键 `comp:{task_id}:{attempt_no}` 落库，重复执行同一补偿在下游只会生效一次（`undo_log` 去重）。
- 对账作业有持久化队列（`reconcile_job`），进程崩溃重启后 `recover_stale_jobs()` 把残留的 RUNNING 作业收回 PENDING，流程可重入、不丢不重。

### 3. 回调明细逐步放入 + 最终核对

- 回调可携带分片明细（`fragment_seq`），按 seq 去重落库、乱序到达后按序重排。
- 终态回调声明 `detail_n`（分片总数）与 `checksum`（所有分片规范化 JSON 拼接后的 SHA-256）。
- 分片不全、seq 不连续或 checksum 不符时，任务进入 `DETAIL_HELD` 而非伪成功，并触发 `detail` 对账作业从下游拉取权威明细，修复后才收敛为 `SUCCESS`。

## 状态机

| 任务状态 | 含义 | 是否终态 |
|---|---|---|
| PENDING / IN_FLIGHT / RETRYING | 生命周期中间态 | 否 |
| TIMEOUT_PENDING | 本地超时，等待对账结论 | 否 |
| COMPENSATING | 确认下游已生效，补偿进行中 | 否 |
| DETAIL_HELD | 终态回调已到但明细校验未过 | 否（对账修复后转 SUCCESS） |
| SUCCESS / FAILED / COMPENSATED | 业务终态 | 是 |
| SUSPENDED | 对账持续未知/补偿耗尽，需人工介入 | 是 |

## 目录结构

```
coord/
  storage.py         SQLite 表结构、连接（线程局部）、立即写事务
  models.py          状态枚举与回调结果码
  coordinator.py     状态机核心：栅栏校验、回调受理、超时对账、补偿
  mock_downstream.py 模拟下游（持久化 effect、乱序/重投回调、故障注入、幂等 undo）
  worker.py          后台循环：超时扫描 → 对账 → 补偿
  server.py          stdlib HTTP 门面
  app.py             启动入口
tests/
  test_coordinator.py 10 个测试：栅栏、幂等、checksum、补偿、重启恢复、并发
demo/
  scenarios.py       四个端到端叙事场景
```

## HTTP API

| 方法/路径 | 说明 |
|---|---|
| `POST /tasks` | `{task_id, op_type, payload}` 创建并派发尝试 #1 |
| `POST /callbacks` | 下游回调入口，乱序/重投安全 |
| `GET /tasks/{id}` | 任务 + 全部尝试 + 明细分片 + 回调审计 + 补偿/对账记录 |
| `POST /internal/tick` | 手动驱动一轮后台扫描（默认有后台线程自动运行） |
| `GET /health` | 健康检查 |

## 运行

```bash
# 端到端演示（乱序、旧成功、超时补偿、明细核对、UNKNOWN 挂起）
python3 -m demo.scenarios

# 单元 + 集成测试
python3 -m unittest tests.test_coordinator -v

# HTTP 服务
python3 -m coord.app --port 8080
```

## 关键设计取舍

- **超时后为什么是补偿而不是重试？** 对账确认下游已生效时，再发一次正向请求会产生重复业务效应；只有确认 NOT_APPLIED 才开新尝试。UNKNOWN 时宁可不推进（退避后 SUSPENDED），也不猜结果。
- **stale 回调为什么还要落库？** 尝试行只保留首个终态判定，但 `callback_log` 完整保留每个回调（含 stale 标记与是否 accepted），用于事后审计和排障。
- **SQLite 作为持久化介质**：WAL + `BEGIN IMMEDIATE` + 线程局部连接，写入串行化；换成 Postgres 只需替换 `Storage`，状态机代码不变（事务与条件更新语义一一对应）。
