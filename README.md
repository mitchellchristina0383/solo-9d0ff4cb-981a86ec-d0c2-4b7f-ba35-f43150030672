# 只读查询 API（带守护的 PostgreSQL 连接池）

数据库值班场景：多个应用共用有限的 PostgreSQL 连接。客户端等慢 SQL 超时后，
查询可能仍占着会话；未清理的事务也可能影响下一个请求。本服务用
FastAPI + 自研异步连接池解决这些问题：

- 每个请求可**分别设置**“等待连接的超时”（`acquire_timeout`）和
  “执行查询的超时”（`query_timeout`）；
- 排队中的请求被取消后**立即释放队列名额**（排队不占用连接）；
- 查询超时、客户端断开、SQL 报错后，连接标记为“脏”，**在确认会话
  状态已清理之前绝不再借出**：先做服务端取消（`pg_cancel_backend` 等价的
  CancelRequest）、回滚遗留事务、再用 `SELECT 1` 验证；无法确认的一律
  **关闭并更换新连接**；
- 连接数有硬上限：每条连接始终属于一个槽位，更换时先关旧再开新，
  取消与归还交错也不会越限；
- 会话级只读（`default_transaction_read_only=on`），本 API 只暴露
  固定的 SELECT 查询，不执行任意 SQL。

## 快速开始

```bash
docker compose up --build
```

- API 文档（Swagger UI）：http://localhost:8000/docs
- 连接池指标：http://localhost:8000/pool/stats
- PostgreSQL：`localhost:5432`，用户/密码/库均为 `app`/`app`/`demo`

连接池大小用环境变量 `POOL_SIZE` 控制（默认 4，见 `docker-compose.yml`）。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/query/normal` | 普通查询（`SELECT ... FROM items LIMIT n`） |
| POST | `/query/slow` | 延时查询（`SELECT pg_sleep(n)`），模拟慢 SQL |
| POST | `/query/error` | 报错查询：`autocommit` 单语句报错；`transaction` 在 `BEGIN` 后报错，故意留下失败事务 |
| GET | `/export/pricelist` | 导出完整价目表 CSV：按 `boundary` 分两段，两条连接并行分批读取、共享同一快照；参数见下 |
| GET | `/pool/stats` | 连接池指标：`in_use`（在用数）、`waiting`（等待数）、`idle`、`cleaning`、`opened_connections` 及各类计数器 |
| POST | `/demo/burst` | 后台并发发起 n 个慢查询，便于在 `/pool/stats` 观察排队 |

查询类接口的请求体都支持 `acquire_timeout` / `query_timeout`（秒，`null` 表示不限）。

响应码约定：`200` 成功；`503` 等连接超时；`504` 查询执行超时；
`400` SQL 错误；`500` 导出失败（`export_failed`）；`502` 连接级故障
（如后端被杀）；`499` 客户端中途断开。

## 价目表导出（两段并行、同一快照）

`GET /export/pricelist` 供报价组导出完整价目表，改价照常进行：

- `boundary`（必填）：商品编号分界点，`id ≤ boundary` 为第 1 段，
  其余为第 2 段；
- `batch_size`（默认 500）：每批读取行数；
- `acquire_timeout`（默认 10 秒）：等待每路连接的最长时间；
- `demo_fail_segment`（演示/自测用）：让第 1 或第 2 段读完第一批后失败。

实现要点（`app/exporter.py`）：

- **同一快照**：连接 A 开启 `REPEATABLE READ` 只读事务并
  `pg_export_snapshot()`，连接 B 开启同级事务后
  `SET TRANSACTION SNAPSHOT` 导入该快照。两段读到的是同一个一致性
  快照——一笔同时改动两段价格的事务，在 CSV 里只会**整体呈现改前、
  或整体呈现改后**的价格，绝不混合；
- **分批流式**：每段各用一个服务端游标（named cursor）按
  `batch_size` 分批取数，当前批次立即写入各自的临时 spool 文件，
  **内存只保留当前批次**；最终拼接与 HTTP 发送也都是流式的；
- **读完才交付**：两段都读完后才拼接（段 1 的 id 全部 ≤ 分界点 <
  段 2，段内按 id 升序，顺序拼接即全局有序）并返回
  `text/csv`（`id,name,price`，含 `X-Export-Rows` /
  `X-Export-Batches` 响应头）；
- **失败即释放**：任一段失败时 `TaskGroup` 取消另一段，两路连接都
  按“脏”交还连接池（服务端取消 / 回滚 / 会话验证后再复用或更换），
  临时文件删除，请求返回 `500 export_failed`；
- 导出占用池中 **2 条**连接，与其余请求共享连接池；客户端中途断开
  会取消导出并同样释放两路连接。

## 在 /docs 里演示

1. **排队**：先 `POST /demo/burst`（默认 8 个 3 秒慢查询，池只有 4 条
   连接），立刻反复 `GET /pool/stats`，可看到 `in_use=4, waiting=4`。
2. **等连接超时**：池被 burst 占满时，`POST /query/slow` 设
   `acquire_timeout=1`，约 1 秒后返回 `503`。
3. **查询超时**：`POST /query/slow` 设 `delay_seconds=10, query_timeout=1`，
   约 1 秒后返回 `504`；`/pool/stats` 里 `cleaning` 短暂为 1（服务端
   取消 + 验证会话），随后恢复，`counters.recycled` 或 `replaced` 加一。
4. **SQL 报错**：`POST /query/error`，`mode=transaction` 会留下失败
   事务；连接池回滚并验证后才复用（`counters.recycled` 加一）。
5. **客户端取消**：用 `curl` 发一个慢查询后按 `Ctrl-C` 中断，
   `/pool/stats` 的 `waiting`/`in_use` 会立刻回落，
   `counters.queue_cancelled` 或 `client_disconnects` 加一。
6. **并发改价（同一快照）**：终端 1 开启一笔跨两段的改价事务，
   中间停 10 秒：

   ```bash
   docker compose exec db psql -U app -d demo -c "
   BEGIN;
   UPDATE items SET price = price + 10 WHERE id <= 10;
   SELECT pg_sleep(10);
   UPDATE items SET price = price + 10 WHERE id > 10;
   COMMIT;"
   ```

   在这 10 秒窗口内，于 `/docs` 调用 `GET /export/pricelist`
   （`boundary=10`）下载 CSV：两段都是**改前**价格（导出快照建立
   时改价事务尚未提交）。等事务提交后再导一次：两段都是**改后**
   价格。任何时刻都不会出现一段新、一段旧的混合文件。
7. **单路失败**：`/docs` 调用 `GET /export/pricelist` 时设
   `demo_fail_segment=2`（或 1），该段读完第一批后失败，返回
   `500 {"error":"export_failed","detail":"...segment 2..."}`；
   立刻查 `/pool/stats` 可见 `cleaning` 短暂为 1~2（两路脏连接被
   清理/更换），随后恢复 `idle=4`；再次正常导出仍成功。

## 自测

```bash
pip install -r tests/requirements.txt
# 价目表导出用例需要直连数据库（默认即 compose 映射的地址）
export PGDSN="postgresql://app:app@127.0.0.1:5432/demo"
# 可选：提供 psql 连接串后，会额外做服务端核对
# （连接数不超过上限、无残留活跃查询、无失败事务）
export PSQL="postgresql://app:app@127.0.0.1:5432/demo"
python tests/selftest.py
```

覆盖：普通查询、排队与并发上限、等连接超时、排队取消释放名额、
查询超时后的连接清理/更换、两类 SQL 报错后的复用、客户端断开、
取消与归还交错的压力测试（全程校验
`idle + in_use + cleaning == size` 且 `opened_connections <= size`），
以及价目表导出：

- **基本导出**：CSV 表头/行数/排序与库内一致，`batch_size=1` 时
  批次数等于行数，分界点在编号范围外时导出仍完整；
- **并发改价**：先插入 2 万行让第 2 段分批读取持续数秒，改价事务
  在导出中途提交——导出文件必须**全部**是改前价格（快照建立于提交
  之前），提交后再导必须**全部**是改后价格；用例结束自动还原数据；
- **单路失败**：`demo_fail_segment=1/2` 各失败一次，均返回
  `500 export_failed` 且指明失败段；随后校验连接池完全恢复、
  服务端无残留事务/活跃查询、连接数等于池上限，且再次导出成功。

## 关键设计

- **槽位模型**：池大小固定，每条连接处于 `idle / in_use / cleaning`
  三态之一；更换连接时先关闭旧连接再创建新连接，因此打开的连接数
  永远不会超过上限。
- **排队不持名额**：`acquire()` 的等待者只挂在条件变量上，被取消
  （如客户端断开）时从队列移除即可，不会泄漏连接。
- **脏连接不出借**：归还时若连接可能仍在执行查询（超时/断开）或
  刚发生 SQL 错误，则转入后台清理：先发服务端取消并等待查询任务
  收尾，再回滚遗留事务，最后 `SELECT 1` 验证会话空闲；任何一步失败
  即关闭并新建连接顶替。清理期间该槽位不可借出。
- **借出前把关**：借出时先做本地状态检查；空闲超过
  `checkout_verify_after` 秒（默认 10）的连接额外做一次 `SELECT 1`
  活性验证，防止数据库重启后把已死的空闲连接借给请求。

## 已知限制

- 导出期间两路快照事务会一直存续到两段读完：超大表的长耗时导出会
  撑住快照（xmin 不前进），阻碍 vacuum 清理死元组；请按表规模选择
  合适的导出窗口。导出本身不设总超时（只有等连接超时），需要时可
  在反向代理层加限制。
- 导出的临时 spool 文件落在 API 容器的临时目录，响应发送完毕后
  删除；进程异常退出时可能残留，需按 `pricelist_*` 前缀定期清理。
- 连接关闭失败（如对端无响应且超时）时，客户端按已关闭记账，服务端
  可能短暂残留连接，依赖 PostgreSQL 的 TCP keepalive 最终回收；
  `/pool/stats` 的 `opened_connections` 是客户端视角。
- 排队唤醒不保证严格 FIFO（`asyncio.Condition` 语义），极端情况下
  可能有请求等待偏久，但都会在自己的 `acquire_timeout` 内得到确定结果。
- 单进程单事件循环；多副本部署时每个进程各有自己的连接池，
  总连接数 = 副本数 × `POOL_SIZE`，需要值班员按此规划。
- 活性验证只在“借出时”进行；验证通过与实际执行之间的极短窗口内
  连接仍可能失效（此时该请求报错、连接被更换，属正常兜底路径）。
