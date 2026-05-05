# xFlow Code Review Report

## Context

xFlow 是一个自托管的 Rust 服务，从 X/Twitter 时间线抓取推文，缓存到 SQLite，并以 RSS/JSON feeds 提供服务，支持关键词分析和 Telegram 投递。本次评审覆盖全部源码文件，按优先级分类记录。

---

## P0 - 必须修复（影响正确性）

### 1. Fetch 失败阻断 Delivery

- **文件**: `src/worker.rs:14-18`, `src/pipeline.rs:43-46`
- **问题**: `run_once` 中 fetch 失败直接返回 Err，跳过后续所有 delivery。一个源的临时 API 错误会导致已缓存的重要推文也无法投递。
- **修复**: 将 fetch 和 delivery 解耦。fetch 中单个源失败时记录错误并继续处理其他源；delivery 独立运行，不依赖 fetch 的返回状态。

### 2. Pipeline 首个源失败中止全部源

- **文件**: `src/pipeline.rs:43-46`
- **问题**: 当一个源 fetch 失败时 `Err(err)` 立即返回，跳过所有剩余源。6 个源中第 1 个 API 超时，其余 5 个都不会被处理。
- **修复**: 保存失败状态后继续迭代，最终收集所有错误统一返回或记录。

### 3. Pipeline fetch state 消息报告累计计数

- **文件**: `src/pipeline.rs:39`
- **问题**: `format!("Fetched {fetched} tweets.")` 中 `fetched` 是跨所有源的累计值。源 A 产出 5 条、源 B 产出 3 条，源 B 的状态记录会显示 "Fetched 8 tweets."
- **修复**: 在循环内计算 `source_count = tweets.len()`，用 `source_count` 写入状态。

### 4. `deliveries` 表缺少唯一约束

- **文件**: `src/db.rs:68-77`
- **问题**: 无 `UNIQUE(tweet_id, channel)` 约束。重试或并发场景下会产生重复投递记录。
- **修复**: 添加约束，`save_delivery` 改用 `INSERT ... ON CONFLICT DO UPDATE`。

---

## P1 - 建议修复（影响性能/安全性）

### 5. `get_auth_account` 加载全部账户密钥到内存

- **文件**: `src/storage.rs:374-382`
- **问题**: 调用 `list_auth_accounts()` 拉取所有账户（含完整 unmasked token），再内存过滤。只为了查一个 label 暴露了所有密钥。
- **修复**: 直接 `WHERE label = ?` 查询。

### 6. `@` 前缀处理不一致

- **文件**: `src/storage.rs:94` (strip @) vs `src/storage.rs:69` (不 strip)
- **问题**: `disable_source` 去掉 `@` 前缀，但 `upsert_source_enabled` 不去。如果源以 `@openai` 存入，调用 `disable_source("@openai")` 查询的是 `openai`，匹配不到，源无法被禁用。
- **修复**: 在 `upsert_source_enabled` 和所有 source 写入路径统一 strip `@`。

### 7. SQL `LOWER()` 阻止索引使用

- **文件**: `src/storage.rs:227`
- **问题**: `LOWER(t.author_username) = LOWER(?)` 双侧函数调用使 SQLite 无法使用索引，大数据量时全表扫描。
- **修复**: 使用 `COLLATE NOCASE` 或存储归一化的小写列。

### 8. `XWebFetcher` 每次创建新 `reqwest::Client`

- **文件**: `src/fetch.rs:86-101`
- **问题**: `reqwest::Client` 设计为复用（内部维护连接池），但每次 fetch source 都创建新实例。
- **修复**: 在 Pipeline 或 Fetcher 层级创建一次，通过引用共享。

---

## P2 - 架构改进（影响可维护性/可扩展性）

### 9. Source 双重来源导致心智模型混乱

- **文件**: `src/pipeline.rs:18-22`, `src/config.rs`
- **问题**: Source 同时存在于 `config.yaml` 和 SQLite `sources` 表。Pipeline 先查 DB，空了再从 config 种子。但无 API 端点或 CLI 命令管理 DB 中的 source（`disable_source` 无 CLI 入口）。
- **修复**: 明确单一 source of truth。建议 DB 为主，config 仅做初始种子。

### 10. Delivery 路径重复

- **文件**: `src/telegram.rs:147-165`, `src/channel.rs:40-84`, `src/worker.rs:14-18`
- **问题**: 两条投递路径：`cli.rs` → `telegram::send_undelivered()` → `channel::send_undelivered()` 和 `worker.rs` → `channel::send_undelivered()`。前者是旧接口遗留包装。
- **修复**: 统一使用 `channel::send_undelivered`，移除 `telegram::send_undelivered` 的独立入口。

### 11. 缺乏优雅关闭

- **文件**: `src/worker.rs:21-29`
- **问题**: `run_forever` 是无退出条件的无限循环。`Cargo.toml` 声明了 `tokio/signal` 但未使用。进程被杀时可能处于 delivery 写入中间状态。
- **修复**: 监听 SIGTERM/SIGINT，优雅退出当前 cycle。

### 12. 数据库迁移不可扩展

- **文件**: `src/db.rs:105-133`
- **问题**: `migrate_sources` 用 `PRAGMA table_info` 逐列检查。随着 schema 演进，这种 ad-hoc 方式难以维护。
- **修复**: 引入 `schema_version` 表 + 版本化迁移。

### 13. 无重试/退避机制

- **文件**: `src/fetch.rs:229-231`, `src/worker.rs:27`
- **问题**: X API 429 限流直接 bail，worker 固定间隔 sleep。无指数退避、无每源独立重试策略。
- **修复**: 在 fetch 层实现 per-source 退避，或记录限流状态到 DB 供 worker 调度。

---

## P3 - 可以改进（代码质量）

### 14. RSS feed 硬编码 `localhost` 链接

- **文件**: `src/server.rs:85-88, 106-108, 124-126`
- **问题**: 所有 RSS self-link 使用 `http://localhost`，非实际 host:port。
- **修复**: 使用 `AppState.config` 中的 `server.host:server.port`。

### 15. 健康检查太浅

- **文件**: `src/server.rs:43-45`
- **问题**: `/health` 返回硬编码 `{"status": "ok"}`，不检查数据库连接。
- **修复**: 执行简单 DB 查询（如 `SELECT 1`）验证连通性。

### 16. 未使用的 `thiserror` 依赖

- **文件**: `Cargo.toml`
- **问题**: 声明了 `thiserror` 但全项目使用 `anyhow`，从未用到。
- **修复**: 从 `Cargo.toml` 移除。

### 17. `row_to_tweet` 冗余 `unwrap_or_default()`

- **文件**: `src/storage.rs:429`
- **问题**: 检查 `importance_score.is_some()` 后又用 `unwrap_or_default()`，应直接 `unwrap()`。
- **修复**: 改为 `importance_score.unwrap()`。

### 18. `save_delivery` 不必要的 `String::clone()`

- **文件**: `src/storage.rs:312`
- **问题**: `now.clone()` 不必要，可用 `&str` 借用。
- **修复**: 调整为借用或重新排序使用。

---

## P4 - 测试覆盖缺口

### 19. 缺少 `disable_source` 测试

- 无测试验证 `disable_source` 排除源后 `list_sources(pool, true)` 不返回该源。

### 20. 缺少 Delivery 错误路径测试

- `MockChannel` 总是成功。无测试覆盖 `send_tweet` 返回 Err 时 `save_delivery` 记录 `"error"` 状态。

### 21. 缺少 HTTP handler 测试

- 无测试覆盖 Axum 路由处理器（可用 `tower::ServiceExt` 或 `axum::test`）。
