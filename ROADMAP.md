# xFlow Roadmap

This document tracks the next engineering work after the Rust service migration.

## References

- [zedeus/nitter](https://github.com/zedeus/nitter) — 成熟的 X/Twitter 替代前端，X 数据获取方案的参考项目。关注其反爬策略适配、GraphQL 端点变更、rate limit 处理等。

## 1. Real X Fetcher

- Implemented first-pass `x_web` account timeline fetcher in Rust using `reqwest`.
- Implemented reading imported `auth_token` and `ct0` from SQLite inside the fetcher boundary.
- Keep cookie/header construction inside the fetcher boundary.
- Ensure token, cookie, and header values are never logged or passed to agent code.
- Next: add list/search support, stronger retry policy, live endpoint drift checks, and richer rate-limit handling.
- **按时间范围回溯获取** — 支持获取某 username 近 N 天的所有推文。当前每次只拉最近 N 条，不支持翻页和时间过滤。需要：
  - 翻页拉取：循环调用 X API 获取 timeline 直到覆盖目标时间范围。
  - 时间过滤：按 `created_at` 过滤掉范围外的推文。
  - 按需触发：TG 命令如 `/fetch @openai 7d` 或 CLI 参数。
- **获取 Likes 推文** — 支持获取某 username 点赞的推文，作为新的 source type（如 `SourceType::Likes`）。通过 X 的 Likes API 端点获取，可用于追踪某用户关注的内容。

## 2. Auth Improvements

- Extend `xflow auth check` with an optional live check mode.
- Keep the default check local-only and safe for offline use.
- Mask tokens in all CLI output, logs, and error messages.
- Add tests for malformed token JSON and manual token import arguments.

## 3. Docker Deployment

- Verified `docker compose` build and run with shared `./data` volume.
- Added `RUST_LOG=xflow=info` to docker-compose for proper log output.
- Pinned Rust toolchain to 1.95.0 in Dockerfile for CI consistency.
- **Next: optimize Docker build speed** — current full Rust recompile on every source change takes 3-4 min. Options:
  - Use `cargo-chef` for layered dependency caching (compile deps once, only recompile app code).
  - Cross-compile Linux binary on macOS host, COPY into slim image (build in seconds).
  - Add `.dockerignore` to exclude `target/`, `data/`, `logs/`, `.env` from build context.

## 4. Telegram Integration

- Test `xflow telegram send` against a real bot and channel.
- Register Telegram slash command menus with `xflow telegram commands set` during production setup.
- Add retry behavior for transient Telegram API failures.
- Add message length truncation for Telegram limits.
- **Implemented bot command handler via long-polling** — `/help`, `/add`, `/remove`, `/list`, `/status`, `/fetch`.
- Poller runs as a `tokio::spawn` task alongside worker loop.
- Source management (add/remove) operates directly on database at runtime.
- **Next**: configurable message templates, `/latest` for recent tweets, `/digest` command.
- **Bot 支持群组** — 当前 bot 仅支持私聊对话，不支持群组。需要：
  - 识别群组消息（消息来源于群组 chat）。
  - 群组中仅响应 bot 被回复或 @提及的消息，避免干扰正常群聊。
  - 支持将群组设置为推送目标 channel（与现有 Telegram channel 对接）。

## 5. API Enhancements

- Add `limit` query parameters to JSON and RSS endpoints.
- Add pagination for larger tweet caches.
- Add source/fetch-state endpoints for monitoring worker health.
- Keep RSS/JSON handlers read-only; fetching stays in CLI/worker paths.

## 6. CI/CD

- GitHub Actions CI: `cargo fmt --check`, `cargo clippy`, `cargo test`.
- Pinned Rust toolchain to 1.95.0 via `rust-toolchain.toml`.
- Cache Cargo dependencies with `Swatinem/rust-cache`.
- **Next**: release build artifacts for Linux server deployment.

## 7. Server Operations

- Add `systemd` service examples for `xflow serve` and `xflow worker`.
- Document binary install, upgrade, and Docker deployment steps.
- Add backup guidance for `config.yaml` and `data/xflow.db`.
- Document Telegram command registration in production setup.

## 9. Agent Analysis Optimization

- Current agent uses simple keyword matching (`src/agent.rs`) — importance score = `hits / 4`, category by hardcoded if-else, chinese_summary is a fixed template.
- Default disabled (`agent.enabled: false`) due to low analysis quality.
- **Next: improve analysis quality** — options:
  - Call LLM API (Claude/GPT) for real relevance scoring, categorization, and Chinese summarization.
  - Configurable analysis backend: `rule` (current) vs `llm` (API-based).
  - Per-source importance threshold override (e.g., @openai is always important).
  - Better keyword matching: support phrase matching, regex, negative keywords.
  - User feedback loop: `/important` / `/ignore` commands to train preferences.

## 8. Risk Control

- Multi-account rotation via `next_auth_account_secret` (round-robin by `last_used_at`).
- Adaptive worker interval based on failure ratio (backoff ×2, recovery ×2/3).
- Token freshness warning (7-day threshold check).
- Request randomization (User-Agent pool, source delay jitter).
- HTTP client 30s timeout to prevent hung requests.
- **Next: fetch 失败时发送 Telegram 告警** — 当前失败只写日志，用户无法及时发现异常。在 worker 每次周期结束后，如果有 source 失败或整体报错，发送告警消息到 TG，包含失败详情。
