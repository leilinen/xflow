# Repository Guidelines

## Project Structure & Module Organization

`src/` contains the Rust main service. Core modules include `cli.rs` for `clap` commands, `server.rs` for `axum` routes, `db.rs` and `storage.rs` for SQLite, `pipeline.rs` for fetch/analyze orchestration, and `auth.rs` for token import. `tools/xflow_auth_export.py` is the only Python component and is intended for local token export, not server deployment. Runtime files from `xflow init` include `config.yaml` and `data/xflow.db`; do not commit them.

## Build, Test, and Development Commands

- `cargo build`: compile the Rust service.
- `cargo run -- init`: create default config and initialize SQLite.
- `cargo run -- fetch`: populate SQLite using the configured fetcher.
- `cargo run -- serve`: run the local RSS/JSON API.
- `cargo run -- worker`: run scheduled fetch plus Telegram delivery.
- `cargo run -- auth import xflow-token.json`: import locally exported X tokens.
- `cargo test`: run Rust unit and integration tests.
- `cargo fmt --check` and `cargo clippy`: verify formatting and lint quality before PRs.

## Coding Style & Naming Conventions

Target stable Rust. Use `rustfmt` defaults, `snake_case` for functions/modules, `PascalCase` for types, and `SCREAMING_SNAKE_CASE` for constants. Keep module boundaries direct: config parsing in `config.rs`, database access in `storage.rs`, request routing in `server.rs`, and command behavior in `cli.rs`. Prefer typed structs with `serde` derives over untyped JSON except at external API boundaries.

## Testing Guidelines

Use Rust tests near the module under test. Prefer temp directories or SQLite test databases so tests never touch `data/xflow.db`. Cover config loading, token import/masking, fetch dedupe, RSS/JSON output, digest generation, and Telegram delivery tracking. The Python exporter should avoid printing raw tokens and should be tested manually against a local browser profile when auth flow changes.

## Commit & Pull Request Guidelines

Use clear, imperative commit subjects such as `Add token import CLI` or `Migrate RSS API to axum`. Keep commits scoped to one behavior change. Pull requests should include a short summary, test results, linked issues when applicable, and sample CLI/API output for user-facing changes.

## Security & Configuration Tips

Token JSON files are sensitive login state. Never commit `data/`, SQLite databases, browser profiles, `.env`, `xflow-token.json`, or `*.token.json`. Delete uploaded token JSON after `xflow auth import`, keep SQLite private with `chmod 600`, and never pass token/cookie/header values to agent or LLM code.
