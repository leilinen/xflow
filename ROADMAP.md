# xFlow Roadmap

This document tracks the next engineering work after the Rust service migration.

## 1. Real X Fetcher

- Implemented first-pass `x_web` account timeline fetcher in Rust using `reqwest`.
- Implemented reading imported `auth_token` and `ct0` from SQLite inside the fetcher boundary.
- Keep cookie/header construction inside the fetcher boundary.
- Ensure token, cookie, and header values are never logged or passed to agent code.
- Next: add list/search support, stronger retry policy, live endpoint drift checks, and richer rate-limit handling.

## 2. Auth Improvements

- Extend `xflow auth check` with an optional live check mode.
- Keep the default check local-only and safe for offline use.
- Mask tokens in all CLI output, logs, and error messages.
- Add tests for malformed token JSON and manual token import arguments.

## 3. Docker Deployment Verification

- Run `docker compose config` on a machine with Docker installed.
- Build the Docker image from the Rust binary Dockerfile.
- Verify `api` and `worker` services share `./data` correctly.
- Document any required host permissions for `data/xflow.db`.

## 4. Telegram Integration

- Test `xflow telegram send` against a real bot and channel.
- Add retry behavior for transient Telegram API failures.
- Add message length truncation for Telegram limits.
- Consider configurable message templates after the basic integration is stable.

## 5. API Enhancements

- Add `limit` query parameters to JSON and RSS endpoints.
- Add pagination for larger tweet caches.
- Add source/fetch-state endpoints for monitoring worker health.
- Keep RSS/JSON handlers read-only; fetching stays in CLI/worker paths.

## 6. CI/CD

- Add GitHub Actions for:
  - `cargo fmt --check`
  - `cargo test`
  - `cargo clippy --all-targets -- -D warnings`
  - release build
- Cache Cargo dependencies to keep CI fast.
- Add release artifacts for Linux server deployment.

## 7. Server Operations

- Add a `systemd` service example for `xflow serve`.
- Add a `systemd` service example for `xflow worker`.
- Document binary install/update steps.
- Add backup guidance for `config.yaml` and `data/xflow.db`.
