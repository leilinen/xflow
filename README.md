# xFlow

> Self-hosted X/Twitter monitoring for RSS readers, JSON integrations, and Telegram delivery.

[中文文档](README.zh-CN.md) · [Quick Start](#quick-start) · [Configuration](#configuration) · [API](#rss--json-api)

xFlow turns selected X/Twitter accounts into a private, queryable feed pipeline. It fetches posts with locally stored browser auth, deduplicates them in SQLite, optionally scores and summarizes them with rule-based analysis, then exposes the result through RSS, JSON, Markdown digests, and Telegram.

```text
X accounts
   -> x_web fetcher with auth rotation and backoff
   -> SQLite cache, dedupe, fetch state, delivery state
   -> RSS / JSON / Markdown digest / Telegram
```

## Why xFlow

X is useful for fast-moving signals, but hard to consume reliably in a calmer workflow. xFlow is built for people who want to monitor specific accounts, keep the data local, and read or route updates through tools they already control.

It is not a hosted SaaS, a scraping farm, or a public API proxy. It is a small Rust service intended to run on your own machine or server with your own X session cookies.

## Highlights

- **Private by default**: tokens and cached tweets stay in local SQLite.
- **RSS and JSON output**: use any feed reader or custom integration.
- **Telegram delivery**: push new posts to a chat or channel, with retry and delivery tracking.
- **Rich media in Telegram**: images, videos, animated GIFs, external link previews, quoted/replied tweet threading, and Twitter Articles.
- **Display name in Telegram**: shows author's display name alongside @username.
- **Tweet comments**: on-demand comment fetching with inline "Load comments" button, paginated replies as message threads, and spam keyword filtering.
- **Interactive Telegram bot**: manage sources, spam keywords, and trigger fetches from Telegram.
- **Browsable tweet history**: `/latest @user` auto-syncs from X, paginates through cached tweets, and can load older tweets on demand.
- **Multi-account auth rotation**: rotate imported X auth accounts and skip limited or rejected ones.
- **Adaptive rate control**: record X rate-limit headers, back off on failures, and recover after success.
- **Digest generation**: render a Markdown digest from cached and analyzed posts.
- **Docker or binary deployment**: run with Docker Compose, systemd, or a local Rust build.

## Core Capabilities

### 1. Fetch

The production fetcher, `x_web`, uses browser-compatible X Web API requests with imported `auth_token` and `ct0` cookies. It currently supports account timelines. Use the CLI `backfill` command to fetch all historical tweets from an account via cursor-based pagination.

The mock fetcher is useful for local setup and tests because it does not call X.

### 2. Store

xFlow uses one SQLite database for tweets, auth accounts, source definitions, rate-limit state, fetch status, and Telegram delivery state. There is no external database service to run.

### 3. Publish

Cached posts can be consumed through:

- RSS feeds for all posts, important posts, or one account.
- JSON endpoints with pagination.
- Telegram push delivery with rich media support:
  - **Images** — sent as photos or albums via `sendPhoto` / `sendMediaGroup`.
  - **Videos & GIFs** — sent inline via `sendVideo`.
  - **External links** — shown with link preview cards.
  - **Quoted/replied tweets** — displayed as a threaded conversation (quoted tweet first, reply linked via Telegram `reply_parameters`).
  - **Twitter Articles** — rendered with title, text, and article link.
  - **Fallback** — if media delivery fails, falls back to text-only to ensure no posts are lost.
- **Tweet comments**: click "Load comments" on any delivered tweet to fetch and display replies as a threaded conversation. Comments show inline media (images, links) and are paginated. Spam keywords are managed via the bot (`/spam add`, `/spam remove`, `/spam list`).
- Markdown digest output from the CLI.

## Quick Start

### Option A: Docker Compose

```bash
cp .env.example .env
# Edit .env and set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.

docker compose up --build -d
```

Import one X auth account into the Docker database:

```bash
cargo build --release
./target/release/xflow init --config config.docker.yaml
./target/release/xflow auth import \
  --config config.docker.yaml \
  --label main \
  --auth-token YOUR_AUTH_TOKEN \
  --ct0 YOUR_CT0
```

Register Telegram command hints:

```bash
./target/release/xflow telegram commands set --config config.docker.yaml
```

The API listens on `http://127.0.0.1:8000`.

### Option B: Local Binary

```bash
cargo build --release
./target/release/xflow init
```

Edit `config.yaml` and switch the fetcher to real X data:

```yaml
fetch:
  fetcher: x_web
```

Import auth and start services:

```bash
./target/release/xflow auth import --label main --auth-token YOUR_AUTH_TOKEN --ct0 YOUR_CT0
./target/release/xflow serve
./target/release/xflow worker
```

Use two terminals for `serve` and `worker`, or run them as separate services in production.

## X Auth

The `x_web` fetcher requires cookies from a browser session that is already logged in to X.

1. Open `https://x.com` in your browser.
2. Open developer tools.
3. Go to Application or Storage, then Cookies, then `https://x.com`.
4. Copy the values for `auth_token` and `ct0`.
5. Import them into xFlow:

```bash
xflow auth import --label main --auth-token YOUR_AUTH_TOKEN --ct0 YOUR_CT0
```

You can also import from a token JSON file:

```bash
xflow auth import /path/to/xflow-token.json
```

Token management commands:

```bash
xflow auth list
xflow auth check --label main
xflow auth check --label main --live
xflow auth delete --label main
```

CLI output masks token values. Do not paste real tokens into issues, logs, prompts, or shared chat history.

## Configuration

`xflow init` creates `config.yaml` and `data/xflow.db`.

Minimal production-style configuration:

```yaml
server:
  host: 127.0.0.1
  port: 8000

storage:
  database: data/xflow.db

fetch:
  interval_seconds: 900
  default_limit: 5
  fetcher: x_web
  rate_limit_safety_margin: 10
  source_delay_min_seconds: 60
  source_delay_max_seconds: 120

sources:
  accounts:
    - username: openai
      limit: 5
  lists: []
  searches: []

agent:
  enabled: false
  importance_threshold: 0.45
  push_threshold: 0.7

telegram:
  enabled: true
  bot_token_env: TELEGRAM_BOT_TOKEN
  chat_id_env: TELEGRAM_CHAT_ID
  send_all: true
  parse_mode: HTML
  disable_web_page_preview: false

comments:
  enabled: true
  max_comments: 20
  spam_keywords:
    - "follow me"
    - "free crypto"
    - "airdrop"
```

Notes:

- `fetcher: mock` generates local sample data and does not require X auth.
- `fetcher: x_web` currently fetches account sources only.
- `telegram.enabled: true` requires `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in the environment.
- Database paths are resolved relative to the config file.

## RSS & JSON API

Start the API server:

```bash
xflow serve
```

Available endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Health check |
| `GET /rss/all` | RSS feed for all cached posts |
| `GET /rss/account/:username` | RSS feed for one account |
| `GET /rss/important` | RSS feed for posts marked important |
| `GET /json/all?limit=200&offset=0` | JSON feed for all cached posts |
| `GET /json/important?limit=200&offset=0` | JSON feed for important posts |
| `GET /api/sources` | Configured sources with fetch state |
| `GET /api/fetch-state` | Last fetch status per source |

Example:

```bash
curl http://127.0.0.1:8000/json/all?limit=20
```

## Telegram

Set environment variables:

```bash
TELEGRAM_BOT_TOKEN=123456:replace-me
TELEGRAM_CHAT_ID=@your_channel_or_numeric_chat_id
```

Register command hints with Telegram:

```bash
xflow telegram commands set
```

Supported bot commands:

| Command | Purpose |
| --- | --- |
| `/help` | Show commands |
| `/add @openai` | Add an account source |
| `/remove @openai` | Remove an account source |
| `/list` | List sources |
| `/status` | Show service status |
| `/fetch` | Trigger a fetch |
| `/latest @openai` | Browse tweets (auto-sync, paginated, load older) |
| `/digest` | Show digest summary |
| `/spam` | Show spam keyword usage |
| `/spam list` | List all spam keywords |
| `/spam add <keyword>` | Add a spam keyword |
| `/spam remove <keyword>` | Remove a spam keyword |

You can also manually send undelivered posts:

```bash
xflow telegram send --limit 100
```

## CLI Reference

```bash
xflow init
xflow fetch
xflow serve
xflow worker
xflow backfill --username openai --max-pages 10 --page-delay 2
xflow digest --output digest.md

xflow auth import /path/to/token.json
xflow auth import --label main --auth-token TOKEN --ct0 CT0
xflow auth list
xflow auth check --label main
xflow auth check --label main --live
xflow auth delete --label main

xflow telegram send --limit 100
xflow telegram commands set
xflow telegram commands list
xflow telegram commands clear
```

## Deployment

### Docker

```bash
cp .env.example .env
docker compose up --build -d
```

Docker Compose runs two processes:

- `api`: `xflow serve --config config.docker.yaml`
- `worker`: `xflow worker --config config.docker.yaml`

Data is persisted in `./data` on the host.

### systemd

Build and install the binary:

```bash
cargo build --release
sudo cp target/release/xflow /usr/local/bin/
sudo mkdir -p /opt/xflow/data
```

Create `/opt/xflow/config.yaml`, `/opt/xflow/.env`, and run:

```bash
cd /opt/xflow
xflow init --config /opt/xflow/config.yaml
xflow telegram commands set --config /opt/xflow/config.yaml
```

Run `xflow serve` and `xflow worker` as separate systemd units so API availability and background fetching can restart independently.

## Security

- Never commit `data/`, `*.db`, `.env`, `xflow-token.json`, `*.token.json`, browser profiles, or copied cookies.
- Delete token JSON files after import.
- Keep the database private: `chmod 600 data/xflow.db`.
- Treat `auth_token` and `ct0` as active login state.
- Avoid sending raw token values to LLMs, agent code, issue trackers, or logs.

## Development

```bash
cargo fmt --check
cargo clippy
cargo test
```

Useful local workflow:

```bash
cargo run -- init
cargo run -- fetch
cargo run -- serve
```

Runtime files created by `xflow init`, including `config.yaml` and `data/xflow.db`, should stay out of version control.

## License

MIT
