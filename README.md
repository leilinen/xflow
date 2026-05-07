# xFlow

[中文文档](README.zh-CN.md)

xFlow is a self-hosted Rust service that fetches X/Twitter content, caches it in SQLite, and outputs via RSS/JSON feeds and Telegram delivery. It includes multi-account rotation, adaptive rate control, rule-based analysis, and an interactive Telegram bot.

```text
X sources -> Fetcher (multi-account rotation, adaptive backoff) -> SQLite cache/dedupe -> Agent -> RSS/JSON/Telegram Bot
```

## Features

- **X Web Fetcher** — Fetches real account timelines via X Web API with browser-compatible headers
- **Multi-account Rotation** — Round-robin across auth accounts, auto-skips limited/rejected ones
- **Adaptive Rate Control** — Auto-backoff on failures, recovery on success, request randomization (UA pool, jitter)
- **Telegram Bot** — Interactive commands: `/add`, `/remove`, `/list`, `/status`, `/fetch`, `/help`
- **Telegram Push** — Auto-delivers new tweets with retry and truncation
- **RSS/JSON Feeds** — HTTP server for feed readers and integrations
- **SQLite Storage** — Single-file database, no external dependencies
- **Docker Compose** — One-command deployment

## Quick Start

### Docker Compose (Recommended)

```bash
cp .env.example .env
# Edit .env: set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
docker compose up --build -d

# Import X auth token (run once)
source .env
cargo build --release
./target/release/xflow init --config config.docker.yaml
./target/release/xflow auth import --label main --auth-token TOKEN --ct0 CT0 --config config.docker.yaml

# Register Telegram bot commands (run once)
./target/release/xflow telegram commands set --config config.docker.yaml
```

### Build from Source

```bash
cargo build --release
./target/release/xflow init
./target/release/xflow auth import --label main --auth-token TOKEN --ct0 CT0
# Edit config.yaml: set fetcher: x_web
./target/release/xflow serve    # API server (port 8000)
./target/release/xflow worker   # Fetch + Telegram delivery loop
```

## Configuration

Default `config.yaml`:

```yaml
server:
  host: 127.0.0.1
  port: 8000
storage:
  database: data/xflow.db
fetch:
  interval_seconds: 900
  default_limit: 5
  fetcher: mock          # Change to "x_web" for real data
  source_delay_min_seconds: 60
  source_delay_max_seconds: 120
sources:
  accounts:
    - username: openai
      limit: 5
agent:
  enabled: false         # Rule-based analysis (disabled by default)
  importance_threshold: 0.45
  push_threshold: 0.7
telegram:
  enabled: true
  bot_token_env: TELEGRAM_BOT_TOKEN
  chat_id_env: TELEGRAM_CHAT_ID
  send_all: true
  parse_mode: HTML
```

Sources can also be managed at runtime via Telegram bot commands.

## Telegram Bot Commands

The bot uses long-polling to receive and respond to commands:

| Command | Description |
|---------|-------------|
| `/help` | Show available commands |
| `/add @username` | Add a source to monitor |
| `/remove @username` | Remove a source |
| `/list` | List all sources and status |
| `/status` | Show system status |
| `/fetch` | Trigger immediate fetch |

## X Auth Token

### Manual Cookie Export

1. Log in to `https://x.com`
2. Open DevTools (`Cmd+Option+I` on macOS, `F12` on Windows)
3. Go to Application → Cookies → `https://x.com`
4. Copy the **Value** for `auth_token` and `ct0`
5. Import:

```bash
xflow auth import --label main --auth-token YOUR_TOKEN --ct0 YOUR_CT0
```

### Token Management

```bash
xflow auth list                              # List accounts (masked tokens)
xflow auth check --label main                # Check stored token shape
xflow auth check --label main --live         # Live validation against X API
xflow auth delete --label main               # Remove account
```

## RSS/JSON Endpoints

```
http://127.0.0.1:8000/rss/all              # All tweets
http://127.0.0.1:8000/rss/account/openai   # Per-account
http://127.0.0.1:8000/rss/important        # Important only
http://127.0.0.1:8000/json/all             # JSON format
http://127.0.0.1:8000/health               # Health check
```

## Risk Control

- **Account Rotation** — Round-robin by `last_used_at`, skips limited/rejected accounts
- **Adaptive Interval** — Backoff on failures (up to ×8), recovery on success
- **Request Randomization** — Random User-Agent pool, variable source delays
- **Token Freshness** — Warns when tokens haven't been updated in 7+ days
- **HTTP Timeout** — 30s timeout to prevent hung connections

## CLI Commands

```bash
xflow init                                 # Initialize config + database
xflow fetch                                # One-time fetch
xflow serve                                # Start HTTP API server
xflow worker                               # Fetch + Telegram loop with bot poller
xflow digest --output digest.md            # Generate Markdown digest
xflow auth import /path/to/token.json      # Import from JSON file
xflow auth import --label L --auth-token X --ct0 Y  # Import directly
xflow auth list                            # List accounts
xflow auth check --label L                 # Check account
xflow auth delete --label L                # Delete account
xflow telegram commands set                # Register bot command menu
xflow telegram commands list               # List registered commands
xflow telegram commands clear              # Clear commands
```

## Security

- Never commit `auth_token`, `ct0`, `.env`, or `*.token.json` files
- Delete token files immediately after import
- Keep SQLite private: `chmod 600 data/xflow.db`
- All CLI output and logs mask token values
- Agent/analysis code never receives raw tokens

## Production Deployment

### Binary Install

```bash
cargo build --release
sudo cp target/release/xflow /usr/local/bin/
sudo mkdir -p /opt/xflow/data
cd /opt/xflow
xflow init
# Edit config.yaml, create .env with Telegram tokens
xflow telegram commands set
```

### systemd

See `README.zh-CN.md` for full systemd unit files.

### Docker

```bash
cp .env.example .env
# Edit .env
docker compose up --build -d
```

Data persists in `./data/` on the host. `docker compose down` does not delete data.

### Backup

```bash
sqlite3 /opt/xflow/data/xflow.db ".backup /opt/xflow/data/xflow.db.bak"
```

## License

MIT
