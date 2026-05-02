# xFlow

xFlow is a self-hosted Rust service that turns configured X/Twitter-like sources into cached RSS and JSON feeds, with optional analysis, Markdown digests, and Telegram delivery. The server runs as a Rust binary with SQLite and `config.yaml`; it does not require Playwright, Chromium, Python browser dependencies, or a GUI environment.

```text
X sources -> Fetcher -> SQLite cache/dedupe -> RuleBasedAgent -> RSS/JSON/Telegram
```

The current Rust MVP includes a deterministic `MockFetcher`. Real X fetching can use imported `auth_token`/`ct0` in a later fetcher without changing the server auth model.

## Server Installation

```bash
cargo build --release
./target/release/xflow init
./target/release/xflow fetch
./target/release/xflow serve
```

Default files:

- `config.yaml`
- `data/xflow.db`

RSS URLs:

- `http://127.0.0.1:8000/rss/all`
- `http://127.0.0.1:8000/rss/account/openai`
- `http://127.0.0.1:8000/rss/important`

JSON URLs:

- `http://127.0.0.1:8000/json/all`
- `http://127.0.0.1:8000/json/important`

## Local Token Export

Run the exporter on your own computer, not on the server. It works on Windows, macOS, and Linux.

Windows:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install playwright
playwright install chromium
python tools\xflow_auth_export.py --label account1 --out xflow-token.json
```

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install playwright
playwright install chromium
python tools/xflow_auth_export.py --label account1 --out xflow-token.json
```

Upload and import on the server:

```bash
scp xflow-token.json user@server:/tmp/xflow-token.json
xflow auth import /tmp/xflow-token.json
rm /tmp/xflow-token.json
```

Token JSON format:

```json
{
  "label": "account1",
  "domain": "x.com",
  "auth_token": "...",
  "ct0": "...",
  "exported_at": "2026-05-02T09:30:00Z"
}
```

Manual fallback:

```bash
xflow auth import --label account1 --auth-token xxx --ct0 yyy
```

Other auth commands:

```bash
xflow auth list
xflow auth check --label account1
xflow auth delete --label account1
```

## Configuration

Edit `config.yaml`:

```yaml
server:
  host: 127.0.0.1
  port: 8000
storage:
  database: data/xflow.db
fetch:
  interval_seconds: 900
  default_limit: 20
  fetcher: mock
sources:
  accounts:
    - username: openai
      limit: 5
  lists: []
  searches: []
agent:
  enabled: true
  keywords: [AI, agent, LLM, OpenAI, coding, model, paper, GitHub]
  importance_threshold: 0.45
  push_threshold: 0.7
telegram:
  enabled: false
  bot_token_env: TELEGRAM_BOT_TOKEN
  chat_id_env: TELEGRAM_CHAT_ID
  send_all: true
  parse_mode: HTML
  disable_web_page_preview: false
```

## Commands

```bash
xflow init
xflow fetch
xflow serve
xflow worker
xflow digest --output digest.md
xflow telegram send
```

`xflow worker` runs fetch plus Telegram delivery every `fetch.interval_seconds`.

## Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

Compose starts `api` and `worker` services sharing `./data`. The image contains the Rust binary only; it does not install Playwright or Chromium.

## Security

- Token JSON files are sensitive login state. Do not commit them.
- Delete uploaded token JSON immediately after `xflow auth import`.
- Keep SQLite private, for example `chmod 600 data/xflow.db`.
- CLI and logs must mask `auth_token` and `ct0`.
- Agent/LLM code must never receive token, cookie, or header values.
