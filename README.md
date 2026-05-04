# xFlow

[中文文档](README.zh-CN.md)

xFlow is a self-hosted Rust service that turns configured X/Twitter-like sources into cached RSS and JSON feeds, with optional analysis, Markdown digests, and Telegram delivery. The server runs as a Rust binary with SQLite and `config.yaml`; it does not require Playwright, Chromium, Python browser dependencies, or a GUI environment.

```text
X sources -> Fetcher -> SQLite cache/dedupe -> RuleBasedAgent -> RSS/JSON/Telegram
```

The default fetcher is a deterministic `MockFetcher`. Set `fetch.fetcher: x_web` to fetch real account timelines with imported X `auth_token`/`ct0` cookies.

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

If Cargo cannot reach crates.io reliably, configure a mirror before building:

```toml
# ~/.cargo/config.toml
[source.crates-io]
replace-with = "ustc"

[source.ustc]
registry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"
```

RSS URLs:

- `http://127.0.0.1:8000/rss/all`
- `http://127.0.0.1:8000/rss/account/openai`
- `http://127.0.0.1:8000/rss/important`

JSON URLs:

- `http://127.0.0.1:8000/json/all`
- `http://127.0.0.1:8000/json/important`

## Local Token Export

Run token export on your own computer, not on the server. Token files contain sensitive login state and should be deleted after import.

Browser-assisted export uses Playwright. On macOS, the script can use an installed Chrome, Edge, or Chromium instead of Playwright's bundled browser:

```bash
python3 -m pip install playwright
python3 tools/xflow_auth_export.py --label account1 --out /tmp/xflow-token.json
```

If browser download is slow or unavailable, point the script at an installed browser:

```bash
python3 tools/xflow_auth_export.py \
  --label account1 \
  --out /tmp/xflow-token.json \
  --executable-path "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
```

Windows example:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install playwright
playwright install chromium
python tools\xflow_auth_export.py --label account1 --out xflow-token.json
```

Manual cookie fallback:

1. Log in to `https://x.com`.
2. Open DevTools:
   - Chrome/Edge on macOS: `Cmd + Option + I`
   - Chrome/Edge on Windows/Linux: `F12`
   - Or right-click the page and choose Inspect.
3. Open the cookies table:
   - English UI: Application -> Storage -> Cookies -> `https://x.com`
   - Chinese UI: 应用 -> 存储 -> Cookie -> `https://x.com`
   - If the Application/应用 tab is hidden, click the `>>` overflow menu in DevTools.
4. In the cookie table, copy only the Value/值 for these cookie names:
   - `auth_token`
   - `ct0`
5. Do not copy the whole row, the `auth_token=` prefix, quotes, or a trailing semicolon.
6. Generate the import file without printing token values:

```bash
python3 tools/xflow_token_json.py \
  --label account1 \
  --auth-token 'YOUR_AUTH_TOKEN' \
  --ct0 'YOUR_CT0' \
  --out /tmp/xflow-token.json
```

Upload and import on the server:

```bash
scp xflow-token.json user@server:/tmp/xflow-token.json
xflow auth import /tmp/xflow-token.json
rm /tmp/xflow-token.json
```

For local testing from the repository:

```bash
cargo run -- auth import /tmp/xflow-token.json
cargo run -- auth list
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

## Real X Fetching

After importing at least one auth account, enable the X Web fetcher. `x_web` has been verified with a real `auth_token`/`ct0` pair against account timelines.

```yaml
fetch:
  interval_seconds: 900
  default_limit: 20
  fetcher: x_web
sources:
  accounts:
    - username: openai
      limit: 20
  lists: []
  searches: []
```

`x_web` currently supports account sources only. If multiple auth accounts are stored, xFlow uses the first account by label. The fetcher keeps cookie/header construction inside Rust fetch code and never passes token values to analysis, RSS, JSON, or Telegram code.

Run a fetch:

```bash
cargo run -- fetch
```

Expected success output:

```text
Fetched 5 tweets from 1 sources; analyzed 5.
```

X Web internals can change. The fetcher uses browser-compatible headers, the `x.com/i/api/graphql` endpoint, and current defaults based on the public web client behavior. GraphQL query ids can be overridden without rebuilding:

```bash
export XFLOW_X_USER_BY_SCREEN_NAME_QUERY_ID=...
export XFLOW_X_USER_TWEETS_QUERY_ID=...
```

If X returns `Could not authenticate you`, refresh `auth_token` and `ct0` from the same logged-in browser session. If X returns GraphQL errors after authentication succeeds, the query ids or response shape may need updating.

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
- Never paste `auth_token` or `ct0` into issues, PRs, chat, logs, or prompts.
- Delete uploaded token JSON immediately after `xflow auth import`.
- Keep SQLite private, for example `chmod 600 data/xflow.db`.
- CLI and logs must mask `auth_token` and `ct0`.
- Agent/LLM code must never receive token, cookie, or header values.
