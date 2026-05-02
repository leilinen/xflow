# xFlow

xFlow is a self-hosted Python service that turns X/Twitter-like sources into clean RSS and JSON feeds, with optional analysis and Markdown digests. The MVP uses a deterministic `MockFetcher`, so the full pipeline works without real X scraping.

Pipeline:

```text
X/Twitter sources -> Fetcher -> SQLite cache/dedupe -> Agent analysis -> RSS/JSON API
```

## Security Model

- xFlow does not store X usernames or passwords.
- xFlow does not automate password entry, CAPTCHA, or 2FA.
- Manual login uses Playwright with a persistent Chromium profile under `data/x_profiles/<label>`.
- Only `auth_token` and `ct0` are saved in SQLite.
- Raw tokens are masked in CLI output and are never passed to the agent layer.
- RSS and JSON endpoints only read from SQLite; they never fetch X live.
- v1 is single-user/self-hosted.

## Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
playwright install chromium
```

## Quick Start

```bash
xflow init
xflow fetch
xflow serve
```

The default config creates `config.yaml`, `data/xflow.db`, and `data/x_profiles/`.

RSS URLs:

- `http://127.0.0.1:8000/rss/all`
- `http://127.0.0.1:8000/rss/account/openai`
- `http://127.0.0.1:8000/rss/important`

JSON URLs:

- `http://127.0.0.1:8000/json/all`
- `http://127.0.0.1:8000/json/important`

## Manual Auth

```bash
xflow auth login --label account1
xflow auth check --label account1
xflow auth list
xflow auth delete --label account1
```

`xflow auth login` opens `https://x.com/home` in Chromium. Log in manually. xFlow waits until the browser profile has `auth_token` and `ct0`, then stores only those cookies.

`xflow auth check` is local-only in v1. It reports whether stored cookies are present and shaped like real tokens, but it does not make a live request to X.

## Configure Sources

Edit `config.yaml`:

```yaml
server:
  host: 127.0.0.1
  port: 8000
storage:
  database: data/xflow.db
  profile_dir: data/x_profiles
fetch:
  interval_seconds: 900
  default_limit: 20
  fetcher: mock
sources:
  accounts:
    - username: openai
      limit: 5
  lists:
    - list_id: ai-builders
      limit: 5
  searches:
    - query: AI agent
      limit: 5
agent:
  enabled: true
  importance_threshold: 0.45
  push_threshold: 0.7
```

## Digest

```bash
xflow digest
xflow digest --output digest.md
```

The digest groups analyzed tweets by category and includes tweets above `agent.importance_threshold`.

## Development

```bash
pytest
```

## Troubleshooting

- If Playwright cannot launch Chromium, run `playwright install chromium`.
- If feeds are empty, run `xflow fetch` first.
- If config changes are ignored, confirm you are passing the same `--config` path to `fetch`, `serve`, and `digest`.
- If auth login times out, rerun `xflow auth login --label account1 --timeout 600` and complete login manually.
