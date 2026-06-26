# xFlow 中文文档

[English README](README.md)

xFlow 是一个自托管的 Rust 服务，用于抓取 X/Twitter 内容，缓存到 PostgreSQL，并通过 RSS/JSON 和 Telegram 输出。支持多账号轮换、自适应频率控制、规则分析和 Telegram 机器人交互。

```text
X 数据源 -> Fetcher（多账号轮换、自适应退避） -> PostgreSQL 缓存/去重 -> Agent -> RSS/JSON/Telegram Bot
```

## 功能特性

- **X Web 抓取** — 通过 X Web API 抓取真实账号时间线，使用浏览器兼容的请求头
- **多账号轮换** — 自动轮换 auth 账号，跳过被限制/拒绝的账号
- **自适应频率控制** — 失败自动退避，成功逐步恢复，请求随机化（UA 池、抖动）
- **Telegram Bot** — 交互式命令：`/add`、`/remove`、`/list`、`/status`、`/fetch`、`/spam`、`/help`
- **Telegram 推送** — 自动推送新推文，支持图片、视频、GIF、链接预览、引用/回复推文线程、Twitter 文章，媒体发送失败自动降级为纯文本
- **推文评论** — 点击 "Load comments" 按钮按需获取推文评论，评论以回复线程形式展示，支持翻页、富媒体（图片、链接）内嵌显示和垃圾关键词过滤
- **推文浏览** — `/latest @user` 自动从 X 同步最新推文，分页浏览缓存，翻到底可加载更早推文
- **RSS/JSON Feed** — HTTP 服务器输出 feed，方便集成
- **PostgreSQL 存储** — 通过 `database_url` 配置连接，数据安全可靠
- **Docker Compose** — 一键部署

## 快速开始

### Docker Compose（推荐）

```bash
cp .env.example .env
# 编辑 .env：填入 TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID
docker compose up --build -d

# 导入 X auth token（首次运行）
source .env
cargo build --release
./target/release/xflow init --config config.docker.yaml
./target/release/xflow auth import --label main --auth-token TOKEN --ct0 CT0 --config config.docker.yaml

# 注册 Telegram bot 命令菜单（首次运行）
./target/release/xflow telegram commands set --config config.docker.yaml
```

### 从源码构建

```bash
cargo build --release
./target/release/xflow init
./target/release/xflow auth import --label main --auth-token TOKEN --ct0 CT0
# 编辑 config.yaml：设置 fetcher: x_web
./target/release/xflow serve    # API 服务器（端口 8000）
./target/release/xflow worker   # 抓取 + Telegram 推送循环
```

## 配置

默认 `config.yaml`：

```yaml
server:
  host: 127.0.0.1
  port: 8000
storage:
  database_url: postgres://localhost/xflow
fetch:
  interval_seconds: 900
  default_limit: 5
  fetcher: mock          # 改为 "x_web" 抓取真实数据
  source_delay_min_seconds: 60
  source_delay_max_seconds: 120
agent:
  enabled: false         # 规则分析（默认关闭）
  importance_threshold: 0.45
  push_threshold: 0.7
telegram:
  enabled: true
  bot_token_env: TELEGRAM_BOT_TOKEN
  chat_id_env: TELEGRAM_CHAT_ID
  send_all: true
  parse_mode: HTML
```

数据源也可以通过 Telegram bot 命令在运行时动态管理。

## Telegram Bot 命令

### 富媒体推送

xFlow 在推送推文到 Telegram 时自动识别内容类型，使用对应的 Telegram API：

| 内容类型 | 发送方式 |
|---------|---------|
| 单张图片 | `sendPhoto` + caption |
| 多张图片 | `sendMediaGroup`（相册，最多 10 张） |
| 视频 / GIF | `sendVideo` |
| 外部链接 | `sendMessage` + 链接预览卡片 |
| 引用/回复推文 | 先发被引用推文（引用块样式 `▎`），再用 Telegram `reply_parameters` 形成对话线程 |
| Twitter 文章 | `sendMessage` + `[Article]` 标记 + 链接预览 |
| 媒体发送失败 | 自动降级为纯文本，确保推文不丢失 |

如果 caption 内容超过 1024 字符限制，会自动追加一条 `sendMessage` 补充完整文本。

### 推文评论

每条推文消息底部附带 "Load comments" 按钮。点击后 bot 会通过 X API `TweetDetail` 接口获取该推文的直接回复，经过垃圾关键词过滤后，以回复线程形式逐条发送到原消息下方。评论中的图片、链接等富媒体信息会内嵌显示。每页显示 5 条评论，超过时可翻页加载更多。

垃圾关键词通过 `/spam` 命令动态管理，存储在数据库中，无需重启即可生效：

| 命令 | 说明 |
|------|------|
| `/spam` | 显示用法 |
| `/spam list` | 列出所有过滤关键词 |
| `/spam add <关键词>` | 添加过滤关键词 |
| `/spam remove <关键词>` | 删除过滤关键词 |

也可以在 `config.yaml` 中配置初始关键词（作为 fallback）：

```yaml
comments:
  enabled: true
  max_comments: 20
  spam_keywords:
    - "follow me"
    - "free crypto"
```

### 交互式命令

Bot 通过长轮询接收和处理命令：

| 命令 | 说明 |
|------|------|
| `/help` | 显示所有可用命令 |
| `/add @username` | 添加监控源 |
| `/remove @username` | 移除监控源 |
| `/list` | 列出所有源及状态 |
| `/status` | 查看系统状态 |
| `/fetch` | 立即触发一次抓取 |
| `/latest @username` | 浏览推文（自动同步、翻页、加载更早） |
| `/latest @username 7d` | 浏览最近 7 天推文 |
| `/digest` | 查看分析摘要 |
| `/spam` | 显示垃圾关键词用法 |
| `/spam list` | 列出所有过滤关键词 |
| `/spam add <关键词>` | 添加过滤关键词 |
| `/spam remove <关键词>` | 删除过滤关键词 |

## X Auth Token

### 手动获取 Cookie

1. 在浏览器登录 `https://x.com`
2. 打开开发者工具（macOS: `Cmd+Option+I`，Windows: `F12`）
3. 进入 Application → Cookies → `https://x.com`
4. 复制 `auth_token` 和 `ct0` 的**值**
5. 导入：

```bash
xflow auth import --label main --auth-token YOUR_TOKEN --ct0 YOUR_CT0
```

### Token 管理

```bash
xflow auth list                              # 列出账号（脱敏显示）
xflow auth check --label main                # 检查 token 格式
xflow auth check --label main --live         # 在线验证（请求 X API）
xflow auth delete --label main               # 删除账号
```

## RSS/JSON 端点

```
http://127.0.0.1:8000/rss/all              # 所有推文
http://127.0.0.1:8000/rss/account/openai   # 按账号
http://127.0.0.1:8000/rss/important        # 重要推文
http://127.0.0.1:8000/json/all             # JSON 格式
http://127.0.0.1:8000/health               # 健康检查
```

## 风控策略

- **账号轮换** — 按 `last_used_at` 轮换，自动跳过被限制/拒绝的账号
- **自适应间隔** — 失败退避（最高 ×8），成功逐步恢复
- **请求随机化** — 随机 User-Agent 池，随机 source 间隔
- **Token 新鲜度** — Token 超过 7 天未更新时发出警告
- **HTTP 超时** — 30 秒超时防止请求挂起

## CLI 命令

```bash
xflow init                                 # 初始化配置和数据库
xflow fetch                                # 一次性抓取
xflow serve                                # 启动 HTTP API 服务器
xflow worker                               # 抓取 + Telegram 循环（含 bot poller）
xflow digest --output digest.md            # 生成 Markdown 摘要
xflow auth import /path/to/token.json      # 从 JSON 文件导入
xflow auth import --label L --auth-token X --ct0 Y  # 直接导入
xflow auth list                            # 列出账号
xflow auth check --label L                 # 检查账号
xflow auth delete --label L                # 删除账号
xflow telegram commands set                # 注册 bot 命令菜单
xflow telegram commands list               # 查看已注册命令
xflow telegram commands clear              # 清除命令
```

## 安全注意事项

- 不要提交 `auth_token`、`ct0`、`.env`、`*.token.json` 文件
- 导入后立即删除 token 文件
- 保持数据库连接凭据安全
- CLI 和日志中 token 均脱敏显示
- Agent/分析代码不会接收原始 token

## 从 SQLite 迁移到 PostgreSQL

如果你之前使用的是 SQLite，可以使用独立的迁移工具将数据迁移到 PostgreSQL：

```bash
cd tools/migrate_sqlite_to_pg
cargo run --release -- \
  --from /path/to/data/xflow.db \
  --to postgres://user:password@localhost/xflow
```

迁移工具会按顺序读取 SQLite 中的所有表数据并写入 PostgreSQL。冲突行会被跳过（`ON CONFLICT DO NOTHING`），因此迁移是幂等的，可以安全地重复运行。

迁移顺序遵循外键依赖：`auth_accounts` → `auth_rate_limits` → `sources` → `tweets` → `tweet_analysis` → `fetch_state` → `deliveries` → `spam_keywords`。

## 生产部署

### 二进制安装

```bash
cargo build --release
sudo cp target/release/xflow /usr/local/bin/
cd /opt/xflow
xflow init
# 编辑 config.yaml，设置 database_url 和 Telegram token
xflow telegram commands set
```

### systemd

`/etc/systemd/system/xflow-serve.service`：

```ini
[Unit]
Description=xFlow API Server
After=network.target

[Service]
Type=simple
User=xflow
WorkingDirectory=/opt/xflow
ExecStart=/usr/local/bin/xflow serve --config /opt/xflow/config.yaml
Restart=on-failure
RestartSec=5
EnvironmentFile=/opt/xflow/.env
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/xflow/data

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/xflow-worker.service`：

```ini
[Unit]
Description=xFlow Worker (fetch + Telegram)
After=network.target

[Service]
Type=simple
User=xflow
WorkingDirectory=/opt/xflow
ExecStart=/usr/local/bin/xflow worker --config /opt/xflow/config.yaml
Restart=on-failure
RestartSec=10
EnvironmentFile=/opt/xflow/.env
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/xflow/data

[Install]
WantedBy=multi-user.target
```

创建服务用户并启用：

```bash
sudo useradd -r -s /bin/false xflow
sudo chown -R xflow:xflow /opt/xflow
sudo systemctl daemon-reload
sudo systemctl enable --now xflow-serve xflow-worker
```

环境变量文件（`/opt/xflow/.env`）：

```bash
TELEGRAM_BOT_TOKEN=your-bot-token
TELEGRAM_CHAT_ID=your-chat-id
```

### Docker

```bash
cp .env.example .env
# 编辑 .env
docker compose up --build -d
```

数据持久化依赖外部 PostgreSQL 服务，请在 `config.docker.yaml` 中配置 `database_url`。

## License

MIT
