# xFlow 中文文档

[English README](README.md)

xFlow 是一个自托管 Rust 服务，用来把配置好的 X/Twitter 账号源抓取到本地 SQLite 缓存，并输出 RSS/JSON feed。它也包含简单的规则分析、Markdown 摘要和可选 Telegram 推送。

```text
X sources -> Fetcher -> SQLite cache/dedupe -> RuleBasedAgent -> RSS/JSON/Telegram
```

默认 fetcher 是确定性的 `mock`。要读取真实 X 账号时间线，需要导入浏览器里的 `auth_token` 和 `ct0` cookie，并把 `fetch.fetcher` 设置为 `x_web`。

## 安装和初始化

```bash
cargo build --release
./target/release/xflow init
./target/release/xflow fetch
./target/release/xflow serve
```

开发时也可以直接运行：

```bash
cargo run -- init
cargo run -- fetch
cargo run -- serve
```

默认运行文件：

- `config.yaml`
- `data/xflow.db`

如果 Cargo 访问 crates.io 很慢，可以配置国内镜像：

```toml
# ~/.cargo/config.toml
[source.crates-io]
replace-with = "ustc"

[source.ustc]
registry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"
```

## 手动获取 X Token

不要把 `auth_token` 或 `ct0` 发到聊天、Issue、PR、日志或任何 LLM prompt 里。它们等价于登录态。

1. 在浏览器里登录 `https://x.com`。
2. 打开开发者工具：
   - macOS Chrome/Edge：`Cmd + Option + I`
   - Windows/Linux Chrome/Edge：`F12`
   - 或右键页面，选择“检查”。
3. 打开 cookie 表：
   - 中文界面：应用 -> 存储 -> Cookie -> `https://x.com`
   - 英文界面：Application -> Storage -> Cookies -> `https://x.com`
   - 如果看不到“应用/Application”，点开发者工具顶部的 `>>` 展开更多面板。
4. 在 cookie 表中找到名称为 `auth_token` 和 `ct0` 的两行。
5. 只复制它们的“值/Value”，不要复制整行、`auth_token=` 前缀、引号或末尾分号。
6. 在本机生成 xFlow 可导入的 JSON 文件：

```bash
python3 tools/xflow_token_json.py \
  --label account1 \
  --auth-token 'YOUR_AUTH_TOKEN' \
  --ct0 'YOUR_CT0' \
  --out /tmp/xflow-token.json
```

导入到本地数据库：

```bash
cargo run -- auth import /tmp/xflow-token.json
cargo run -- auth list
rm /tmp/xflow-token.json
```

`auth list` 只会显示脱敏 token。

## 浏览器辅助导出

也可以用 Playwright 打开浏览器辅助导出 cookie：

```bash
python3 -m pip install playwright
python3 tools/xflow_auth_export.py --label account1 --out /tmp/xflow-token.json
```

如果 Playwright 自带浏览器下载慢，可以指定本机 Chrome/Edge：

```bash
python3 tools/xflow_auth_export.py \
  --label account1 \
  --out /tmp/xflow-token.json \
  --executable-path "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
```

## 真实读取 X 推文

导入至少一个 auth account 后，编辑 `config.yaml`：

```yaml
fetch:
  interval_seconds: 900
  default_limit: 5
  fetcher: x_web
sources:
  accounts:
    - username: openai
      limit: 5
  lists: []
  searches: []
```

当前 `x_web` 只支持账号源，不支持 list/search。多个 auth account 存在时，xFlow 会按 label 排序使用第一个。

执行抓取：

```bash
cargo run -- fetch
```

成功时会看到类似输出：

```text
Fetched 5 tweets from 1 sources; analyzed 5.
```

如果返回 `Could not authenticate you`，请从同一个已登录浏览器会话重新复制 `auth_token` 和 `ct0`。如果认证通过后出现 GraphQL 错误，可能是 X Web query id 或响应结构发生变化。

可以用环境变量覆盖 GraphQL query id：

```bash
export XFLOW_X_USER_BY_SCREEN_NAME_QUERY_ID=...
export XFLOW_X_USER_TWEETS_QUERY_ID=...
```

## 访问 RSS/JSON

启动服务：

```bash
cargo run -- serve
```

RSS：

- `http://127.0.0.1:8000/rss/all`
- `http://127.0.0.1:8000/rss/account/openai`
- `http://127.0.0.1:8000/rss/important`

JSON：

- `http://127.0.0.1:8000/json/all`
- `http://127.0.0.1:8000/json/important`

## 常用命令

```bash
cargo run -- init
cargo run -- fetch
cargo run -- serve
cargo run -- worker
cargo run -- digest --output digest.md
cargo run -- telegram send
```

鉴权命令：

```bash
cargo run -- auth import /tmp/xflow-token.json
cargo run -- auth list
cargo run -- auth check --label account1
cargo run -- auth delete --label account1
```

## 安全注意事项

- 不要提交 `data/`、SQLite 数据库、`.env`、`xflow-token.json`、`*.token.json` 或浏览器 profile。
- 导入后立即删除 token JSON。
- SQLite 数据库建议保持私有权限，例如 `chmod 600 data/xflow.db`。
- CLI 和日志必须只显示脱敏 token。
- 任何 Agent/LLM 代码都不应该接收 token、cookie 或 header 明文。
