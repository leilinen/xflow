# Share to Bot 功能设计文档

> 状态：修订设计稿，仅调研与设计，未实现。代码依据为当前 HEAD `45cdc79`。
> 下文严格区分“已验证事实”“设计结论”和“待确认问题”，不把推测写成现状。

## 1. 目标、边界与原则

用户在 Telegram 与 Bot 的**私聊**中发送 X 推文 URL，Bot 提取 Tweet ID，优先查 PostgreSQL；未命中时复用现有 X Web `TweetDetail` 抓取能力，保存推文和收藏关系，再在该私聊中回复现有样式的推文卡片。

第一版范围：

- 私聊文本中的单个推文 URL；
- 单推查库/抓取、推文入库、收藏幂等写入；
- 复用现有文本、媒体、引用推文和按钮渲染；
- 继续使用现有**单用户** `TELEGRAM_CHAT_ID` 白名单。

不在第一版范围：多用户授权、`/favorites` 查询命令、自动拉取评论、视频下载、AI 总结、频道推送行为调整、持久化任务队列或新增中间件。

最小改动原则：优先新增 `src/bot/share.rs`，`src/bot/mod.rs` 只保留鉴权后的分流和执行器接线；不继续把抓取、URL 解析和收藏流程堆入已超过 2500 行的 `bot/mod.rs`。

## 2. 代码核验结论

### 2.1 Telegram Update 与权限（已验证）

- `src/bot/mod.rs::run_poller()` 使用 `getUpdates` 长轮询；一次返回的 Update 在 `for` 循环中逐条处理，`handle_command(...).await` 和 `handle_callback_query(...).await` 都会阻塞该批次后续 Update 的处理。
- 每条 Update 开始处理前，代码先执行 `offset = Some(update.update_id + 1)`；这个 offset 目前只在内存中，下一次 `getUpdates(offset)` 才会让 Telegram 确认此前 Update，进程不持久化 offset。
- 私聊路径仅在 `TELEGRAM_CHAT_ID` 环境变量存在时比较 `message.chat.id`。若变量缺失，当前代码会放行所有私聊，而不是 fail-closed。
- 群聊和频道走不同分支；群聊还会检查回复/@bot。第一版 Share 不接入群聊或频道。
- 评论 callback 另有 `TELEGRAM_ALLOWED_USER_IDS`；`fetch_allowed_users()` 的空列表语义是允许所有人。这不是第一版 Share 的授权来源。

**设计结论：** Share 只在 `chat.type == "private"` 且 `chat.id` 精确等于已成功读取、解析的 `TELEGRAM_CHAT_ID` 后入队。启动 poller 时若该变量缺失或不是合法 `i64`，应报错退出或禁用整个私聊交互，不能以 `None` 表示“允许所有人”。未授权请求必须在 URL 解析、查库和抓取之前返回，且不能通过 callback 或收藏查询旁路访问数据。

### 2.2 抓取、额度与账号选择（已验证）

- `src/fetch/mod.rs::XWebFetcher::fetch_comments()` 已使用 `TweetDetail`，变量名为 `focalTweetId`。
- `graphql_get()` 在每个请求前调用 `ensure_rate_limit_budget(operation_name)`，并把响应头保存到 `auth_rate_limits`，键为 `(auth_label, endpoint)`。
- 评论缓存未命中会发出一次 `TweetDetail`；拟议 Share 在数据库未命中时也会发出一次 `TweetDetail`。二者因此共享同一账号的 `TweetDetail` 平台额度，不存在独立的“Share 免费额度”。
- 现有实时账号抓取使用 `UserByScreenName`/`UserTweets`，与 `TweetDetail` 是不同 endpoint 记录；但它们共享 `auth_accounts` 的状态、`limited_until` 和账号选择机制。
- `XWebFetcher::new()` 每次通过 `next_auth_account_secret()` 选择未 rejected/deleted、未处于 limited 状态且最久未使用的账号。一次 fetcher 请求遇到安全余量或 429 后只报错，不会在该请求内换账号重试；后续新建 fetcher 可能选到另一个账号。

**设计结论：**

1. Share 必须先查本地库，只有 cache miss 才消耗 `TweetDetail`；评论和 Share 沿用现有安全余量，不预留或虚构独立额度。
2. Share 的一次请求不得在安全余量、429、401/403 后自动换另一个账号重试；向用户返回可重试/不可用提示。
3. 有界并发只控制突发量，不替代平台限流。
4. 不新增“为了 Share 扩大账号池”或“限流即切号”的策略。现有跨请求 LRU 轮换是否符合 X 的条款，代码库没有合规依据，必须由部署者在开发前确认所有账号均有合法授权、用途符合平台条款且轮换不是为了规避平台限制。未确认前不得把多账号轮换描述为额度扩容方案；必要时应将 Share/评论固定到一个合规账号或保持功能关闭，此项属于上线前配置/合规决策，不在本文擅自定案。

### 2.3 TweetDetail focal tweet 结构（已验证与未确认）

已验证：

- `collect_comments()` 会递归寻找任意 `tweet_results.result`，`parse_comment_result()` 再以 `legacy.in_reply_to_status_id_str == focal_tweet_id` 过滤直接回复。
- `parse_tweet_result()` 接受 `result` 或 `result.tweet`，从 `legacy`、`rest_id`、`core.user_results.result` 构造 `Tweet`，并把解包后的 tweet JSON 保存为 `raw`。
- `collect_tweets()` 同样递归扫描所有 `tweet_results`；如果直接用于 TweetDetail，会把评论、引用推文等候选一并收集，不能保证取到 focal tweet。
- `extract_media()` 只读取 focal `raw.legacy.extended_entities.media`（回退到 `entities.media`）；`extract_quoted_tweet()` 依赖 focal `legacy.quoted_status_id_str`，再尝试 `raw.quoted_status_results.result` 等路径。
- 仓库没有真实 TweetDetail 响应 fixture；引用推文/媒体测试使用的是手写 JSON。现有运行代码只从 TweetDetail 提取评论，没有验证或保存 focal tweet。

**设计结论：** 新增 `fetch_single_tweet()` 时不能复用“递归收集后取第一条”。应新增精确选择器：递归检查候选 `tweet_results.result`，兼容现有 `result.tweet` 解包，解析候选 ID 后只接受 `ID == focalTweetId` 的项；找不到、tombstone/visibility wrapper 无法解包或 ID 不一致都返回明确错误。选中的完整 tweet 节点必须保留为 `raw`，以便现有媒体/引用渲染器消费。

**开发前仍待确认：** 必须用经脱敏的真实 TweetDetail 样本验证 focal 节点、`TweetWithVisibilityResults` 等 wrapper、引用推文 `quoted_status_results`、图片、视频/GIF、note tweet 的真实路径。若真实结构不符合现有解析器，只做兼容该结构的最小解析修正；验证完成前不能声称引用推文和媒体已适配 TweetDetail。

### 2.4 PostgreSQL UPSERT（已验证并纠正）

- `storage::upsert_tweet()` 使用 `ON CONFLICT(tweet_id) DO UPDATE SET fetched_at=..., raw_json=...`，随后返回 `rows_affected() > 0`。
- PostgreSQL 对实际执行的 INSERT 和 `DO UPDATE` 通常都会报告一行受影响，所以该布尔值不能区分“新插入”和“冲突更新”。原设计中“`rows_affected()==0` 表示别人已插入”是错误的。
- 调用点检查结果：
  - `fetch::backfill_user()` 依赖该 bool 统计 `new`/`duplicate`，因此现有统计可能把更新也算作新增；
  - `worker/pipeline.rs`、`bot/mod.rs::fetch_latest_for_user()` 和测试中的多数调用忽略返回值；
  - 尚未发现频道投递或抓取去重以该 bool 决定业务分支。

**设计结论：** Share 不依赖现有 bool 判断是否新增；无论 insert/update 都继续按 tweet ID 写收藏。实现阶段应把 `upsert_tweet()` 改成可正确表达插入结果的 SQL，例如 `RETURNING (xmax = 0) AS inserted`，并用 PostgreSQL 集成测试覆盖 insert 与 conflict-update；或者将返回值改为无新增语义并单独修正 `backfill_user()` 的统计查询。不能继续把 `rows_affected()` 当新增信号。此修正只服务现有语义正确性，不扩展功能。

### 2.5 频道投递与私聊回复（已验证）

- `channel::send_undelivered()` 负责频道待投递查询、调用 `DeliveryChannel::send_tweet()`、写 `deliveries` 状态及重试计数。
- `TelegramChannel::send_tweet()` 内部复用了 `format_tweet_message()`、`format_tweet_caption()`、媒体/外链/引用提取和 comment markup，但所有底层 payload 都写死 `self.credentials.chat_id`。
- 现有格式化函数是纯函数；完整的媒体/引用/文章路由和 Telegram 发送仍耦合在 `TelegramChannel` 方法中。

**设计接口：**

```rust
// 纯渲染计划：无 HTTP、无 deliveries 写入、无目标 chat。
fn render_tweet_card(
    tweet: &StoredTweet,
    translation: Option<&str>,
    comments_enabled: bool,
) -> TelegramTweetCard;

// 仅执行 Telegram API 投递；目标由调用方显式传入。
async fn send_rendered_card(
    &self,
    target_chat_id: &str,
    card: &TelegramTweetCard,
) -> Result<ChannelSendReceipt, DeliveryError>;
```

频道适配器继续用配置的 channel id，并且只有 `channel::send_undelivered()` 管理 `deliveries` 副作用；Share 只在鉴权后以当前私聊 chat id 调用相同渲染和发送接口，不查询/写入 `deliveries`，不会把私聊成功误记为频道已投递。现有 `send_with_retry()` 的 Telegram 瞬时/永久错误分类可复用。为控制改动量，接口也可实现为等价的内部 `send_tweet_to(target, ...)`，但必须保持“渲染可复用、目标显式、频道 delivery 状态仅由频道流程写入”这三个边界。

## 3. 异步处理与 Update 语义

### 3.1 为什么不能在 poller 内直接 await

TweetDetail 最长受 30 秒 HTTP timeout 约束，Telegram client 为 35 秒；媒体卡片还可能多次调用 Telegram API。若在现有 `for update` 中直接 await Share，后续命令和 callback 都会被头阻塞。

### 3.2 第一版执行模型

在 `run_poller()` 启动一个进程内 `ShareExecutor`：有界 `mpsc` 队列（建议 32）+ 固定并发 worker/`Semaphore`（建议 4）。这些数字先作为小常量，避免为第一版增加配置面；实现和压测后再决定是否配置化。

1. poller 完成私聊白名单检查和安全 URL 解析；
2. 以 `try_send(ShareRequest { update_id, chat_id, message_id, tweet_id, canonical_url })` 入队，不等待抓取/发送；
3. 队列满时不启动抓取，异步回复“请求繁忙，请稍后重试”，并记录结构化日志；不得改为无界 `tokio::spawn`；
4. 最多 4 个 Share worker 并行。单任务内部按“查库 → 必要时抓取 → upsert → 收藏 → 渲染 → 私聊发送”顺序执行；同 tweet 的竞争依靠数据库主键和收藏复合主键收敛，不要求第一版增加进程内 single-flight；
5. 任务普通错误必须被捕获并分类记录，尽力给授权用户发送失败提示；worker panic 由 supervisor/`JoinSet` 记录并补充 worker，不能终止 poller；日志不得包含 token/cookie/header。

现有命令和 callback 仍按原路径处理；本次只解除 Share 对 Update 接收的阻塞，不顺带重构所有命令并发。

### 3.3 Update 确认、失败与重启

Telegram 的确认由**后续** `getUpdates(offset)` 发生，而不是本地变量赋值本身。第一版在 Share 成功进入有界队列（或明确拒绝为“繁忙”）后允许 poller 继续并推进 offset，因此业务任务可能在完成前被 Telegram 确认。

对应语义明确为：

- 进程正常运行：队列内任务至少执行一次尝试；收藏写入幂等，Telegram 发送不是 exactly-once。
- 抓取/数据库失败：不写收藏或在事务边界前失败，向用户提示重试；不会靠 Telegram 自动重放。
- 收藏成功、Telegram 发送失败：保留收藏，记录发送失败并提示；用户重复分享会幂等更新/命中收藏并重新发送卡片。
- 并发重复请求：推文和收藏各保持一行，但可能产生两次私聊回复；第一版不承诺消息 exactly-once。
- 进程崩溃/重启：已被下一次 `getUpdates(offset)` 确认但仍在内存队列/执行中的请求可能丢失；尚未被 Telegram 确认的 Update 可能重投，靠数据库幂等收敛。重启后不扫描收藏表补发，因为收藏不是任务队列。

这是“不增加持久化 inbox/任务中间件”的明确取舍。若产品要求 crash 后自动恢复所有已接收 Share，则必须另立范围，增加数据库 inbox 状态机并在持久化后才确认 Update；不能假称当前内存队列提供 at-least-once。第一版以用户可重复分享恢复为准。

## 4. 安全 URL 解析

新增纯函数放在 `src/bot/share.rs`，使用现有依赖可访问的标准 URL 解析能力（优先 `reqwest::Url`/`url::Url`，不使用字符串 `contains("x.com")` 或正则拼接 host）：

1. 从 Telegram 文本中逐个取 URL 候选；只接受 `https`，拒绝 userinfo、非默认端口、控制字符和无法解析的 URL；
2. host 做规范化后只允许精确白名单：`x.com`、`www.x.com`、`twitter.com`、`www.twitter.com`、`mobile.twitter.com`；拒绝 `x.com.evil.test`、`evilx.com` 等后缀欺骗；
3. 只接受路径 `/{user}/status/{id}`、`/i/web/status/{id}`（以及经真实分享样本确认后需要的 `/i/status/{id}`）；路径段数量必须匹配，不跟随短链或重定向；
4. Tweet ID 只允许 1–20 位 ASCII 数字并可解析为 `u64`；拒绝空 ID、符号、百分号编码数字、超长值；
5. query/fragment 不参与 ID，输出固定 canonical URL `https://x.com/i/status/{id}`；多个 URL 取第一个**合法推文 URL**。

必须覆盖的异常测试：HTTP/FTP、userinfo、显式异常端口、host 大小写/尾点、Unicode/IDN 欺骗、`x.com.evil.test`、`twitter.com@evil.test`、URL 编码斜杠/数字、双斜杠、缺 user/status/ID、多余路径段、负数/小数/超长 ID、query 中伪造 `/status/`、换行与尾随中英文标点、普通 X 主页、短链、同一文本先恶意 URL 后合法 URL。解析失败绝不触发 DB/X 请求。

## 5. 数据库设计与幂等语义

```sql
CREATE TABLE IF NOT EXISTS telegram_favorites (
    chat_id     BIGINT      NOT NULL,
    tweet_id    TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, tweet_id),
    FOREIGN KEY (tweet_id) REFERENCES tweets(tweet_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_telegram_favorites_chat_created
    ON telegram_favorites (chat_id, created_at DESC);
```

幂等语义固定为“首次收藏时间”：

```sql
INSERT INTO telegram_favorites (chat_id, tweet_id)
VALUES ($1, $2)
ON CONFLICT (chat_id, tweet_id) DO NOTHING;
```

重复或并发分享不会更新 `created_at`，也不会新增行；是否本次新增可直接由 `DO NOTHING` 的 `rows_affected() == 1/0` 判断，因为该语句没有 `DO UPDATE`。Tweet upsert 与 favorite insert 应放在一个数据库事务中，避免产生已入库但本请求未收藏的半完成状态；Telegram 发送在事务提交后进行，绝不能持有数据库事务等待外部 HTTP。发送失败保留收藏，重复分享负责重试展示。

第一版虽然表保留 `chat_id` 以维持合理主键，但所有读写都必须额外绑定当前授权 `TELEGRAM_CHAT_ID`；不提供任意 chat_id 查询接口。

## 6. 端到端流程

1. `run_poller()` 收到 private message；先验证 fail-closed 单用户白名单。
2. 命令仍优先走现有命令解析；非命令文本交给 `share::parse_x_tweet_url()`。
3. 合法 Share 进入有界执行器；poller 立即继续接收 Update。
4. worker `get_tweet(tweet_id)`：命中则不访问 X。
5. 未命中则调用新增 `fetch_single_tweet()`；精确按 focal ID 选取节点，遵守现有 `TweetDetail` 额度，无换号重试。
6. 在短事务内 upsert tweet、`ON CONFLICT DO NOTHING` 写收藏并提交。
7. 调用纯卡片渲染计划，再显式投递到授权私聊；不触碰 `deliveries`。
8. 用户点击评论按钮时仍走现有评论 viewer；该路径现有 `TELEGRAM_ALLOWED_USER_IDS` 的开放语义是独立安全问题。第一版验收必须将其配置为仅授权用户，或统一改为同一 fail-closed 判定，确保未授权用户不能借频道按钮触发抓取。

## 7. 预计文件改动（开发阶段）

| 文件 | 最小职责 |
|---|---|
| `src/bot/share.rs`（新增，优先） | URL 安全解析、`ShareRequest`、Share 业务编排与执行器 |
| `src/bot/mod.rs` | `mod share`、fail-closed 私聊鉴权、鉴权后入队 |
| `src/fetch/mod.rs` | `fetch_single_tweet` 和按 focal ID 精确选择器 |
| `src/storage/db.rs` | `telegram_favorites`，`created_at TIMESTAMPTZ` |
| `src/storage/mod.rs` | 正确的 tweet 插入结果语义；收藏幂等写入/授权用户查询 |
| `src/channel/telegram.rs` | 抽出可复用卡片计划，发送目标显式参数化 |
| `src/channel/mod.rs` | 保持频道 `deliveries` 副作用边界；按需只调整接口接线 |

不修改定时调度、RSS/JSON API、daily digest 或频道业务规则。

## 8. 测试与故障恢复验收

### 8.1 纯函数/fixture 测试

- 上述合法与异常 URL 全集，尤其 host 欺骗、编码路径、超长 ID 和多 URL。
- 真实脱敏 TweetDetail fixtures：普通推文、图片、多媒体 album、视频/GIF、引用推文、note tweet、visibility wrapper/tombstone；断言只选择 ID 完全匹配的 focal tweet，评论/引用 tweet 不会误选。
- `render_tweet_card` 对文本、超长 caption 退化、图片、album、视频/GIF、引用推文、外链/文章和评论按钮的计划测试。

### 8.2 PostgreSQL 集成测试

- `upsert_tweet` 首次 insert 与 conflict-update 的返回语义正确；补测 `backfill_user` 的 new/duplicate 统计。
- 重复及并发 `add_favorite` 只有一行，`created_at` 不变化且数据库类型为 `TIMESTAMPTZ`。
- tweet + favorite 事务中途失败时回滚；外部 Telegram 失败不回滚已提交收藏。
- 两个 worker 同时处理同一 tweet：无 PK 错误、单收藏行，允许两次回复的第一版语义有明确断言。

### 8.3 并发、恢复与 Telegram 失败

- 慢/挂起的 Share 不阻塞后续普通 Update、命令或 callback 入站分发。
- 同时请求数不超过并发上限，队列满时不触发 X 请求并收到繁忙提示；worker error/panic 后容量恢复。
- 401/403、429、安全余量、timeout、TweetDetail 无 focal、DB 错误分别映射为安全提示；不泄漏凭证，不换号重试。
- Telegram 429/5xx/网络错误沿用 transient retry；4xx permanent error 不无限重试；收藏已提交，重复分享可重新投递。
- 模拟确认前重启会重投并幂等；模拟确认后、任务完成前崩溃会丢失，并验证启动日志明确报告内存队列不具恢复能力。

### 8.4 权限与既有功能回归

- `TELEGRAM_CHAT_ID` 缺失/非法时 fail-closed；非授权 private/group/channel 的 URL 均不能查库、抓取或查看收藏。
- 评论 callback 不能绕过授权触发 TweetDetail。
- 现有 `/start /add /remove /list /status /fetch /latest /spam` 行为不变。
- 现有频道文本、图片、album、视频、引用推文、评论按钮、翻译、daily digest 和 delivery 去重/重试回归通过；私聊发送不写频道 `deliveries`，频道发送仍只投配置 channel id。
- 开发完成后执行 `cargo fmt --check`、`cargo clippy`、`cargo test`；需要 PostgreSQL 的测试使用隔离测试库，不触碰运行库。

## 9. 待确认项与开发门槛

以下问题不能从当前仓库代码得出结论，开发/上线前必须确认：

1. 真实 TweetDetail focal、引用推文、媒体及 visibility wrapper 的响应结构；当前只有手写 fixture，不能据此宣称兼容。
2. X 多账号 LRU 轮换的授权与平台条款合规性。不得以规避限流为目的上线；Share 不增加换号重试。
3. 受保护、删除、年龄/地域限制推文的真实错误形态及用户文案。
4. 产品是否接受“入队后确认、崩溃窗口内可丢、用户重发恢复”的第一版语义；若不接受，需单独批准数据库 inbox/任务状态机，属于范围变更。
5. `/favorites` 不在第一版；如果未来提供，仍只能查询当前已授权 chat id。

在上述真实结构、合规和交付语义确认前，本文件只作为设计，不进入业务功能开发。
