use crate::channel;
use crate::channel::telegram;
use crate::config::AppConfig;
use crate::fetch;
use crate::models::{Source, SourceType};
use crate::storage;
use crate::worker::pipeline;
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
    channel_post: Option<TelegramMessage>,
    callback_query: Option<TelegramCallbackQuery>,
}

#[derive(Debug, Deserialize)]
struct TelegramMessage {
    #[allow(dead_code)]
    message_id: i64,
    chat: TelegramChat,
    text: Option<String>,
    #[serde(default)]
    reply_to_message: Option<Box<TelegramMessage>>,
    #[serde(default)]
    entities: Vec<TelegramEntity>,
    #[serde(default)]
    from: Option<TelegramUser>,
}

#[derive(Debug, Deserialize)]
struct TelegramChat {
    id: i64,
    #[serde(default, rename = "type")]
    chat_type: String,
}

#[derive(Debug, Deserialize)]
struct TelegramUser {
    #[serde(default)]
    id: i64,
    first_name: String,
    #[serde(default)]
    last_name: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    username: Option<String>,
}

impl TelegramUser {
    fn display_name(&self) -> String {
        match &self.last_name {
            Some(last) => format!("{} {}", self.first_name, last),
            None => self.first_name.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct TelegramEntity {
    #[serde(rename = "type")]
    entity_type: String,
    offset: i64,
    length: i64,
}

#[derive(Debug, Deserialize)]
struct TelegramCallbackQuery {
    id: String,
    #[allow(dead_code)]
    from: TelegramUser,
    message: Option<TelegramCallbackMessage>,
    data: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramCallbackMessage {
    message_id: i64,
    chat: TelegramChat,
}

fn is_group_chat(chat: &TelegramChat) -> bool {
    chat.chat_type == "group" || chat.chat_type == "supergroup"
}

pub async fn run_poller(config: AppConfig, pool: PgPool) -> anyhow::Result<()> {
    let bot_token = telegram::load_bot_token(&config.telegram)?;
    let allowed_chat_id = std::env::var(&config.telegram.chat_id_env).ok();
    let bot_username = get_bot_username(&bot_token).await;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(35))
        .build()?;
    let mut offset: Option<i64> = None;

    tracing::info!("Telegram bot poller started");

    loop {
        match poll_updates(&client, &bot_token, offset).await {
            Ok(updates) => {
                for update in updates {
                    offset = Some(update.update_id + 1);

                    // Process channel_post (commands sent in channels)
                    let message = update.message.or(update.channel_post);
                    if let Some(message) = message {
                        let is_group = is_group_chat(&message.chat);
                        let is_channel = message.chat.chat_type == "channel";

                        // In private chat: check authorization
                        if !is_group && !is_channel {
                            if let Some(ref allowed) = allowed_chat_id {
                                if message.chat.id.to_string() != *allowed {
                                    tracing::warn!(
                                        chat_id = message.chat.id,
                                        "ignoring message from unauthorized chat"
                                    );
                                    continue;
                                }
                            }
                        }

                        let Some(text) = message.text else {
                            continue;
                        };

                        // In groups: only respond to replies to bot or @mentions
                        if is_group
                            && !is_bot_addressed(
                                &text,
                                &message.entities,
                                &message.reply_to_message,
                                bot_username.as_deref(),
                            )
                        {
                            continue;
                        }

                        let sender_name = message.from.as_ref().map(|u| u.display_name());
                        handle_command(
                            &config,
                            &pool,
                            &client,
                            &bot_token,
                            message.chat.id,
                            &text,
                            sender_name.as_deref(),
                        )
                        .await;
                    }

                    if let Some(callback) = update.callback_query {
                        handle_callback_query(&config, &pool, &client, &bot_token, callback).await;
                    }
                }
            }
            Err(err) => {
                tracing::error!(?err, "Telegram poll error, will retry");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

async fn get_bot_username(bot_token: &str) -> Option<String> {
    let client = reqwest::Client::new();
    let response = client
        .get(telegram::telegram_api_url(bot_token, "getMe"))
        .send()
        .await
        .ok()?;

    #[derive(Debug, Deserialize)]
    struct BotInfo {
        username: Option<String>,
    }
    #[derive(Debug, Deserialize)]
    struct ApiResponse {
        ok: bool,
        result: Option<BotInfo>,
    }

    let body: ApiResponse = response.json().await.ok()?;
    if body.ok {
        body.result.and_then(|r| r.username)
    } else {
        None
    }
}

/// In a group, the bot should only respond when:
/// 1. The message is a reply to one of the bot's messages, OR
/// 2. The message starts with @bot_username mention before the command, OR
/// 3. The message contains a bot_command entity (e.g. /help)
fn is_bot_addressed(
    text: &str,
    entities: &[TelegramEntity],
    reply_to: &Option<Box<TelegramMessage>>,
    bot_username: Option<&str>,
) -> bool {
    // Check if replying to a message (likely from the bot)
    if reply_to.is_some() {
        return true;
    }

    // Bot commands (e.g. /help, /latest) are always addressed to the bot
    for entity in entities {
        if entity.entity_type == "bot_command" {
            return true;
        }
    }

    // Check if the text contains a mention entity pointing to the bot
    if let Some(username) = bot_username {
        for entity in entities {
            if entity.entity_type == "mention" {
                let mention = text
                    .chars()
                    .skip(entity.offset as usize)
                    .take(entity.length as usize)
                    .collect::<String>();
                if mention == format!("@{username}") {
                    return true;
                }
            }
        }
    }

    false
}

async fn poll_updates(
    client: &reqwest::Client,
    bot_token: &str,
    offset: Option<i64>,
) -> anyhow::Result<Vec<TelegramUpdate>> {
    #[derive(Debug, serde::Serialize)]
    struct GetUpdatesPayload {
        offset: Option<i64>,
        timeout: u32,
        allowed_updates: Vec<String>,
    }

    let payload = GetUpdatesPayload {
        offset,
        timeout: 30,
        allowed_updates: vec![
            "message".to_string(),
            "channel_post".to_string(),
            "callback_query".to_string(),
        ],
    };

    let response = client
        .post(telegram::telegram_api_url(bot_token, "getUpdates"))
        .json(&payload)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("getUpdates request failed: {e}"))?;

    #[derive(Debug, Deserialize)]
    struct ApiResponse {
        ok: bool,
        result: Option<Vec<TelegramUpdate>>,
    }

    let body: ApiResponse = response.json().await?;
    if !body.ok {
        anyhow::bail!("getUpdates returned ok=false");
    }
    Ok(body.result.unwrap_or_default())
}

async fn handle_command(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: i64,
    text: &str,
    sender: Option<&str>,
) {
    let (command, args) = parse_command(text);
    let chat_id_str = chat_id.to_string();

    tracing::info!(command = %command, sender = sender.unwrap_or("unknown"), "processing command");

    let response = match command.as_str() {
        "/help" | "/start" => cmd_help(),
        "/add" => cmd_add(config, pool, &args).await,
        "/remove" => cmd_remove(pool, &args).await,
        "/list" => cmd_list(pool).await,
        "/status" => cmd_status(pool, config).await,
        "/fetch" => {
            // Send immediate acknowledgment
            if let Err(err) = send_reply(client, bot_token, &chat_id_str, "Fetching...").await {
                tracing::error!(?err, "failed to send fetch ack");
            }
            // Spawn in background with 120s timeout
            let config = config.clone();
            let pool = pool.clone();
            let chat_id_str = chat_id_str.clone();
            let client = client.clone();
            let bot_token = bot_token.to_string();
            let timeout_secs = config.fetch.command_timeout_seconds;
            tokio::spawn(async move {
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_secs),
                    cmd_fetch(&config, &pool),
                )
                .await;
                let reply = match result {
                    Ok(Ok(msg)) => msg,
                    Ok(Err(err)) => {
                        tracing::error!(?err, "fetch command error");
                        format!("Error: {err}")
                    }
                    Err(_) => {
                        tracing::error!("fetch command timed out after {timeout_secs}s");
                        format!("Fetch timed out after {timeout_secs} seconds.")
                    }
                };
                if let Err(err) = send_reply(&client, &bot_token, &chat_id_str, &reply).await {
                    tracing::error!(?err, "failed to send fetch reply");
                }
            });
            return;
        }
        "/latest" => {
            let config = config.clone();
            let pool = pool.clone();
            let client = client.clone();
            let bot_token = bot_token.to_string();
            tokio::spawn(async move {
                cmd_latest(&config, &pool, &client, &bot_token, chat_id, &args).await;
            });
            return;
        }
        "/spam" => cmd_spam(pool, &args).await,
        _ => cmd_help(),
    };

    let reply = match response {
        Ok(msg) => msg,
        Err(err) => {
            tracing::error!(?err, command = %command, "command handler error");
            format!("Error: {err}")
        }
    };

    // Truncate to Telegram limit
    let reply = if reply.len() > 4096 {
        format!(
            "{}\n\n... (truncated)",
            &reply[..reply.floor_char_boundary(3900)]
        )
    } else {
        reply
    };

    tracing::info!(command = %command, chat_id = %chat_id_str, reply_len = reply.len(), "sending reply");
    if let Err(err) = send_reply(client, bot_token, &chat_id_str, &reply).await {
        tracing::error!(?err, chat_id = %chat_id_str, "failed to send bot reply");
    } else {
        tracing::info!(command = %command, chat_id = %chat_id_str, "reply sent ok");
    }
}

fn parse_command(text: &str) -> (String, String) {
    let text = text.trim();
    let mut parts = text.splitn(2, char::is_whitespace);
    let cmd = parts.next().unwrap_or("");
    let args = parts.next().unwrap_or("").trim().to_string();
    let cmd = cmd.split('@').next().unwrap_or(cmd).to_string();
    (cmd, args)
}

async fn send_reply(
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: &str,
    text: &str,
) -> anyhow::Result<()> {
    #[derive(Debug, serde::Serialize)]
    struct SendMessagePayload {
        chat_id: String,
        text: String,
    }

    #[derive(Debug, serde::Deserialize)]
    struct TelegramResponse {
        ok: bool,
        #[serde(default)]
        description: Option<String>,
    }

    let response = client
        .post(telegram::telegram_api_url(bot_token, "sendMessage"))
        .json(&SendMessagePayload {
            chat_id: chat_id.to_string(),
            text: text.to_string(),
        })
        .send()
        .await?;

    let status = response.status();
    let body: TelegramResponse = response.json().await.unwrap_or(TelegramResponse {
        ok: false,
        description: Some(format!("invalid response with status {status}")),
    });
    if !body.ok {
        anyhow::bail!(
            "sendMessage failed: {} - {}",
            status,
            body.description.unwrap_or_default()
        );
    }
    Ok(())
}

// --- Callback query handling ---

async fn handle_callback_query(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    callback: TelegramCallbackQuery,
) {
    let Some(data) = &callback.data else {
        // Still answer so the client spinner does not hang.
        let _ = answer_callback_query(client, bot_token, &callback.id).await;
        return;
    };
    let Some(msg) = &callback.message else {
        let _ = answer_callback_query(client, bot_token, &callback.id).await;
        return;
    };

    if let Some(rest) = data.strip_prefix("latest:") {
        // Parse username:page
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 {
            return;
        }
        let username = parts[0].to_string();
        let page: i64 = match parts[1].parse::<i64>() {
            Ok(p) => p.max(1),
            Err(_) => return,
        };

        // Always answer the callback to remove the loading spinner
        let _ = answer_callback_query(client, bot_token, &callback.id).await;
        handle_latest_callback(config, pool, client, bot_token, msg, &username, page).await;
    } else if let Some(rest) = data.strip_prefix("latest_more:") {
        // Parse username:current_page — trigger backfill then show next page
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 {
            return;
        }
        let username = parts[0].to_string();
        let current_page: i64 = match parts[1].parse::<i64>() {
            Ok(p) => p.max(1),
            Err(_) => return,
        };

        let _ = answer_callback_query(client, bot_token, &callback.id).await;
        handle_latest_more_callback(
            config,
            pool,
            client,
            bot_token,
            msg,
            &username,
            current_page,
        )
        .await;
    } else if let Some(view) = data
        .strip_prefix("comments:")
        .and_then(parse_legacy_comments_data)
        .or_else(|| parse_comments_view(data))
    {
        // Comment viewer: enforce the on-demand fetch allowlist before any
        // X API traffic.
        let allowed = fetch_allowed_users();
        let from_id = if callback.from.id != 0 {
            Some(callback.from.id)
        } else {
            None
        };
        if !allowed.is_empty() && !from_id.map(|id| allowed.contains(&id)).unwrap_or(false) {
            let _ = answer_callback_query_with_text(
                client,
                bot_token,
                &callback.id,
                Some("当前账号无权触发评论拉取。"),
            )
            .await;
            return;
        }

        // The pressed channel post is NOT the viewer message (editing a media
        // post is impossible anyway). `comments:` first renders always send a
        // NEW viewer message; `cv:` actions carry the viewer's own message id
        // in callback data semantics and edit in place via the same rule:
        // the viewer message is the one this bot sent earlier, which we track
        // in the viewer state cache keyed by (chat, pressed message).
        tracing::info!(
            user_id = ?from_id,
            chat_id = msg.chat.id,
            pressed_message_id = msg.message_id,
            view = %data,
            "comment viewer callback received"
        );
        let ctx = CallbackContext {
            callback_query_id: &callback.id,
            from_id,
            chat_id: msg.chat.id,
            pressed_message_id: Some(msg.message_id),
        };

        // First press: answer AFTER the (possibly slow) X fetch so the client
        // spinner stays until content is ready; the answer also carries the
        // friendly notice for fallback cases.
        handle_comments_view(config, pool, client, bot_token, &ctx, view).await;
    } else {
        let _ = answer_callback_query(client, bot_token, &callback.id).await;
    }
}

/// `comments:{tweet_id}` or `comments:{tweet_id}:{page}` — the legacy
/// discussion-group button format, now routed to the inline viewer.
fn parse_legacy_comments_data(rest: &str) -> Option<CommentsView> {
    let mut parts = rest.splitn(2, ':');
    let tweet_id = parts.next()?.to_string();
    if tweet_id.is_empty() {
        return None;
    }
    let page = parts
        .next()
        .and_then(|p| p.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    Some(CommentsView::Page { tweet_id, page })
}

async fn answer_callback_query(
    client: &reqwest::Client,
    bot_token: &str,
    callback_query_id: &str,
) -> anyhow::Result<()> {
    answer_callback_query_with_text(client, bot_token, callback_query_id, None).await
}

async fn answer_callback_query_with_text(
    client: &reqwest::Client,
    bot_token: &str,
    callback_query_id: &str,
    text: Option<&str>,
) -> anyhow::Result<()> {
    #[derive(Debug, serde::Serialize)]
    struct AnswerCallbackPayload {
        callback_query_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    }
    let response = client
        .post(telegram::telegram_api_url(bot_token, "answerCallbackQuery"))
        .json(&AnswerCallbackPayload {
            callback_query_id: callback_query_id.to_string(),
            text: text.map(str::to_string),
        })
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("answerCallbackQuery failed: {status} {body}");
    }
    Ok(())
}

async fn edit_message_text(
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: &str,
    message_id: i64,
    text: &str,
    parse_mode: Option<&str>,
    reply_markup: Option<&telegram::InlineKeyboardMarkup>,
) -> anyhow::Result<()> {
    #[derive(Debug, serde::Serialize)]
    struct EditMessagePayload {
        chat_id: String,
        message_id: i64,
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        parse_mode: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_markup: Option<telegram::InlineKeyboardMarkup>,
    }

    // editMessageText replaces the whole message: without parse_mode Telegram
    // renders our HTML tags literally, and over-long edits fail outright.
    let text = match parse_mode {
        Some("HTML") => truncate_html(text, TELEGRAM_MESSAGE_MAX_CHARS),
        _ => truncate_plain(text, TELEGRAM_MESSAGE_MAX_CHARS),
    };

    let response = client
        .post(telegram::telegram_api_url(bot_token, "editMessageText"))
        .json(&EditMessagePayload {
            chat_id: chat_id.to_string(),
            message_id,
            text,
            parse_mode: parse_mode.map(str::to_string),
            reply_markup: reply_markup.cloned(),
        })
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("editMessageText failed: {status} {body}");
    }
    Ok(())
}

async fn send_reply_with_keyboard(
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: &str,
    text: &str,
    reply_markup: Option<&telegram::InlineKeyboardMarkup>,
) -> anyhow::Result<()> {
    #[derive(Debug, serde::Serialize)]
    struct SendMessageWithKeyboard {
        chat_id: String,
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_markup: Option<telegram::InlineKeyboardMarkup>,
    }

    let text = truncate_plain(text, TELEGRAM_MESSAGE_MAX_CHARS);

    let response = client
        .post(telegram::telegram_api_url(bot_token, "sendMessage"))
        .json(&SendMessageWithKeyboard {
            chat_id: chat_id.to_string(),
            text,
            reply_markup: reply_markup.cloned(),
        })
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("sendMessage failed: {status} {body}");
    }
    Ok(())
}

async fn handle_latest_callback(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    msg: &TelegramCallbackMessage,
    username: &str,
    page: i64,
) {
    let per_page = config.fetch.default_limit.max(1);
    let filter = storage::TweetFilter {
        username: if username.is_empty() {
            None
        } else {
            Some(username.to_string())
        },
        limit: per_page,
        offset: (page - 1) * per_page,
    };

    let tweets = match storage::list_tweets(pool, filter.clone()).await {
        Ok(t) => t,
        Err(err) => {
            tracing::error!(?err, "failed to list tweets for latest callback");
            return;
        }
    };

    if tweets.is_empty() {
        let _ = edit_message_text(
            client,
            bot_token,
            &msg.chat.id.to_string(),
            msg.message_id,
            "No more tweets.",
            None,
            None,
        )
        .await;
        return;
    }

    let total = match storage::count_tweets(pool, &filter).await {
        Ok(c) => c,
        Err(_) => (page - 1) * per_page + tweets.len() as i64,
    };

    let can_load_older = !username.is_empty();
    let (text, reply_markup) =
        format_latest_message(&tweets, username, page, total, per_page, can_load_older);

    if let Err(err) = edit_message_text(
        client,
        bot_token,
        &msg.chat.id.to_string(),
        msg.message_id,
        &text,
        None,
        reply_markup.as_ref(),
    )
    .await
    {
        tracing::error!(?err, "failed to edit latest message");
    }
}

async fn handle_latest_more_callback(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    msg: &TelegramCallbackMessage,
    username: &str,
    current_page: i64,
) {
    // Show loading state
    let _ = edit_message_text(
        client,
        bot_token,
        &msg.chat.id.to_string(),
        msg.message_id,
        &format!("Loading older tweets for @{username}..."),
        None,
        None,
    )
    .await;

    // Trigger backfill (fetch a few pages of older tweets)
    let backfill_pages = 3;
    match fetch::backfill_user(config, pool, username, backfill_pages, 2, None).await {
        Ok(result) => {
            tracing::info!(
                username = %username,
                new = result.new,
                total = result.total,
                "backfill for /latest load older"
            );
        }
        Err(err) => {
            tracing::error!(?err, username = %username, "backfill failed for /latest load older");
            let _ = edit_message_text(
                client,
                bot_token,
                &msg.chat.id.to_string(),
                msg.message_id,
                &format!("Failed to load older tweets: {err}"),
                None,
                None,
            )
            .await;
            return;
        }
    }

    // Now show the next page after the current one
    let next_page = current_page + 1;
    let per_page = config.fetch.default_limit.max(1);
    let filter = storage::TweetFilter {
        username: Some(username.to_string()),
        limit: per_page,
        offset: (next_page - 1) * per_page,
    };

    let tweets = match storage::list_tweets(pool, filter.clone()).await {
        Ok(t) => t,
        Err(err) => {
            tracing::error!(?err, "failed to list tweets after backfill");
            return;
        }
    };

    if tweets.is_empty() {
        let _ = edit_message_text(
            client,
            bot_token,
            &msg.chat.id.to_string(),
            msg.message_id,
            "No older tweets found.",
            None,
            None,
        )
        .await;
        return;
    }

    let total = match storage::count_tweets(pool, &filter).await {
        Ok(c) => c,
        Err(_) => (next_page - 1) * per_page + tweets.len() as i64,
    };

    let (text, reply_markup) =
        format_latest_message(&tweets, username, next_page, total, per_page, true);

    if let Err(err) = edit_message_text(
        client,
        bot_token,
        &msg.chat.id.to_string(),
        msg.message_id,
        &text,
        None,
        reply_markup.as_ref(),
    )
    .await
    {
        tracing::error!(?err, "failed to edit latest message after backfill");
    }
}

// --- Inline comment viewer ---

/// Context extracted from a callback query, decoupled from Telegram's
/// serde structs so viewer functions stay testable.
struct CallbackContext<'a> {
    callback_query_id: &'a str,
    /// The user who pressed the button (Telegram `from.id`), when present.
    from_id: Option<i64>,
    chat_id: i64,
    /// The message the button is attached to (the channel post), never the
    /// viewer message itself.
    pressed_message_id: Option<i64>,
}

/// Per-tweet in-memory comment cache with TTL, avoiding repeated X API calls
/// when users page back and forth. Bounded; oldest entries are evicted.
struct CommentCacheEntry {
    comments: Vec<crate::models::TweetComment>,
    fetched_at: std::time::Instant,
}

const COMMENT_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(600);
const COMMENT_CACHE_MAX_ENTRIES: usize = 64;

use std::sync::{Mutex, OnceLock};

fn comment_cache() -> &'static Mutex<std::collections::HashMap<String, CommentCacheEntry>> {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<String, CommentCacheEntry>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn cache_get_comments(tweet_id: &str) -> Option<Vec<crate::models::TweetComment>> {
    let mut cache = comment_cache().lock().ok()?;
    let entry = cache.get_mut(tweet_id)?;
    if entry.fetched_at.elapsed() > COMMENT_CACHE_TTL {
        cache.remove(tweet_id);
        return None;
    }
    Some(entry.comments.clone())
}

fn cache_put_comments(tweet_id: &str, comments: Vec<crate::models::TweetComment>) {
    let Ok(mut cache) = comment_cache().lock() else {
        return;
    };
    if cache.len() >= COMMENT_CACHE_MAX_ENTRIES && !cache.contains_key(tweet_id) {
        // Evict the oldest entry.
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, e)| e.fetched_at)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(
        tweet_id.to_string(),
        CommentCacheEntry {
            comments,
            fetched_at: std::time::Instant::now(),
        },
    );
}

/// Reply-cache keyed by parent comment id for expanded reply views.
fn reply_cache() -> &'static Mutex<std::collections::HashMap<String, CommentCacheEntry>> {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<String, CommentCacheEntry>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Users allowed to trigger on-demand X fetches via inline buttons.
/// Empty list means everyone is allowed (single-user deployments).
fn fetch_allowed_users() -> Vec<i64> {
    std::env::var("TELEGRAM_ALLOWED_USER_IDS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|part| part.trim().parse::<i64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

const COMMENTS_PER_PAGE: usize = 5;

/// Telegram caps message text at 4096 characters.
const TELEGRAM_MESSAGE_MAX_CHARS: usize = 4096;

/// Per-comment body budget before we ellipsize; keeps pages scannable and
/// leaves room for several comments inside a single Telegram message.
const COMMENT_BODY_MAX_CHARS: usize = 500;

/// Cut plain text to `max_chars` characters on a char boundary, adding "…".
fn truncate_plain(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let cut = text.floor_char_boundary(max_chars.saturating_sub(1));
    let mut out = text[..cut].to_string();
    out.push('…');
    out
}

/// Cut HTML-formatted text to `max_chars` characters without splitting tags or
/// entities, then re-close any tags the cut left open.
fn truncate_html(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    // Reserve room for the ellipsis plus closing tags.
    let budget = max_chars.saturating_sub(48).max(1);
    let mut cut = text.floor_char_boundary(budget);
    // Never cut inside a tag or an entity.
    if let Some(lt) = text[..cut].rfind('<') {
        if !text[lt..cut].contains('>') {
            cut = lt;
        }
    }
    if let Some(amp) = text[..cut].rfind('&') {
        if !text[amp..cut].contains(';') {
            cut = amp;
        }
    }
    let head = &text[..cut];
    let mut open: Vec<&str> = Vec::new();
    let mut rest = head;
    while let Some(lt) = rest.find('<') {
        let after = &rest[lt + 1..];
        let Some(gt) = after.find('>') else { break };
        let tag = &after[..gt];
        rest = &after[gt + 1..];
        if let Some(name) = tag.strip_prefix('/') {
            if open.last().is_some_and(|top| *top == name) {
                open.pop();
            }
        } else if !tag.ends_with('/') {
            if let Some(name) = tag.split_whitespace().next() {
                open.push(name);
            }
        }
    }
    let mut out = String::with_capacity(max_chars);
    out.push_str(head);
    out.push('…');
    for name in open.iter().rev() {
        out.push_str("</");
        out.push_str(name);
        out.push('>');
    }
    out
}

/// Parsed `comments_view:` callback payload.
/// List view:  `cv:{tweet_id}:{page}`
/// Replies:    `cv:{tweet_id}:r:{comment_id}`
/// Back:       `cv:{tweet_id}:b:{comment_id}` (reply page for comment_id)
#[derive(Debug, PartialEq)]
enum CommentsView {
    Page {
        tweet_id: String,
        page: usize,
    },
    Replies {
        tweet_id: String,
        comment_id: String,
    },
}

fn parse_comments_view(data: &str) -> Option<CommentsView> {
    let rest = data.strip_prefix("cv:")?;
    let parts: Vec<&str> = rest.splitn(3, ':').collect();
    if parts.len() < 2 {
        return None;
    }
    let tweet_id = parts[0].to_string();
    match parts[1] {
        "r" | "b" => parts.get(2).map(|cid| CommentsView::Replies {
            tweet_id,
            comment_id: (*cid).to_string(),
        }),
        page => page.parse::<usize>().ok().map(|page| CommentsView::Page {
            tweet_id,
            page: page.max(1),
        }),
    }
}

fn comment_page_keyboard(
    tweet_id: &str,
    page: usize,
    total_pages: usize,
    comments: &[crate::models::TweetComment],
) -> telegram::InlineKeyboardMarkup {
    let mut rows: Vec<Vec<telegram::InlineKeyboardButton>> = Vec::new();

    // Expand-replies row: one button per comment that has replies on this
    // page, numbered to match the list above.
    let start = (page.saturating_sub(1)) * COMMENTS_PER_PAGE;
    for (idx, comment) in comments.iter().enumerate() {
        if let Some(count) = comment.reply_count.filter(|c| *c > 0) {
            rows.push(vec![telegram::InlineKeyboardButton::callback(
                &format!(
                    "↩ {}. @{} 的回复 ({count})",
                    start + idx + 1,
                    comment.author_username
                ),
                format!("cv:{}:r:{}", tweet_id, comment.tweet_id),
            )]);
        }
    }

    // Pagination row.
    let mut pager = Vec::new();
    if page > 1 {
        pager.push(telegram::InlineKeyboardButton::callback(
            "◀",
            format!("cv:{}:{}", tweet_id, page - 1),
        ));
    }
    pager.push(telegram::InlineKeyboardButton::callback(
        &format!("{page}/{total_pages}"),
        format!("cv:{}:{page}", tweet_id),
    ));
    if page < total_pages {
        pager.push(telegram::InlineKeyboardButton::callback(
            "▶",
            format!("cv:{}:{}", tweet_id, page + 1),
        ));
    }
    rows.push(pager);

    telegram::InlineKeyboardMarkup {
        inline_keyboard: rows,
    }
}

fn replies_keyboard(tweet_id: &str, comment_id: &str) -> telegram::InlineKeyboardMarkup {
    telegram::InlineKeyboardMarkup {
        inline_keyboard: vec![vec![telegram::InlineKeyboardButton::callback(
            "◀ 返回评论列表",
            format!("cv:{tweet_id}:b:{comment_id}"),
        )]],
    }
}

fn format_comment_list(
    header: &str,
    comments: &[crate::models::TweetComment],
    start_index: usize,
    parent_username: Option<&str>,
) -> String {
    let mut blocks = vec![header.to_string()];
    for (idx, comment) in comments.iter().enumerate() {
        blocks.push(format_single_comment(
            comment,
            start_index + idx + 1,
            parent_username,
        ));
    }
    blocks.join("\n\n")
}

async fn fetch_comments_cached(
    config: &AppConfig,
    pool: &PgPool,
    tweet_id: &str,
) -> anyhow::Result<Vec<crate::models::TweetComment>> {
    if let Some(cached) = cache_get_comments(tweet_id) {
        return Ok(cached);
    }
    let comments =
        fetch::fetch_tweet_comments(config, pool, tweet_id, config.comments.max_comments).await?;
    cache_put_comments(tweet_id, comments.clone());
    Ok(comments)
}

async fn fetch_replies_cached(
    config: &AppConfig,
    pool: &PgPool,
    comment_id: &str,
) -> anyhow::Result<Vec<crate::models::TweetComment>> {
    {
        let cache = reply_cache().lock().ok();
        if let Some(cache) = cache {
            if let Some(entry) = cache.get(comment_id) {
                if entry.fetched_at.elapsed() <= COMMENT_CACHE_TTL {
                    return Ok(entry.comments.clone());
                }
            }
        }
    }
    let replies =
        fetch::fetch_tweet_comments(config, pool, comment_id, config.comments.max_comments).await?;
    if let Ok(mut cache) = reply_cache().lock() {
        if cache.len() >= COMMENT_CACHE_MAX_ENTRIES && !cache.contains_key(comment_id) {
            if let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, e)| e.fetched_at)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(
            comment_id.to_string(),
            CommentCacheEntry {
                comments: replies.clone(),
                fetched_at: std::time::Instant::now(),
            },
        );
    }
    Ok(replies)
}

/// Per-user result of the private-chat probe, so the silent probe message is
/// sent at most once per user per TTL window instead of on every press.
fn probe_cache() -> &'static Mutex<std::collections::HashMap<i64, std::time::Instant>> {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<i64, std::time::Instant>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

const PROBE_TTL: std::time::Duration = std::time::Duration::from_secs(24 * 3600);
const PROBE_CACHE_MAX: usize = 256;

fn probe_recently_ok(user_id: i64) -> bool {
    let mut cache = probe_cache().lock().ok();
    let Some(cache) = cache.as_mut() else {
        return false;
    };
    match cache.get(&user_id) {
        Some(at) if at.elapsed() <= PROBE_TTL => true,
        Some(_) => {
            cache.remove(&user_id);
            false
        }
        None => false,
    }
}

fn probe_mark_ok(user_id: i64) {
    let Ok(mut cache) = probe_cache().lock() else {
        return;
    };
    if cache.len() >= PROBE_CACHE_MAX && !cache.contains_key(&user_id) {
        if let Some(oldest) = cache.iter().min_by_key(|(_, at)| *at).map(|(k, _)| *k) {
            cache.remove(&oldest);
        }
    }
    cache.insert(user_id, std::time::Instant::now());
}

/// Binds each channel post to the viewer message this bot opened for it, so
/// later presses edit the viewer in place instead of spawning duplicates.
/// Keyed by (chat_id, pressed_message_id); bounded and TTL-expired.
fn viewer_state_cache() -> &'static Mutex<std::collections::HashMap<(i64, i64), ViewerStateEntry>> {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<(i64, i64), ViewerStateEntry>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Viewer message id + the instant it was bound.
type ViewerStateEntry = (i64, std::time::Instant);

const VIEWER_STATE_TTL: std::time::Duration = std::time::Duration::from_secs(3600);
const VIEWER_STATE_MAX: usize = 256;

fn viewer_state_get(chat_id: i64, pressed_message_id: i64) -> Option<i64> {
    let mut cache = viewer_state_cache().lock().ok()?;
    let entry = cache.get_mut(&(chat_id, pressed_message_id))?;
    if entry.1.elapsed() > VIEWER_STATE_TTL {
        cache.remove(&(chat_id, pressed_message_id));
        return None;
    }
    Some(entry.0)
}

fn viewer_state_put(chat_id: i64, pressed_message_id: i64, viewer_message_id: i64) {
    let Ok(mut cache) = viewer_state_cache().lock() else {
        return;
    };
    if cache.len() >= VIEWER_STATE_MAX && !cache.contains_key(&(chat_id, pressed_message_id)) {
        if let Some(oldest) = cache.iter().min_by_key(|(_, (_, at))| *at).map(|(k, _)| *k) {
            cache.remove(&oldest);
        }
    }
    cache.insert(
        (chat_id, pressed_message_id),
        (viewer_message_id, std::time::Instant::now()),
    );
}

/// Where the comment viewer renders and whether it edits an existing message.
#[derive(Default)]
struct ViewerTarget<'a> {
    chat_id: &'a str,
    /// Existing viewer message to edit in place; None = send a new one.
    message_id: Option<i64>,
    /// Filled by `render_into` when a fresh viewer message was sent.
    last_sent_message_id: Option<i64>,
}

/// Entry point for `comments:` (legacy button) and `cv:` callbacks.
/// Sends/edits a single "comment viewer" message instead of fanning out
/// messages into a discussion group.
async fn handle_comments_view(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    callback: &CallbackContext<'_>,
    view: CommentsView,
) {
    // A viewer message exists only if this bot already sent one for the
    // pressed channel post. First press sends a fresh viewer message; later
    // `cv:` presses edit that viewer message in place.
    let viewer_message_id = callback
        .pressed_message_id
        .and_then(|pressed| viewer_state_get(callback.chat_id, pressed));

    // Show the viewer in the user's private chat when known, so the channel
    // stays clean; fall back to the chat the button was pressed in. The
    // probe (a silent, immediately-deleted message) runs at most once per
    // user per TTL window.
    let (target_chat_id, answered_notice) = match callback.from_id {
        Some(user_id) if probe_recently_ok(user_id) => (user_id.to_string(), None),
        Some(user_id) => {
            let user_chat = user_id.to_string();
            match telegram::send_private_probe(client, bot_token, &user_chat).await {
                Ok(()) => {
                    probe_mark_ok(user_id);
                    tracing::debug!(
                        user_id,
                        "private chat probe ok, viewer opens in private chat"
                    );
                    (user_chat, None)
                }
                Err(err) => {
                    // User has not started the bot (or blocked it): guide and
                    // fall back to the current chat.
                    tracing::warn!(?err, user_id, "private chat probe failed, falling back");
                    let notice =
                        "先在私聊中 /start 本 bot 即可在私聊查看评论；此次在当前聊天展示。";
                    (callback.chat_id.to_string(), Some(notice.to_string()))
                }
            }
        }
        None => (callback.chat_id.to_string(), None),
    };

    let mut target = ViewerTarget {
        chat_id: &target_chat_id,
        message_id: viewer_message_id,
        last_sent_message_id: None,
    };

    let result = match &view {
        CommentsView::Page { tweet_id, page } => {
            render_comment_page(
                config,
                pool,
                client,
                bot_token,
                &mut target,
                tweet_id,
                *page,
            )
            .await
        }
        CommentsView::Replies {
            tweet_id,
            comment_id,
        } => {
            render_replies_view(
                config,
                pool,
                client,
                bot_token,
                &mut target,
                tweet_id,
                comment_id,
            )
            .await
        }
    };

    // Answer the callback query: with the fallback notice when present, so
    // the user always learns where the viewer opened.
    match &answered_notice {
        Some(notice) => {
            let _ = answer_callback_query_with_text(
                client,
                bot_token,
                callback.callback_query_id,
                Some(notice),
            )
            .await;
        }
        None => {
            let _ = answer_callback_query(client, bot_token, callback.callback_query_id).await;
        }
    }

    match &result {
        Ok((_text, _markup)) => {
            // First successful render: bind the new viewer message to the
            // pressed channel post so later presses edit in place.
            if viewer_message_id.is_none() {
                if let (Some(pressed), Some(sent)) =
                    (callback.pressed_message_id, target.last_sent_message_id)
                {
                    viewer_state_put(callback.chat_id, pressed, sent);
                }
            }
        }
        Err(err) => {
            tracing::warn!(?err, "comment viewer failed");
            // Anti-silence: surface the failure as an editable message update or
            // a plain message in the target chat.
            let text = format!("评论加载失败：{err}");
            if let Some(message_id) = viewer_message_id {
                let _ = edit_message_text(
                    client,
                    bot_token,
                    &target_chat_id,
                    message_id,
                    &text,
                    None,
                    None,
                )
                .await;
            } else {
                let _ = send_reply(client, bot_token, &target_chat_id, &text).await;
            }
        }
    }
}

type RenderResult = anyhow::Result<(String, telegram::InlineKeyboardMarkup)>;

async fn render_comment_page(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    target: &mut ViewerTarget<'_>,
    tweet_id: &str,
    page: usize,
) -> RenderResult {
    if !config.comments.enabled {
        return Ok((
            "Comment fetching is disabled.".to_string(),
            telegram::InlineKeyboardMarkup {
                inline_keyboard: vec![],
            },
        ));
    }

    let comments = fetch_comments_cached(config, pool, tweet_id).await?;
    if comments.is_empty() {
        return Ok((
            "No comments found.".to_string(),
            telegram::InlineKeyboardMarkup {
                inline_keyboard: vec![],
            },
        ));
    }

    let total = comments.len();
    let total_pages = total.div_ceil(COMMENTS_PER_PAGE).max(1);
    let start = (page - 1) * COMMENTS_PER_PAGE;
    if start >= total {
        return Ok((
            "No more comments.".to_string(),
            telegram::InlineKeyboardMarkup {
                inline_keyboard: vec![],
            },
        ));
    }
    let end = (start + COMMENTS_PER_PAGE).min(total);
    let page_comments = &comments[start..end];

    let author = storage::get_tweet(pool, tweet_id)
        .await
        .ok()
        .flatten()
        .map(|stored| (stored.tweet.author_name, stored.tweet.author_username));
    let range = format!("{}–{end}", start + 1);
    let header = match &author {
        Some((name, username)) => format!(
            "💬 <b>{}</b> (<a href=\"https://x.com/{username}\">@{username}</a>) 的评论 · 共 {total} 条 · 第 {page}/{total_pages} 页（{range}）",
            telegram::html_escape(name),
        ),
        None => format!(
            "💬 推文 {tweet_id} 的评论 · 共 {total} 条 · 第 {page}/{total_pages} 页（{range}）"
        ),
    };
    let header = format!("{header}\n🔗 <a href=\"https://x.com/i/status/{tweet_id}\">原推文</a>");
    let parent_username = author.as_ref().map(|(_, username)| username.as_str());
    let text = format_comment_list(&header, page_comments, start, parent_username);
    let markup = comment_page_keyboard(tweet_id, page, total_pages, page_comments);

    // First render sends a new (silent) message; paging edits in place.
    render_into(client, bot_token, target, &text, &markup).await;
    Ok((text, markup))
}

async fn render_replies_view(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    target: &mut ViewerTarget<'_>,
    tweet_id: &str,
    comment_id: &str,
) -> RenderResult {
    let replies = fetch_replies_cached(config, pool, comment_id).await?;
    let parent = cache_get_comments(tweet_id)
        .and_then(|list| list.into_iter().find(|c| c.tweet_id == comment_id));
    let parent_username = parent.as_ref().map(|c| c.author_username.clone());
    let link = format!("🔗 <a href=\"https://x.com/i/status/{comment_id}\">原评论</a>");
    let text = if replies.is_empty() {
        format!("该评论暂无已加载的回复。\n{link}")
    } else {
        let header = match &parent {
            Some(parent) => format!(
                "↩ <b>@{}</b> 的回复（{} 条）\n{link}",
                telegram::html_escape(&parent.author_username),
                replies.len()
            ),
            None => format!("↩ <b>回复</b>（{} 条）\n{link}", replies.len()),
        };
        format_comment_list(&header, &replies, 0, parent_username.as_deref())
    };
    let markup = replies_keyboard(tweet_id, comment_id);

    render_into(client, bot_token, target, &text, &markup).await;
    Ok((text, markup))
}

/// First render sends a new silent message; later renders edit in place.
async fn render_into(
    client: &reqwest::Client,
    bot_token: &str,
    target: &mut ViewerTarget<'_>,
    text: &str,
    markup: &telegram::InlineKeyboardMarkup,
) {
    match target.message_id {
        Some(message_id) => {
            let _ = edit_message_text(
                client,
                bot_token,
                target.chat_id,
                message_id,
                text,
                Some("HTML"),
                Some(markup),
            )
            .await;
        }
        None => {
            match send_viewer_message(client, bot_token, target.chat_id, text, Some(markup)).await {
                Ok(sent_id) => target.last_sent_message_id = Some(sent_id),
                Err(err) => {
                    tracing::error!(
                        ?err,
                        chat_id = %target.chat_id,
                        "failed to send viewer message"
                    )
                }
            }
        }
    }
}

async fn send_viewer_message(
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: &str,
    text: &str,
    reply_markup: Option<&telegram::InlineKeyboardMarkup>,
) -> anyhow::Result<i64> {
    #[derive(Debug, serde::Serialize)]
    struct SendMessagePayload {
        chat_id: String,
        text: String,
        parse_mode: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        message_thread_id: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_markup: Option<telegram::InlineKeyboardMarkup>,
        disable_notification: bool,
    }
    #[derive(Debug, serde::Deserialize)]
    struct MessageResult {
        message_id: i64,
    }
    #[derive(Debug, serde::Deserialize)]
    struct TelegramResponse {
        ok: bool,
        #[serde(default)]
        description: Option<String>,
        result: Option<MessageResult>,
    }

    let text = truncate_html(text, TELEGRAM_MESSAGE_MAX_CHARS);

    let response = client
        .post(telegram::telegram_api_url(bot_token, "sendMessage"))
        .json(&SendMessagePayload {
            chat_id: chat_id.to_string(),
            text,
            parse_mode: "HTML".to_string(),
            message_thread_id: None,
            reply_markup: reply_markup.cloned(),
            disable_notification: true,
        })
        .send()
        .await?;
    let status = response.status();
    let body: TelegramResponse = response.json().await.unwrap_or(TelegramResponse {
        ok: false,
        description: Some(format!("invalid response with status {status}")),
        result: None,
    });
    if !body.ok {
        anyhow::bail!(
            "sendMessage failed: {} - {}",
            status,
            body.description.unwrap_or_default()
        );
    }
    body.result
        .map(|r| r.message_id)
        .ok_or_else(|| anyhow::anyhow!("sendMessage returned no result"))
}

fn format_single_comment(
    comment: &crate::models::TweetComment,
    number: usize,
    parent_username: Option<&str>,
) -> String {
    let profile = format!("https://x.com/{}", comment.author_username);
    let permalink = format!("https://x.com/i/status/{}", comment.tweet_id);
    let time = crate::utils::format_utc8(&comment.created_at);

    let mut out = format!(
        "<b>{}. {}</b> <a href=\"{}\">@{}</a> · <a href=\"{}\">{}</a>",
        number,
        telegram::html_escape(&comment.author_name),
        telegram::html_escape(&profile),
        telegram::html_escape(&comment.author_username),
        telegram::html_escape(&permalink),
        time,
    );

    let body = clean_comment_body(
        &comment.text,
        parent_username,
        Some(comment.author_username.as_str()),
    );
    if !body.is_empty() {
        out.push_str("\n<blockquote>");
        out.push_str(&telegram::html_escape(&body));
        out.push_str("</blockquote>");
    }

    let mut attachments = Vec::new();
    for (idx, url) in comment.media_urls.iter().enumerate() {
        attachments.push(format!(
            "🖼 <a href=\"{}\">图{}</a>",
            telegram::html_escape(url),
            idx + 1
        ));
    }
    for url in &comment.external_links {
        attachments.push(format!(
            "🔗 <a href=\"{}\">{}</a>",
            telegram::html_escape(url),
            telegram::html_escape(&link_label(url))
        ));
    }
    if !attachments.is_empty() {
        out.push('\n');
        out.push_str(&attachments.join(" "));
    }
    out
}

/// Normalize a comment body for display: drop leading mentions of the thread
/// parent / the author itself (pure noise in reply threads), collapse runs of
/// blank lines and ellipsize over-long bodies.
fn clean_comment_body(
    text: &str,
    parent_username: Option<&str>,
    self_username: Option<&str>,
) -> String {
    let mut body = text.replace("\r\n", "\n").replace('\r', "\n");
    loop {
        let trimmed = body.trim_start();
        let Some(rest) = trimmed.strip_prefix('@') else {
            break;
        };
        let name_len = rest
            .char_indices()
            .find(|(_, ch)| !(ch.is_ascii_alphanumeric() || *ch == '_'))
            .map(|(idx, _)| idx)
            .unwrap_or(rest.len());
        let name = &rest[..name_len];
        let is_noise =
            |candidate: Option<&str>| candidate.is_some_and(|c| c.eq_ignore_ascii_case(name));
        if !is_noise(parent_username) && !is_noise(self_username) {
            break;
        }
        body = rest[name_len..].to_string();
    }

    let mut collapsed = String::with_capacity(body.len());
    let mut blank_run = 0usize;
    for line in body.split('\n') {
        if line.trim().is_empty() {
            blank_run += 1;
            if blank_run > 1 {
                continue;
            }
        } else {
            blank_run = 0;
        }
        if !collapsed.is_empty() {
            collapsed.push('\n');
        }
        collapsed.push_str(line.trim_end());
    }
    truncate_plain(collapsed.trim(), COMMENT_BODY_MAX_CHARS)
}

/// Short human label for an external link (host without scheme/www).
fn link_label(url: &str) -> String {
    let without_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = without_scheme.split('/').next().unwrap_or(without_scheme);
    let host = host.strip_prefix("www.").unwrap_or(host);
    if host.is_empty() {
        "link".to_string()
    } else {
        host.to_string()
    }
}

fn format_latest_message(
    tweets: &[crate::models::StoredTweet],
    username: &str,
    page: i64,
    total: i64,
    per_page: i64,
    can_load_older: bool,
) -> (String, Option<telegram::InlineKeyboardMarkup>) {
    let mut lines = vec![];
    if username.is_empty() {
        lines.push("Latest tweets:\n".to_string());
    } else {
        lines.push(format!("Latest tweets @{username}:\n"));
    }

    for stored in tweets {
        let time = crate::utils::format_utc8(&stored.tweet.created_at);
        lines.push(format!(
            "@{} [{}] {}\n{}",
            stored.tweet.author_username,
            time,
            stored.tweet.text.chars().take(100).collect::<String>(),
            stored.tweet.url,
        ));
    }

    let total_pages = ((total as f64) / (per_page as f64)).ceil() as i64;
    if total_pages > 1 || can_load_older {
        lines.push(format!("\nPage {page}/{}", total_pages.max(page)));
    }

    let text = lines.join("\n\n");

    // Build pagination buttons
    let mut buttons = Vec::new();
    let user_key = if username.is_empty() {
        "_all"
    } else {
        username
    };

    if page > 1 {
        buttons.push(telegram::InlineKeyboardButton::callback(
            "< Prev",
            format!("latest:{}:{}", user_key, page - 1),
        ));
    }
    if page < total_pages {
        buttons.push(telegram::InlineKeyboardButton::callback(
            "Next >",
            format!("latest:{}:{}", user_key, page + 1),
        ));
    } else if can_load_older && !username.is_empty() {
        // On last page with a specific user, offer to load older tweets
        buttons.push(telegram::InlineKeyboardButton::callback(
            "Load older",
            format!("latest_more:{}:{}", user_key, page),
        ));
    }

    let reply_markup = if buttons.is_empty() {
        None
    } else {
        Some(telegram::InlineKeyboardMarkup {
            inline_keyboard: vec![buttons],
        })
    };

    (text, reply_markup)
}

fn cmd_help() -> anyhow::Result<String> {
    Ok("xFlow Bot Commands:\n\n\
         /help - Show this help\n\
         /add @username - Add a source to monitor\n\
         /remove @username - Remove a source\n\
         /list - List all sources and status\n\
         /status - Show system status\n\
         /fetch - Trigger an immediate fetch\n\
         /latest @username [7d] - Browse tweets (auto-sync, optional time range)\n\
         /spam [list|add|remove] - Manage spam keywords\n\n\
         Channel posts have a \"💬 X 评论\" button: press it to lazily load\n\
         that tweet's comments from X (pages of 5; press \"↩ @user 的回复\"\n\
         to expand replies). The viewer opens in your private chat with the\n\
         bot — send /start to the bot first so it can message you."
        .to_string())
}

async fn cmd_add(config: &AppConfig, pool: &PgPool, args: &str) -> anyhow::Result<String> {
    let username = args.trim().trim_start_matches('@').to_string();
    if username.is_empty() {
        return Ok("Usage: /add @username".to_string());
    }
    if !fetch::validate_account(config, pool, &username).await {
        return Ok(format!(
            "@{username} not found on X, please check the username"
        ));
    }
    let source = Source {
        source_type: SourceType::Account,
        value: username.clone(),
        label: None,
        limit: None,
    };
    storage::upsert_source(pool, &source).await?;
    Ok(format!("Added source: @{username}"))
}

async fn cmd_remove(pool: &PgPool, args: &str) -> anyhow::Result<String> {
    let username = args.trim().trim_start_matches('@').to_string();
    if username.is_empty() {
        return Ok("Usage: /remove @username".to_string());
    }
    let deleted = storage::delete_source(pool, SourceType::Account, &username).await?;
    if deleted {
        Ok(format!("Removed source: @{username}"))
    } else {
        Ok(format!("Source @{username} not found"))
    }
}

async fn cmd_list(pool: &PgPool) -> anyhow::Result<String> {
    let sources = storage::list_sources(pool, false).await?;
    if sources.is_empty() {
        return Ok("No sources configured.".to_string());
    }

    // Get last fetch time per source
    let fetch_states = sqlx::query(
        "SELECT source_type, source_value, last_fetch_at, last_status FROM fetch_state",
    )
    .fetch_all(pool)
    .await?;

    use sqlx::Row;
    let mut state_map = std::collections::HashMap::new();
    for row in &fetch_states {
        let key = format!(
            "{}:{}",
            row.get::<String, _>("source_type"),
            row.get::<String, _>("source_value")
        );
        let last_at: Option<String> = row.get("last_fetch_at");
        let status: String = row.get("last_status");
        state_map.insert(key, (last_at, status));
    }

    let mut lines = vec!["Sources:\n".to_string()];
    for source in &sources {
        let icon = match source.source_type {
            SourceType::Account => "@",
            SourceType::List => "#",
            SourceType::Search => "?",
        };
        let key = format!("{}:{}", source.source_type.as_str(), source.value);
        let fetch_info = state_map.get(&key).map(|(lat, st)| {
            let time = crate::utils::format_db_timestamp(lat.as_deref());
            format!(" | {st} @ {time}")
        });
        lines.push(format!(
            "  {icon}{} (limit: {}){}",
            source.value,
            source
                .limit
                .map(|l| l.to_string())
                .unwrap_or_else(|| "5".to_string()),
            fetch_info.unwrap_or_default(),
        ));
    }
    Ok(lines.join("\n"))
}

async fn cmd_status(pool: &PgPool, config: &AppConfig) -> anyhow::Result<String> {
    let total_tweets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tweets")
        .fetch_one(pool)
        .await
        .unwrap_or(0);

    let sources = storage::list_sources(pool, true).await?;

    let last_fetch: Option<String> =
        sqlx::query_scalar("SELECT MAX(last_fetch_at) FROM fetch_state WHERE last_status = 'ok'")
            .fetch_optional(pool)
            .await?
            .flatten();

    let last_fetch_display = match &last_fetch {
        Some(s) => crate::utils::format_db_timestamp(Some(s)),
        None => "never".to_string(),
    };
    let mut status = format!(
        "xFlow Status\n\n\
         Tweets: {total_tweets}\n\
         Sources: {} (enabled)\n\
         Interval: {}s\n\
         Last fetch: {last_fetch_display}\n\
         Fetcher: {}\n",
        sources.len(),
        config.fetch.interval_seconds,
        config.fetch.fetcher,
    );

    // Recent fetch states
    let rows = sqlx::query(
        "SELECT source_type, source_value, last_fetch_at, last_status \
         FROM fetch_state ORDER BY last_fetch_at DESC LIMIT 5",
    )
    .fetch_all(pool)
    .await?;

    if !rows.is_empty() {
        use sqlx::Row;
        status.push_str("\nRecent fetches:\n");
        for row in &rows {
            let stype: String = row.get("source_type");
            let sval: String = row.get("source_value");
            let lst: String = row.get("last_status");
            let lat: Option<String> = row.get("last_fetch_at");
            status.push_str(&format!(
                "  {stype}:{sval} - {lst} ({})\n",
                crate::utils::format_db_timestamp(lat.as_deref())
            ));
        }
    }

    Ok(status)
}

async fn cmd_fetch(config: &AppConfig, pool: &PgPool) -> anyhow::Result<String> {
    tracing::info!("cmd_fetch: starting run_fetch");
    let fetch = pipeline::run_fetch(config, pool).await?;
    tracing::info!(
        fetched = fetch.fetched,
        sources = fetch.sources,
        "cmd_fetch: run_fetch done"
    );
    let channels = channel::configured_channels(config)?;
    tracing::info!("cmd_fetch: starting send_undelivered");
    let delivery =
        channel::send_undelivered(pool, &channels, 100, config.fetch.max_delivery_retries).await?;
    tracing::info!(
        sent = delivery.sent,
        failed = delivery.failed,
        "cmd_fetch: send_undelivered done"
    );

    let mut msg = format!(
        "Fetch complete:\n\
         Fetched: {} tweets from {} sources\n\
         Failed: {}\n",
        fetch.fetched, fetch.sources, fetch.failed,
    );

    if delivery.sent > 0 || delivery.failed > 0 {
        msg.push_str(&format!(
            "Delivered: {} sent, {} failed\n",
            delivery.sent, delivery.failed
        ));
    }

    if !fetch.errors.is_empty() {
        msg.push_str("\nErrors:\n");
        for err in &fetch.errors {
            msg.push_str(&format!(
                "  {}:{} - {}\n",
                err.source_type, err.source_value, err.message
            ));
        }
    }

    Ok(msg)
}

async fn cmd_latest(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: i64,
    args: &str,
) {
    // Parse args: "@username [time_range]" e.g. "@openai 7d" or "openai 30d"
    let parts: Vec<&str> = args.split_whitespace().collect();
    let username = parts
        .first()
        .map(|s| s.trim_start_matches('@').to_string())
        .unwrap_or_default();
    let time_range = parts.get(1).map(|s| s.to_string());

    // Parse time range if provided
    let since = match &time_range {
        Some(tr) => {
            match parse_bot_duration(tr) {
                Ok(d) => Some(d),
                Err(_) => {
                    let _ = send_reply(client, bot_token, &chat_id.to_string(),
                    "Invalid time range. Use e.g. `/latest @openai 7d` or `/latest @openai 12h`").await;
                    return;
                }
            }
        }
        None => None,
    };

    // When a username is specified with a time range, trigger backfill
    if !username.is_empty() {
        if let Some(since_dur) = &since {
            // Time range specified: backfill with since
            let _ = send_reply(
                client,
                bot_token,
                &chat_id.to_string(),
                &format!(
                    "Fetching tweets for @{username} from last {}...",
                    time_range.as_deref().unwrap_or("?")
                ),
            )
            .await;
            if let Err(err) =
                fetch::backfill_user(config, pool, &username, 0, 2, Some(*since_dur)).await
            {
                tracing::warn!(?err, username = %username, "backfill failed for /latest");
                let _ = send_reply(
                    client,
                    bot_token,
                    &chat_id.to_string(),
                    &format!("Failed to fetch: {err}"),
                )
                .await;
                return;
            }
        } else {
            // No time range: just fetch latest batch
            if let Err(err) = fetch_latest_for_user(config, pool, &username).await {
                tracing::warn!(?err, username = %username, "failed to fetch latest for /latest");
            }
        }
    }

    let per_page = config.fetch.default_limit.max(1);
    let filter = storage::TweetFilter {
        username: if username.is_empty() {
            None
        } else {
            Some(username.clone())
        },
        limit: per_page,
        ..Default::default()
    };

    let tweets = match storage::list_tweets(pool, filter.clone()).await {
        Ok(t) => t,
        Err(err) => {
            let _ = send_reply(
                client,
                bot_token,
                &chat_id.to_string(),
                &format!("Error: {err}"),
            )
            .await;
            return;
        }
    };

    if tweets.is_empty() {
        let msg = if username.is_empty() {
            "No tweets found.".to_string()
        } else {
            format!("No tweets found for @{username}.")
        };
        let _ = send_reply(client, bot_token, &chat_id.to_string(), &msg).await;
        return;
    }

    let total = match storage::count_tweets(pool, &filter).await {
        Ok(c) => c,
        Err(_) => tweets.len() as i64,
    };

    let (text, reply_markup) =
        format_latest_message(&tweets, &username, 1, total, per_page, !username.is_empty());

    if let Err(err) = send_reply_with_keyboard(
        client,
        bot_token,
        &chat_id.to_string(),
        &text,
        reply_markup.as_ref(),
    )
    .await
    {
        tracing::error!(?err, "failed to send latest reply");
    }
}

/// Parse a human duration string like "7d", "30d", "12h" into a chrono Duration.
fn parse_bot_duration(input: &str) -> anyhow::Result<chrono::Duration> {
    let input = input.trim();
    if input.is_empty() {
        anyhow::bail!("empty");
    }
    let (num_str, unit) = if let Some(stripped) = input.strip_suffix('d') {
        (stripped, 'd')
    } else if let Some(stripped) = input.strip_suffix('h') {
        (stripped, 'h')
    } else {
        anyhow::bail!("must end with 'd' or 'h'");
    };
    let value: i64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid number"))?;
    if value <= 0 {
        anyhow::bail!("must be positive");
    }
    match unit {
        'd' => Ok(chrono::Duration::days(value)),
        'h' => Ok(chrono::Duration::hours(value)),
        _ => unreachable!(),
    }
}

async fn fetch_latest_for_user(
    config: &AppConfig,
    pool: &PgPool,
    username: &str,
) -> anyhow::Result<()> {
    let source = Source {
        source_type: SourceType::Account,
        value: username.to_string(),
        label: None,
        limit: None,
    };
    let tweets = fetch::fetch_source(config, pool, &source).await?;
    for tweet in &tweets {
        storage::upsert_tweet(pool, tweet).await?;
    }
    tracing::info!(username = %username, count = tweets.len(), "fetched latest for /latest");
    Ok(())
}

async fn cmd_spam(pool: &PgPool, args: &str) -> anyhow::Result<String> {
    let (subcmd, keyword) = parse_command(args);
    match subcmd.as_str() {
        "list" => {
            let keywords = storage::list_spam_keywords(pool).await?;
            if keywords.is_empty() {
                Ok("No spam keywords configured.\nUse /spam add <keyword> to add one.".to_string())
            } else {
                let items: Vec<String> = keywords.iter().map(|k| format!("  - {}", k)).collect();
                Ok(format!("Spam keywords ({}):\n{}", keywords.len(), items.join("\n")))
            }
        }
        "add" => {
            if keyword.is_empty() {
                return Ok("Usage: /spam add <keyword>".to_string());
            }
            let success = storage::add_spam_keyword(pool, &keyword).await?;
            if success {
                Ok(format!("Added spam keyword: {}", keyword))
            } else {
                Ok(format!("Keyword already exists or is empty: {}", keyword))
            }
        }
        "remove" => {
            if keyword.is_empty() {
                return Ok("Usage: /spam remove <keyword>".to_string());
            }
            let success = storage::remove_spam_keyword(pool, &keyword).await?;
            if success {
                Ok(format!("Removed spam keyword: {}", keyword))
            } else {
                Ok(format!("Keyword not found: {}", keyword))
            }
        }
        _ => Ok(
            "Usage:\n  /spam list - List all spam keywords\n  /spam add <keyword> - Add a keyword\n  /spam remove <keyword> - Remove a keyword"
                .to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_command_basic() {
        let (cmd, args) = parse_command("/help");
        assert_eq!(cmd, "/help");
        assert_eq!(args, "");
    }

    #[test]
    fn parse_command_with_args() {
        let (cmd, args) = parse_command("/add @openai");
        assert_eq!(cmd, "/add");
        assert_eq!(args, "@openai");
    }

    #[test]
    fn parse_command_with_botname() {
        let (cmd, args) = parse_command("/help@xflow_bot");
        assert_eq!(cmd, "/help");
        assert_eq!(args, "");
    }

    #[test]
    fn parse_command_with_args_and_botname() {
        let (cmd, args) = parse_command("/add@xflow_bot @openai");
        assert_eq!(cmd, "/add");
        assert_eq!(args, "@openai");
    }

    fn make_comment(id: &str, reply_count: Option<u64>) -> crate::models::TweetComment {
        crate::models::TweetComment {
            tweet_id: id.to_string(),
            author_username: "alice".to_string(),
            author_name: "Alice".to_string(),
            text: "hello".to_string(),
            created_at: chrono::Utc::now(),
            media_urls: vec![],
            external_links: vec![],
            reply_count,
        }
    }

    #[test]
    fn parse_legacy_comments_data_with_page() {
        let view = parse_legacy_comments_data("123456:2").unwrap();
        assert_eq!(
            view,
            CommentsView::Page {
                tweet_id: "123456".to_string(),
                page: 2
            }
        );
    }

    #[test]
    fn parse_legacy_comments_data_defaults_to_page_one() {
        let view = parse_legacy_comments_data("123456").unwrap();
        assert_eq!(
            view,
            CommentsView::Page {
                tweet_id: "123456".to_string(),
                page: 1
            }
        );
    }

    #[test]
    fn parse_legacy_comments_data_rejects_empty_id() {
        assert!(parse_legacy_comments_data("").is_none());
        assert!(parse_legacy_comments_data(":3").is_none());
    }

    #[test]
    fn parse_comments_view_page() {
        let view = parse_comments_view("cv:999:3").unwrap();
        assert_eq!(
            view,
            CommentsView::Page {
                tweet_id: "999".to_string(),
                page: 3
            }
        );
    }

    #[test]
    fn parse_comments_view_replies() {
        let view = parse_comments_view("cv:999:r:42").unwrap();
        assert_eq!(
            view,
            CommentsView::Replies {
                tweet_id: "999".to_string(),
                comment_id: "42".to_string(),
            }
        );
    }

    #[test]
    fn parse_comments_view_back_treated_as_replies() {
        // Back buttons re-render the reply view; paging back to the list uses
        // cv:{tid}:{page} directly.
        let view = parse_comments_view("cv:999:b:42").unwrap();
        assert_eq!(
            view,
            CommentsView::Replies {
                tweet_id: "999".to_string(),
                comment_id: "42".to_string(),
            }
        );
    }

    #[test]
    fn comment_page_keyboard_pager_rows() {
        let comments: Vec<_> = (1..=5)
            .map(|i| make_comment(&i.to_string(), if i == 2 { Some(3) } else { None }))
            .collect();
        let markup = comment_page_keyboard("777", 2, 4, &comments);

        // One expand row (only comment 2 has replies) + one pager row.
        assert_eq!(markup.inline_keyboard.len(), 2);
        let expand = &markup.inline_keyboard[0][0];
        assert_eq!(expand.callback_data.as_deref(), Some("cv:777:r:2"));
        assert!(expand.text.contains('3'));

        let pager = &markup.inline_keyboard[1];
        assert_eq!(pager.len(), 3); // ◀ 2/4 ▶
        assert_eq!(pager[0].callback_data.as_deref(), Some("cv:777:1"));
        assert_eq!(pager[1].text, "2/4");
        assert_eq!(pager[2].callback_data.as_deref(), Some("cv:777:3"));
    }

    #[test]
    fn comment_page_keyboard_first_page_has_no_prev() {
        let comments: Vec<_> = (1..=5)
            .map(|i| make_comment(&i.to_string(), None))
            .collect();
        let markup = comment_page_keyboard("777", 1, 3, &comments);
        let pager = &markup.inline_keyboard[0];
        assert_eq!(pager.len(), 2); // 1/3 ▶
        assert_eq!(pager[0].text, "1/3");
    }

    #[test]
    fn comment_page_keyboard_last_page_has_no_next() {
        let comments: Vec<_> = (1..=5)
            .map(|i| make_comment(&i.to_string(), None))
            .collect();
        let markup = comment_page_keyboard("777", 3, 3, &comments);
        let pager = &markup.inline_keyboard[0];
        assert_eq!(pager.len(), 2); // ◀ 3/3
        assert_eq!(pager[1].text, "3/3");
    }

    #[test]
    fn format_comment_list_numbers_entries() {
        let comments: Vec<_> = (1..=2)
            .map(|i| make_comment(&i.to_string(), None))
            .collect();
        let text = format_comment_list("Header", &comments, 0, None);
        assert!(text.starts_with("Header"));
        assert!(text.contains("<b>1. Alice</b>"));
        assert!(text.contains("<b>2. Alice</b>"));
        assert!(text.contains("<blockquote>hello</blockquote>"));
        // Entries are separated by a blank line so bodies never blend.
        assert!(text.contains("</blockquote>\n\n<b>2."));
    }

    #[test]
    fn format_comment_list_numbering_continues_across_pages() {
        let comments: Vec<_> = (6..=7)
            .map(|i| make_comment(&i.to_string(), None))
            .collect();
        let text = format_comment_list("Header", &comments, 5, None);
        assert!(text.contains("<b>6. Alice</b>"));
        assert!(text.contains("<b>7. Alice</b>"));
    }

    fn make_comment_with_text(id: &str, text: &str) -> crate::models::TweetComment {
        let mut comment = make_comment(id, None);
        comment.text = text.to_string();
        comment
    }

    #[test]
    fn format_single_comment_links_author_and_permalink() {
        let comment = make_comment("42", None);
        let out = format_single_comment(&comment, 1, None);
        assert!(out.contains("<a href=\"https://x.com/alice\">@alice</a>"));
        assert!(out.contains("<a href=\"https://x.com/i/status/42\">"));
    }

    #[test]
    fn format_single_comment_strips_parent_mention() {
        let comment = make_comment_with_text("1", "@wufantouzi 屁股决定脑袋这事");
        let out = format_single_comment(&comment, 1, Some("wufantouzi"));
        assert!(out.contains("<blockquote>屁股决定脑袋这事</blockquote>"));
    }

    #[test]
    fn format_single_comment_keeps_unrelated_mentions() {
        let comment = make_comment_with_text("1", "@someone_else 看这里");
        let out = format_single_comment(&comment, 1, Some("wufantouzi"));
        assert!(out.contains("<blockquote>@someone_else 看这里</blockquote>"));
    }

    #[test]
    fn format_single_comment_multiline_body_stays_one_blockquote() {
        let comment = make_comment_with_text("1", "第一段\n\n第二段\n\n\n第三段");
        let out = format_single_comment(&comment, 1, None);
        assert!(out.contains("<blockquote>第一段\n\n第二段\n\n第三段</blockquote>"));
        assert_eq!(out.matches("<blockquote>").count(), 1);
    }

    #[test]
    fn format_single_comment_truncates_long_body() {
        let comment = make_comment_with_text("1", &"长".repeat(COMMENT_BODY_MAX_CHARS + 50));
        let out = format_single_comment(&comment, 1, None);
        assert!(out.ends_with("…</blockquote>"));
        assert_eq!(out.matches('…').count(), 1);
    }

    #[test]
    fn format_single_comment_attachments_line() {
        let mut comment = make_comment("1", None);
        comment.media_urls = vec!["https://pbs.example.com/a.jpg".to_string()];
        comment.external_links = vec!["https://www.example.com/path".to_string()];
        let out = format_single_comment(&comment, 1, None);
        assert!(out.contains("🖼 <a href=\"https://pbs.example.com/a.jpg\">图1</a>"));
        assert!(out.contains("🔗 <a href=\"https://www.example.com/path\">example.com</a>"));
    }

    #[test]
    fn truncate_plain_appends_ellipsis() {
        assert_eq!(truncate_plain(&"x".repeat(10), 5), "xxxx…");
        assert_eq!(truncate_plain("short", 5), "short");
    }

    #[test]
    fn truncate_html_closes_open_tags() {
        let text = format!(
            "<b>bold</b> <a href=\"https://x.com\">{}</a>",
            "字".repeat(5000)
        );
        let out = truncate_html(&text, 200);
        assert!(out.chars().count() <= 200);
        assert!(out.ends_with("</a>"));
        assert!(out.contains('…'));
    }

    #[test]
    fn truncate_html_does_not_split_entities() {
        let text = format!("{}&amp;{}", "a".repeat(40), "b".repeat(4000));
        let out = truncate_html(&text, 90);
        assert!(!out.contains('&'));
        assert!(out.ends_with('…'));
    }

    #[test]
    fn truncate_html_keeps_short_text() {
        let text = "<b>ok</b> &amp; fine";
        assert_eq!(truncate_html(text, 4096), text);
    }

    #[test]
    fn callback_data_fits_telegram_limit() {
        // Telegram caps callback_data at 64 bytes.
        let data = format!("cv:{}:r:{}", "12345678901234567890", "98765432109876543210");
        assert!(data.len() <= 64, "callback_data too long: {}", data.len());
    }
}
