use crate::channel;
use crate::config::AppConfig;
use crate::fetch;
use crate::models::{Source, SourceType};
use crate::worker::pipeline;
use crate::storage;
use crate::channel::telegram;
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
                            && !is_bot_addressed(&text, &message.entities, &message.reply_to_message, bot_username.as_deref())
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
                        handle_callback_query(
                            &config,
                            &pool,
                            &client,
                            &bot_token,
                            callback,
                        )
                        .await;
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
        allowed_updates: vec!["message".to_string(), "channel_post".to_string(), "callback_query".to_string()],
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
            tokio::spawn(async move {
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(120),
                    cmd_fetch(&config, &pool),
                ).await;
                let reply = match result {
                    Ok(Ok(msg)) => msg,
                    Ok(Err(err)) => {
                        tracing::error!(?err, "fetch command error");
                        format!("Error: {err}")
                    }
                    Err(_) => {
                        tracing::error!("fetch command timed out after 120s");
                        "Fetch timed out after 120 seconds.".to_string()
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
        "/digest" => cmd_digest(pool, config).await,
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
        format!("{}\n\n... (truncated)", &reply[..reply.floor_char_boundary(3900)])
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
        anyhow::bail!("sendMessage failed: {} - {}", status, body.description.unwrap_or_default());
    }
    Ok(())
}

async fn send_reply_get_id(
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: &str,
    text: &str,
) -> anyhow::Result<i64> {
    #[derive(Debug, serde::Serialize)]
    struct SendMessagePayload {
        chat_id: String,
        text: String,
        parse_mode: String,
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

    let text = if text.len() > 4096 {
        format!("{}\n...", &text[..text.floor_char_boundary(4090)])
    } else {
        text.to_string()
    };

    let response = client
        .post(telegram::telegram_api_url(bot_token, "sendMessage"))
        .json(&SendMessagePayload {
            chat_id: chat_id.to_string(),
            text,
            parse_mode: "HTML".to_string(),
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
        anyhow::bail!("sendMessage failed: {} - {}", status, body.description.unwrap_or_default());
    }
    body.result
        .map(|r| r.message_id)
        .ok_or_else(|| anyhow::anyhow!("sendMessage returned no result"))
}

// --- Callback query handling ---

async fn handle_callback_query(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    callback: TelegramCallbackQuery,
) {
    // Always answer the callback to remove the loading spinner
    let _ = answer_callback_query(client, bot_token, &callback.id).await;

    let Some(data) = &callback.data else { return };
    let Some(msg) = &callback.message else { return };

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

        handle_latest_more_callback(config, pool, client, bot_token, msg, &username, current_page).await;
    } else if let Some(rest) = data.strip_prefix("comments:") {
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        let tweet_id = parts[0];
        let page: usize = if parts.len() > 1 {
            parts[1].parse().unwrap_or(1).max(1)
        } else {
            1
        };

        handle_comments_callback(config, pool, client, bot_token, msg, tweet_id, page).await;
    }
}

async fn answer_callback_query(
    client: &reqwest::Client,
    bot_token: &str,
    callback_query_id: &str,
) -> anyhow::Result<()> {
    #[derive(Debug, serde::Serialize)]
    struct AnswerCallbackPayload {
        callback_query_id: String,
    }
    let response = client
        .post(telegram::telegram_api_url(bot_token, "answerCallbackQuery"))
        .json(&AnswerCallbackPayload {
            callback_query_id: callback_query_id.to_string(),
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
    reply_markup: Option<&telegram::InlineKeyboardMarkup>,
) -> anyhow::Result<()> {
    #[derive(Debug, serde::Serialize)]
    struct EditMessagePayload {
        chat_id: String,
        message_id: i64,
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_markup: Option<telegram::InlineKeyboardMarkup>,
    }

    let response = client
        .post(telegram::telegram_api_url(bot_token, "editMessageText"))
        .json(&EditMessagePayload {
            chat_id: chat_id.to_string(),
            message_id,
            text: text.to_string(),
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

    let text = if text.len() > 4096 {
        format!("{}\n...", &text[..text.floor_char_boundary(4090)])
    } else {
        text.to_string()
    };

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
        username: if username.is_empty() { None } else { Some(username.to_string()) },
        limit: per_page,
        offset: (page - 1) * per_page,
        ..Default::default()
    };

    let tweets = match storage::list_tweets(pool, filter.clone()).await {
        Ok(t) => t,
        Err(err) => {
            tracing::error!(?err, "failed to list tweets for latest callback");
            return;
        }
    };

    if tweets.is_empty() {
        let _ = edit_message_text(client, bot_token, &msg.chat.id.to_string(), msg.message_id, "No more tweets.", None).await;
        return;
    }

    let total = match storage::count_tweets(pool, &filter).await {
        Ok(c) => c,
        Err(_) => (page - 1) * per_page + tweets.len() as i64,
    };

    let can_load_older = !username.is_empty();
    let (text, reply_markup) = format_latest_message(&tweets, username, page, total, per_page, can_load_older);

    if let Err(err) = edit_message_text(client, bot_token, &msg.chat.id.to_string(), msg.message_id, &text, reply_markup.as_ref()).await {
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
        client, bot_token, &msg.chat.id.to_string(), msg.message_id,
        &format!("Loading older tweets for @{username}..."), None,
    ).await;

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
                client, bot_token, &msg.chat.id.to_string(), msg.message_id,
                &format!("Failed to load older tweets: {err}"), None,
            ).await;
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
        ..Default::default()
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
            client, bot_token, &msg.chat.id.to_string(), msg.message_id,
            "No older tweets found.", None,
        ).await;
        return;
    }

    let total = match storage::count_tweets(pool, &filter).await {
        Ok(c) => c,
        Err(_) => (next_page - 1) * per_page + tweets.len() as i64,
    };

    let (text, reply_markup) = format_latest_message(&tweets, username, next_page, total, per_page, true);

    if let Err(err) = edit_message_text(client, bot_token, &msg.chat.id.to_string(), msg.message_id, &text, reply_markup.as_ref()).await {
        tracing::error!(?err, "failed to edit latest message after backfill");
    }
}

async fn send_reply_to_message(
    client: &reqwest::Client,
    bot_token: &str,
    chat_id: &str,
    reply_to_message_id: i64,
    text: &str,
) -> anyhow::Result<()> {
    #[derive(Debug, serde::Serialize)]
    struct ReplyParams {
        message_id: i64,
    }
    #[derive(Debug, serde::Serialize)]
    struct SendMessageWithReply {
        chat_id: String,
        text: String,
        parse_mode: String,
        reply_parameters: ReplyParams,
    }

    let text = if text.len() > 4096 {
        format!("{}\n...", &text[..text.floor_char_boundary(4090)])
    } else {
        text.to_string()
    };

    let response = client
        .post(telegram::telegram_api_url(bot_token, "sendMessage"))
        .json(&SendMessageWithReply {
            chat_id: chat_id.to_string(),
            text,
            parse_mode: "HTML".to_string(),
            reply_parameters: ReplyParams {
                message_id: reply_to_message_id,
            },
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

async fn handle_comments_callback(
    config: &AppConfig,
    pool: &PgPool,
    client: &reqwest::Client,
    bot_token: &str,
    msg: &TelegramCallbackMessage,
    tweet_id: &str,
    page: usize,
) {
    if !config.comments.enabled {
        let _ = send_reply(client, bot_token, &msg.chat.id.to_string(), "Comment fetching is disabled.").await;
        return;
    }

    let is_channel = msg.chat.chat_type == "channel";
    let discussion_group_id = std::env::var(&config.telegram.discussion_group_id_env).ok();
    let is_discussion_group = discussion_group_id
        .as_ref()
        .map(|gid| msg.chat.id.to_string() == *gid)
        .unwrap_or(false);

    // Route: channel -> send to group with context header; group pagination -> continue in group; else inline
    let (send_chat_id, use_context_header) = if is_channel {
        if let Some(ref group_id) = discussion_group_id {
            (group_id.clone(), true)
        } else {
            (msg.chat.id.to_string(), false)
        }
    } else if is_discussion_group {
        (msg.chat.id.to_string(), false)
    } else {
        (msg.chat.id.to_string(), false)
    };

    tracing::info!(tweet_id = %tweet_id, page, is_channel, is_discussion_group, send_chat_id = %send_chat_id, "fetching comments on demand");

    match fetch::fetch_tweet_comments(
        config,
        pool,
        tweet_id,
        config.comments.max_comments,
    )
    .await
    {
        Ok(comments) => {
            if comments.is_empty() {
                let _ = send_reply(client, bot_token, &send_chat_id, "No comments found.").await;
                return;
            }

            let per_page = 5;
            let total = comments.len();
            let start = (page - 1) * per_page;
            if start >= total {
                return;
            }
            let end = (start + per_page).min(total);
            let page_comments = &comments[start..end];
            let has_more = end < total;

            // For channel posts: send context header to group, thread comments under it
            let reply_to_id = if use_context_header {
                let header = match storage::get_tweet(pool, tweet_id).await {
                    Ok(Some(stored)) => format!(
                        "<b>Comments</b> for <b>{}</b> (@{})\n{}",
                        telegram::html_escape(&stored.tweet.author_name),
                        telegram::html_escape(&stored.tweet.author_username),
                        stored.tweet.url,
                    ),
                    _ => format!("Comments for tweet https://x.com/i/status/{tweet_id}"),
                };
                match send_reply_get_id(client, bot_token, &send_chat_id, &header).await {
                    Ok(id) => Some(id),
                    Err(err) => {
                        tracing::warn!(?err, "failed to send comment header to group");
                        None
                    }
                }
            } else {
                Some(msg.message_id)
            };

            for comment in page_comments {
                let text = format_single_comment(comment);
                if let Some(id) = reply_to_id {
                    let _ = send_reply_to_message(
                        client, bot_token, &send_chat_id, id, &text,
                    ).await;
                } else {
                    let _ = send_reply(client, bot_token, &send_chat_id, &text).await;
                }
            }

            if has_more {
                let callback_data = format!("comments:{tweet_id}:{}", page + 1);
                let _ = send_reply_with_keyboard(
                    client,
                    bot_token,
                    &send_chat_id,
                    &format!("Page {}/{} — Load more:", page, total.div_ceil(per_page)),
                    Some(&telegram::comment_button_markup_with_text("Next page >", &callback_data)),
                ).await;
            }
        }
        Err(err) => {
            tracing::error!(?err, tweet_id = %tweet_id, "failed to fetch comments");
            let _ = send_reply(
                client, bot_token, &send_chat_id,
                &format!("Failed to fetch comments: {err}"),
            ).await;
        }
    }
}

fn format_single_comment(comment: &crate::models::TweetComment) -> String {
    let mut parts = vec![format!(
        "<b>{}</b> (@{}): {}",
        telegram::html_escape(&comment.author_name),
        telegram::html_escape(&comment.author_username),
        telegram::html_escape(&comment.text),
    )];

    for url in &comment.media_urls {
        parts.push(format!("\n  <a href=\"{}\">[image]</a>", url));
    }
    for url in &comment.external_links {
        parts.push(format!("\n  <a href=\"{}\">{}</a>", url, url));
    }

    parts.join("")
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
    let user_key = if username.is_empty() { "_all" } else { username };

    if page > 1 {
        buttons.push(telegram::InlineKeyboardButton {
            text: "< Prev".to_string(),
            callback_data: format!("latest:{}:{}", user_key, page - 1),
        });
    }
    if page < total_pages {
        buttons.push(telegram::InlineKeyboardButton {
            text: "Next >".to_string(),
            callback_data: format!("latest:{}:{}", user_key, page + 1),
        });
    } else if can_load_older && !username.is_empty() {
        // On last page with a specific user, offer to load older tweets
        buttons.push(telegram::InlineKeyboardButton {
            text: "Load older".to_string(),
            callback_data: format!("latest_more:{}:{}", user_key, page),
        });
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
    Ok(
        "xFlow Bot Commands:\n\n\
         /help - Show this help\n\
         /add @username - Add a source to monitor\n\
         /remove @username - Remove a source\n\
         /list - List all sources and status\n\
         /status - Show system status\n\
         /fetch - Trigger an immediate fetch\n\
         /latest @username [7d] - Browse tweets (auto-sync, optional time range)\n\
         /digest - Show analyzed digest summary\n\
         /spam [list|add|remove] - Manage spam keywords"
            .to_string(),
    )
}

async fn cmd_add(config: &AppConfig, pool: &PgPool, args: &str) -> anyhow::Result<String> {
    let username = args.trim().trim_start_matches('@').to_string();
    if username.is_empty() {
        return Ok("Usage: /add @username".to_string());
    }
    if !fetch::validate_account(config, pool, &username).await {
        return Ok(format!("@{username} not found on X, please check the username"));
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
            source.limit.map(|l| l.to_string()).unwrap_or_else(|| "5".to_string()),
            fetch_info.unwrap_or_default(),
        ));
    }
    Ok(lines.join("\n"))
}

async fn cmd_status(pool: &PgPool, config: &AppConfig) -> anyhow::Result<String> {
    let total_tweets: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tweets")
            .fetch_one(pool)
            .await
            .unwrap_or(0);

    let sources = storage::list_sources(pool, true).await?;

    let last_fetch: Option<String> = sqlx::query_scalar(
        "SELECT MAX(last_fetch_at) FROM fetch_state WHERE last_status = 'ok'",
    )
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
    tracing::info!(fetched = fetch.fetched, sources = fetch.sources, "cmd_fetch: run_fetch done");
    let channels = channel::configured_channels(config)?;
    tracing::info!("cmd_fetch: starting send_undelivered");
    let delivery = channel::send_undelivered(pool, &channels, 100).await?;
    tracing::info!(sent = delivery.sent, failed = delivery.failed, "cmd_fetch: send_undelivered done");

    let mut msg = format!(
        "Fetch complete:\n\
         Fetched: {} tweets from {} sources\n\
         Analyzed: {}\n\
         Failed: {}\n",
        fetch.fetched, fetch.sources, fetch.analyzed, fetch.failed,
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
    let parts: Vec<&str> = args.trim().split_whitespace().collect();
    let username = parts.first()
        .map(|s| s.trim_start_matches('@').to_string())
        .unwrap_or_default();
    let time_range = parts.get(1).map(|s| s.to_string());

    // Parse time range if provided
    let since = match &time_range {
        Some(tr) => match parse_bot_duration(tr) {
            Ok(d) => Some(d),
            Err(_) => {
                let _ = send_reply(client, bot_token, &chat_id.to_string(),
                    "Invalid time range. Use e.g. `/latest @openai 7d` or `/latest @openai 12h`").await;
                return;
            }
        },
        None => None,
    };

    // When a username is specified with a time range, trigger backfill
    if !username.is_empty() {
        if let Some(since_dur) = &since {
            // Time range specified: backfill with since
            let _ = send_reply(client, bot_token, &chat_id.to_string(),
                &format!("Fetching tweets for @{username} from last {}...", time_range.as_deref().unwrap_or("?"))).await;
            if let Err(err) = fetch::backfill_user(config, pool, &username, 0, 2, Some(*since_dur)).await {
                tracing::warn!(?err, username = %username, "backfill failed for /latest");
                let _ = send_reply(client, bot_token, &chat_id.to_string(),
                    &format!("Failed to fetch: {err}")).await;
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
        username: if username.is_empty() { None } else { Some(username.clone()) },
        limit: per_page,
        ..Default::default()
    };

    let tweets = match storage::list_tweets(pool, filter.clone()).await {
        Ok(t) => t,
        Err(err) => {
            let _ = send_reply(client, bot_token, &chat_id.to_string(), &format!("Error: {err}")).await;
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

    let (text, reply_markup) = format_latest_message(&tweets, &username, 1, total, per_page, !username.is_empty());

    if let Err(err) = send_reply_with_keyboard(client, bot_token, &chat_id.to_string(), &text, reply_markup.as_ref()).await {
        tracing::error!(?err, "failed to send latest reply");
    }
}

/// Parse a human duration string like "7d", "30d", "12h" into a chrono Duration.
fn parse_bot_duration(input: &str) -> anyhow::Result<chrono::Duration> {
    let input = input.trim();
    if input.is_empty() {
        anyhow::bail!("empty");
    }
    let (num_str, unit) = if input.ends_with('d') {
        (&input[..input.len() - 1], 'd')
    } else if input.ends_with('h') {
        (&input[..input.len() - 1], 'h')
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

async fn fetch_latest_for_user(config: &AppConfig, pool: &PgPool, username: &str) -> anyhow::Result<()> {
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

async fn cmd_digest(pool: &PgPool, config: &AppConfig) -> anyhow::Result<String> {
    if !config.agent.enabled {
        return Ok("Digest requires agent analysis to be enabled.".to_string());
    }
    let tweets =
        storage::list_analyzed_for_digest(pool, config.agent.importance_threshold, 20).await?;
    if tweets.is_empty() {
        return Ok("No analyzed tweets found for digest.".to_string());
    }
    let mut lines = vec!["xFlow Digest:\n".to_string()];
    let mut current_category = String::new();
    for stored in &tweets {
        let Some(ref analysis) = stored.analysis else {
            continue;
        };
        if analysis.category != current_category {
            current_category = analysis.category.clone();
            lines.push(format!("\n[{}]", current_category));
        }
        let time = crate::utils::format_utc8(&stored.tweet.created_at);
        lines.push(format!(
            "  @{}/{} | {}\n  {}",
            stored.tweet.author_username,
            time,
            analysis.chinese_summary.chars().take(80).collect::<String>(),
            stored.tweet.url,
        ));
    }
    Ok(lines.join("\n"))
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
}
