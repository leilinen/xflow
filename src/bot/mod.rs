use crate::channel;
use crate::config::AppConfig;
use crate::fetch;
use crate::models::{Source, SourceType};
use crate::worker::pipeline;
use crate::storage;
use crate::channel::telegram;
use serde::Deserialize;
use sqlx::SqlitePool;

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Debug, Deserialize)]
struct TelegramMessage {
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

fn is_group_chat(chat: &TelegramChat) -> bool {
    chat.chat_type == "group" || chat.chat_type == "supergroup"
}

pub async fn run_poller(config: AppConfig, pool: SqlitePool) -> anyhow::Result<()> {
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
                    if let Some(message) = update.message {
                        let is_group = is_group_chat(&message.chat);

                        // In private chat: check authorization
                        if !is_group {
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
/// 2. The message starts with @bot_username mention before the command
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
        allowed_updates: vec!["message".to_string()],
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
    pool: &SqlitePool,
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
        "/fetch" => cmd_fetch(config, pool).await,
        "/latest" => cmd_latest(pool, &args).await,
        "/digest" => cmd_digest(pool, config).await,
        _ => return,
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

    if let Err(err) = send_reply(client, bot_token, &chat_id_str, &reply).await {
        tracing::error!(?err, "failed to send bot reply");
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

    let response = client
        .post(telegram::telegram_api_url(bot_token, "sendMessage"))
        .json(&SendMessagePayload {
            chat_id: chat_id.to_string(),
            text: text.to_string(),
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

fn cmd_help() -> anyhow::Result<String> {
    Ok(
        "xFlow Bot Commands:\n\n\
         /help - Show this help\n\
         /add @username - Add a source to monitor\n\
         /remove @username - Remove a source\n\
         /list - List all sources and status\n\
         /status - Show system status\n\
         /fetch - Trigger an immediate fetch\n\
         /latest [@user] - Show recent tweets (default 5)\n\
         /digest - Show analyzed digest summary"
            .to_string(),
    )
}

async fn cmd_add(config: &AppConfig, pool: &SqlitePool, args: &str) -> anyhow::Result<String> {
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

async fn cmd_remove(pool: &SqlitePool, args: &str) -> anyhow::Result<String> {
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

async fn cmd_list(pool: &SqlitePool) -> anyhow::Result<String> {
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

async fn cmd_status(pool: &SqlitePool, config: &AppConfig) -> anyhow::Result<String> {
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

async fn cmd_fetch(config: &AppConfig, pool: &SqlitePool) -> anyhow::Result<String> {
    let fetch = pipeline::run_fetch(config, pool).await?;
    let channels = channel::configured_channels(config)?;
    let delivery = channel::send_undelivered(pool, &channels, 100).await?;

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

async fn cmd_latest(pool: &SqlitePool, args: &str) -> anyhow::Result<String> {
    let username = args.trim().trim_start_matches('@').to_string();
    let filter = if username.is_empty() {
        storage::TweetFilter {
            limit: 5,
            ..Default::default()
        }
    } else {
        storage::TweetFilter {
            username: Some(username.clone()),
            limit: 5,
            ..Default::default()
        }
    };
    let tweets = storage::list_tweets(pool, filter).await?;
    if tweets.is_empty() {
        return Ok(if username.is_empty() {
            "No tweets found.".to_string()
        } else {
            format!("No tweets found for @{username}.")
        });
    }
    let mut lines = vec!["Latest tweets:\n".to_string()];
    for stored in &tweets {
        let time = crate::utils::format_utc8(&stored.tweet.created_at);
        lines.push(format!(
            "@{} [{}] {}\n{}",
            stored.tweet.author_username,
            time,
            stored.tweet.text.chars().take(100).collect::<String>(),
            stored.tweet.url,
        ));
    }
    Ok(lines.join("\n\n"))
}

async fn cmd_digest(pool: &SqlitePool, config: &AppConfig) -> anyhow::Result<String> {
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
