use crate::channel;
use crate::config::AppConfig;
use crate::models::{Source, SourceType};
use crate::pipeline;
use crate::storage;
use crate::telegram;
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
}

#[derive(Debug, Deserialize)]
struct TelegramChat {
    id: i64,
}

pub async fn run_poller(config: AppConfig, pool: SqlitePool) -> anyhow::Result<()> {
    let bot_token = telegram::load_bot_token(&config.telegram)?;
    let allowed_chat_id = std::env::var(&config.telegram.chat_id_env).ok();
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
                        if let Some(ref allowed) = allowed_chat_id {
                            if message.chat.id.to_string() != *allowed {
                                tracing::warn!(
                                    chat_id = message.chat.id,
                                    "ignoring message from unauthorized chat"
                                );
                                continue;
                            }
                        }
                        if let Some(text) = message.text {
                            handle_command(
                                &config,
                                &pool,
                                &client,
                                &bot_token,
                                message.chat.id,
                                &text,
                            )
                            .await;
                        }
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
) {
    let (command, args) = parse_command(text);
    let chat_id_str = chat_id.to_string();

    let response = match command.as_str() {
        "/help" | "/start" => cmd_help(),
        "/add" => cmd_add(pool, &args).await,
        "/remove" => cmd_remove(pool, &args).await,
        "/list" => cmd_list(pool).await,
        "/status" => cmd_status(pool, config).await,
        "/fetch" => cmd_fetch(config, pool).await,
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
         /fetch - Trigger an immediate fetch"
            .to_string(),
    )
}

async fn cmd_add(pool: &SqlitePool, args: &str) -> anyhow::Result<String> {
    let username = args.trim().trim_start_matches('@').to_string();
    if username.is_empty() {
        return Ok("Usage: /add @username".to_string());
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
            let time = lat
                .as_deref()
                .unwrap_or("-")
                .get(..19)
                .unwrap_or("-");
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

    let mut status = format!(
        "xFlow Status\n\n\
         Tweets: {total_tweets}\n\
         Sources: {} (enabled)\n\
         Interval: {}s\n\
         Last fetch: {}\n\
         Fetcher: {}\n",
        sources.len(),
        config.fetch.interval_seconds,
        last_fetch.as_deref().unwrap_or("never"),
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
                lat.as_deref().unwrap_or("-")
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
