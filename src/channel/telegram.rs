use super::{
    ChannelDeliveryResult, ChannelSendFuture, ChannelSendReceipt, DeliveryChannel,
};
use crate::config::TelegramConfig;
use crate::models::StoredTweet;
use crate::worker::pipeline::FetchSourceError;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct TelegramCredentials {
    pub bot_token: String,
    pub chat_id: String,
}

impl TelegramCredentials {
    pub fn channel(&self) -> String {
        format!("telegram:{}", self.chat_id)
    }
}

#[derive(Debug, Clone, Serialize)]
struct SendMessagePayload {
    chat_id: String,
    text: String,
    parse_mode: Option<String>,
    disable_web_page_preview: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct TelegramApiResponse<T> {
    ok: bool,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    result: Option<T>,
}

pub type TelegramResult = ChannelDeliveryResult;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TelegramBotCommand {
    pub command: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct TelegramChannel {
    credentials: TelegramCredentials,
    send_all: bool,
    parse_mode: String,
    disable_web_page_preview: bool,
    client: Client,
}

impl TelegramChannel {
    pub fn from_config(config: &TelegramConfig) -> anyhow::Result<Self> {
        Ok(Self {
            credentials: load_credentials(config)?,
            send_all: config.send_all,
            parse_mode: config.parse_mode.clone(),
            disable_web_page_preview: config.disable_web_page_preview,
            client: Client::new(),
        })
    }
}

pub fn load_credentials(config: &TelegramConfig) -> anyhow::Result<TelegramCredentials> {
    let bot_token = load_bot_token(config)?;
    let chat_id = std::env::var(&config.chat_id_env)
        .map_err(|_| anyhow::anyhow!("missing Telegram chat id env var: {}", config.chat_id_env))?;
    Ok(TelegramCredentials { bot_token, chat_id })
}

pub fn load_bot_token(config: &TelegramConfig) -> anyhow::Result<String> {
    let bot_token = std::env::var(&config.bot_token_env).map_err(|_| {
        anyhow::anyhow!(
            "missing Telegram bot token env var: {}",
            config.bot_token_env
        )
    })?;
    Ok(bot_token)
}

pub fn default_bot_commands() -> Vec<TelegramBotCommand> {
    vec![
        TelegramBotCommand {
            command: "help".to_string(),
            description: "Show available commands".to_string(),
        },
        TelegramBotCommand {
            command: "add".to_string(),
            description: "Add a source (e.g. /add @openai)".to_string(),
        },
        TelegramBotCommand {
            command: "remove".to_string(),
            description: "Remove a source".to_string(),
        },
        TelegramBotCommand {
            command: "list".to_string(),
            description: "List all sources".to_string(),
        },
        TelegramBotCommand {
            command: "status".to_string(),
            description: "Show system status".to_string(),
        },
        TelegramBotCommand {
            command: "fetch".to_string(),
            description: "Trigger immediate fetch".to_string(),
        },
        TelegramBotCommand {
            command: "latest".to_string(),
            description: "Show recent tweets (e.g. /latest @openai)".to_string(),
        },
        TelegramBotCommand {
            command: "digest".to_string(),
            description: "Show analyzed digest summary".to_string(),
        },
    ]
}

pub async fn set_bot_commands(config: &TelegramConfig) -> anyhow::Result<Vec<TelegramBotCommand>> {
    let commands = default_bot_commands();
    set_bot_commands_to(config, commands.clone()).await?;
    Ok(commands)
}

pub async fn clear_bot_commands(config: &TelegramConfig) -> anyhow::Result<()> {
    set_bot_commands_to(config, Vec::new()).await
}

pub async fn list_bot_commands(config: &TelegramConfig) -> anyhow::Result<Vec<TelegramBotCommand>> {
    let bot_token = load_bot_token(config)?;
    get_telegram_json::<Vec<TelegramBotCommand>>(&Client::new(), &bot_token, "getMyCommands").await
}

async fn set_bot_commands_to(
    config: &TelegramConfig,
    commands: Vec<TelegramBotCommand>,
) -> anyhow::Result<()> {
    #[derive(Debug, Serialize)]
    struct SetMyCommandsPayload {
        commands: Vec<TelegramBotCommand>,
    }

    let bot_token = load_bot_token(config)?;
    post_telegram_json::<_, bool>(
        &Client::new(),
        &bot_token,
        "setMyCommands",
        &SetMyCommandsPayload { commands },
    )
    .await?;
    Ok(())
}

const TELEGRAM_MESSAGE_LIMIT: usize = 4096;
const TRUNCATION_MARKER: &str = "\n\n…";
const MAX_RETRIES: u32 = 2;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

enum TelegramSendError {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

pub fn format_tweet_message(stored: &StoredTweet) -> String {
    let mut parts = vec![
        format!(
            "<b>@{}</b> · {} UTC+8",
            html_escape(&stored.tweet.author_username),
            crate::utils::format_utc8(&stored.tweet.created_at)
        ),
        html_escape(&stored.tweet.text),
    ];
    if let Some(analysis) = &stored.analysis {
        if analysis.chinese_summary != stored.tweet.text {
            parts.push(format!("<i>{}</i>", html_escape(&analysis.chinese_summary)));
        }
        if !analysis.tags.is_empty() {
            parts.push(format!("Tags: {}", html_escape(&analysis.tags.join(", "))));
        }
    }
    let footer = format!(
        "<a href=\"{}\">Open tweet</a>",
        html_escape(&stored.tweet.url)
    );
    parts.push(footer);
    let message = parts.join("\n\n");
    if message.len() <= TELEGRAM_MESSAGE_LIMIT {
        return message;
    }
    truncate_message(parts, TELEGRAM_MESSAGE_LIMIT)
}

/// Truncate by removing middle parts (summary, tags) before the footer,
/// then truncating the tweet text if still too long.
fn truncate_message(mut parts: Vec<String>, limit: usize) -> String {
    // Footer is always the last part and must be preserved.
    let footer = parts.pop().unwrap_or_default();
    // Build the skeleton: header + text + marker + footer
    // Drop middle parts (summary, tags).
    let header = parts.first().cloned().unwrap_or_default();
    let mut text = parts.get(1).cloned().unwrap_or_default();
    let marker = TRUNCATION_MARKER.to_string();
    let sep = "\n\n";
    // Calculate overhead: header + sep + marker + sep + footer + sep
    let overhead = header.len() + sep.len() + marker.len() + sep.len() + footer.len() + sep.len();
    let budget = limit.saturating_sub(overhead);
    if text.len() > budget {
        text.truncate(text.floor_char_boundary(budget));
        // Avoid splitting inside an HTML entity.
        if let Some(pos) = text.rfind('&') {
            if !text[pos..].contains(';') {
                text.truncate(pos);
            }
        }
    }
    [header, text, marker, footer].join(sep)
}

impl DeliveryChannel for TelegramChannel {
    fn id(&self) -> String {
        self.credentials.channel()
    }

    fn send_all(&self) -> bool {
        self.send_all
    }

    fn send_tweet<'a>(&'a self, tweet: &'a StoredTweet) -> ChannelSendFuture<'a> {
        Box::pin(async move {
            let payload = SendMessagePayload {
                chat_id: self.credentials.chat_id.clone(),
                text: format_tweet_message(tweet),
                parse_mode: if self.parse_mode.is_empty() {
                    None
                } else {
                    Some(self.parse_mode.clone())
                },
                disable_web_page_preview: self.disable_web_page_preview,
            };
            let mut last_err = None;
            for attempt in 0..=MAX_RETRIES {
                if attempt > 0 {
                    tokio::time::sleep(RETRY_DELAY * attempt).await;
                }
                match self.try_send(&payload).await {
                    Ok(receipt) => return Ok(receipt),
                    Err(TelegramSendError::Transient(err)) => {
                        tracing::warn!(
                            attempt,
                            tweet_id = %tweet.tweet.tweet_id,
                            "Telegram transient error, will retry: {err}"
                        );
                        last_err = Some(err);
                    }
                    Err(TelegramSendError::Permanent(err)) => return Err(err),
                }
            }
            Err(last_err.unwrap_or_else(|| anyhow::anyhow!("max retries exceeded")))
        })
    }
}

impl TelegramChannel {
    async fn try_send(
        &self,
        payload: &SendMessagePayload,
    ) -> Result<ChannelSendReceipt, TelegramSendError> {
        let response = self
            .client
            .post(telegram_api_url(&self.credentials.bot_token, "sendMessage"))
            .json(payload)
            .send()
            .await;
        let response = match response {
            Ok(r) => r,
            Err(err) if is_transient_reqwest_error(&err) => {
                return Err(TelegramSendError::Transient(anyhow::anyhow!(
                    "Telegram connection error: {err}"
                )))
            }
            Err(err) => {
                return Err(TelegramSendError::Permanent(anyhow::anyhow!(
                    "Telegram request failed: {err}"
                )))
            }
        };
        let status = response.status();
        if status.is_server_error() || status.as_u16() == 429 {
            return Err(TelegramSendError::Transient(anyhow::anyhow!(
                "Telegram returned HTTP {status}"
            )));
        }
        let body = response
            .json::<TelegramApiResponse<serde_json::Value>>()
            .await
            .unwrap_or(TelegramApiResponse {
                ok: false,
                description: Some(format!("invalid Telegram response with status {status}")),
                result: None,
            });
        if status.is_success() && body.ok {
            Ok(ChannelSendReceipt {
                payload: serde_json::to_value(body).unwrap_or_default(),
            })
        } else {
            Err(TelegramSendError::Permanent(anyhow::anyhow!(
                "{}",
                serde_json::json!({"request": payload, "response": body})
            )))
        }
    }
}

fn is_transient_reqwest_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout() || err.is_request()
}

pub async fn send_undelivered(
    pool: &SqlitePool,
    config: &TelegramConfig,
    limit: i64,
) -> anyhow::Result<TelegramResult> {
    if !config.enabled {
        return Ok(TelegramResult {
            sent: 0,
            failed: 0,
            skipped: 0,
        });
    }
    super::send_undelivered(
        pool,
        &[Box::new(TelegramChannel::from_config(config)?)],
        limit,
    )
    .await
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn get_telegram_json<T: DeserializeOwned>(
    client: &Client,
    bot_token: &str,
    method: &str,
) -> anyhow::Result<T> {
    let response = client
        .get(telegram_api_url(bot_token, method))
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("Telegram API request failed for {method}"))?;
    parse_telegram_response(response, method).await
}

async fn post_telegram_json<TBody: Serialize + ?Sized, TResponse: DeserializeOwned>(
    client: &Client,
    bot_token: &str,
    method: &str,
    payload: &TBody,
) -> anyhow::Result<TResponse> {
    let response = client
        .post(telegram_api_url(bot_token, method))
        .json(payload)
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("Telegram API request failed for {method}"))?;
    parse_telegram_response(response, method).await
}

async fn parse_telegram_response<T: DeserializeOwned>(
    response: reqwest::Response,
    method: &str,
) -> anyhow::Result<T> {
    let status = response.status();
    let body = response
        .json::<TelegramApiResponse<T>>()
        .await
        .map_err(|_| {
            anyhow::anyhow!("invalid Telegram response for {method} with status {status}")
        })?;
    if status.is_success() && body.ok {
        return body
            .result
            .ok_or_else(|| anyhow::anyhow!("missing Telegram result for {method}"));
    }
    anyhow::bail!(
        "Telegram API {method} failed: {}",
        body.description
            .unwrap_or_else(|| format!("HTTP status {status}"))
    )
}

pub fn telegram_api_url(bot_token: &str, method: &str) -> String {
    format!("https://api.telegram.org/bot{bot_token}/{method}")
}

/// Send a fetch failure alert via Telegram. Best-effort: errors are logged but not propagated.
pub async fn send_fetch_alert(
    config: &TelegramConfig,
    errors: &[FetchSourceError],
) -> anyhow::Result<()> {
    if !config.enabled || errors.is_empty() {
        return Ok(());
    }
    let bot_token = match load_bot_token(config) {
        Ok(t) => t,
        Err(_) => return Ok(()),
    };
    let chat_id = match std::env::var(&config.chat_id_env) {
        Ok(id) => id,
        Err(_) => return Ok(()),
    };

    let now = crate::utils::format_utc8_full(&chrono::Utc::now());
    let mut lines = vec![
        format!("xFlow Fetch Alert ({now} UTC+8)"),
        format!("{} source(s) failed:", errors.len()),
        String::new(),
    ];
    for err in errors {
        lines.push(format!(
            "  {}:{} - {}",
            err.source_type, err.source_value, err.message
        ));
    }
    let text = lines.join("\n");
    let text = if text.len() > TELEGRAM_MESSAGE_LIMIT {
        format!("{}\n...", &text[..text.floor_char_boundary(4090)])
    } else {
        text
    };

    let client = Client::new();
    let payload = SendMessagePayload {
        chat_id,
        text,
        parse_mode: None,
        disable_web_page_preview: true,
    };

    match client
        .post(telegram_api_url(&bot_token, "sendMessage"))
        .json(&payload)
        .send()
        .await
    {
        Ok(response) if !response.status().is_success() => {
            let body = response.text().await.unwrap_or_default();
            tracing::warn!(?body, "failed to send fetch alert via Telegram");
        }
        Err(err) => {
            tracing::warn!(?err, "failed to send fetch alert via Telegram");
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{SourceType, Tweet, TweetAnalysis};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn escapes_html() {
        let stored = StoredTweet {
            tweet: Tweet {
                tweet_id: "1".to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: "AI <agent> & update".to_string(),
                url: "https://x.com/openai/status/1?x=1&y=2".to_string(),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw: json!({}),
            },
            analysis: None,
        };
        let message = format_tweet_message(&stored);
        assert!(message.contains("AI &lt;agent&gt; &amp; update"));
        assert!(message.contains("x=1&amp;y=2"));
    }

    #[test]
    fn default_bot_commands_match_supported_menu() {
        let commands = default_bot_commands();
        assert_eq!(
            commands
                .iter()
                .map(|command| command.command.as_str())
                .collect::<Vec<_>>(),
            vec!["help", "add", "remove", "list", "status", "fetch", "latest", "digest"]
        );
        assert!(commands
            .iter()
            .all(|command| !command.description.trim().is_empty()));
    }

    #[test]
    fn truncates_long_message_to_telegram_limit() {
        let long_text = "A".repeat(5000);
        let stored = StoredTweet {
            tweet: Tweet {
                tweet_id: "2".to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: long_text,
                url: "https://x.com/openai/status/2".to_string(),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw: json!({}),
            },
            analysis: Some(TweetAnalysis {
                tweet_id: "2".to_string(),
                relevance: 0.9,
                importance_score: 0.8,
                category: "research".to_string(),
                tags: vec!["AI".to_string(), "LLM".to_string()],
                chinese_summary: "这是一段很长的中文摘要".to_string(),
                reason: "important".to_string(),
                should_push: true,
                analyzed_at: Utc::now(),
            }),
        };
        let message = format_tweet_message(&stored);
        assert!(message.len() <= TELEGRAM_MESSAGE_LIMIT);
        assert!(message.contains("<b>@openai</b>"));
        assert!(message.contains("Open tweet"));
        assert!(message.contains(TRUNCATION_MARKER.trim()));
        // Summary and tags should be dropped when truncated.
        assert!(!message.contains("这是一段很长的中文摘要"));
    }

    #[test]
    fn short_message_is_not_truncated() {
        let stored = StoredTweet {
            tweet: Tweet {
                tweet_id: "3".to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: "Short tweet".to_string(),
                url: "https://x.com/openai/status/3".to_string(),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw: json!({}),
            },
            analysis: None,
        };
        let message = format_tweet_message(&stored);
        assert!(!message.contains(TRUNCATION_MARKER.trim()));
        assert!(message.contains("Short tweet"));
    }
}
