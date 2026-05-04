use crate::channel::{
    ChannelDeliveryResult, ChannelSendFuture, ChannelSendReceipt, DeliveryChannel,
};
use crate::config::TelegramConfig;
use crate::models::StoredTweet;
use reqwest::Client;
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
struct TelegramResponse {
    ok: bool,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    result: serde_json::Value,
}

pub type TelegramResult = ChannelDeliveryResult;

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
    let bot_token = std::env::var(&config.bot_token_env).map_err(|_| {
        anyhow::anyhow!(
            "missing Telegram bot token env var: {}",
            config.bot_token_env
        )
    })?;
    let chat_id = std::env::var(&config.chat_id_env)
        .map_err(|_| anyhow::anyhow!("missing Telegram chat id env var: {}", config.chat_id_env))?;
    Ok(TelegramCredentials { bot_token, chat_id })
}

pub fn format_tweet_message(stored: &StoredTweet) -> String {
    let mut parts = vec![
        format!("<b>@{}</b>", html_escape(&stored.tweet.author_username)),
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
    parts.push(format!(
        "<a href=\"{}\">Open tweet</a>",
        html_escape(&stored.tweet.url)
    ));
    parts.join("\n\n")
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
            let response = self
                .client
                .post(format!(
                    "https://api.telegram.org/bot{}/sendMessage",
                    self.credentials.bot_token
                ))
                .json(&payload)
                .send()
                .await?;
            let status = response.status();
            let body = response
                .json::<TelegramResponse>()
                .await
                .unwrap_or(TelegramResponse {
                    ok: false,
                    description: Some(format!("invalid Telegram response with status {status}")),
                    result: serde_json::Value::Null,
                });
            if status.is_success() && body.ok {
                Ok(ChannelSendReceipt {
                    payload: serde_json::to_value(body)?,
                })
            } else {
                anyhow::bail!(
                    "{}",
                    serde_json::json!({"request": payload, "response": body})
                )
            }
        })
    }
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
    crate::channel::send_undelivered(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{SourceType, Tweet};
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
}
