use crate::config::TelegramConfig;
use crate::models::StoredTweet;
use crate::storage::{self, delivery_payload};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TelegramResult {
    pub sent: i64,
    pub failed: i64,
    pub skipped: i64,
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
    let credentials = load_credentials(config)?;
    let channel = credentials.channel();
    let tweets = storage::list_undelivered_tweets(pool, &channel, !config.send_all, limit).await?;
    let client = Client::new();
    let mut result = TelegramResult {
        sent: 0,
        failed: 0,
        skipped: 0,
    };
    for tweet in tweets {
        let payload = SendMessagePayload {
            chat_id: credentials.chat_id.clone(),
            text: format_tweet_message(&tweet),
            parse_mode: if config.parse_mode.is_empty() {
                None
            } else {
                Some(config.parse_mode.clone())
            },
            disable_web_page_preview: config.disable_web_page_preview,
        };
        let response = client
            .post(format!(
                "https://api.telegram.org/bot{}/sendMessage",
                credentials.bot_token
            ))
            .json(&payload)
            .send()
            .await;
        match response {
            Ok(response) => {
                let status = response.status();
                let body = response
                    .json::<TelegramResponse>()
                    .await
                    .unwrap_or(TelegramResponse {
                        ok: false,
                        description: Some(format!(
                            "invalid Telegram response with status {status}"
                        )),
                        result: serde_json::Value::Null,
                    });
                if status.is_success() && body.ok {
                    storage::save_delivery(
                        pool,
                        &tweet.tweet.tweet_id,
                        &channel,
                        "delivered",
                        &delivery_payload(&body),
                        true,
                    )
                    .await?;
                    result.sent += 1;
                } else {
                    storage::save_delivery(
                        pool,
                        &tweet.tweet.tweet_id,
                        &channel,
                        "error",
                        &json!({"request": payload, "response": body}),
                        false,
                    )
                    .await?;
                    result.failed += 1;
                }
            }
            Err(err) => {
                storage::save_delivery(
                    pool,
                    &tweet.tweet.tweet_id,
                    &channel,
                    "error",
                    &json!({"request": payload, "error": err.to_string()}),
                    false,
                )
                .await?;
                result.failed += 1;
            }
        }
    }
    Ok(result)
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
