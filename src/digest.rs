use crate::config::DailyDigestConfig;
use crate::models::StoredTweet;
use crate::storage;
use anyhow::Context;
use chrono::{DateTime, Duration, FixedOffset, TimeZone, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct DailyDigestWindow {
    pub digest_date: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DailyAccountDigest {
    pub digest_date: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub text: String,
    pub account_count: usize,
    pub tweet_count: usize,
    pub llm_error: Option<String>,
}

pub fn daily_digest_window(
    now: DateTime<Utc>,
    config: &DailyDigestConfig,
) -> anyhow::Result<DailyDigestWindow> {
    let offset = digest_offset(config)?;
    let (hour, minute) = parse_send_time(&config.send_time)?;
    let local_now = now.with_timezone(&offset);
    let local_date = local_now.date_naive();
    let today_naive = local_date
        .and_hms_opt(hour, minute, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid daily digest send time"))?;
    let mut window_end_local = offset
        .from_local_datetime(&today_naive)
        .single()
        .ok_or_else(|| anyhow::anyhow!("invalid local daily digest time"))?;
    if local_now < window_end_local {
        window_end_local -= Duration::days(1);
    }
    let window_start_local = window_end_local - Duration::days(1);
    Ok(DailyDigestWindow {
        digest_date: window_end_local.format("%Y-%m-%d").to_string(),
        window_start: window_start_local.with_timezone(&Utc),
        window_end: window_end_local.with_timezone(&Utc),
    })
}

pub fn next_daily_digest_due_at(
    now: DateTime<Utc>,
    config: &DailyDigestConfig,
) -> anyhow::Result<DateTime<Utc>> {
    let offset = digest_offset(config)?;
    let (hour, minute) = parse_send_time(&config.send_time)?;
    let local_now = now.with_timezone(&offset);
    let local_date = local_now.date_naive();
    let today_naive = local_date
        .and_hms_opt(hour, minute, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid daily digest send time"))?;
    let mut due_local = offset
        .from_local_datetime(&today_naive)
        .single()
        .ok_or_else(|| anyhow::anyhow!("invalid local daily digest time"))?;
    if local_now >= due_local {
        due_local += Duration::days(1);
    }
    Ok(due_local.with_timezone(&Utc))
}

pub fn daily_digest_due_now(
    now: DateTime<Utc>,
    config: &DailyDigestConfig,
) -> anyhow::Result<bool> {
    let offset = digest_offset(config)?;
    let (hour, minute) = parse_send_time(&config.send_time)?;
    let local_now = now.with_timezone(&offset);
    let due_naive = local_now
        .date_naive()
        .and_hms_opt(hour, minute, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid daily digest send time"))?;
    let due_local = offset
        .from_local_datetime(&due_naive)
        .single()
        .ok_or_else(|| anyhow::anyhow!("invalid local daily digest time"))?;
    Ok(local_now >= due_local)
}

pub async fn generate_daily_account_digest(
    pool: &PgPool,
    config: &DailyDigestConfig,
    now: DateTime<Utc>,
) -> anyhow::Result<DailyAccountDigest> {
    let window = daily_digest_window(now, config)?;
    let query_limit = (config.max_tweets_per_account.max(1) * 200) as i64;
    let tweets = storage::list_account_tweets_for_window(
        pool,
        window.window_start,
        window.window_end,
        query_limit,
    )
    .await?;
    let grouped = group_tweets_by_account(&tweets, config.max_tweets_per_account.max(1));
    let tweet_count = tweets.len();
    let account_count = grouped.len();
    let fallback = format_local_account_digest(&window, &grouped, tweet_count);

    if grouped.is_empty() {
        return Ok(DailyAccountDigest {
            digest_date: window.digest_date,
            window_start: window.window_start,
            window_end: window.window_end,
            text: fallback,
            account_count,
            tweet_count,
            llm_error: None,
        });
    }

    let (text, llm_error) = match DigestChatClient::from_config(config) {
        Ok(client) => match client.summarize(&build_llm_prompt(&window, &grouped)).await {
            Ok(summary) => (
                format_digest_with_header(&window, tweet_count, account_count, &summary),
                None,
            ),
            Err(err) => {
                let message = err.to_string();
                tracing::warn!(
                    ?err,
                    "daily digest LLM summary failed, using local fallback"
                );
                (fallback, Some(message))
            }
        },
        Err(err) => {
            let message = err.to_string();
            tracing::warn!(
                ?err,
                "daily digest LLM client unavailable, using local fallback"
            );
            (fallback, Some(message))
        }
    };

    Ok(DailyAccountDigest {
        digest_date: window.digest_date,
        window_start: window.window_start,
        window_end: window.window_end,
        text,
        account_count,
        tweet_count,
        llm_error,
    })
}

fn group_tweets_by_account(
    tweets: &[StoredTweet],
    max_tweets_per_account: usize,
) -> BTreeMap<String, Vec<StoredTweet>> {
    let mut grouped: BTreeMap<String, Vec<StoredTweet>> = BTreeMap::new();
    for tweet in tweets {
        let account = tweet.tweet.author_username.to_lowercase();
        let entry = grouped.entry(account).or_default();
        if entry.len() < max_tweets_per_account {
            entry.push(tweet.clone());
        }
    }
    grouped
}

fn format_local_account_digest(
    window: &DailyDigestWindow,
    grouped: &BTreeMap<String, Vec<StoredTweet>>,
    total_tweets: usize,
) -> String {
    let mut lines = vec![format_digest_title(window, total_tweets, grouped.len())];
    if grouped.is_empty() {
        lines.push("今日无新增账号推文。".to_string());
        return lines.join("\n\n");
    }
    for tweets in grouped.values() {
        let Some(first) = tweets.first() else {
            continue;
        };
        lines.push(format!(
            "@{} ({})",
            first.tweet.author_username,
            plural_tweets(tweets.len())
        ));
        for stored in tweets {
            let time = crate::utils::format_utc8(&stored.tweet.created_at);
            let summary = &stored.tweet.text;
            lines.push(format!(
                "- {} {} {}",
                time,
                truncate_plain(summary, 120),
                stored.tweet.url
            ));
        }
        lines.push(String::new());
    }
    lines.join("\n")
}

fn format_digest_with_header(
    window: &DailyDigestWindow,
    tweet_count: usize,
    account_count: usize,
    summary: &str,
) -> String {
    let mut text = format_digest_title(window, tweet_count, account_count);
    text.push_str("\n\n");
    text.push_str(summary.trim());
    text
}

fn format_digest_title(
    window: &DailyDigestWindow,
    tweet_count: usize,
    account_count: usize,
) -> String {
    let start = crate::utils::format_utc8(&window.window_start);
    let end = crate::utils::format_utc8(&window.window_end);
    if tweet_count == 0 && account_count == 0 {
        format!(
            "xFlow 每日账号摘要 {} UTC+8\n范围：{} - {}",
            window.digest_date, start, end
        )
    } else {
        format!(
            "xFlow 每日账号摘要 {} UTC+8\n范围：{} - {}\n账号 {} 个，推文 {} 条",
            window.digest_date, start, end, account_count, tweet_count
        )
    }
}

fn build_llm_prompt(
    window: &DailyDigestWindow,
    grouped: &BTreeMap<String, Vec<StoredTweet>>,
) -> String {
    let mut prompt = vec![
        format!(
            "请总结以下 X/Twitter 账号在 {} UTC+8 这个统计日的推文。",
            window.digest_date
        ),
        "要求：按账号分段；每个账号用 2-4 个要点；保留重要推文链接；没有足够信息时直接说明。"
            .to_string(),
        String::new(),
    ];
    for tweets in grouped.values() {
        let Some(first) = tweets.first() else {
            continue;
        };
        prompt.push(format!("@{}:", first.tweet.author_username));
        for stored in tweets {
            let time = crate::utils::format_utc8(&stored.tweet.created_at);
            prompt.push(format!(
                "- [{}] {} ({})",
                time,
                truncate_plain(&stored.tweet.text, 500),
                stored.tweet.url
            ));
        }
        prompt.push(String::new());
    }
    prompt.join("\n")
}

fn digest_offset(config: &DailyDigestConfig) -> anyhow::Result<FixedOffset> {
    FixedOffset::east_opt(config.timezone_offset_hours * 3600)
        .ok_or_else(|| anyhow::anyhow!("invalid daily digest timezone offset"))
}

fn parse_send_time(value: &str) -> anyhow::Result<(u32, u32)> {
    let (hour, minute) = value
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("daily_digest.send_time must use HH:MM"))?;
    let hour: u32 = hour
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid daily_digest.send_time hour"))?;
    let minute: u32 = minute
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid daily_digest.send_time minute"))?;
    if hour > 23 || minute > 59 {
        anyhow::bail!("daily_digest.send_time must be between 00:00 and 23:59");
    }
    Ok((hour, minute))
}

fn plural_tweets(count: usize) -> String {
    format!("{count} 条")
}

fn truncate_plain(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut text: String = value.chars().take(max_chars.saturating_sub(1)).collect();
    text.push('…');
    text
}

#[derive(Debug, Clone)]
struct DigestChatClient {
    http: Client,
    api_key: String,
    base_url: String,
    model: String,
    max_tokens: u32,
    temperature: f32,
    system_prompt: String,
}

impl DigestChatClient {
    fn from_config(config: &DailyDigestConfig) -> anyhow::Result<Self> {
        let api_key = std::env::var(&config.api_key_env).with_context(|| {
            format!(
                "daily digest API key not found in env var {}",
                config.api_key_env
            )
        })?;
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .context("failed to build HTTP client for daily digest")?;
        Ok(Self {
            http,
            api_key,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            model: config.model.clone(),
            max_tokens: config.max_tokens,
            temperature: config.temperature,
            system_prompt: config.system_prompt.clone(),
        })
    }

    async fn summarize(&self, text: &str) -> anyhow::Result<String> {
        let request = ChatRequest {
            model: self.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: self.system_prompt.clone(),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: text.to_string(),
                },
            ],
            max_tokens: self.max_tokens,
            temperature: self.temperature,
        };
        let response = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .context("daily digest API request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("daily digest API returned status {}: {}", status, body);
        }
        let body: ChatResponse = response
            .json()
            .await
            .context("failed to parse daily digest API response")?;
        let content = body
            .choices
            .first()
            .and_then(|choice| choice.message.content.clone())
            .unwrap_or_default()
            .trim()
            .to_string();
        if content.is_empty() {
            anyhow::bail!("daily digest API returned empty content");
        }
        Ok(content)
    }
}

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
}

#[derive(Deserialize)]
struct ChatMessageResponse {
    content: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn daily_digest_window_uses_previous_boundary_before_send_time() {
        let config = DailyDigestConfig::default();
        let now = Utc.with_ymd_and_hms(2026, 6, 26, 9, 0, 0).unwrap();
        let window = daily_digest_window(now, &config).unwrap();
        assert_eq!(window.digest_date, "2026-06-25");
        assert_eq!(
            window.window_end,
            Utc.with_ymd_and_hms(2026, 6, 25, 10, 0, 0).unwrap()
        );
    }

    #[test]
    fn daily_digest_window_uses_today_boundary_after_send_time() {
        let config = DailyDigestConfig::default();
        let now = Utc.with_ymd_and_hms(2026, 6, 26, 11, 0, 0).unwrap();
        let window = daily_digest_window(now, &config).unwrap();
        assert_eq!(window.digest_date, "2026-06-26");
        assert_eq!(
            window.window_end,
            Utc.with_ymd_and_hms(2026, 6, 26, 10, 0, 0).unwrap()
        );
    }

    #[test]
    fn next_daily_digest_due_at_rolls_to_next_day_after_boundary() {
        let config = DailyDigestConfig::default();
        let now = Utc.with_ymd_and_hms(2026, 6, 26, 10, 0, 0).unwrap();
        let due = next_daily_digest_due_at(now, &config).unwrap();
        assert_eq!(due, Utc.with_ymd_and_hms(2026, 6, 27, 10, 0, 0).unwrap());
    }

    #[test]
    fn daily_digest_due_now_is_false_before_boundary() {
        let config = DailyDigestConfig::default();
        let now = Utc.with_ymd_and_hms(2026, 6, 26, 9, 59, 59).unwrap();
        assert!(!daily_digest_due_now(now, &config).unwrap());
    }
}
