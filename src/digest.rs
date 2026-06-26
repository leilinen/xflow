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

const DAILY_DIGEST_TWEET_INPUT_CHARS: usize = 240;
const DAILY_DIGEST_RETRY_TWEET_INPUT_CHARS: usize = 140;
const DAILY_DIGEST_COMPACT_TWEET_INPUT_CHARS: usize = 90;
const DAILY_DIGEST_CHUNK_CHAR_BUDGET: usize = 8_000;
const DAILY_DIGEST_RETRY_CHUNK_CHAR_BUDGET: usize = 4_000;
const DAILY_DIGEST_COMPACT_CHUNK_CHAR_BUDGET: usize = 2_000;
const DAILY_DIGEST_FALLBACK_TWEETS_PER_ACCOUNT: usize = 5;
const DAILY_DIGEST_MIN_OUTPUT_TOKENS: u32 = 2_400;

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

    if grouped.is_empty() {
        return Ok(DailyAccountDigest {
            digest_date: window.digest_date.clone(),
            window_start: window.window_start,
            window_end: window.window_end,
            text: format_local_account_digest(&window, &grouped, tweet_count),
            account_count,
            tweet_count,
            llm_error: None,
        });
    }

    let client = match DigestChatClient::from_config(config) {
        Ok(client) => Some(client),
        Err(err) => {
            tracing::warn!(
                ?err,
                "daily digest LLM client unavailable, using account fallbacks"
            );
            None
        }
    };

    let mut sections = Vec::new();
    let mut errors = Vec::new();
    for tweets in grouped.values() {
        let Some(first) = tweets.first() else {
            continue;
        };
        let username = first.tweet.author_username.clone();
        let display_name = first.tweet.author_name.clone();
        let section = match &client {
            Some(client) => {
                match summarize_account(client, &window, &display_name, &username, tweets).await {
                    Ok(section) => section,
                    Err(err) => {
                        tracing::warn!(
                            account = %username,
                            tweet_count = tweets.len(),
                            ?err,
                            "daily digest account summary failed, using account fallback"
                        );
                        errors.push(format!("@{username}: {err}"));
                        format_account_fallback_section(&display_name, &username, tweets)
                    }
                }
            }
            None => {
                errors.push(format!("@{username}: daily digest LLM client unavailable"));
                format_account_fallback_section(&display_name, &username, tweets)
            }
        };
        sections.push(section);
    }

    let text =
        format_digest_with_header(&window, tweet_count, account_count, &sections.join("\n\n"));
    let llm_error = if errors.is_empty() {
        None
    } else {
        Some(errors.join("; "))
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
        lines.push(format_account_fallback_section(
            &first.tweet.author_name,
            &first.tweet.author_username,
            tweets,
        ));
    }
    lines.join("\n\n")
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

async fn summarize_account(
    client: &DigestChatClient,
    window: &DailyDigestWindow,
    display_name: &str,
    username: &str,
    tweets: &[StoredTweet],
) -> anyhow::Result<String> {
    match summarize_account_with_budget(
        client,
        window,
        display_name,
        username,
        tweets,
        DAILY_DIGEST_TWEET_INPUT_CHARS,
        DAILY_DIGEST_CHUNK_CHAR_BUDGET,
    )
    .await
    {
        Ok(section) => Ok(section),
        Err(first_err) => {
            tracing::warn!(
                account = %username,
                ?first_err,
                "daily digest account summary failed, retrying with smaller chunks"
            );
            match summarize_account_with_budget(
                client,
                window,
                display_name,
                username,
                tweets,
                DAILY_DIGEST_RETRY_TWEET_INPUT_CHARS,
                DAILY_DIGEST_RETRY_CHUNK_CHAR_BUDGET,
            )
            .await
            {
                Ok(section) => Ok(section),
                Err(second_err) => {
                    tracing::warn!(
                        account = %username,
                        ?second_err,
                        "daily digest account summary retry failed, retrying with compact prompt"
                    );
                    summarize_account_with_budget(
                        client,
                        window,
                        display_name,
                        username,
                        tweets,
                        DAILY_DIGEST_COMPACT_TWEET_INPUT_CHARS,
                        DAILY_DIGEST_COMPACT_CHUNK_CHAR_BUDGET,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "compact retry after failures: initial={first_err}; retry={second_err}"
                        )
                    })
                }
            }
        }
    }
}

async fn summarize_account_with_budget(
    client: &DigestChatClient,
    window: &DailyDigestWindow,
    display_name: &str,
    username: &str,
    tweets: &[StoredTweet],
    tweet_char_limit: usize,
    chunk_char_budget: usize,
) -> anyhow::Result<String> {
    let chunks = build_account_digest_chunks(username, tweets, tweet_char_limit, chunk_char_budget);
    tracing::info!(
        account = %username,
        tweet_count = tweets.len(),
        chunk_count = chunks.len(),
        chunk_char_budget,
        "daily digest account summary started"
    );
    let mut summaries = Vec::new();
    for (index, chunk) in chunks.iter().enumerate() {
        let prompt = build_account_chunk_prompt(
            window,
            display_name,
            username,
            tweets.len(),
            index,
            chunks.len(),
            chunk,
        );
        let summary = client
            .summarize(&prompt)
            .await
            .with_context(|| format!("chunk {}/{} failed", index + 1, chunks.len()))?;
        tracing::info!(
            account = %username,
            chunk_index = index + 1,
            chunk_count = chunks.len(),
            input_chars = prompt.chars().count(),
            output_chars = summary.chars().count(),
            "daily digest account chunk summarized"
        );
        summaries.push(summary);
    }

    let body = if summaries.len() == 1 {
        summaries.remove(0)
    } else {
        let prompt =
            build_account_merge_prompt(window, display_name, username, tweets.len(), &summaries);
        let merged = client.summarize(&prompt).await.with_context(|| {
            format!("failed to merge {} account summary chunks", summaries.len())
        })?;
        tracing::info!(
            account = %username,
            input_chars = prompt.chars().count(),
            output_chars = merged.chars().count(),
            "daily digest account chunks merged"
        );
        merged
    };

    Ok(format_account_llm_section(
        display_name,
        username,
        tweets.len(),
        &body,
    ))
}

fn build_account_digest_chunks(
    username: &str,
    tweets: &[StoredTweet],
    tweet_char_limit: usize,
    chunk_char_budget: usize,
) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_len = 0usize;
    for stored in tweets {
        let line = format_digest_tweet_input(stored, tweet_char_limit);
        let line_len = line.chars().count() + 1;
        if !current.is_empty() && current_len + line_len > chunk_char_budget {
            chunks.push(current.join("\n"));
            current.clear();
            current_len = 0;
        }
        current_len += line_len;
        current.push(line);
    }
    if !current.is_empty() {
        chunks.push(current.join("\n"));
    }
    if chunks.is_empty() {
        chunks.push(format!("@{username} 无可总结推文。"));
    }
    chunks
}

fn format_digest_tweet_input(stored: &StoredTweet, tweet_char_limit: usize) -> String {
    let time = crate::utils::format_utc8(&stored.tweet.created_at);
    let text = normalize_whitespace(&stored.tweet.text);
    format!(
        "- [{}] {} | {}",
        time,
        truncate_plain(&text, tweet_char_limit),
        stored.tweet.url
    )
}

fn build_account_chunk_prompt(
    window: &DailyDigestWindow,
    display_name: &str,
    username: &str,
    total_tweets: usize,
    chunk_index: usize,
    chunk_count: usize,
    chunk: &str,
) -> String {
    format!(
        "请总结 {display_name}（@{username}）在 {} UTC+8 的推文。\n\
这是第 {}/{} 组，共 {} 条推文。\n\
要求：只输出最终摘要，不输出 Markdown，不输出推理过程。输出 2-3 条纯文本编号要点，每条尽量 80 个中文以内；必须总结主题、观点变化、风险或机会；不要逐条罗列；只保留最关键的原推文链接。\n\n\
推文：\n{}",
        window.digest_date,
        chunk_index + 1,
        chunk_count,
        total_tweets,
        chunk
    )
}

fn build_account_merge_prompt(
    window: &DailyDigestWindow,
    display_name: &str,
    username: &str,
    total_tweets: usize,
    summaries: &[String],
) -> String {
    let joined = summaries
        .iter()
        .enumerate()
        .map(|(index, summary)| format!("中间摘要 {}:\n{}", index + 1, summary.trim()))
        .collect::<Vec<_>>()
        .join("\n\n");
    format!(
        "请把 {display_name}（@{username}）在 {} UTC+8 的 {} 条推文中间摘要合并成最终账号摘要。\n\
要求：只输出最终摘要，不输出 Markdown，不输出推理过程。输出 2-3 条纯文本编号要点；去重；先结论后证据；保留最关键链接；不要逐条罗列。\n\n{}",
        window.digest_date, total_tweets, joined
    )
}

fn format_account_llm_section(
    display_name: &str,
    username: &str,
    tweet_count: usize,
    body: &str,
) -> String {
    let body = sanitize_digest_text(body);
    format!(
        "{}（@{}，{}）\n{}",
        display_name,
        username,
        plural_tweets(tweet_count),
        body
    )
}

fn format_account_fallback_section(
    display_name: &str,
    username: &str,
    tweets: &[StoredTweet],
) -> String {
    let mut lines = vec![format!(
        "{}（@{}，{}，本账号使用本地摘要）",
        display_name,
        username,
        plural_tweets(tweets.len())
    )];
    lines.push("1. LLM 未生成可用账号摘要，以下仅保留代表性更新，避免罗列全部推文。".to_string());
    for stored in tweets.iter().take(DAILY_DIGEST_FALLBACK_TWEETS_PER_ACCOUNT) {
        let time = crate::utils::format_utc8(&stored.tweet.created_at);
        let summary = &stored.tweet.text;
        lines.push(format!(
            "{}. {} {} {}",
            lines.len(),
            time,
            truncate_plain(&normalize_whitespace(summary), 100),
            stored.tweet.url
        ));
    }
    if tweets.len() > DAILY_DIGEST_FALLBACK_TWEETS_PER_ACCOUNT {
        lines.push(format!(
            "{}. 其余 {} 条已省略。",
            lines.len(),
            tweets.len() - DAILY_DIGEST_FALLBACK_TWEETS_PER_ACCOUNT
        ));
    }
    lines.join("\n")
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

fn normalize_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sanitize_digest_text(value: &str) -> String {
    let mut lines = Vec::new();
    for line in value.lines() {
        let mut cleaned = line.trim();
        while let Some(rest) = cleaned.strip_prefix('#') {
            cleaned = rest.trim_start();
        }
        cleaned = cleaned.trim_start_matches(['-', '*', '•']).trim_start();
        let cleaned = markdown_links_to_plain(cleaned)
            .replace("**", "")
            .replace("__", "")
            .replace('`', "");
        let cleaned = cleaned.trim();
        if !cleaned.is_empty() {
            lines.push(cleaned.to_string());
        }
    }
    lines.join("\n")
}

fn markdown_links_to_plain(value: &str) -> String {
    let mut output = String::new();
    let mut rest = value;
    while let Some(start) = rest.find('[') {
        let before = &rest[..start];
        let candidate = &rest[start..];
        let Some(mid) = candidate.find("](") else {
            output.push_str(rest);
            return output;
        };
        let after_mid = &candidate[mid + 2..];
        let Some(end) = after_mid.find(')') else {
            output.push_str(rest);
            return output;
        };
        output.push_str(before);
        let label = &candidate[1..mid];
        let url = &after_mid[..end];
        output.push_str(label);
        if !url.is_empty() {
            output.push(' ');
            output.push_str(url);
        }
        rest = &after_mid[end + 1..];
    }
    output.push_str(rest);
    output
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
            max_tokens: config.max_tokens.max(DAILY_DIGEST_MIN_OUTPUT_TOKENS),
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
        let first_choice = body.choices.first();
        let content = first_choice
            .and_then(|choice| choice.message.content.clone())
            .unwrap_or_default()
            .trim()
            .to_string();
        tracing::debug!(
            choices = body.choices.len(),
            finish_reason = first_choice
                .and_then(|choice| choice.finish_reason.as_deref())
                .unwrap_or("unknown"),
            content_chars = content.chars().count(),
            "daily digest API response parsed"
        );
        if content.is_empty() {
            anyhow::bail!(
                "daily digest API returned empty content (choices={}, finish_reason={})",
                body.choices.len(),
                first_choice
                    .and_then(|choice| choice.finish_reason.as_deref())
                    .unwrap_or("unknown")
            );
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
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ChatMessageResponse {
    content: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{SourceType, Tweet};
    use chrono::TimeZone;
    use serde_json::json;

    fn make_stored_tweet(id: usize, text: &str) -> StoredTweet {
        StoredTweet {
            tweet: Tweet {
                tweet_id: id.to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: text.to_string(),
                url: format!("https://x.com/openai/status/{id}"),
                created_at: Utc.with_ymd_and_hms(2026, 6, 26, 8, id as u32, 0).unwrap(),
                fetched_at: Utc.with_ymd_and_hms(2026, 6, 26, 8, id as u32, 1).unwrap(),
                raw: json!({}),
            },
        }
    }

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

    #[test]
    fn digest_tweet_input_truncates_and_normalizes_text() {
        let stored = make_stored_tweet(1, "first line\n\nsecond line   third line");

        let line = format_digest_tweet_input(&stored, 24);

        assert!(line.contains("first line second"));
        assert!(line.contains('…'));
        assert!(line.contains("https://x.com/openai/status/1"));
        assert!(!line.contains("\n\n"));
        assert!(!line.contains("   "));
    }

    #[test]
    fn account_digest_chunks_split_by_character_budget() {
        let tweets = (1..=8)
            .map(|id| make_stored_tweet(id, &"memory semiconductors demand ".repeat(12)))
            .collect::<Vec<_>>();

        let chunks = build_account_digest_chunks("openai", &tweets, 80, 360);

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 360));
    }

    #[test]
    fn account_fallback_limits_representative_tweets() {
        let tweets = (1..=7)
            .map(|id| make_stored_tweet(id, &format!("tweet {id}")))
            .collect::<Vec<_>>();

        let section = format_account_fallback_section("OpenAI", "openai", &tweets);

        assert!(section.contains("OpenAI（@openai，7 条，本账号使用本地摘要）"));
        assert!(section.contains("tweet 1"));
        assert_eq!(section.matches("https://x.com/openai/status/").count(), 5);
        assert!(section.contains("其余 2 条已省略"));
    }

    #[test]
    fn account_llm_section_adds_display_name_header() {
        let section = format_account_llm_section("OpenAI", "openai", 3, "1. 要点一\n2. 要点二");

        assert!(section.starts_with("OpenAI（@openai，3 条）\n"));
        assert!(section.contains("1. 要点一"));
    }

    #[test]
    fn digest_text_sanitizer_removes_markdown() {
        let text =
            sanitize_digest_text("### **OpenAI**\n- [发布更新](https://x.com/openai/status/1)");

        assert_eq!(text, "OpenAI\n发布更新 https://x.com/openai/status/1");
    }
}
