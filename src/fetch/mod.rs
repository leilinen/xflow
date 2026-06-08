pub mod auth;
pub mod media;

use crate::config::AppConfig;
use crate::models::{Source, SourceType, Tweet, TweetComment};
use crate::storage::{self, AuthAccountSecret};
use chrono::{DateTime, Duration, TimeZone, Utc};
use reqwest::{header, Response};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::HashSet;

const X_WEB_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA";
const DEFAULT_USER_BY_SCREEN_NAME_QUERY_ID: &str = "sLVLhk0bGj3MVFEKTdax1w";
const DEFAULT_USER_TWEETS_QUERY_ID: &str = "HuTx74BxAnezK1gWvYY7zg";

#[derive(Debug, Clone, Serialize)]
pub struct BackfillResult {
    pub total: usize,
    pub new: usize,
    pub duplicate: usize,
    pub pages: usize,
}

pub async fn backfill_user(
    config: &AppConfig,
    pool: &PgPool,
    username: &str,
    max_pages: usize,
    page_delay: u64,
    since: Option<chrono::Duration>,
) -> anyhow::Result<BackfillResult> {
    let fetcher = XWebFetcher::new(config, pool).await?;
    let username = username.trim_start_matches('@');
    let user = fetcher.lookup_user(username).await?;
    let source = Source {
        source_type: SourceType::Account,
        value: username.to_string(),
        label: None,
        limit: None,
    };
    let cutoff = since.map(|d| Utc::now() - d);
    let (tweets, pages) = fetcher
        .fetch_user_tweets_paginated(&source, &user, max_pages, page_delay, cutoff)
        .await?;
    let total = tweets.len();
    let mut new = 0;
    for tweet in &tweets {
        if storage::upsert_tweet(pool, tweet).await? {
            new += 1;
        }
    }
    Ok(BackfillResult {
        total,
        new,
        duplicate: total - new,
        pages,
    })
}

pub async fn fetch_source(
    config: &AppConfig,
    pool: &PgPool,
    source: &Source,
) -> anyhow::Result<Vec<Tweet>> {
    match config.fetch.fetcher.as_str() {
        "mock" => mock_fetch_source(config, source),
        "x_web" => {
            XWebFetcher::new(config, pool)
                .await?
                .fetch_source(source)
                .await
        }
        other => anyhow::bail!(
            "unsupported fetcher '{other}'; supported fetchers are 'mock' and 'x_web'"
        ),
    }
}

pub async fn validate_account(config: &AppConfig, pool: &PgPool, username: &str) -> bool {
    match XWebFetcher::new(config, pool).await {
        Ok(fetcher) => fetcher.lookup_user(username).await.is_ok(),
        Err(_) => false,
    }
}

pub async fn fetch_tweet_comments(
    config: &AppConfig,
    pool: &PgPool,
    tweet_id: &str,
    max_comments: usize,
) -> anyhow::Result<Vec<TweetComment>> {
    let fetcher = XWebFetcher::new(config, pool).await?;
    let mut spam_keywords = storage::list_spam_keywords(pool).await?;
    // Merge config keywords as fallback
    for kw in &config.comments.spam_keywords {
        let lower = kw.to_lowercase();
        if !spam_keywords.iter().any(|k| k.to_lowercase() == lower) {
            spam_keywords.push(lower);
        }
    }
    fetcher.fetch_comments(tweet_id, max_comments, &spam_keywords).await
}

pub async fn verify_auth(
    _config: &AppConfig,
    secret: &storage::AuthAccountSecret,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .build()?;
    let bearer_token = std::env::var("XFLOW_X_WEB_BEARER_TOKEN")
        .unwrap_or_else(|_| X_WEB_BEARER_TOKEN.to_string());
    let query_id = std::env::var("XFLOW_X_USER_BY_SCREEN_NAME_QUERY_ID")
        .unwrap_or_else(|_| DEFAULT_USER_BY_SCREEN_NAME_QUERY_ID.to_string());
    let url = format!("https://x.com/i/api/graphql/{query_id}/UserByScreenName");
    let variables = json!({"screen_name": "x", "withSafetyModeUserFields": true});
    let features = common_features();
    let response = client
        .get(url)
        .query(&[
            ("variables", variables.to_string()),
            ("features", features.to_string()),
        ])
        .header(header::AUTHORIZATION, format!("Bearer {bearer_token}"))
        .header(
            header::COOKIE,
            format!("auth_token={}; ct0={}", secret.auth_token, secret.ct0),
        )
        .header("x-csrf-token", secret.ct0.as_str())
        .header("x-twitter-active-user", "yes")
        .header("x-twitter-auth-type", "OAuth2Session")
        .header("x-twitter-client-language", "en")
        .header(header::ACCEPT, "*/*")
        .header(header::REFERER, "https://x.com/")
        .send()
        .await?;
    let status = response.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        anyhow::bail!("token rejected (HTTP {status})");
    }
    if !status.is_success() {
        anyhow::bail!("HTTP {status}");
    }
    Ok(())
}

fn mock_fetch_source(config: &AppConfig, source: &Source) -> anyhow::Result<Vec<Tweet>> {
    let limit = source.limit.unwrap_or(config.fetch.default_limit).max(0);
    let mut tweets = Vec::new();
    for index in 0..limit {
        let created_at = Utc::now() - Duration::minutes(index);
        let (author_username, author_name, text, url_id) = match source.source_type {
            SourceType::Account => (
                source.value.clone(),
                source.value.clone(),
                format!(
                    "Mock update {index} from @{} about AI agent coding model",
                    source.value
                ),
                format!("{}-{index}", source.value),
            ),
            SourceType::List => (
                "list_author".to_string(),
                "List Author".to_string(),
                format!("Mock list {} item {index} about AI research", source.value),
                format!("list-{}-{index}", source.value),
            ),
            SourceType::Search => (
                "search_author".to_string(),
                "Search Author".to_string(),
                format!("Mock search result for '{}' item {index}", source.value),
                format!("search-{}-{index}", source.value.replace(' ', "-")),
            ),
        };
        tweets.push(Tweet {
            tweet_id: format!("mock-{url_id}"),
            source_type: source.source_type.clone(),
            source_value: source.value.clone(),
            author_username,
            author_name,
            text,
            url: format!("https://x.com/{}/status/mock-{url_id}", source.value),
            created_at,
            fetched_at: Utc::now(),
            raw: json!({"mock": true, "source": source.value}),
        });
    }
    Ok(tweets)
}

struct XWebFetcher {
    config: AppConfig,
    auth: AuthAccountSecret,
    client: reqwest::Client,
    pool: PgPool,
    bearer_token: String,
    user_by_screen_name_query_id: String,
    user_tweets_query_id: String,
    tweet_detail_query_id: String,
}

fn random_user_agent() -> &'static str {
    use rand::seq::SliceRandom;
    static AGENTS: &[&str] = &[
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    ];
    AGENTS.choose(&mut rand::thread_rng()).unwrap()
}

impl XWebFetcher {
    async fn new(config: &AppConfig, pool: &PgPool) -> anyhow::Result<Self> {
        let auth = storage::next_auth_account_secret(pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("x_web fetcher requires an imported auth account"))?;
        let ua = random_user_agent();
        tracing::debug!(user_agent = ua, "selected user agent");
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent(ua)
            .build()?;
        Ok(Self {
            config: config.clone(),
            auth,
            client,
            pool: pool.clone(),
            bearer_token: std::env::var("XFLOW_X_WEB_BEARER_TOKEN")
                .unwrap_or_else(|_| X_WEB_BEARER_TOKEN.to_string()),
            user_by_screen_name_query_id: std::env::var("XFLOW_X_USER_BY_SCREEN_NAME_QUERY_ID")
                .unwrap_or_else(|_| DEFAULT_USER_BY_SCREEN_NAME_QUERY_ID.to_string()),
            user_tweets_query_id: std::env::var("XFLOW_X_USER_TWEETS_QUERY_ID")
                .unwrap_or_else(|_| DEFAULT_USER_TWEETS_QUERY_ID.to_string()),
            tweet_detail_query_id: std::env::var("XFLOW_X_TWEET_DETAIL_QUERY_ID")
                .unwrap_or_else(|_| config.comments.tweet_detail_query_id.clone()),
        })
    }

    async fn fetch_source(&self, source: &Source) -> anyhow::Result<Vec<Tweet>> {
        if source.source_type != SourceType::Account {
            anyhow::bail!("x_web fetcher currently supports only account sources");
        }
        let limit = source
            .limit
            .unwrap_or(self.config.fetch.default_limit)
            .max(0);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let username = source.value.trim_start_matches('@');
        let user = self.lookup_user(username).await?;
        self.fetch_user_tweets(source, &user, limit).await
    }

    async fn lookup_user(&self, username: &str) -> anyhow::Result<XUser> {
        let variables = json!({
            "screen_name": username,
            "withSafetyModeUserFields": true
        });
        let features = common_features();
        let field_toggles = json!({
            "withAuxiliaryUserLabels": false
        });
        let value = self
            .graphql_get(
                &self.user_by_screen_name_query_id,
                "UserByScreenName",
                variables,
                features,
                Some(field_toggles),
            )
            .await?;
        parse_user(&value).ok_or_else(|| anyhow::anyhow!("X user @{username} was not found"))
    }

    async fn fetch_user_tweets(
        &self,
        source: &Source,
        user: &XUser,
        limit: i64,
    ) -> anyhow::Result<Vec<Tweet>> {
        let count = limit.clamp(1, 100);
        let variables = json!({
            "userId": user.id,
            "count": count,
            "includePromotedContent": false,
            "withQuickPromoteEligibilityTweetFields": false,
            "withVoice": false,
            "withV2Timeline": true
        });
        let features = common_features();
        let field_toggles = json!({
            "withArticlePlainText": false
        });
        let value = self
            .graphql_get(
                &self.user_tweets_query_id,
                "UserTweets",
                variables,
                features,
                Some(field_toggles),
            )
            .await?;
        let mut seen = HashSet::new();
        let mut tweets = Vec::new();
        collect_tweets(&value, source, user, &mut seen, &mut tweets);
        tweets.sort_by_key(|b| std::cmp::Reverse(b.created_at));
        tweets.truncate(limit as usize);
        Ok(tweets)
    }

    async fn fetch_user_tweets_paginated(
        &self,
        source: &Source,
        user: &XUser,
        max_pages: usize,
        page_delay: u64,
        cutoff: Option<DateTime<Utc>>,
    ) -> anyhow::Result<(Vec<Tweet>, usize)> {
        let mut all_seen = HashSet::new();
        let mut all_tweets = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0;

        loop {
            let mut variables = json!({
                "userId": user.id,
                "count": 100,
                "includePromotedContent": false,
                "withQuickPromoteEligibilityTweetFields": false,
                "withVoice": false,
                "withV2Timeline": true
            });
            if let Some(ref c) = cursor {
                variables["cursor"] = json!(c);
            }
            let features = common_features();
            let field_toggles = json!({
                "withArticlePlainText": false
            });
            let value = match self
                .graphql_get(
                    &self.user_tweets_query_id,
                    "UserTweets",
                    variables,
                    features,
                    Some(field_toggles),
                )
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(?err, "backfill page request failed, stopping pagination");
                    break;
                }
            };
            pages += 1;
            let page_count_before = all_tweets.len();
            collect_tweets(&value, source, user, &mut all_seen, &mut all_tweets);
            let page_new = all_tweets.len() - page_count_before;
            tracing::info!(
                page = pages,
                page_new,
                total = all_tweets.len(),
                "backfill page collected"
            );
            if page_new == 0 {
                tracing::info!("backfill: page returned 0 new tweets, stopping");
                break;
            }
            // Stop if all new tweets on this page are older than the cutoff
            if let Some(cutoff) = cutoff {
                let all_old = all_tweets.iter().rev().take(page_new).all(|t| t.created_at < cutoff);
                if all_old {
                    tracing::info!(
                        ?cutoff,
                        "backfill: all new tweets are older than cutoff, stopping"
                    );
                    // Remove the tweets that are older than cutoff
                    all_tweets.retain(|t| t.created_at >= cutoff);
                    break;
                }
            }
            cursor = extract_cursor(&value, "Bottom");
            if cursor.is_none() {
                tracing::info!("backfill: no bottom cursor found, reached end");
                break;
            }
            if max_pages > 0 && pages >= max_pages {
                tracing::info!(max_pages, "backfill: reached max pages limit");
                break;
            }
            if page_delay > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(page_delay)).await;
            }
        }
        all_tweets.sort_by_key(|b| std::cmp::Reverse(b.created_at));
        Ok((all_tweets, pages))
    }

    async fn fetch_comments(
        &self,
        tweet_id: &str,
        max_comments: usize,
        spam_keywords: &[String],
    ) -> anyhow::Result<Vec<TweetComment>> {
        let variables = json!({
            "focalTweetId": tweet_id,
            "count": 20,
            "withSafetyModeUserFields": true,
            "includePromotedContent": false,
            "withQuickPromoteEligibilityTweetFields": true,
            "withVoice": true,
            "withV2Timeline": true,
            "withDownvotePerspective": false,
            "withBirdwatchNotes": false,
            "withCommunity": true,
            "withSuperFollowsUserFields": true,
            "withReactionsMetadata": false,
            "withReactionsPerspective": false,
            "withSuperFollowsTweetFields": true,
            "isMetatagsQuery": false,
            "withReplays": true,
            "withClientEventToken": false,
            "withAttachments": true,
            "withConversationQueryHighlights": true,
            "withMessageQueryHighlights": true,
            "withMessages": true
        });
        let features = common_features();
        let field_toggles = json!({
            "withArticlePlainText": false
        });
        let value = self
            .graphql_get(
                &self.tweet_detail_query_id,
                "TweetDetail",
                variables,
                features,
                Some(field_toggles),
            )
            .await?;
        let mut comments = collect_comments(&value, tweet_id);
        // Filter spam
        let lower_keywords: Vec<String> = spam_keywords.iter().map(|k| k.to_lowercase()).collect();
        comments.retain(|c| !is_spam(&c.text, &lower_keywords));
        comments.truncate(max_comments);
        Ok(comments)
    }

    async fn graphql_get(
        &self,
        query_id: &str,
        operation_name: &str,
        variables: Value,
        features: Value,
        field_toggles: Option<Value>,
    ) -> anyhow::Result<Value> {
        self.ensure_rate_limit_budget(operation_name).await?;
        let url = format!("https://x.com/i/api/graphql/{query_id}/{operation_name}");
        let mut query = vec![
            ("variables", variables.to_string()),
            ("features", features.to_string()),
        ];
        if let Some(field_toggles) = field_toggles {
            query.push(("fieldToggles", field_toggles.to_string()));
        }
        let response = self
            .client
            .get(url)
            .query(&query)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.bearer_token),
            )
            .header(header::COOKIE, self.cookie_header())
            .header("x-csrf-token", self.auth.ct0.as_str())
            .header("x-twitter-active-user", "yes")
            .header("x-twitter-auth-type", "OAuth2Session")
            .header("x-twitter-client-language", "en")
            .header(header::ACCEPT, "*/*")
            .header(header::ACCEPT_LANGUAGE, "en-US,en;q=0.9")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ORIGIN, "https://x.com")
            .header(header::REFERER, "https://x.com/")
            .header(
                "sec-ch-ua",
                "\"Google Chrome\";v=\"142\", \"Chromium\";v=\"142\", \"Not A(Brand\";v=\"24\"",
            )
            .header("sec-ch-ua-mobile", "?0")
            .header("sec-ch-ua-platform", "Windows")
            .header("sec-fetch-dest", "empty")
            .header("sec-fetch-mode", "cors")
            .header("sec-fetch-site", "same-site")
            .send()
            .await?;
        self.save_rate_limit_headers(operation_name, &response)
            .await?;
        let status = response.status();
        if !status.is_success() {
            match status.as_u16() {
                401 | 403 => {
                    storage::mark_auth_rejected(&self.pool, &self.auth.label, "rejected").await?;
                    anyhow::bail!(
                        "X Web auth was rejected; refresh the imported token for account '{}'",
                        self.auth.label
                    )
                }
                429 => {
                    let limited_until = rate_limit_reset_at(&response)
                        .unwrap_or_else(|| Utc::now() + Duration::hours(1))
                        .to_rfc3339();
                    storage::mark_auth_limited(&self.pool, &self.auth.label, &limited_until)
                        .await?;
                    anyhow::bail!("X Web rate limit reached for account '{}'", self.auth.label)
                }
                _ => anyhow::bail!("X Web request failed with status {status}"),
            }
        }
        storage::mark_auth_used(&self.pool, &self.auth.label).await?;
        let value = response.json::<Value>().await?;
        if let Some(errors) = value.get("errors").and_then(Value::as_array) {
            if !errors.is_empty() {
                let messages = errors
                    .iter()
                    .filter_map(|error| error.get("message").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("; ");
                if messages.is_empty() {
                    anyhow::bail!("X Web returned GraphQL errors");
                }
                anyhow::bail!("X Web returned GraphQL errors: {messages}");
            }
        }
        Ok(value)
    }

    async fn ensure_rate_limit_budget(&self, endpoint: &str) -> anyhow::Result<()> {
        let margin = self.config.fetch.rate_limit_safety_margin.max(0);
        if margin == 0 {
            return Ok(());
        }
        let Some(limit) =
            storage::get_auth_rate_limit(&self.pool, &self.auth.label, endpoint).await?
        else {
            return Ok(());
        };
        let Some(remaining) = limit.remaining else {
            return Ok(());
        };
        if remaining > margin {
            return Ok(());
        }
        let reset_at = limit
            .reset_at
            .as_deref()
            .and_then(parse_rfc3339_utc)
            .unwrap_or_else(|| Utc::now() + Duration::minutes(15));
        if reset_at <= Utc::now() {
            return Ok(());
        }
        storage::mark_auth_limited(&self.pool, &self.auth.label, &reset_at.to_rfc3339()).await?;
        anyhow::bail!(
            "X Web rate limit safety margin reached for account '{}' endpoint '{}' (remaining {}, reset_at {})",
            self.auth.label,
            endpoint,
            remaining,
            reset_at.to_rfc3339()
        )
    }

    async fn save_rate_limit_headers(
        &self,
        endpoint: &str,
        response: &Response,
    ) -> anyhow::Result<()> {
        let headers = response.headers();
        let remaining = parse_i64_header(headers, "x-rate-limit-remaining");
        let reset_at = rate_limit_reset_at(response).map(|dt| dt.to_rfc3339());
        let limit_value = parse_i64_header(headers, "x-rate-limit-limit");
        if remaining.is_some() || reset_at.is_some() || limit_value.is_some() {
            storage::save_auth_rate_limit(
                &self.pool,
                &storage::AuthRateLimitUpdate {
                    auth_label: self.auth.label.clone(),
                    endpoint: endpoint.to_string(),
                    remaining,
                    reset_at,
                    limit_value,
                },
            )
            .await?;
        }
        Ok(())
    }

    fn cookie_header(&self) -> String {
        format!("auth_token={}; ct0={}", self.auth.auth_token, self.auth.ct0)
    }
}

fn parse_i64_header(headers: &header::HeaderMap, name: &str) -> Option<i64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
}

fn rate_limit_reset_at(response: &Response) -> Option<DateTime<Utc>> {
    parse_i64_header(response.headers(), "x-rate-limit-reset")
        .and_then(|timestamp| Utc.timestamp_opt(timestamp, 0).single())
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[derive(Debug, Clone)]
struct XUser {
    id: String,
    username: String,
    name: String,
}

fn common_features() -> Value {
    json!({
        "c9s_tweet_anatomy_moderator_badge_enabled": true,
        "responsive_web_home_pinned_timelines_enabled": true,
        "blue_business_profile_image_shape_enabled": true,
        "creator_subscriptions_tweet_preview_api_enabled": true,
        "freedom_of_speech_not_reach_fetch_enabled": true,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": true,
        "graphql_timeline_v2_bookmark_timeline": true,
        "hidden_profile_likes_enabled": true,
        "highlights_tweets_tab_ui_enabled": true,
        "interactive_text_enabled": true,
        "longform_notetweets_consumption_enabled": true,
        "longform_notetweets_inline_media_enabled": true,
        "longform_notetweets_rich_text_read_enabled": true,
        "longform_notetweets_richtext_consumption_enabled": true,
        "profile_foundations_tweet_stats_enabled": true,
        "profile_foundations_tweet_stats_tweet_frequency": true,
        "responsive_web_birdwatch_note_limit_enabled": true,
        "responsive_web_edit_tweet_api_enabled": true,
        "responsive_web_enhance_cards_enabled": false,
        "responsive_web_graphql_exclude_directive_enabled": true,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": false,
        "responsive_web_graphql_timeline_navigation_enabled": true,
        "responsive_web_media_download_video_enabled": false,
        "responsive_web_text_conversations_enabled": false,
        "responsive_web_twitter_article_data_v2_enabled": true,
        "responsive_web_twitter_article_tweet_consumption_enabled": false,
        "responsive_web_twitter_blue_verified_badge_is_enabled": true,
        "rweb_lists_timeline_redesign_enabled": true,
        "spaces_2022_h2_clipping": true,
        "spaces_2022_h2_spaces_communities": true,
        "standardized_nudges_misinfo": true,
        "subscriptions_verification_info_verified_since_enabled": true,
        "tweet_awards_web_tipping_enabled": false,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": true,
        "tweetypie_unmention_optimization_enabled": true,
        "verified_phone_label_enabled": false,
        "vibe_api_enabled": true,
        "view_counts_everywhere_api_enabled": true
    })
}

fn parse_user(value: &Value) -> Option<XUser> {
    let result = value.pointer("/data/user/result")?;
    parse_user_result(result)
}

fn parse_user_result(result: &Value) -> Option<XUser> {
    let legacy = result.get("legacy").unwrap_or(result);
    let core = result.get("core").unwrap_or(legacy);
    let id = result
        .get("rest_id")
        .or_else(|| legacy.get("id_str"))
        .and_then(Value::as_str)?
        .to_string();
    let username = core
        .get("screen_name")
        .or_else(|| legacy.get("screen_name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let name = core
        .get("name")
        .or_else(|| legacy.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| username.clone());
    if username.is_empty() {
        return None;
    }
    Some(XUser { id, username, name })
}

fn collect_tweets(
    value: &Value,
    source: &Source,
    fallback_user: &XUser,
    seen: &mut HashSet<String>,
    out: &mut Vec<Tweet>,
) {
    match value {
        Value::Object(map) => {
            if let Some(tweet_results) = map.get("tweet_results") {
                if let Some(result) = tweet_results.get("result") {
                    if let Some(tweet) = parse_tweet_result(result, source, fallback_user) {
                        if seen.insert(tweet.tweet_id.clone()) {
                            out.push(tweet);
                        }
                    }
                }
            }
            for child in map.values() {
                collect_tweets(child, source, fallback_user, seen, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_tweets(item, source, fallback_user, seen, out);
            }
        }
        _ => {}
    }
}

fn parse_tweet_result(result: &Value, source: &Source, fallback_user: &XUser) -> Option<Tweet> {
    let tweet = result.get("tweet").unwrap_or(result);
    let legacy = tweet.get("legacy")?;
    let tweet_id = tweet
        .get("rest_id")
        .or_else(|| legacy.get("id_str"))
        .and_then(Value::as_str)?
        .to_string();
    let text = note_tweet_text(tweet)
        .or_else(|| {
            legacy
                .get("full_text")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| {
            legacy
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string)
        })?;
    let created_at = legacy
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(parse_x_datetime)
        .unwrap_or_else(Utc::now);
    let user = tweet
        .pointer("/core/user_results/result")
        .and_then(parse_user_result)
        .unwrap_or_else(|| fallback_user.clone());
    let author_username = user.username.clone();
    let author_name = user.name.clone();
    Some(Tweet {
        tweet_id: tweet_id.clone(),
        source_type: source.source_type.clone(),
        source_value: source.value.clone(),
        author_username: author_username.clone(),
        author_name,
        text,
        url: format!("https://x.com/{author_username}/status/{tweet_id}"),
        created_at,
        fetched_at: Utc::now(),
        raw: tweet.clone(),
    })
}

fn note_tweet_text(tweet: &Value) -> Option<String> {
    tweet
        .pointer("/note_tweet/note_tweet_results/result/text")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn parse_x_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_str(value, "%a %b %d %H:%M:%S %z %Y")
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn extract_cursor(value: &Value, cursor_type: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(content) = map.get("content") {
                if let Some(ct) = content.get("cursorType").and_then(Value::as_str) {
                    if ct == cursor_type {
                        return content.get("value").and_then(Value::as_str).map(String::from);
                    }
                }
            }
            for child in map.values() {
                if let Some(cursor) = extract_cursor(child, cursor_type) {
                    return Some(cursor);
                }
            }
            None
        }
        Value::Array(items) => {
            for item in items {
                if let Some(cursor) = extract_cursor(item, cursor_type) {
                    return Some(cursor);
                }
            }
            None
        }
        _ => None,
    }
}

fn collect_comments(value: &Value, focal_tweet_id: &str) -> Vec<TweetComment> {
    let mut comments = Vec::new();
    let mut seen = HashSet::new();
    collect_comments_recursive(value, focal_tweet_id, &mut seen, &mut comments);
    comments.sort_by_key(|a| a.created_at);
    comments
}

fn collect_comments_recursive(
    value: &Value,
    focal_tweet_id: &str,
    seen: &mut HashSet<String>,
    out: &mut Vec<TweetComment>,
) {
    match value {
        Value::Object(map) => {
            if let Some(tweet_results) = map.get("tweet_results") {
                if let Some(result) = tweet_results.get("result") {
                    if let Some(comment) = parse_comment_result(result, focal_tweet_id) {
                        if seen.insert(comment.tweet_id.clone()) {
                            out.push(comment);
                        }
                    }
                }
            }
            for child in map.values() {
                collect_comments_recursive(child, focal_tweet_id, seen, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_comments_recursive(item, focal_tweet_id, seen, out);
            }
        }
        _ => {}
    }
}

fn parse_comment_result(result: &Value, focal_tweet_id: &str) -> Option<TweetComment> {
    let tweet = result.get("tweet").unwrap_or(result);
    let legacy = tweet.get("legacy")?;

    // Only include direct replies to the focal tweet
    let reply_to = legacy
        .get("in_reply_to_status_id_str")
        .and_then(Value::as_str)
        .unwrap_or("");
    if reply_to != focal_tweet_id {
        return None;
    }

    let tweet_id = tweet
        .get("rest_id")
        .or_else(|| legacy.get("id_str"))
        .and_then(Value::as_str)?
        .to_string();

    let text = note_tweet_text(tweet)
        .or_else(|| {
            legacy
                .get("full_text")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| {
            legacy
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string)
        })?;

    let created_at = legacy
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(parse_x_datetime)
        .unwrap_or_else(Utc::now);

    let user = tweet
        .pointer("/core/user_results/result")
        .and_then(parse_user_result)
        .unwrap_or_else(|| XUser {
            id: String::new(),
            username: "unknown".to_string(),
            name: "Unknown".to_string(),
        });

    // Extract media URLs from extended_entities
    let media_urls: Vec<String> = legacy
        .pointer("/extended_entities/media")
        .and_then(Value::as_array)
        .map(|media| {
            media
                .iter()
                .filter_map(|m| m.get("media_url_https").and_then(Value::as_str).map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Extract external links from entities.urls, filtering out x.com/twitter.com internal links
    let external_links: Vec<String> = legacy
        .pointer("/entities/urls")
        .and_then(Value::as_array)
        .map(|urls| {
            urls.iter()
                .filter_map(|u| u.get("expanded_url").and_then(Value::as_str).map(String::from))
                .filter(|url| {
                    !url.starts_with("https://x.com/")
                        && !url.starts_with("https://twitter.com/")
                        && !url.starts_with("https://t.co/")
                })
                .collect()
        })
        .unwrap_or_default();

    Some(TweetComment {
        tweet_id,
        author_username: user.username,
        author_name: user.name,
        text,
        created_at,
        media_urls,
        external_links,
    })
}

fn is_spam(text: &str, lower_keywords: &[String]) -> bool {
    if lower_keywords.is_empty() {
        return false;
    }
    let text_lower = text.to_lowercase();
    lower_keywords.iter().any(|kw| text_lower.contains(kw))
}

#[cfg(test)]
mod tests {
    use super::{collect_tweets, parse_user, random_user_agent, XUser};
    use crate::models::{Source, SourceType};
    use serde_json::json;
    use std::collections::HashSet;

    #[test]
    fn parses_user_by_screen_name_response() {
        let value = json!({
            "data": {
                "user": {
                    "result": {
                        "rest_id": "42",
                        "legacy": {
                            "screen_name": "openai",
                            "name": "OpenAI"
                        }
                    }
                }
            }
        });
        let user = parse_user(&value).unwrap();
        assert_eq!(user.id, "42");
        assert_eq!(user.username, "openai");
    }

    #[test]
    fn extracts_tweets_from_timeline_shape() {
        let source = Source {
            source_type: SourceType::Account,
            value: "openai".to_string(),
            label: None,
            limit: Some(10),
        };
        let fallback_user = XUser {
            id: "42".to_string(),
            username: "openai".to_string(),
            name: "OpenAI".to_string(),
        };
        let value = json!({
            "data": {
                "user": {
                    "result": {
                        "timeline_v2": {
                            "timeline": {
                                "instructions": [{
                                    "entries": [{
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "rest_id": "100",
                                                        "core": {
                                                            "user_results": {
                                                                "result": {
                                                                    "rest_id": "42",
                                                                    "legacy": {
                                                                        "screen_name": "openai",
                                                                        "name": "OpenAI"
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        "legacy": {
                                                            "created_at": "Wed Oct 10 20:19:24 +0000 2018",
                                                            "full_text": "hello from xFlow"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }]
                                }]
                            }
                        }
                    }
                }
            }
        });
        let mut seen = HashSet::new();
        let mut tweets = Vec::new();
        collect_tweets(&value, &source, &fallback_user, &mut seen, &mut tweets);
        assert_eq!(tweets.len(), 1);
        assert_eq!(tweets[0].tweet_id, "100");
        assert_eq!(tweets[0].text, "hello from xFlow");
        assert_eq!(tweets[0].author_username, "openai");
    }

    #[test]
    fn random_user_agent_returns_valid_strings() {
        for _ in 0..20 {
            let ua = random_user_agent();
            assert!(ua.starts_with("Mozilla/5.0"));
            assert!(ua.contains("Chrome") || ua.contains("Firefox"));
        }
    }

    #[test]
    fn random_user_agent_varies_across_calls() {
        let agents: HashSet<&str> = (0..50).map(|_| random_user_agent()).collect();
        assert!(agents.len() > 1, "user agent should vary across calls");
    }
}
