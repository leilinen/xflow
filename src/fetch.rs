use crate::config::AppConfig;
use crate::models::{Source, SourceType, Tweet};
use crate::storage::{self, AuthAccountSecret};
use chrono::{DateTime, Duration, TimeZone, Utc};
use reqwest::{header, Response};
use serde_json::{json, Value};
use sqlx::SqlitePool;
use std::collections::HashSet;

const X_WEB_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA";
const DEFAULT_USER_BY_SCREEN_NAME_QUERY_ID: &str = "-oaLodhGbbnzJBACb1kk2Q";
const DEFAULT_USER_TWEETS_QUERY_ID: &str = "oRJs8SLCRNRbQzuZG93_oA";

pub async fn fetch_source(
    config: &AppConfig,
    pool: &SqlitePool,
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

pub async fn verify_auth(
    _config: &AppConfig,
    secret: &storage::AuthAccountSecret,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
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
    pool: SqlitePool,
    bearer_token: String,
    user_by_screen_name_query_id: String,
    user_tweets_query_id: String,
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
    async fn new(config: &AppConfig, pool: &SqlitePool) -> anyhow::Result<Self> {
        let auth = storage::next_auth_account_secret(pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("x_web fetcher requires an imported auth account"))?;
        let ua = random_user_agent();
        tracing::debug!(user_agent = ua, "selected user agent");
        let client = reqwest::Client::builder().user_agent(ua).build()?;
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
        tweets.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        tweets.truncate(limit as usize);
        Ok(tweets)
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
        "responsive_web_graphql_exclude_directive_enabled": true,
        "verified_phone_label_enabled": false,
        "creator_subscriptions_tweet_preview_api_enabled": true,
        "responsive_web_graphql_timeline_navigation_enabled": true,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": false,
        "c9s_tweet_anatomy_moderator_badge_enabled": true,
        "tweetypie_unmention_optimization_enabled": true,
        "responsive_web_edit_tweet_api_enabled": true,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": true,
        "view_counts_everywhere_api_enabled": true,
        "longform_notetweets_consumption_enabled": true,
        "responsive_web_twitter_article_tweet_consumption_enabled": true,
        "tweet_awards_web_tipping_enabled": false,
        "freedom_of_speech_not_reach_fetch_enabled": true,
        "standardized_nudges_misinfo": true,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": true,
        "rweb_video_timestamps_enabled": true,
        "longform_notetweets_rich_text_read_enabled": true,
        "longform_notetweets_inline_media_enabled": true,
        "responsive_web_enhance_cards_enabled": false
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
