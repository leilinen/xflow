use crate::config::AppConfig;
use crate::models::{Source, SourceType, Tweet};
use chrono::{Duration, Utc};
use serde_json::json;

pub fn fetch_source(config: &AppConfig, source: &Source) -> anyhow::Result<Vec<Tweet>> {
    if config.fetch.fetcher != "mock" {
        anyhow::bail!(
            "unsupported fetcher '{}'; Rust MVP supports only 'mock'",
            config.fetch.fetcher
        );
    }
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
