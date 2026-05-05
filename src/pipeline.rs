use crate::agent;
use crate::config::AppConfig;
use crate::fetch::fetch_source;
use crate::storage;
use serde::Serialize;
use sqlx::SqlitePool;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FetchResult {
    pub fetched: i64,
    pub analyzed: i64,
    pub sources: i64,
}

pub async fn run_fetch(config: &AppConfig, pool: &SqlitePool) -> anyhow::Result<FetchResult> {
    let mut fetched = 0;
    let mut analyzed = 0;
    let mut sources = storage::list_sources(pool, true).await?;
    if sources.is_empty() {
        sources = config.parsed_sources();
        storage::ensure_config_sources(pool, &sources).await?;
    }
    for (index, source) in sources.iter().enumerate() {
        if index > 0 {
            delay_between_sources(config).await;
        }
        match fetch_source(config, pool, source).await {
            Ok(tweets) => {
                for tweet in tweets {
                    storage::upsert_tweet(pool, &tweet).await?;
                    fetched += 1;
                    if config.agent.enabled {
                        storage::save_analysis(pool, &agent::analyze(&tweet, &config.agent))
                            .await?;
                        analyzed += 1;
                    }
                }
                storage::save_fetch_state(
                    pool,
                    source,
                    "ok",
                    Some(&format!("Fetched {fetched} tweets.")),
                )
                .await?;
            }
            Err(err) => {
                storage::save_fetch_state(pool, source, "error", Some(&err.to_string())).await?;
                return Err(err);
            }
        }
    }
    Ok(FetchResult {
        fetched,
        analyzed,
        sources: sources.len() as i64,
    })
}

async fn delay_between_sources(config: &AppConfig) {
    if config.fetch.fetcher != "x_web" {
        return;
    }
    let min = config.fetch.source_delay_min_seconds;
    let max = config.fetch.source_delay_max_seconds;
    let delay = if max > min {
        min + ((max - min) / 2)
    } else {
        min
    };
    if delay > 0 {
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}
