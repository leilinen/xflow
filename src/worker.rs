use crate::config::AppConfig;
use crate::pipeline::{self, FetchResult};
use crate::storage;
use crate::{channel, telegram::TelegramResult};
use serde::Serialize;
use sqlx::SqlitePool;
use std::time::Duration;

const TOKEN_FRESHNESS_DAYS: i64 = 7;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkerOnceResult {
    pub fetch: FetchResult,
    pub telegram: TelegramResult,
}

pub async fn run_once(config: &AppConfig, pool: &SqlitePool) -> anyhow::Result<WorkerOnceResult> {
    let fetch = pipeline::run_fetch(config, pool).await?;
    if fetch.failed > 0 {
        tracing::warn!(?fetch.errors, "fetch completed with source failures");
    }
    let channels = channel::configured_channels(config)?;
    let telegram = channel::send_undelivered(pool, &channels, 100).await?;
    Ok(WorkerOnceResult { fetch, telegram })
}

/// Pure function to compute the next worker interval based on cycle outcome.
/// Returns (new_interval, new_consecutive_successes).
pub fn adjust_interval(
    current_interval: u64,
    base_interval: u64,
    consecutive_successes: u32,
    failed: i64,
    sources: i64,
) -> (u64, u32) {
    if failed == 0 {
        let successes = consecutive_successes + 1;
        if successes >= 2 && current_interval > base_interval {
            ((current_interval * 2 / 3).max(base_interval), successes)
        } else {
            (current_interval, successes)
        }
    } else if sources > 0 {
        let failure_ratio = failed as f64 / sources as f64;
        if failure_ratio >= 1.0 {
            ((current_interval * 2).min(base_interval * 8), 0)
        } else {
            ((current_interval * 3 / 2).min(base_interval * 4), 0)
        }
    } else {
        (current_interval, consecutive_successes)
    }
}

pub async fn run_forever(config: AppConfig, pool: SqlitePool) -> anyhow::Result<()> {
    if config.telegram.enabled {
        let bot_config = config.clone();
        let bot_pool = pool.clone();
        tokio::spawn(async move {
            if let Err(err) = crate::bot::run_poller(bot_config, bot_pool).await {
                tracing::error!(?err, "bot poller crashed");
            }
        });
    }

    let base_interval = config.fetch.interval_seconds;
    let mut current_interval = base_interval;
    let mut consecutive_successes: u32 = 0;

    loop {
        let cycle_result = run_once(&config, &pool).await;
        match &cycle_result {
            Ok(result) => {
                tracing::info!(?result, "worker cycle complete");
                let (next, successes) = adjust_interval(
                    current_interval,
                    base_interval,
                    consecutive_successes,
                    result.fetch.failed,
                    result.fetch.sources,
                );
                current_interval = next;
                consecutive_successes = successes;
            }
            Err(err) => {
                tracing::error!(?err, "worker cycle failed");
                current_interval = (current_interval * 2).min(base_interval * 8);
                consecutive_successes = 0;
            }
        }

        warn_stale_tokens(&pool).await;

        tracing::info!(
            current_interval,
            next_cycle_in = current_interval,
            "sleeping until next cycle"
        );
        tokio::time::sleep(Duration::from_secs(current_interval)).await;
    }
}

async fn warn_stale_tokens(pool: &SqlitePool) {
    if let Ok(stale) = storage::check_token_freshness(pool, TOKEN_FRESHNESS_DAYS).await {
        for (label, updated_at) in &stale {
            tracing::warn!(
                label,
                updated_at,
                "auth token may be stale (not used/updated in {TOKEN_FRESHNESS_DAYS}+ days), consider refreshing"
            );
        }
    }
}
