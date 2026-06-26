pub mod pipeline;

use self::pipeline::FetchResult;
use crate::channel;
use crate::channel::telegram;
use crate::channel::telegram::TelegramResult;
use crate::config::AppConfig;
use crate::digest;
use crate::storage;
use serde::Serialize;
use sqlx::PgPool;
use std::time::Duration;

const TOKEN_FRESHNESS_DAYS: i64 = 7;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkerOnceResult {
    pub fetch: FetchResult,
    pub telegram: TelegramResult,
}

pub async fn run_once(config: &AppConfig, pool: &PgPool) -> anyhow::Result<WorkerOnceResult> {
    let fetch = pipeline::run_fetch(config, pool).await?;
    if fetch.failed > 0 {
        tracing::warn!(?fetch.errors, "fetch completed with source failures");
    }
    let channels = channel::configured_channels(config)?;
    let telegram =
        channel::send_undelivered(pool, &channels, 100, config.fetch.max_delivery_retries).await?;
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

pub async fn run_forever(config: AppConfig, pool: PgPool) -> anyhow::Result<()> {
    if config.telegram.enabled {
        let bot_config = config.clone();
        let bot_pool = pool.clone();
        tokio::spawn(async move {
            if let Err(err) = crate::bot::run_poller(bot_config, bot_pool).await {
                tracing::error!(?err, "bot poller crashed");
            }
        });
    }
    if config.telegram.enabled && config.daily_digest.enabled {
        let digest_config = config.clone();
        let digest_pool = pool.clone();
        tokio::spawn(async move {
            if let Err(err) = run_daily_digest_scheduler(digest_config, digest_pool).await {
                tracing::error!(?err, "daily digest scheduler crashed");
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
                if result.fetch.failed > 0 {
                    if let Err(err) =
                        telegram::send_fetch_alert(&config.telegram, &result.fetch.errors).await
                    {
                        tracing::warn!(?err, "failed to send fetch alert");
                    }
                }
            }
            Err(err) => {
                tracing::error!(?err, "worker cycle failed");
                current_interval = (current_interval * 2).min(base_interval * 8);
                consecutive_successes = 0;
                let generic_error = pipeline::FetchSourceError {
                    source_type: "worker".to_string(),
                    source_value: "cycle".to_string(),
                    message: err.to_string(),
                };
                if let Err(alert_err) =
                    telegram::send_fetch_alert(&config.telegram, &[generic_error]).await
                {
                    tracing::warn!(?alert_err, "failed to send cycle failure alert");
                }
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

async fn warn_stale_tokens(pool: &PgPool) {
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

async fn run_daily_digest_scheduler(config: AppConfig, pool: PgPool) -> anyhow::Result<()> {
    tracing::info!(
        send_time = %config.daily_digest.send_time,
        timezone_offset_hours = config.daily_digest.timezone_offset_hours,
        "daily digest scheduler started"
    );
    loop {
        let now = chrono::Utc::now();
        let due = digest::daily_digest_due_now(now, &config.daily_digest)?;
        if due {
            match send_daily_digest_once(&config, &pool, now, false).await {
                Ok(sent) => {
                    if sent {
                        tracing::info!("daily digest delivered");
                    }
                    let next_due =
                        digest::next_daily_digest_due_at(chrono::Utc::now(), &config.daily_digest)?;
                    sleep_until(next_due).await;
                    continue;
                }
                Err(err) => {
                    tracing::warn!(?err, "daily digest attempt failed, will retry");
                    tokio::time::sleep(Duration::from_secs(900)).await;
                    continue;
                }
            }
        }

        let next_due = digest::next_daily_digest_due_at(now, &config.daily_digest)?;
        sleep_until(next_due).await;
    }
}

async fn sleep_until(when: chrono::DateTime<chrono::Utc>) {
    let now = chrono::Utc::now();
    let duration = when
        .signed_duration_since(now)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(0));
    tracing::info!(next_daily_digest_at = %when.to_rfc3339(), "sleeping until daily digest");
    tokio::time::sleep(duration).await;
}

pub async fn send_daily_digest_once(
    config: &AppConfig,
    pool: &PgPool,
    now: chrono::DateTime<chrono::Utc>,
    force: bool,
) -> anyhow::Result<bool> {
    let window = digest::daily_digest_window(now, &config.daily_digest)?;
    let channel = telegram::channel_id(&config.telegram)?;
    if !force && storage::daily_digest_delivered(pool, &window.digest_date, &channel).await? {
        tracing::debug!(
            digest_date = %window.digest_date,
            channel = %channel,
            "daily digest already delivered"
        );
        return Ok(false);
    }
    if force {
        tracing::info!(
            digest_date = %window.digest_date,
            channel = %channel,
            "forcing daily digest delivery"
        );
    }

    let generated =
        match digest::generate_daily_account_digest(pool, &config.daily_digest, now).await {
            Ok(value) => value,
            Err(err) => {
                storage::save_daily_digest_run(
                    pool,
                    storage::DailyDigestRunUpdate {
                        digest_date: &window.digest_date,
                        channel: &channel,
                        window_start: window.window_start,
                        window_end: window.window_end,
                        status: "error",
                        payload: &serde_json::json!({"stage": "generate"}),
                        error: Some(&err.to_string()),
                    },
                )
                .await?;
                return Err(err);
            }
        };

    let payload = serde_json::json!({
        "digest_date": generated.digest_date,
        "account_count": generated.account_count,
        "tweet_count": generated.tweet_count,
        "llm_error": generated.llm_error,
    });

    match telegram::send_daily_digest(&config.telegram, &generated.text).await {
        Ok(receipt) => {
            let mut payload = payload;
            payload["telegram"] = receipt;
            storage::save_daily_digest_run(
                pool,
                storage::DailyDigestRunUpdate {
                    digest_date: &generated.digest_date,
                    channel: &channel,
                    window_start: generated.window_start,
                    window_end: generated.window_end,
                    status: "delivered",
                    payload: &payload,
                    error: None,
                },
            )
            .await?;
            Ok(true)
        }
        Err(err) => {
            storage::save_daily_digest_run(
                pool,
                storage::DailyDigestRunUpdate {
                    digest_date: &generated.digest_date,
                    channel: &channel,
                    window_start: generated.window_start,
                    window_end: generated.window_end,
                    status: "error",
                    payload: &payload,
                    error: Some(&err.to_string()),
                },
            )
            .await?;
            Err(err)
        }
    }
}
