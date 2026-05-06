use crate::config::AppConfig;
use crate::pipeline::{self, FetchResult};
use crate::{channel, telegram::TelegramResult};
use serde::Serialize;
use sqlx::SqlitePool;
use std::time::Duration;

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

pub async fn run_forever(config: AppConfig, pool: SqlitePool) -> anyhow::Result<()> {
    loop {
        match run_once(&config, &pool).await {
            Ok(result) => tracing::info!(?result, "worker cycle complete"),
            Err(err) => tracing::error!(?err, "worker cycle failed"),
        }
        tokio::time::sleep(Duration::from_secs(config.fetch.interval_seconds)).await;
    }
}
