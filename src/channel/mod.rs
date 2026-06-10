pub mod telegram;

use crate::config::AppConfig;
use crate::models::StoredTweet;
use crate::storage::{self, delivery_payload};
use self::telegram::TelegramChannel;
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChannelDeliveryResult {
    pub sent: i64,
    pub failed: i64,
    pub skipped: i64,
}

#[derive(Debug, Clone)]
pub struct ChannelSendReceipt {
    pub payload: Value,
}

#[derive(Debug)]
pub enum DeliveryError {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

pub type ChannelSendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ChannelSendReceipt, DeliveryError>> + Send + 'a>>;

pub trait DeliveryChannel: Send + Sync {
    fn id(&self) -> String;
    fn send_all(&self) -> bool;
    fn send_tweet<'a>(&'a self, tweet: &'a StoredTweet) -> ChannelSendFuture<'a>;
}

pub fn configured_channels(config: &AppConfig) -> anyhow::Result<Vec<Box<dyn DeliveryChannel>>> {
    let mut channels: Vec<Box<dyn DeliveryChannel>> = Vec::new();
    if config.telegram.enabled {
        channels.push(Box::new(TelegramChannel::from_config(&config.telegram, &config.comments)?));
    }
    Ok(channels)
}

pub async fn send_undelivered(
    pool: &PgPool,
    channels: &[Box<dyn DeliveryChannel>],
    limit: i64,
    max_retries: i64,
) -> anyhow::Result<ChannelDeliveryResult> {
    let mut result = ChannelDeliveryResult {
        sent: 0,
        failed: 0,
        skipped: 0,
    };
    for channel in channels {
        let channel_id = channel.id();
        let tweets =
            storage::list_undelivered_tweets(pool, &channel_id, !channel.send_all(), limit, max_retries).await?;
        for tweet in tweets {
            match channel.send_tweet(&tweet).await {
                Ok(receipt) => {
                    if let Err(err) = storage::save_delivery(
                        pool,
                        &tweet.tweet.tweet_id,
                        &channel_id,
                        "delivered",
                        &delivery_payload(&receipt.payload),
                        true,
                    )
                    .await
                    {
                        tracing::error!(
                            tweet_id = %tweet.tweet.tweet_id,
                            channel = %channel_id,
                            ?err,
                            "failed to save delivery record"
                        );
                        result.skipped += 1;
                    } else {
                        result.sent += 1;
                    }
                }
                Err(DeliveryError::Permanent(err)) => {
                    tracing::warn!(
                        tweet_id = %tweet.tweet.tweet_id,
                        channel = %channel_id,
                        ?err,
                        "permanent delivery failure, marking as dead"
                    );
                    if let Err(err2) = storage::save_delivery(
                        pool,
                        &tweet.tweet.tweet_id,
                        &channel_id,
                        "dead",
                        &serde_json::json!({"error": err.to_string()}),
                        false,
                    )
                    .await
                    {
                        tracing::error!(
                            tweet_id = %tweet.tweet.tweet_id,
                            channel = %channel_id,
                            ?err2,
                            "failed to save dead delivery record"
                        );
                        result.skipped += 1;
                    } else {
                        result.failed += 1;
                    }
                }
                Err(DeliveryError::Transient(err)) => {
                    tracing::warn!(
                        tweet_id = %tweet.tweet.tweet_id,
                        channel = %channel_id,
                        ?err,
                        "transient delivery failure"
                    );
                    if let Err(err2) = storage::save_delivery(
                        pool,
                        &tweet.tweet.tweet_id,
                        &channel_id,
                        "error",
                        &serde_json::json!({"error": err.to_string()}),
                        false,
                    )
                    .await
                    {
                        tracing::error!(
                            tweet_id = %tweet.tweet.tweet_id,
                            channel = %channel_id,
                            ?err2,
                            "failed to save error delivery record"
                        );
                        result.skipped += 1;
                    } else {
                        result.failed += 1;
                    }
                }
            }
        }
    }
    Ok(result)
}
