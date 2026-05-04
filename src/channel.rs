use crate::config::AppConfig;
use crate::models::StoredTweet;
use crate::storage::{self, delivery_payload};
use crate::telegram::TelegramChannel;
use serde::Serialize;
use serde_json::Value;
use sqlx::SqlitePool;
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

pub type ChannelSendFuture<'a> =
    Pin<Box<dyn Future<Output = anyhow::Result<ChannelSendReceipt>> + Send + 'a>>;

pub trait DeliveryChannel: Send + Sync {
    fn id(&self) -> String;
    fn send_all(&self) -> bool;
    fn send_tweet<'a>(&'a self, tweet: &'a StoredTweet) -> ChannelSendFuture<'a>;
}

pub fn configured_channels(config: &AppConfig) -> anyhow::Result<Vec<Box<dyn DeliveryChannel>>> {
    let mut channels: Vec<Box<dyn DeliveryChannel>> = Vec::new();
    if config.telegram.enabled {
        channels.push(Box::new(TelegramChannel::from_config(&config.telegram)?));
    }
    Ok(channels)
}

pub async fn send_undelivered(
    pool: &SqlitePool,
    channels: &[Box<dyn DeliveryChannel>],
    limit: i64,
) -> anyhow::Result<ChannelDeliveryResult> {
    let mut result = ChannelDeliveryResult {
        sent: 0,
        failed: 0,
        skipped: 0,
    };
    for channel in channels {
        let channel_id = channel.id();
        let tweets =
            storage::list_undelivered_tweets(pool, &channel_id, !channel.send_all(), limit).await?;
        for tweet in tweets {
            match channel.send_tweet(&tweet).await {
                Ok(receipt) => {
                    storage::save_delivery(
                        pool,
                        &tweet.tweet.tweet_id,
                        &channel_id,
                        "delivered",
                        &delivery_payload(&receipt.payload),
                        true,
                    )
                    .await?;
                    result.sent += 1;
                }
                Err(err) => {
                    storage::save_delivery(
                        pool,
                        &tweet.tweet.tweet_id,
                        &channel_id,
                        "error",
                        &serde_json::json!({"error": err.to_string()}),
                        false,
                    )
                    .await?;
                    result.failed += 1;
                }
            }
        }
    }
    Ok(result)
}
