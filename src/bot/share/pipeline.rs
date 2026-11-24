//! Share pipeline (Stage 4B): cache lookup → X fetch → persist → card send.
//!
//! The production processor behind the Stage 4A executor. For every
//! [`ShareRequest`] it runs, in order:
//!
//! 1. PostgreSQL cache lookup (`get_tweet`). A database error fails the
//!    request immediately — it is never treated as a cache miss, so a broken
//!    database can never trigger an X API fetch.
//! 2. Cache classification ([`classify_cached_tweet`]): hit / miss /
//!    incomplete.
//! 3. Cache hit: only `add_favorite` (no tweet UPSERT, no X request).
//!    Miss or incomplete: `fetch_single_tweet`, then
//!    `save_tweet_and_favorite` (single transaction).
//! 4. Render + send the tweet card to the requester's private `chat_id`.
//!
//! Telegram send failures leave the persisted favorite in place and surface
//! as request failures; a user may simply share the URL again.
//!
//! External effects are expressed as three narrow ports ([`ShareTweetStore`],
//! [`ShareTweetSource`], [`ShareCardSender`]) so tests can inject fakes
//! without touching the real X API, Telegram API, or database.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use super::executor::{ShareTaskFuture, ShareTaskProcessor, TryEnqueueError};
use super::ShareRequest;
use crate::channel::telegram::TelegramChannel;
use crate::config::AppConfig;
use crate::fetch;
use crate::models::{StoredTweet, Tweet};
use crate::storage::{self, FavoriteWriteResult};

/// Best-effort notice sent when the share queue is full.
pub const SHARE_BUSY_NOTICE: &str = "当前处理队列繁忙，请稍后重新分享。";

/// Hard bound on the busy-notice send so the poller cannot stall on a slow
/// Telegram API.
const SHARE_BUSY_NOTICE_TIMEOUT: Duration = Duration::from_secs(10);

/// Boxed future returned by the pipeline ports.
pub type SharePortFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Future returned by [`SharePipeline::handle`].
pub type SharePipelineFuture<'a> = SharePortFuture<'a, anyhow::Result<()>>;

// --- Ports ---

/// Persistence port: tweet cache and favorite writes.
pub trait ShareTweetStore: Send + Sync {
    /// Look up one cached tweet. `Ok(None)` means cache miss.
    fn get_tweet<'a>(
        &'a self,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<Option<StoredTweet>>>;

    /// Add the favorite for an authorized private chat. Idempotent; must not
    /// touch the tweet row.
    fn add_favorite<'a>(
        &'a self,
        chat_id: i64,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<bool>>;

    /// Atomically upsert the fetched tweet and add the favorite in one
    /// transaction.
    fn save_tweet_and_favorite<'a>(
        &'a self,
        chat_id: i64,
        tweet: &'a Tweet,
    ) -> SharePortFuture<'a, anyhow::Result<FavoriteWriteResult>>;
}

/// X fetch port.
pub trait ShareTweetSource: Send + Sync {
    fn fetch_single_tweet<'a>(
        &'a self,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<Tweet>>;
}

/// Telegram send port for the share pipeline.
pub trait ShareCardSender: Send + Sync {
    /// Render and send the tweet card to the given private chat.
    fn send_tweet_card<'a>(
        &'a self,
        chat_id: i64,
        tweet: &'a StoredTweet,
    ) -> SharePortFuture<'a, anyhow::Result<()>>;

    /// Best-effort busy notice; failures are logged by the caller.
    fn send_busy_notice<'a>(&'a self, chat_id: i64) -> SharePortFuture<'a, anyhow::Result<()>>;
}

// --- Production ports ---

/// PostgreSQL-backed store.
pub struct PgShareTweetStore {
    pool: PgPool,
}

impl PgShareTweetStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl ShareTweetStore for PgShareTweetStore {
    fn get_tweet<'a>(
        &'a self,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<Option<StoredTweet>>> {
        Box::pin(async move { storage::get_tweet(&self.pool, tweet_id).await })
    }

    fn add_favorite<'a>(
        &'a self,
        chat_id: i64,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move { storage::add_favorite(&self.pool, chat_id, tweet_id).await })
    }

    fn save_tweet_and_favorite<'a>(
        &'a self,
        chat_id: i64,
        tweet: &'a Tweet,
    ) -> SharePortFuture<'a, anyhow::Result<FavoriteWriteResult>> {
        Box::pin(async move { storage::save_tweet_and_favorite(&self.pool, chat_id, tweet).await })
    }
}

/// Real X web fetcher source.
pub struct XWebShareSource {
    config: AppConfig,
    pool: PgPool,
}

impl XWebShareSource {
    pub fn new(config: AppConfig, pool: PgPool) -> Self {
        Self { config, pool }
    }
}

impl ShareTweetSource for XWebShareSource {
    fn fetch_single_tweet<'a>(
        &'a self,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<Tweet>> {
        Box::pin(async move { fetch::fetch_single_tweet(&self.config, &self.pool, tweet_id).await })
    }
}

/// Telegram sender that composes the card through the shared channel
/// renderer and targets an explicit private chat. Never writes `deliveries`.
pub struct TelegramShareCardSender {
    channel: TelegramChannel,
}

impl TelegramShareCardSender {
    pub fn new(channel: TelegramChannel) -> Self {
        Self { channel }
    }
}

impl ShareCardSender for TelegramShareCardSender {
    fn send_tweet_card<'a>(
        &'a self,
        chat_id: i64,
        tweet: &'a StoredTweet,
    ) -> SharePortFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { self.channel.send_tweet_card_to_chat(chat_id, tweet).await })
    }

    fn send_busy_notice<'a>(&'a self, chat_id: i64) -> SharePortFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.channel
                .send_plain_text_to_chat(chat_id, SHARE_BUSY_NOTICE)
                .await
        })
    }
}

// --- Pipeline ---

/// Outcome of the cache classification.
#[derive(Debug, Clone)]
pub enum CacheLookup {
    /// Cached tweet can be rendered as-is; no X request is needed.
    Complete(StoredTweet),
    /// Cached row exists but cannot be rendered faithfully; refetch.
    Incomplete(StoredTweet),
    /// No cached row; refetch.
    Miss,
}

/// Classify a cache lookup result.
///
/// The renderer needs the identity fields plus a usable `raw` object:
/// media, quoted tweets and articles are all extracted from `raw`.
/// `storage::get_tweet` masks an unreadable `raw_json` into an empty JSON
/// object, so an empty raw object means the cached copy is damaged and must
/// be refetched instead of rendered. An empty `text` is legal (media-only
/// tweets) and is never treated as damage.
pub fn classify_cached_tweet(stored: Option<StoredTweet>) -> CacheLookup {
    let Some(stored) = stored else {
        return CacheLookup::Miss;
    };
    let raw_usable = stored
        .tweet
        .raw
        .as_object()
        .is_some_and(|raw| !raw.is_empty());
    let identity_usable = !stored.tweet.tweet_id.is_empty()
        && !stored.tweet.author_username.is_empty()
        && !stored.tweet.url.is_empty();
    if raw_usable && identity_usable {
        CacheLookup::Complete(stored)
    } else {
        CacheLookup::Incomplete(stored)
    }
}

/// The wired share pipeline: one implementation of each port.
pub struct SharePipeline {
    store: Arc<dyn ShareTweetStore>,
    source: Arc<dyn ShareTweetSource>,
    /// Exposed so the poller can send the queue-full busy notice without
    /// running pipeline logic.
    pub sender: Arc<dyn ShareCardSender>,
}

impl SharePipeline {
    pub fn new(
        store: Arc<dyn ShareTweetStore>,
        source: Arc<dyn ShareTweetSource>,
        sender: Arc<dyn ShareCardSender>,
    ) -> Self {
        Self {
            store,
            source,
            sender,
        }
    }

    /// Process one share request end to end.
    pub fn handle(&self, request: ShareRequest) -> SharePipelineFuture<'_> {
        Box::pin(self.process(request))
    }

    async fn process(&self, request: ShareRequest) -> anyhow::Result<()> {
        let chat_id = request.chat_id;
        let tweet_id = request.tweet_id;

        // Stage 1: cache lookup. A database error must never be treated as a
        // cache miss, so it fails the request before any X API call.
        let cached = match self.store.get_tweet(&tweet_id).await {
            Ok(cached) => cached,
            Err(err) => {
                tracing::error!(
                    chat_id,
                    tweet_id = %tweet_id,
                    stage = "cache_lookup",
                    reason = "db_error",
                    error = %err,
                    "share db lookup failed; request fails without any X fetch"
                );
                anyhow::bail!("share db lookup failed: {err}");
            }
        };

        match classify_cached_tweet(cached) {
            CacheLookup::Complete(stored) => {
                tracing::info!(
                    chat_id,
                    tweet_id = %tweet_id,
                    stage = "cache_lookup",
                    result = "hit",
                    "share cache hit; reusing stored tweet"
                );
                // Cache hit: only the favorite is written; the tweet row is
                // never UPSERTed and X is never called.
                if let Err(err) = self.store.add_favorite(chat_id, &tweet_id).await {
                    tracing::error!(
                        chat_id,
                        tweet_id = %tweet_id,
                        stage = "favorite",
                        reason = "save_failed",
                        error = %err,
                        "share favorite failed on cache hit; no card was sent"
                    );
                    return Err(err);
                }
                self.deliver(chat_id, &stored).await
            }
            CacheLookup::Miss => {
                tracing::info!(
                    chat_id,
                    tweet_id = %tweet_id,
                    stage = "cache_lookup",
                    result = "miss",
                    "share cache miss; fetching from X"
                );
                self.fetch_save_deliver(chat_id, &tweet_id).await
            }
            CacheLookup::Incomplete(stored) => {
                tracing::warn!(
                    chat_id,
                    tweet_id = %tweet_id,
                    stage = "cache_lookup",
                    result = "incomplete",
                    "cached tweet cannot be rendered (unreadable raw_json or missing identity fields); refetching from X"
                );
                let _ = stored;
                self.fetch_save_deliver(chat_id, &tweet_id).await
            }
        }
    }

    /// Cache miss / incomplete path: fetch from X, persist tweet + favorite
    /// in one transaction, then deliver. Any failure before delivery leaves
    /// nothing pretending success.
    async fn fetch_save_deliver(&self, chat_id: i64, tweet_id: &str) -> anyhow::Result<()> {
        let tweet = match self.source.fetch_single_tweet(tweet_id).await {
            Ok(tweet) => tweet,
            Err(err) => {
                tracing::error!(
                    chat_id,
                    tweet_id = %tweet_id,
                    stage = "x_fetch",
                    reason = "fetch_failed",
                    error = %err,
                    "X fetch failed; nothing was favorited and no card was sent"
                );
                return Err(err);
            }
        };

        match self.store.save_tweet_and_favorite(chat_id, &tweet).await {
            Ok(write) => {
                tracing::info!(
                    chat_id,
                    tweet_id = %tweet.tweet_id,
                    stage = "save",
                    tweet_inserted = write.tweet_inserted,
                    favorite_inserted = write.favorite_inserted,
                    "share tweet and favorite persisted in one transaction"
                );
            }
            Err(err) => {
                tracing::error!(
                    chat_id,
                    tweet_id = %tweet_id,
                    stage = "save",
                    reason = "save_failed",
                    error = %err,
                    "share persist failed; no card was sent"
                );
                return Err(err);
            }
        }

        self.deliver(chat_id, &StoredTweet { tweet }).await
    }

    /// Render and send the card. A send failure keeps the persisted favorite
    /// and surfaces as a request failure; the user can share the URL again.
    async fn deliver(&self, chat_id: i64, stored: &StoredTweet) -> anyhow::Result<()> {
        if let Err(err) = self.sender.send_tweet_card(chat_id, stored).await {
            tracing::error!(
                chat_id,
                tweet_id = %stored.tweet.tweet_id,
                stage = "telegram_send",
                reason = "send_failed",
                error = %err,
                "share card send failed; saved favorite is kept"
            );
            return Err(err);
        }
        tracing::info!(
            chat_id,
            tweet_id = %stored.tweet.tweet_id,
            stage = "telegram_send",
            status = "sent",
            "share card delivered to private chat"
        );
        Ok(())
    }
}

/// Production processor wiring the pipeline into the Stage 4A executor.
/// The executor keeps its single-worker, bounded-queue, panic-isolated model.
pub(crate) fn share_pipeline_processor(pipeline: Arc<SharePipeline>) -> ShareTaskProcessor {
    Arc::new(move |request: ShareRequest| {
        let pipeline = Arc::clone(&pipeline);
        Box::pin(async move { pipeline.handle(request).await }) as ShareTaskFuture
    })
}

/// Build the production pipeline from app config and pool.
pub(crate) fn build_share_pipeline(
    config: &AppConfig,
    pool: &PgPool,
) -> anyhow::Result<Arc<SharePipeline>> {
    let channel =
        TelegramChannel::from_config(&config.telegram, &config.comments, &config.translation)?;
    Ok(Arc::new(SharePipeline::new(
        Arc::new(PgShareTweetStore::new(pool.clone())),
        Arc::new(XWebShareSource::new(config.clone(), pool.clone())),
        Arc::new(TelegramShareCardSender::new(channel)),
    )))
}

/// React to a rejected enqueue: structured warn plus a best-effort busy
/// notice for `QueueFull` only. `Closed` never notifies the user — the
/// process is shutting down and the request is already logged. The notice is
/// time-bounded so the poller cannot stall on a slow Telegram API; a failed
/// or timed-out notice is logged and otherwise ignored.
pub async fn handle_enqueue_rejection(sender: &dyn ShareCardSender, error: &TryEnqueueError) {
    match error {
        TryEnqueueError::QueueFull(request) => {
            tracing::warn!(
                chat_id = request.chat_id,
                tweet_id = %request.tweet_id,
                status = "queue_full",
                "share queue is full; request was not accepted, sending busy notice"
            );
            let notice = tokio::time::timeout(
                SHARE_BUSY_NOTICE_TIMEOUT,
                sender.send_busy_notice(request.chat_id),
            );
            match notice.await {
                Ok(Ok(())) => {
                    tracing::info!(chat_id = request.chat_id, "share busy notice sent");
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        chat_id = request.chat_id,
                        error = %err,
                        "share busy notice send failed; ignored"
                    );
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        chat_id = request.chat_id,
                        "share busy notice timed out; ignored"
                    );
                }
            }
        }
        TryEnqueueError::Closed(request) => {
            tracing::warn!(
                chat_id = request.chat_id,
                tweet_id = %request.tweet_id,
                status = "executor_closed",
                "share executor is shut down; request was not accepted, no busy notice sent"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_tweet(tweet_id: &str, text: &str) -> Tweet {
        Tweet {
            tweet_id: tweet_id.to_string(),
            source_type: crate::models::SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "openai".to_string(),
            author_name: "OpenAI".to_string(),
            text: text.to_string(),
            url: format!("https://x.com/openai/status/{tweet_id}"),
            created_at: chrono::Utc::now(),
            fetched_at: chrono::Utc::now(),
            raw: json!({"version": text}),
        }
    }

    fn stored_tweet(tweet_id: &str, text: &str) -> StoredTweet {
        StoredTweet {
            tweet: sample_tweet(tweet_id, text),
        }
    }

    fn share_request(tweet_id: &str) -> ShareRequest {
        ShareRequest {
            chat_id: 7,
            tweet_id: tweet_id.to_string(),
            canonical_url: format!("https://x.com/i/status/{tweet_id}"),
        }
    }

    // --- classify_cached_tweet ---

    #[test]
    fn classifies_none_as_miss() {
        assert!(matches!(classify_cached_tweet(None), CacheLookup::Miss));
    }

    #[test]
    fn classifies_complete_cached_tweet_as_hit() {
        let stored = stored_tweet("100", "hello");
        assert!(matches!(
            classify_cached_tweet(Some(stored)),
            CacheLookup::Complete(_)
        ));
    }

    #[test]
    fn classifies_empty_raw_as_incomplete() {
        let mut tweet = sample_tweet("110", "text");
        tweet.raw = json!({});
        assert!(matches!(
            classify_cached_tweet(Some(StoredTweet { tweet })),
            CacheLookup::Incomplete(_)
        ));
    }

    #[tokio::test]
    async fn cache_hit_calls_add_favorite_only() {
        let store = Arc::new(FakeStore {
            cached: Some(stored_tweet("100", "hello")),
            ..FakeStore::default()
        });
        let source = Arc::new(FakeSource::failing());
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("100"))
            .await
            .expect("hit path succeeds");

        assert_eq!(store.calls(), vec!["get_tweet", "add_favorite"]);
        assert_eq!(source.call_count(), 0);
        assert_eq!(sender.cards(), vec![(7, "100".to_string())]);
        assert!(sender.busy().is_empty());
    }

    #[tokio::test]
    async fn cache_miss_fetches_saves_and_sends_in_order() {
        let store = Arc::new(FakeStore::default());
        let source = Arc::new(FakeSource::returning(sample_tweet("200", "fresh")));
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("200"))
            .await
            .expect("miss path succeeds");

        assert_eq!(store.calls(), vec!["get_tweet", "save_tweet_and_favorite"]);
        assert_eq!(source.call_count(), 1);
        assert_eq!(sender.cards(), vec![(7, "200".to_string())]);
    }

    #[tokio::test]
    async fn incomplete_cache_refetches_and_saves() {
        let store = Arc::new(FakeStore {
            cached: Some(StoredTweet {
                tweet: Tweet {
                    raw: json!({}), // masked unreadable raw_json
                    ..sample_tweet("300", "")
                },
            }),
            ..FakeStore::default()
        });
        let source = Arc::new(FakeSource::returning(sample_tweet("300", "refetched")));
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("300"))
            .await
            .expect("incomplete path succeeds");

        assert_eq!(store.calls(), vec!["get_tweet", "save_tweet_and_favorite"]);
        assert_eq!(source.call_count(), 1);
        assert_eq!(sender.cards().len(), 1);
    }

    #[tokio::test]
    async fn empty_text_with_media_raw_is_a_cache_hit_not_damage() {
        let mut tweet = sample_tweet("400", "");
        tweet.raw = json!({"legacy": {"extended_entities": {"media": []}}});
        let store = Arc::new(FakeStore {
            cached: Some(StoredTweet { tweet }),
            ..FakeStore::default()
        });
        let source = Arc::new(FakeSource::failing());
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("400"))
            .await
            .expect("media-only hit succeeds");

        assert_eq!(source.call_count(), 0);
        assert_eq!(store.calls(), vec!["get_tweet", "add_favorite"]);
    }

    #[tokio::test]
    async fn db_error_fails_without_x_fetch_or_send() {
        let store = Arc::new(FakeStore {
            fail_get: true,
            ..FakeStore::default()
        });
        let source = Arc::new(FakeSource::returning(sample_tweet("500", "never used")));
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        let err = pipeline
            .handle(share_request("500"))
            .await
            .expect_err("db error must fail the request");
        assert!(err.to_string().contains("db lookup failed"));

        assert_eq!(source.call_count(), 0, "db error must not reach X");
        assert!(sender.cards().is_empty());
        assert_eq!(store.calls(), vec!["get_tweet"]);
    }

    #[tokio::test]
    async fn x_fetch_failure_saves_nothing_and_sends_nothing() {
        let store = Arc::new(FakeStore::default());
        let source = Arc::new(FakeSource::failing());
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("600"))
            .await
            .expect_err("fetch failure must fail the request");

        assert_eq!(store.calls(), vec!["get_tweet"], "no favorite, no save");
        assert_eq!(store.favorite_count(), 0);
        assert!(sender.cards().is_empty());
    }

    #[tokio::test]
    async fn save_failure_does_not_send() {
        let store = Arc::new(FakeStore {
            fail_save: true,
            ..FakeStore::default()
        });
        let source = Arc::new(FakeSource::returning(sample_tweet("700", "fresh")));
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("700"))
            .await
            .expect_err("save failure must fail the request");

        assert!(sender.cards().is_empty());
    }

    #[tokio::test]
    async fn send_failure_keeps_persisted_favorite_and_fails_request() {
        let store = Arc::new(FakeStore::default());
        let source = Arc::new(FakeSource::returning(sample_tweet("800", "fresh")));
        let sender = Arc::new(FakeSender {
            fail_cards: true,
            ..FakeSender::default()
        });
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("800"))
            .await
            .expect_err("send failure must fail the request");

        assert_eq!(store.favorite_count(), 1, "favorite must be kept");
    }

    #[tokio::test]
    async fn repeat_share_sends_card_again() {
        let store = Arc::new(FakeStore {
            cached: Some(stored_tweet("900", "hello")),
            ..FakeStore::default()
        });
        let source = Arc::new(FakeSource::failing());
        let sender = Arc::new(FakeSender::default());
        let pipeline = SharePipeline::new(store.clone(), source.clone(), sender.clone());

        pipeline
            .handle(share_request("900"))
            .await
            .expect("first share ok");
        pipeline
            .handle(share_request("900"))
            .await
            .expect("repeat share ok");

        assert_eq!(sender.cards().len(), 2, "cards may be sent repeatedly");
        assert_eq!(
            store.calls(),
            vec!["get_tweet", "add_favorite", "get_tweet", "add_favorite"]
        );
    }

    #[tokio::test]
    async fn queue_full_sends_busy_notice_to_requester() {
        let sender = FakeSender::default();
        handle_enqueue_rejection(&sender, &TryEnqueueError::QueueFull(share_request("950"))).await;
        assert_eq!(sender.busy(), vec![7]);
        assert!(sender.cards().is_empty());
    }

    #[tokio::test]
    async fn closed_rejection_does_not_send_busy_notice() {
        let sender = FakeSender::default();
        handle_enqueue_rejection(&sender, &TryEnqueueError::Closed(share_request("960"))).await;
        assert!(sender.busy().is_empty());
        assert!(sender.cards().is_empty());
    }

    // --- Fakes ---

    #[derive(Default)]
    struct FakeStore {
        cached: Option<StoredTweet>,
        fail_get: bool,
        fail_save: bool,
        calls: std::sync::Mutex<Vec<&'static str>>,
        favorites: std::sync::atomic::AtomicUsize,
    }

    impl FakeStore {
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }

        fn favorite_count(&self) -> usize {
            self.favorites.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl ShareTweetStore for FakeStore {
        fn get_tweet<'a>(
            &'a self,
            _tweet_id: &'a str,
        ) -> SharePortFuture<'a, anyhow::Result<Option<StoredTweet>>> {
            Box::pin(async move {
                self.record("get_tweet");
                if self.fail_get {
                    anyhow::bail!("injected db failure");
                }
                Ok(self.cached.clone())
            })
        }

        fn add_favorite<'a>(
            &'a self,
            _chat_id: i64,
            _tweet_id: &'a str,
        ) -> SharePortFuture<'a, anyhow::Result<bool>> {
            Box::pin(async move {
                self.record("add_favorite");
                self.favorites
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(true)
            })
        }

        fn save_tweet_and_favorite<'a>(
            &'a self,
            _chat_id: i64,
            _tweet: &'a Tweet,
        ) -> SharePortFuture<'a, anyhow::Result<FavoriteWriteResult>> {
            Box::pin(async move {
                self.record("save_tweet_and_favorite");
                if self.fail_save {
                    anyhow::bail!("injected save failure");
                }
                self.favorites
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(FavoriteWriteResult {
                    tweet_inserted: true,
                    favorite_inserted: true,
                })
            })
        }
    }

    impl FakeStore {
        fn record(&self, call: &'static str) {
            self.calls.lock().unwrap().push(call);
        }
    }

    struct FakeSource {
        tweet: Option<Tweet>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl FakeSource {
        fn returning(tweet: Tweet) -> Self {
            Self {
                tweet: Some(tweet),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn failing() -> Self {
            Self {
                tweet: None,
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl ShareTweetSource for FakeSource {
        fn fetch_single_tweet<'a>(
            &'a self,
            _tweet_id: &'a str,
        ) -> SharePortFuture<'a, anyhow::Result<Tweet>> {
            Box::pin(async move {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                match &self.tweet {
                    Some(tweet) => Ok(tweet.clone()),
                    None => anyhow::bail!("injected X fetch failure"),
                }
            })
        }
    }

    #[derive(Default)]
    struct FakeSender {
        cards: std::sync::Mutex<Vec<(i64, String)>>,
        busy: std::sync::Mutex<Vec<i64>>,
        fail_cards: bool,
    }

    impl FakeSender {
        fn cards(&self) -> Vec<(i64, String)> {
            self.cards.lock().unwrap().clone()
        }

        fn busy(&self) -> Vec<i64> {
            self.busy.lock().unwrap().clone()
        }
    }

    impl ShareCardSender for FakeSender {
        fn send_tweet_card<'a>(
            &'a self,
            chat_id: i64,
            tweet: &'a StoredTweet,
        ) -> SharePortFuture<'a, anyhow::Result<()>> {
            Box::pin(async move {
                if self.fail_cards {
                    anyhow::bail!("injected telegram failure");
                }
                self.cards
                    .lock()
                    .unwrap()
                    .push((chat_id, tweet.tweet.tweet_id.clone()));
                Ok(())
            })
        }

        fn send_busy_notice<'a>(&'a self, chat_id: i64) -> SharePortFuture<'a, anyhow::Result<()>> {
            Box::pin(async move {
                self.busy.lock().unwrap().push(chat_id);
                Ok(())
            })
        }
    }
}
