mod support;

use chrono::{TimeZone, Utc};
use std::sync::Arc;
use support::TestDatabase;
use tempfile::tempdir;
use tokio::sync::Barrier;
use tokio::time::{timeout, Duration};
use xflow::channel::{self, ChannelSendFuture, ChannelSendReceipt, DeliveryChannel};
use xflow::config::{load_config, AppConfig};
use xflow::fetch::auth;
use xflow::models::{Source, SourceType, StoredTweet, Tweet};
use xflow::server::rss_feed;
use xflow::storage::db;
use xflow::storage::{self, TokenImport, TweetFilter};
use xflow::worker;
use xflow::worker::pipeline;

async fn test_pool() -> TestDatabase {
    TestDatabase::new(5).await
}

fn test_tweet(tweet_id: &str, text: &str) -> Tweet {
    Tweet {
        tweet_id: tweet_id.to_string(),
        source_type: SourceType::Account,
        source_value: "openai".to_string(),
        author_username: "openai".to_string(),
        author_name: "OpenAI".to_string(),
        text: text.to_string(),
        url: format!("https://x.com/openai/status/{tweet_id}"),
        created_at: Utc::now(),
        fetched_at: Utc::now(),
        raw: serde_json::json!({"version": text}),
    }
}

#[tokio::test]
async fn favorite_insert_is_idempotent_and_preserves_first_created_at() {
    let pool = test_pool().await;
    let tweet = test_tweet("favorite-1", "first");
    storage::upsert_tweet(&pool, &tweet).await.unwrap();

    assert!(storage::add_favorite(&pool, 7, &tweet.tweet_id)
        .await
        .unwrap());
    let first_created_at: chrono::DateTime<Utc> = sqlx::query_scalar(
        "SELECT created_at FROM telegram_favorites WHERE chat_id = 7 AND tweet_id = $1",
    )
    .bind(&tweet.tweet_id)
    .fetch_one(pool.pg_pool())
    .await
    .unwrap();

    assert!(!storage::add_favorite(&pool, 7, &tweet.tweet_id)
        .await
        .unwrap());
    let second_created_at: chrono::DateTime<Utc> = sqlx::query_scalar(
        "SELECT created_at FROM telegram_favorites WHERE chat_id = 7 AND tweet_id = $1",
    )
    .bind(&tweet.tweet_id)
    .fetch_one(pool.pg_pool())
    .await
    .unwrap();
    assert_eq!(first_created_at, second_created_at);
    assert_eq!(
        storage::list_favorites(&pool, 7, 10, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn concurrent_favorite_inserts_create_one_row() {
    let pool = test_pool().await;
    let tweet = test_tweet("favorite-concurrent", "concurrent");
    storage::upsert_tweet(&pool, &tweet).await.unwrap();

    let (first, second) = tokio::join!(
        storage::add_favorite(&pool, 7, &tweet.tweet_id),
        storage::add_favorite(&pool, 7, &tweet.tweet_id)
    );
    let inserted = [first.unwrap(), second.unwrap()]
        .into_iter()
        .filter(|value| *value)
        .count();
    assert_eq!(inserted, 1);
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telegram_favorites WHERE chat_id = 7 AND tweet_id = $1",
    )
    .bind(&tweet.tweet_id)
    .fetch_one(pool.pg_pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn different_chats_can_favorite_the_same_tweet() {
    let pool = test_pool().await;
    let tweet = test_tweet("favorite-two-chats", "shared");
    storage::upsert_tweet(&pool, &tweet).await.unwrap();
    assert!(storage::add_favorite(&pool, 7, &tweet.tweet_id)
        .await
        .unwrap());
    assert!(storage::add_favorite(&pool, 8, &tweet.tweet_id)
        .await
        .unwrap());
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telegram_favorites WHERE tweet_id = $1")
            .bind(&tweet.tweet_id)
            .fetch_one(pool.pg_pool())
            .await
            .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn favorite_foreign_key_rejects_missing_tweet_and_cascades_delete() {
    let pool = test_pool().await;
    assert!(storage::add_favorite(&pool, 7, "missing-tweet")
        .await
        .is_err());

    let tweet = test_tweet("favorite-cascade", "cascade");
    storage::upsert_tweet(&pool, &tweet).await.unwrap();
    storage::add_favorite(&pool, 7, &tweet.tweet_id)
        .await
        .unwrap();
    sqlx::query("DELETE FROM tweets WHERE tweet_id = $1")
        .bind(&tweet.tweet_id)
        .execute(pool.pg_pool())
        .await
        .unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telegram_favorites WHERE tweet_id = $1")
            .bind(&tweet.tweet_id)
            .fetch_one(pool.pg_pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn favorites_use_descending_keyset_pagination() {
    let pool = test_pool().await;
    for (tweet_id, created_at) in [
        ("page-a", "2026-01-01T00:00:01Z"),
        // Equal timestamps exercise the tweet_id tie-breaker in the cursor.
        ("page-b", "2026-01-01T00:00:03Z"),
        ("page-c", "2026-01-01T00:00:03Z"),
    ] {
        storage::upsert_tweet(&pool, &test_tweet(tweet_id, tweet_id))
            .await
            .unwrap();
        storage::add_favorite(&pool, 7, tweet_id).await.unwrap();
        sqlx::query(
            "UPDATE telegram_favorites SET created_at = $1::timestamptz WHERE chat_id = 7 AND tweet_id = $2",
        )
        .bind(created_at)
        .bind(tweet_id)
        .execute(pool.pg_pool())
        .await
        .unwrap();
    }

    let first = storage::list_favorites(&pool, 7, 2, None).await.unwrap();
    assert_eq!(
        first
            .iter()
            .map(|favorite| favorite.tweet.tweet.tweet_id.as_str())
            .collect::<Vec<_>>(),
        vec!["page-c", "page-b"]
    );
    let second = storage::list_favorites(&pool, 7, 2, Some(&first[1].cursor()))
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].tweet.tweet.tweet_id, "page-a");
}

#[tokio::test]
async fn tweet_upsert_and_backfill_summary_distinguish_insert_from_update() {
    let pool = test_pool().await;
    let first = test_tweet("upsert-1", "v1");
    assert!(storage::upsert_tweet(&pool, &first).await.unwrap());

    let mut updated = first.clone();
    updated.fetched_at = Utc::now() + chrono::Duration::seconds(1);
    updated.raw = serde_json::json!({"version": "v2"});
    assert!(!storage::upsert_tweet(&pool, &updated).await.unwrap());
    let stored = storage::get_tweet(&pool, &first.tweet_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.tweet.raw, serde_json::json!({"version": "v2"}));

    let batch = vec![test_tweet("backfill-a", "a"), test_tweet("backfill-b", "b")];
    let initial = storage::upsert_tweets(&pool, &batch).await.unwrap();
    assert_eq!((initial.total, initial.new, initial.duplicate), (2, 2, 0));
    let repeated = storage::upsert_tweets(&pool, &batch).await.unwrap();
    assert_eq!(
        (repeated.total, repeated.new, repeated.duplicate),
        (2, 0, 2)
    );
}

#[tokio::test]
async fn concurrent_upserts_for_same_tweet_report_one_insert_without_deadlock() {
    let pool = test_pool().await;
    let barrier = Arc::new(Barrier::new(3));
    let mut first = test_tweet("upsert-concurrent-same", "first");
    first.raw = serde_json::json!({"version": "first"});
    let mut second = test_tweet("upsert-concurrent-same", "second");
    second.raw = serde_json::json!({"version": "second"});

    let first_pool = pool.pg_pool().clone();
    let first_barrier = barrier.clone();
    let first_task = tokio::spawn(async move {
        first_barrier.wait().await;
        storage::upsert_tweet(&first_pool, &first).await
    });
    let second_pool = pool.pg_pool().clone();
    let second_barrier = barrier.clone();
    let second_task = tokio::spawn(async move {
        second_barrier.wait().await;
        storage::upsert_tweet(&second_pool, &second).await
    });
    barrier.wait().await;

    let (first_result, second_result) = timeout(Duration::from_secs(5), async {
        (first_task.await.unwrap(), second_task.await.unwrap())
    })
    .await
    .expect("same-id UPSERTs must not deadlock");
    let inserted = [first_result.unwrap(), second_result.unwrap()]
        .into_iter()
        .filter(|inserted| *inserted)
        .count();
    assert_eq!(inserted, 1);
    let stored = storage::get_tweet(&pool, "upsert-concurrent-same")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        stored.tweet.raw["version"].as_str(),
        Some("first" | "second")
    ));
}

#[tokio::test]
async fn concurrent_upserts_for_different_tweets_complete_independently() {
    let pool = test_pool().await;
    let barrier = Arc::new(Barrier::new(3));
    let first_pool = pool.pg_pool().clone();
    let first_barrier = barrier.clone();
    let first_task = tokio::spawn(async move {
        first_barrier.wait().await;
        storage::upsert_tweet(&first_pool, &test_tweet("upsert-concurrent-a", "a")).await
    });
    let second_pool = pool.pg_pool().clone();
    let second_barrier = barrier.clone();
    let second_task = tokio::spawn(async move {
        second_barrier.wait().await;
        storage::upsert_tweet(&second_pool, &test_tweet("upsert-concurrent-b", "b")).await
    });
    barrier.wait().await;

    let (first, second) = timeout(Duration::from_secs(5), async {
        (first_task.await.unwrap(), second_task.await.unwrap())
    })
    .await
    .expect("different-id UPSERTs must not block indefinitely");
    assert!(first.unwrap());
    assert!(second.unwrap());
}

#[tokio::test]
async fn concurrent_tweet_and_favorite_transactions_are_idempotent() {
    let pool = test_pool().await;
    let tweet = test_tweet("transaction-concurrent", "shared");
    let barrier = Arc::new(Barrier::new(3));

    let first_pool = pool.pg_pool().clone();
    let first_tweet = tweet.clone();
    let first_barrier = barrier.clone();
    let first_task = tokio::spawn(async move {
        first_barrier.wait().await;
        storage::save_tweet_and_favorite(&first_pool, 7, &first_tweet).await
    });
    let second_pool = pool.pg_pool().clone();
    let second_barrier = barrier.clone();
    let second_task = tokio::spawn(async move {
        second_barrier.wait().await;
        storage::save_tweet_and_favorite(&second_pool, 7, &tweet).await
    });
    barrier.wait().await;

    let (first, second) = timeout(Duration::from_secs(5), async {
        (first_task.await.unwrap(), second_task.await.unwrap())
    })
    .await
    .expect("concurrent Tweet + Favorite transactions must not deadlock");
    let results = [first.unwrap(), second.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|result| result.tweet_inserted)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result.favorite_inserted)
            .count(),
        1
    );
    assert_eq!(
        storage::list_favorites(&pool, 7, 10, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn batch_upsert_handles_duplicate_ids_and_keeps_last_payload() {
    let pool = test_pool().await;
    let batch = vec![
        test_tweet("batch-duplicate", "first"),
        test_tweet("batch-unique", "unique"),
        test_tweet("batch-duplicate", "last"),
    ];
    let initial = storage::upsert_tweets(&pool, &batch).await.unwrap();
    assert_eq!((initial.total, initial.new, initial.duplicate), (3, 2, 1));
    assert_eq!(
        storage::get_tweet(&pool, "batch-duplicate")
            .await
            .unwrap()
            .unwrap()
            .tweet
            .raw["version"],
        "last"
    );

    let repeated = storage::upsert_tweets(&pool, &batch).await.unwrap();
    assert_eq!(
        (repeated.total, repeated.new, repeated.duplicate),
        (3, 0, 3)
    );
}

#[tokio::test]
async fn overlapping_batches_use_consistent_lock_order() {
    let pool = test_pool().await;
    let first = vec![
        test_tweet("batch-lock-a", "a1"),
        test_tweet("batch-lock-b", "b1"),
    ];
    let second = vec![
        test_tweet("batch-lock-b", "b2"),
        test_tweet("batch-lock-a", "a2"),
    ];
    let barrier = Arc::new(Barrier::new(3));
    let first_pool = pool.pg_pool().clone();
    let first_barrier = barrier.clone();
    let first_task = tokio::spawn(async move {
        first_barrier.wait().await;
        storage::upsert_tweets(&first_pool, &first).await
    });
    let second_pool = pool.pg_pool().clone();
    let second_barrier = barrier.clone();
    let second_task = tokio::spawn(async move {
        second_barrier.wait().await;
        storage::upsert_tweets(&second_pool, &second).await
    });
    barrier.wait().await;

    timeout(Duration::from_secs(5), async {
        first_task.await.unwrap().unwrap();
        second_task.await.unwrap().unwrap();
    })
    .await
    .expect("overlapping batches must not deadlock");
}

#[tokio::test]
async fn tweet_and_favorite_transaction_is_idempotent_and_rolls_back_on_failure() {
    let pool = test_pool().await;
    let tweet = test_tweet("transaction-ok", "ok");
    let first = storage::save_tweet_and_favorite(&pool, 7, &tweet)
        .await
        .unwrap();
    assert!(first.tweet_inserted);
    assert!(first.favorite_inserted);
    let repeated = storage::save_tweet_and_favorite(&pool, 7, &tweet)
        .await
        .unwrap();
    assert!(!repeated.tweet_inserted);
    assert!(!repeated.favorite_inserted);

    sqlx::query(
        "ALTER TABLE telegram_favorites ADD CONSTRAINT reject_negative_chat CHECK (chat_id >= 0)",
    )
    .execute(pool.pg_pool())
    .await
    .unwrap();
    let rejected = test_tweet("transaction-rollback", "rollback");
    assert!(storage::save_tweet_and_favorite(&pool, -1, &rejected)
        .await
        .is_err());
    assert!(storage::get_tweet(&pool, &rejected.tweet_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn database_initialization_is_repeatable_with_favorite_schema() {
    let pool = test_pool().await;
    db::init_db(&pool).await.unwrap();
    db::init_db(&pool).await.unwrap();
    let data_type: String = sqlx::query_scalar(
        r#"
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'telegram_favorites'
          AND column_name = 'created_at'
        "#,
    )
    .fetch_one(pool.pg_pool())
    .await
    .unwrap();
    assert_eq!(data_type, "timestamp with time zone");
}

#[test]
fn config_loader_accepts_partial_yaml() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    std::fs::write(
        &path,
        r#"
storage:
  database_url: postgres://localhost/xflow
agent:
  enabled: true
  importance_threshold: 0.45
  push_threshold: 0.7
"#,
    )
    .unwrap();
    let config = load_config(&path).unwrap();
    assert_eq!(config.server.port, 8000);
    assert!(config.storage.database_url.contains("postgres"));
    assert!(!config.daily_digest.enabled);
    assert_eq!(config.daily_digest.send_time, "18:00");
}

#[test]
fn token_validation_rejects_bad_shapes() {
    let mut token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: None,
    };
    assert!(auth::validate_token(&token).is_ok());
    token.label = " ".to_string();
    assert!(auth::validate_token(&token)
        .unwrap_err()
        .to_string()
        .contains("label"));
    token.label = "account1".to_string();
    token.auth_token = "short".to_string();
    assert!(auth::validate_token(&token)
        .unwrap_err()
        .to_string()
        .contains("auth_token"));
    token.auth_token = "abcd1234efgh".to_string();
    token.ct0 = "x".to_string();
    assert!(auth::validate_token(&token)
        .unwrap_err()
        .to_string()
        .contains("ct0"));
}

#[tokio::test]
async fn auth_import_list_check_delete() {
    let pool = test_pool().await;
    let token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: Some("2026-05-02T09:30:00Z".to_string()),
    };
    storage::import_auth_account(&pool, &token).await.unwrap();
    let accounts = storage::list_auth_accounts(&pool).await.unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].auth_token_masked, "abcd...efgh");
    let secret = storage::first_auth_account_secret(&pool)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(secret.auth_token, "abcd1234efgh");
    assert_eq!(secret.ct0, "ct0value123");
    assert!(storage::delete_auth_account(&pool, "account1")
        .await
        .unwrap());
    assert!(storage::list_auth_accounts(&pool).await.unwrap().is_empty());
}

#[tokio::test]
async fn auth_selection_skips_rejected_and_limited_accounts() {
    let pool = test_pool().await;
    let token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token).await.unwrap();
    assert!(storage::first_auth_account_secret(&pool)
        .await
        .unwrap()
        .is_some());

    storage::mark_auth_rejected(&pool, "account1", "rejected")
        .await
        .unwrap();
    assert!(storage::first_auth_account_secret(&pool)
        .await
        .unwrap()
        .is_none());

    storage::import_auth_account(&pool, &token).await.unwrap();
    storage::mark_auth_limited(&pool, "account1", "2999-01-01T00:00:00+00:00")
        .await
        .unwrap();
    assert!(storage::first_auth_account_secret(&pool)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn auth_rate_limit_updates_are_upserted() {
    let pool = test_pool().await;
    let token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token).await.unwrap();
    storage::save_auth_rate_limit(
        &pool,
        &storage::AuthRateLimitUpdate {
            auth_label: "account1".to_string(),
            endpoint: "UserTweets".to_string(),
            remaining: Some(10),
            reset_at: Some("2026-05-04T08:00:00+00:00".to_string()),
            limit_value: Some(50),
        },
    )
    .await
    .unwrap();
    storage::save_auth_rate_limit(
        &pool,
        &storage::AuthRateLimitUpdate {
            auth_label: "account1".to_string(),
            endpoint: "UserTweets".to_string(),
            remaining: Some(9),
            reset_at: Some("2026-05-04T08:00:00+00:00".to_string()),
            limit_value: Some(50),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn x_web_fetcher_stops_before_rate_limit_margin() {
    let pool = test_pool().await;
    let token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token).await.unwrap();
    storage::save_auth_rate_limit(
        &pool,
        &storage::AuthRateLimitUpdate {
            auth_label: "account1".to_string(),
            endpoint: "UserByScreenName".to_string(),
            remaining: Some(1),
            reset_at: Some("2999-01-01T00:00:00+00:00".to_string()),
            limit_value: Some(150),
        },
    )
    .await
    .unwrap();
    let mut config = AppConfig::default();
    config.fetch.fetcher = "x_web".to_string();
    config.fetch.rate_limit_safety_margin = 10;
    let source = Source {
        source_type: SourceType::Account,
        value: "openai".to_string(),
        label: None,
        limit: Some(1),
    };

    let err = xflow::fetch::fetch_source(&config, &pool, &source)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("safety margin"));
    assert!(storage::first_auth_account_secret(&pool)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn auth_import_rejects_malformed_token_json() {
    let pool = test_pool().await;
    let path = pool.path().join("bad-token.json");
    std::fs::write(&path, r#"{"label":"account1","auth_token":"short"}"#).unwrap();
    let err = auth::import_token_json(&pool, &path).await.unwrap_err();
    assert!(err.to_string().contains("missing field"));
}

#[tokio::test]
async fn fetch_dedupes_and_generates_digest() {
    let pool = test_pool().await;
    let config = AppConfig::default();
    for (source_type, value, limit) in [
        (SourceType::Account, "openai", 5),
        (SourceType::List, "ai-builders", 5),
        (SourceType::Search, "AI agent", 5),
    ] {
        storage::upsert_source(
            &pool,
            &Source {
                source_type,
                value: value.to_string(),
                label: None,
                limit: Some(limit),
            },
        )
        .await
        .unwrap();
    }
    let first = pipeline::run_fetch(&config, &pool).await.unwrap();
    let second = pipeline::run_fetch(&config, &pool).await.unwrap();
    assert_eq!(first.sources, 3);
    assert_eq!(first.failed, 0);
    assert!(first.errors.is_empty());
    assert_eq!(second.fetched, first.fetched);
    assert_eq!(second.failed, 0);
    let tweets = storage::list_tweets(
        &pool,
        TweetFilter {
            limit: 500,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(tweets.len() as i64, first.fetched);
}

#[tokio::test]
async fn fetch_uses_enabled_database_sources() {
    let pool = test_pool().await;
    let config = AppConfig::default();
    storage::upsert_source(
        &pool,
        &Source {
            source_type: SourceType::Account,
            value: "custom".to_string(),
            label: Some("Custom".to_string()),
            limit: Some(2),
        },
    )
    .await
    .unwrap();

    let result = pipeline::run_fetch(&config, &pool).await.unwrap();
    let tweets = storage::list_tweets(
        &pool,
        TweetFilter {
            limit: 500,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(result.sources, 1);
    assert_eq!(result.fetched, 2);
    assert_eq!(result.failed, 0);
    assert!(tweets
        .iter()
        .all(|stored| stored.tweet.source_value == "custom"));
}

#[tokio::test]
async fn fetch_continues_after_source_failures() {
    let pool = test_pool().await;
    storage::upsert_source(
        &pool,
        &Source {
            source_type: SourceType::List,
            value: "list-a".to_string(),
            label: None,
            limit: Some(1),
        },
    )
    .await
    .unwrap();
    storage::upsert_source(
        &pool,
        &Source {
            source_type: SourceType::Search,
            value: "AI agent".to_string(),
            label: None,
            limit: Some(1),
        },
    )
    .await
    .unwrap();
    let mut config = AppConfig::default();
    config.fetch.fetcher = "x_web".to_string();
    let token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token).await.unwrap();

    let result = pipeline::run_fetch(&config, &pool).await.unwrap();

    assert_eq!(result.sources, 2);
    assert_eq!(result.failed, 2);
    assert_eq!(result.errors.len(), 2);
    assert!(result
        .errors
        .iter()
        .all(|error| error.message.contains("supports only account sources")));
    let error_states: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM fetch_state WHERE last_status = 'error'")
            .fetch_one(pool.pg_pool())
            .await
            .unwrap();
    assert_eq!(error_states, 2);
}

#[tokio::test]
async fn fetch_state_reports_per_source_count() {
    let pool = test_pool().await;
    storage::upsert_source(
        &pool,
        &Source {
            source_type: SourceType::Account,
            value: "source-a".to_string(),
            label: None,
            limit: Some(2),
        },
    )
    .await
    .unwrap();
    storage::upsert_source(
        &pool,
        &Source {
            source_type: SourceType::Search,
            value: "source b".to_string(),
            label: None,
            limit: Some(3),
        },
    )
    .await
    .unwrap();
    let config = AppConfig::default();

    let result = pipeline::run_fetch(&config, &pool).await.unwrap();

    assert_eq!(result.fetched, 5);
    assert_eq!(result.failed, 0);
    let mut messages: Vec<String> =
        sqlx::query_scalar("SELECT message FROM fetch_state WHERE last_status = 'ok'")
            .fetch_all(pool.pg_pool())
            .await
            .unwrap();
    messages.sort();
    assert_eq!(
        messages,
        vec![
            "Fetched 2 tweets.".to_string(),
            "Fetched 3 tweets.".to_string()
        ]
    );
}

#[tokio::test]
async fn worker_returns_result_after_source_fetch_failure() {
    let pool = test_pool().await;
    storage::upsert_source(
        &pool,
        &Source {
            source_type: SourceType::Search,
            value: "AI agent".to_string(),
            label: None,
            limit: Some(1),
        },
    )
    .await
    .unwrap();
    let mut config = AppConfig::default();
    config.fetch.fetcher = "x_web".to_string();
    config.telegram.enabled = false;
    let token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token).await.unwrap();

    let result = worker::run_once(&config, &pool).await.unwrap();

    assert_eq!(result.fetch.failed, 1);
    assert_eq!(result.telegram.sent, 0);
    assert_eq!(result.telegram.failed, 0);
}

#[tokio::test]
async fn x_web_fetcher_requires_auth_account() {
    let pool = test_pool().await;
    let mut config = AppConfig::default();
    config.fetch.fetcher = "x_web".to_string();
    let source = Source {
        source_type: SourceType::Account,
        value: "openai".to_string(),
        label: None,
        limit: Some(1),
    };
    let err = xflow::fetch::fetch_source(&config, &pool, &source)
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("requires an imported auth account"));
}

#[tokio::test]
async fn x_web_fetcher_rejects_non_account_sources() {
    let pool = test_pool().await;
    let token = TokenImport {
        label: "account1".to_string(),
        domain: "x.com".to_string(),
        auth_token: "abcd1234efgh".to_string(),
        ct0: "ct0value123".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token).await.unwrap();
    let mut config = AppConfig::default();
    config.fetch.fetcher = "x_web".to_string();
    let source = Source {
        source_type: SourceType::Search,
        value: "AI agent".to_string(),
        label: None,
        limit: Some(1),
    };
    let err = xflow::fetch::fetch_source(&config, &pool, &source)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("supports only account sources"));
}

#[tokio::test]
async fn rss_generation_contains_items() {
    let pool = test_pool().await;
    storage::upsert_tweet(
        &pool,
        &Tweet {
            tweet_id: "1".to_string(),
            source_type: SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "OpenAI".to_string(),
            author_name: "OpenAI".to_string(),
            text: "AI update".to_string(),
            url: "https://x.com/openai/status/1".to_string(),
            created_at: Utc::now(),
            fetched_at: Utc::now(),
            raw: serde_json::json!({}),
        },
    )
    .await
    .unwrap();
    let tweets = storage::list_tweets(
        &pool,
        TweetFilter {
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let xml = rss_feed::generate_rss("Test", "http://localhost/rss/all", "desc", &tweets).unwrap();
    assert!(xml.contains("<rss"));
    assert!(xml.contains("AI update"));
}

#[tokio::test]
async fn daily_digest_query_uses_account_sources_and_window() {
    let pool = test_pool().await;
    let inside = Utc.with_ymd_and_hms(2026, 6, 26, 2, 0, 0).unwrap();
    let outside = Utc.with_ymd_and_hms(2026, 6, 25, 8, 0, 0).unwrap();
    let window_start = Utc.with_ymd_and_hms(2026, 6, 25, 10, 0, 0).unwrap();
    let window_end = Utc.with_ymd_and_hms(2026, 6, 26, 10, 0, 0).unwrap();
    storage::upsert_source(
        &pool,
        &Source {
            source_type: SourceType::Account,
            value: "openai".to_string(),
            label: None,
            limit: Some(10),
        },
    )
    .await
    .unwrap();

    for (tweet_id, source_type, author, created_at) in [
        ("account-in", SourceType::Account, "openai", inside),
        ("search-in", SourceType::Search, "openai", inside),
        ("account-out", SourceType::Account, "anthropic", outside),
    ] {
        storage::upsert_tweet(
            &pool,
            &Tweet {
                tweet_id: tweet_id.to_string(),
                source_type,
                source_value: author.to_string(),
                author_username: author.to_string(),
                author_name: author.to_string(),
                text: format!("{tweet_id} update"),
                url: format!("https://x.com/{author}/status/{tweet_id}"),
                created_at,
                fetched_at: created_at,
                raw: serde_json::json!({}),
            },
        )
        .await
        .unwrap();
    }

    let tweets = storage::list_account_tweets_for_window(&pool, window_start, window_end, 20)
        .await
        .unwrap();

    assert_eq!(tweets.len(), 1);
    assert_eq!(tweets[0].tweet.tweet_id, "account-in");
}

#[tokio::test]
async fn daily_digest_run_records_delivery_and_dedupes() {
    let pool = test_pool().await;
    let start = Utc.with_ymd_and_hms(2026, 6, 25, 10, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 6, 26, 10, 0, 0).unwrap();

    assert!(
        !storage::daily_digest_delivered(&pool, "2026-06-26", "telegram:1")
            .await
            .unwrap()
    );

    storage::save_daily_digest_run(
        &pool,
        storage::DailyDigestRunUpdate {
            digest_date: "2026-06-26",
            channel: "telegram:1",
            window_start: start,
            window_end: end,
            status: "error",
            payload: &serde_json::json!({"stage": "send"}),
            error: Some("timeout"),
        },
    )
    .await
    .unwrap();
    storage::save_daily_digest_run(
        &pool,
        storage::DailyDigestRunUpdate {
            digest_date: "2026-06-26",
            channel: "telegram:1",
            window_start: start,
            window_end: end,
            status: "delivered",
            payload: &serde_json::json!({"ok": true}),
            error: None,
        },
    )
    .await
    .unwrap();

    assert!(
        storage::daily_digest_delivered(&pool, "2026-06-26", "telegram:1")
            .await
            .unwrap()
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM daily_digest_runs WHERE digest_date = '2026-06-26'",
    )
    .fetch_one(pool.pg_pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn tweet_username_filter_is_case_insensitive() {
    let pool = test_pool().await;
    storage::upsert_tweet(
        &pool,
        &Tweet {
            tweet_id: "1".to_string(),
            source_type: SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "OpenAI".to_string(),
            author_name: "OpenAI".to_string(),
            text: "AI update".to_string(),
            url: "https://x.com/OpenAI/status/1".to_string(),
            created_at: Utc::now(),
            fetched_at: Utc::now(),
            raw: serde_json::json!({}),
        },
    )
    .await
    .unwrap();

    let tweets = storage::list_tweets(
        &pool,
        TweetFilter {
            username: Some("openai".to_string()),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(tweets.len(), 1);
    assert_eq!(tweets[0].tweet.author_username, "OpenAI");
}

struct MockChannel;

impl DeliveryChannel for MockChannel {
    fn id(&self) -> String {
        "mock:test".to_string()
    }

    fn send_tweet<'a>(&'a self, _tweet: &'a StoredTweet) -> ChannelSendFuture<'a> {
        Box::pin(async {
            Ok(ChannelSendReceipt {
                payload: serde_json::json!({"ok": true}),
            })
        })
    }
}

#[tokio::test]
async fn channel_delivery_records_prevent_duplicate_sends() {
    let pool = test_pool().await;
    storage::upsert_tweet(
        &pool,
        &Tweet {
            tweet_id: "1".to_string(),
            source_type: SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "OpenAI".to_string(),
            author_name: "OpenAI".to_string(),
            text: "AI update".to_string(),
            url: "https://x.com/OpenAI/status/1".to_string(),
            created_at: Utc::now(),
            fetched_at: Utc::now(),
            raw: serde_json::json!({}),
        },
    )
    .await
    .unwrap();

    let channels: Vec<Box<dyn DeliveryChannel>> = vec![Box::new(MockChannel)];
    let first = channel::send_undelivered(&pool, &channels, 10, 3)
        .await
        .unwrap();
    let second = channel::send_undelivered(&pool, &channels, 10, 3)
        .await
        .unwrap();

    assert_eq!(first.sent, 1);
    assert_eq!(first.failed, 0);
    assert_eq!(second.sent, 0);
    assert_eq!(second.failed, 0);
}

#[tokio::test]
async fn save_delivery_upsert_deduplicates() {
    let pool = test_pool().await;
    storage::upsert_tweet(
        &pool,
        &Tweet {
            tweet_id: "dup-1".to_string(),
            source_type: SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "OpenAI".to_string(),
            author_name: "OpenAI".to_string(),
            text: "test".to_string(),
            url: "https://x.com/openai/status/dup-1".to_string(),
            created_at: Utc::now(),
            fetched_at: Utc::now(),
            raw: serde_json::json!({}),
        },
    )
    .await
    .unwrap();

    let channel = "telegram:123";
    storage::save_delivery(
        &pool,
        "dup-1",
        channel,
        "error",
        &serde_json::json!({"error": "timeout"}),
        false,
    )
    .await
    .unwrap();
    storage::save_delivery(
        &pool,
        "dup-1",
        channel,
        "delivered",
        &serde_json::json!({"ok": true}),
        true,
    )
    .await
    .unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM deliveries WHERE tweet_id = 'dup-1'")
        .fetch_one(pool.pg_pool())
        .await
        .unwrap();
    assert_eq!(count, 1);

    let status: String =
        sqlx::query_scalar("SELECT status FROM deliveries WHERE tweet_id = 'dup-1'")
            .fetch_one(pool.pg_pool())
            .await
            .unwrap();
    assert_eq!(status, "delivered");
}

#[tokio::test]
async fn deliveries_unique_constraint_allows_different_channels() {
    let pool = test_pool().await;
    storage::upsert_tweet(
        &pool,
        &Tweet {
            tweet_id: "multi-1".to_string(),
            source_type: SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "OpenAI".to_string(),
            author_name: "OpenAI".to_string(),
            text: "test".to_string(),
            url: "https://x.com/openai/status/multi-1".to_string(),
            created_at: Utc::now(),
            fetched_at: Utc::now(),
            raw: serde_json::json!({}),
        },
    )
    .await
    .unwrap();

    storage::save_delivery(
        &pool,
        "multi-1",
        "telegram:111",
        "delivered",
        &serde_json::json!({}),
        true,
    )
    .await
    .unwrap();
    storage::save_delivery(
        &pool,
        "multi-1",
        "telegram:222",
        "delivered",
        &serde_json::json!({}),
        true,
    )
    .await
    .unwrap();

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM deliveries WHERE tweet_id = 'multi-1'")
            .fetch_one(pool.pg_pool())
            .await
            .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn next_auth_rotates_by_last_used_at() {
    let pool = test_pool().await;
    let token_a = TokenImport {
        label: "alpha".to_string(),
        domain: "x.com".to_string(),
        auth_token: "aaaa1111bbbb".to_string(),
        ct0: "ct0alpha1234".to_string(),
        exported_at: None,
    };
    let token_b = TokenImport {
        label: "beta".to_string(),
        domain: "x.com".to_string(),
        auth_token: "bbbb2222cccc".to_string(),
        ct0: "ct0beta12345".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token_a).await.unwrap();
    storage::import_auth_account(&pool, &token_b).await.unwrap();

    // Both never used, should pick by label order.
    let first = storage::next_auth_account_secret(&pool)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.label, "alpha");

    // Mark alpha as used, next should pick beta.
    storage::mark_auth_used(&pool, "alpha").await.unwrap();
    let second = storage::next_auth_account_secret(&pool)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.label, "beta");

    // Mark beta as used, now alpha was used earlier, should pick alpha again.
    storage::mark_auth_used(&pool, "beta").await.unwrap();
    let third = storage::next_auth_account_secret(&pool)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.label, "alpha");
}

#[tokio::test]
async fn next_auth_skips_limited_and_rejected() {
    let pool = test_pool().await;
    let token_a = TokenImport {
        label: "alpha".to_string(),
        domain: "x.com".to_string(),
        auth_token: "aaaa1111bbbb".to_string(),
        ct0: "ct0alpha1234".to_string(),
        exported_at: None,
    };
    let token_b = TokenImport {
        label: "beta".to_string(),
        domain: "x.com".to_string(),
        auth_token: "bbbb2222cccc".to_string(),
        ct0: "ct0beta12345".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token_a).await.unwrap();
    storage::import_auth_account(&pool, &token_b).await.unwrap();

    storage::mark_auth_rejected(&pool, "alpha", "rejected")
        .await
        .unwrap();
    let selected = storage::next_auth_account_secret(&pool)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.label, "beta");

    storage::mark_auth_limited(&pool, "beta", "2999-01-01T00:00:00+00:00")
        .await
        .unwrap();
    assert!(storage::next_auth_account_secret(&pool)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn check_token_freshness_detects_stale_tokens() {
    let pool = test_pool().await;
    // Import a token and manually set updated_at to 10 days ago.
    let token = TokenImport {
        label: "stale_one".to_string(),
        domain: "x.com".to_string(),
        auth_token: "aaaa1111bbbb".to_string(),
        ct0: "ct0stale1234".to_string(),
        exported_at: None,
    };
    storage::import_auth_account(&pool, &token).await.unwrap();
    let ten_days_ago = (Utc::now() - chrono::Duration::days(10)).to_rfc3339();
    sqlx::query("UPDATE auth_accounts SET updated_at = $1 WHERE label = $2")
        .bind(&ten_days_ago)
        .bind("stale_one")
        .execute(pool.pg_pool())
        .await
        .unwrap();

    let stale = storage::check_token_freshness(&pool, 7).await.unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].0, "stale_one");

    let fresh = storage::check_token_freshness(&pool, 14).await.unwrap();
    assert!(fresh.is_empty());
}

#[test]
fn adjust_interval_backs_off_on_all_failures() {
    let base: u64 = 900;
    // All sources failed: double interval.
    let (interval, successes) = xflow::worker::adjust_interval(900, base, 0, 2, 2);
    assert_eq!(interval, 1800);
    assert_eq!(successes, 0);

    // Back off again.
    let (interval, _) = xflow::worker::adjust_interval(1800, base, 0, 1, 1);
    assert_eq!(interval, 3600);

    // Cap at base * 8.
    let (interval, _) = xflow::worker::adjust_interval(base * 4, base, 0, 3, 3);
    assert_eq!(interval, base * 8);
}

#[test]
fn adjust_interval_moderate_on_partial_failure() {
    let base: u64 = 900;
    let (interval, successes) = xflow::worker::adjust_interval(900, base, 0, 1, 3);
    assert_eq!(interval, 1350); // 900 * 3/2
    assert_eq!(successes, 0);
}

#[test]
fn adjust_interval_recovers_after_consecutive_successes() {
    let base: u64 = 900;
    // After backing off to 1800, first success doesn't reduce yet.
    let (interval, successes) = xflow::worker::adjust_interval(1800, base, 0, 0, 1);
    assert_eq!(interval, 1800);
    assert_eq!(successes, 1);

    // Second success triggers recovery.
    let (interval, successes) = xflow::worker::adjust_interval(1800, base, 1, 0, 1);
    assert_eq!(interval, 1200); // 1800 * 2/3
    assert_eq!(successes, 2);

    // Keeps recovering.
    let (interval, _) = xflow::worker::adjust_interval(1200, base, 2, 0, 1);
    assert_eq!(interval, 900); // 1200 * 2/3 = 800, max(800, 900) = 900
}

#[test]
fn adjust_interval_stays_at_base_when_all_ok() {
    let base: u64 = 900;
    let (interval, successes) = xflow::worker::adjust_interval(900, base, 5, 0, 3);
    assert_eq!(interval, 900);
    assert_eq!(successes, 6);
}

// === Stage 4B: Share pipeline (cache → X fetch → persist → private card send) ===
//
// These tests run the real `SharePipeline` against a real PostgreSQL schema
// (via the production `PgShareTweetStore`) while the X fetcher and the
// Telegram sender are fakes. This proves the storage semantics (favorites,
// transaction consistency, untouched `deliveries`) without any network I/O.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use xflow::bot::share::executor::TryEnqueueError;
use xflow::bot::share::pipeline::{
    handle_enqueue_rejection, PgShareTweetStore, ShareCardSender, SharePipeline, SharePortFuture,
    ShareTweetSource, ShareTweetStore, SHARE_BUSY_NOTICE,
};
use xflow::bot::share::ShareRequest;
use xflow::storage::FavoriteWriteResult;

struct ProbeShareStore {
    inner: PgShareTweetStore,
    calls: Arc<Mutex<Vec<&'static str>>>,
    fail_get: Arc<AtomicBool>,
    fail_save: Arc<AtomicBool>,
}

impl ProbeShareStore {
    fn new(pool: sqlx::PgPool) -> Self {
        Self {
            inner: PgShareTweetStore::new(pool),
            calls: Arc::new(Mutex::new(Vec::new())),
            fail_get: Arc::new(AtomicBool::new(false)),
            fail_save: Arc::new(AtomicBool::new(false)),
        }
    }

    fn record(&self, call: &'static str) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}

impl ShareTweetStore for ProbeShareStore {
    fn get_tweet<'a>(
        &'a self,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<Option<StoredTweet>>> {
        Box::pin(async move {
            self.record("get_tweet");
            if self.fail_get.load(Ordering::SeqCst) {
                anyhow::bail!("injected share db lookup failure");
            }
            self.inner.get_tweet(tweet_id).await
        })
    }

    fn add_favorite<'a>(
        &'a self,
        chat_id: i64,
        tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move {
            self.record("add_favorite");
            self.inner.add_favorite(chat_id, tweet_id).await
        })
    }

    fn save_tweet_and_favorite<'a>(
        &'a self,
        chat_id: i64,
        tweet: &'a Tweet,
    ) -> SharePortFuture<'a, anyhow::Result<FavoriteWriteResult>> {
        Box::pin(async move {
            self.record("save_tweet_and_favorite");
            if self.fail_save.load(Ordering::SeqCst) {
                anyhow::bail!("injected share save failure");
            }
            self.inner.save_tweet_and_favorite(chat_id, tweet).await
        })
    }
}

struct FakeShareSource {
    tweet: Option<Tweet>,
    calls: Arc<AtomicUsize>,
}

impl FakeShareSource {
    fn returning(tweet: Tweet) -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                tweet: Some(tweet),
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }

    fn failing() -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                tweet: None,
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }
}

impl ShareTweetSource for FakeShareSource {
    fn fetch_single_tweet<'a>(
        &'a self,
        _tweet_id: &'a str,
    ) -> SharePortFuture<'a, anyhow::Result<Tweet>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &self.tweet {
                Some(tweet) => Ok(tweet.clone()),
                None => anyhow::bail!("injected X fetch failure"),
            }
        })
    }
}

#[derive(Default)]
struct FakeShareSender {
    cards: Arc<Mutex<Vec<(i64, String)>>>,
    busy: Arc<Mutex<Vec<i64>>>,
    fail_cards: Arc<AtomicBool>,
}

impl FakeShareSender {
    fn cards(&self) -> Vec<(i64, String)> {
        self.cards.lock().unwrap().clone()
    }

    fn busy(&self) -> Vec<i64> {
        self.busy.lock().unwrap().clone()
    }
}

impl ShareCardSender for FakeShareSender {
    fn send_tweet_card<'a>(
        &'a self,
        chat_id: i64,
        tweet: &'a StoredTweet,
    ) -> SharePortFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.fail_cards.load(Ordering::SeqCst) {
                anyhow::bail!("injected Telegram send failure");
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

async fn build_share_test_pipeline(
    pool: &support::TestDatabase,
    source: FakeShareSource,
    sender: FakeShareSender,
) -> (
    Arc<SharePipeline>,
    Arc<ProbeShareStore>,
    Arc<AtomicUsize>,
    FakeShareSender,
) {
    let store = Arc::new(ProbeShareStore::new(pool.pg_pool().clone()));
    let source_calls = Arc::clone(&source.calls);
    // Keep the inner Arcs for the assertion view before upcasting to the
    // trait object stored inside the pipeline.
    let sender_view = FakeShareSender {
        cards: Arc::clone(&sender.cards),
        busy: Arc::clone(&sender.busy),
        fail_cards: Arc::clone(&sender.fail_cards),
    };
    let sender: Arc<dyn ShareCardSender> = Arc::new(sender);
    let pipeline = Arc::new(SharePipeline::new(
        store.clone(),
        Arc::new(source),
        Arc::clone(&sender),
    ));
    (pipeline, store, source_calls, sender_view)
}

fn share_request(tweet_id: &str) -> ShareRequest {
    ShareRequest {
        chat_id: 7,
        tweet_id: tweet_id.to_string(),
        canonical_url: format!("https://x.com/i/status/{tweet_id}"),
    }
}

async fn deliveries_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*)::bigint FROM deliveries")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn favorites_count(pool: &sqlx::PgPool, chat_id: i64, tweet_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM telegram_favorites WHERE chat_id = $1 AND tweet_id = $2",
    )
    .bind(chat_id)
    .bind(tweet_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn tweet_raw_json(pool: &sqlx::PgPool, tweet_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT raw_json FROM tweets WHERE tweet_id = $1")
        .bind(tweet_id)
        .fetch_optional(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn share_cache_hit_adds_favorite_and_skips_x_fetch() {
    let pool = test_pool().await;
    let tweet = test_tweet("share-hit-1", "cached text");
    storage::upsert_tweet(pool.pg_pool(), &tweet).await.unwrap();

    let (source, source_calls) = FakeShareSource::failing();
    let (pipeline, store, _, sender) =
        build_share_test_pipeline(&pool, source, FakeShareSender::default()).await;

    pipeline
        .handle(share_request("share-hit-1"))
        .await
        .expect("cache hit path must succeed");

    assert_eq!(
        store.calls(),
        vec!["get_tweet", "add_favorite"],
        "cache hit must only add the favorite"
    );
    assert_eq!(
        source_calls.load(Ordering::SeqCst),
        0,
        "X must not be called"
    );
    assert_eq!(sender.cards(), vec![(7, "share-hit-1".to_string())]);
    assert_eq!(favorites_count(pool.pg_pool(), 7, "share-hit-1").await, 1);
    assert_eq!(deliveries_count(pool.pg_pool()).await, 0);
}

#[tokio::test]
async fn share_cache_miss_fetches_saves_and_sends() {
    let pool = test_pool().await;
    let (source, source_calls) =
        FakeShareSource::returning(test_tweet("share-miss-1", "fresh text"));
    let (pipeline, store, _, sender) =
        build_share_test_pipeline(&pool, source, FakeShareSender::default()).await;

    pipeline
        .handle(share_request("share-miss-1"))
        .await
        .expect("cache miss path must succeed");

    assert_eq!(
        store.calls(),
        vec!["get_tweet", "save_tweet_and_favorite"],
        "miss path must save tweet and favorite in one transaction"
    );
    assert_eq!(source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(sender.cards(), vec![(7, "share-miss-1".to_string())]);
    let raw = tweet_raw_json(pool.pg_pool(), "share-miss-1").await;
    assert!(raw.is_some());
    assert!(raw.unwrap().contains("fresh text"));
    assert_eq!(favorites_count(pool.pg_pool(), 7, "share-miss-1").await, 1);
    assert_eq!(deliveries_count(pool.pg_pool()).await, 0);
}

#[tokio::test]
async fn share_cache_incomplete_refetches_and_updates() {
    let pool = test_pool().await;
    let mut damaged = test_tweet("share-inc-1", "old");
    // storage::get_tweet masks unreadable raw_json into an empty object;
    // an empty raw object is the pipeline's "cache incomplete" signal.
    damaged.raw = serde_json::json!({});
    storage::upsert_tweet(pool.pg_pool(), &damaged)
        .await
        .unwrap();

    let (source, source_calls) =
        FakeShareSource::returning(test_tweet("share-inc-1", "refetched text"));
    let (pipeline, store, _, sender) =
        build_share_test_pipeline(&pool, source, FakeShareSender::default()).await;

    pipeline
        .handle(share_request("share-inc-1"))
        .await
        .expect("incomplete cache path must succeed");

    assert_eq!(
        store.calls(),
        vec!["get_tweet", "save_tweet_and_favorite"],
        "incomplete cache must refetch and re-save"
    );
    assert_eq!(source_calls.load(Ordering::SeqCst), 1);
    let raw = tweet_raw_json(pool.pg_pool(), "share-inc-1")
        .await
        .expect("tweet row must exist");
    assert!(
        raw.contains("refetched text"),
        "damaged raw must be replaced"
    );
    assert_eq!(favorites_count(pool.pg_pool(), 7, "share-inc-1").await, 1);
    assert_eq!(sender.cards().len(), 1);
    assert_eq!(deliveries_count(pool.pg_pool()).await, 0);
}

#[tokio::test]
async fn share_db_error_does_not_reach_x_api() {
    let pool = test_pool().await;
    let (source, source_calls) =
        FakeShareSource::returning(test_tweet("share-dberr-1", "never used"));
    let (pipeline, store, _, sender) =
        build_share_test_pipeline(&pool, source, FakeShareSender::default()).await;
    store.fail_get.store(true, Ordering::SeqCst);

    let err = pipeline
        .handle(share_request("share-dberr-1"))
        .await
        .expect_err("db error must fail the request");
    assert!(err.to_string().contains("db lookup failed"));

    assert_eq!(
        source_calls.load(Ordering::SeqCst),
        0,
        "db error must not trigger X"
    );
    assert!(
        sender.cards().is_empty(),
        "nothing may be sent after a db error"
    );
    assert_eq!(store.calls(), vec!["get_tweet"]);
    assert_eq!(favorites_count(pool.pg_pool(), 7, "share-dberr-1").await, 0);
}

#[tokio::test]
async fn share_x_fetch_failure_saves_nothing_and_sends_nothing() {
    let pool = test_pool().await;
    let (source, _) = FakeShareSource::failing();
    let (pipeline, store, _, sender) =
        build_share_test_pipeline(&pool, source, FakeShareSender::default()).await;

    pipeline
        .handle(share_request("share-xfail-1"))
        .await
        .expect_err("X fetch failure must fail the request");

    assert_eq!(store.calls(), vec!["get_tweet"], "no save, no favorite");
    assert_eq!(favorites_count(pool.pg_pool(), 7, "share-xfail-1").await, 0);
    assert!(tweet_raw_json(pool.pg_pool(), "share-xfail-1")
        .await
        .is_none());
    assert!(sender.cards().is_empty());
}

#[tokio::test]
async fn share_save_failure_does_not_send_telegram_message() {
    let pool = test_pool().await;
    let (source, _) = FakeShareSource::returning(test_tweet("share-savefail-1", "fresh"));
    let (pipeline, store, _, sender) =
        build_share_test_pipeline(&pool, source, FakeShareSender::default()).await;
    store.fail_save.store(true, Ordering::SeqCst);

    pipeline
        .handle(share_request("share-savefail-1"))
        .await
        .expect_err("save failure must fail the request");

    assert!(
        sender.cards().is_empty(),
        "no card may be sent after save failure"
    );
    assert!(tweet_raw_json(pool.pg_pool(), "share-savefail-1")
        .await
        .is_none());
    assert_eq!(
        favorites_count(pool.pg_pool(), 7, "share-savefail-1").await,
        0
    );
    assert_eq!(deliveries_count(pool.pg_pool()).await, 0);
}

#[tokio::test]
async fn share_send_failure_keeps_favorite_and_writes_no_deliveries() {
    let pool = test_pool().await;
    let (source, _) = FakeShareSource::returning(test_tweet("share-sendfail-1", "fresh"));
    let sender = FakeShareSender {
        fail_cards: Arc::new(AtomicBool::new(true)),
        ..FakeShareSender::default()
    };
    let (pipeline, _, _, _) = build_share_test_pipeline(&pool, source, sender).await;

    pipeline
        .handle(share_request("share-sendfail-1"))
        .await
        .expect_err("send failure must fail the request");

    assert_eq!(
        favorites_count(pool.pg_pool(), 7, "share-sendfail-1").await,
        1,
        "saved favorite must be kept when Telegram send fails"
    );
    assert_eq!(deliveries_count(pool.pg_pool()).await, 0);
}

#[tokio::test]
async fn share_repeat_share_resends_card_and_preserves_first_favorite_time() {
    let pool = test_pool().await;
    let tweet = test_tweet("share-repeat-1", "cached");
    storage::upsert_tweet(pool.pg_pool(), &tweet).await.unwrap();

    let (source, _) = FakeShareSource::failing();
    let (pipeline, _, _, sender) =
        build_share_test_pipeline(&pool, source, FakeShareSender::default()).await;

    pipeline
        .handle(share_request("share-repeat-1"))
        .await
        .unwrap();
    let first_created_at: chrono::DateTime<Utc> = sqlx::query_scalar(
        "SELECT created_at FROM telegram_favorites WHERE chat_id = 7 AND tweet_id = $1",
    )
    .bind("share-repeat-1")
    .fetch_one(pool.pg_pool())
    .await
    .unwrap();

    pipeline
        .handle(share_request("share-repeat-1"))
        .await
        .unwrap();

    assert_eq!(
        sender.cards().len(),
        2,
        "the same tweet may be shared and re-sent"
    );
    assert_eq!(
        favorites_count(pool.pg_pool(), 7, "share-repeat-1").await,
        1,
        "repeated share must not duplicate the favorite row"
    );
    let second_created_at: chrono::DateTime<Utc> = sqlx::query_scalar(
        "SELECT created_at FROM telegram_favorites WHERE chat_id = 7 AND tweet_id = $1",
    )
    .bind("share-repeat-1")
    .fetch_one(pool.pg_pool())
    .await
    .unwrap();
    assert_eq!(first_created_at, second_created_at);
    assert_eq!(deliveries_count(pool.pg_pool()).await, 0);
}

#[tokio::test]
async fn share_busy_notice_sent_only_when_queue_full() {
    let sender = FakeShareSender::default();

    handle_enqueue_rejection(
        &sender,
        &TryEnqueueError::QueueFull(share_request("share-busy-1")),
    )
    .await;
    assert_eq!(
        sender.busy(),
        vec![7],
        "queue full must notify the requester"
    );

    handle_enqueue_rejection(
        &sender,
        &TryEnqueueError::Closed(share_request("share-busy-2")),
    )
    .await;
    assert_eq!(
        sender.busy().len(),
        1,
        "closed executor must not send a busy notice"
    );
    assert!(sender.cards().is_empty());
    assert_eq!(SHARE_BUSY_NOTICE, "当前处理队列繁忙，请稍后重新分享。");
}
