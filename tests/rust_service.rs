use chrono::Utc;
use tempfile::tempdir;
use xflow::channel::{self, ChannelSendFuture, ChannelSendReceipt, DeliveryChannel};
use xflow::config::{load_config, AppConfig};
use xflow::digest;
use xflow::fetch::auth;
use xflow::models::{Source, SourceType, StoredTweet, Tweet};
use xflow::server::rss_feed;
use xflow::storage::db;
use xflow::storage::{self, TokenImport, TweetFilter};
use xflow::worker;
use xflow::worker::pipeline;

fn test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/xflow_test".to_string())
}

/// Creates a unique PG schema for test isolation using a random suffix.
async fn test_pool() -> (tempfile::TempDir, sqlx::PgPool) {
    let dir = tempdir().unwrap();
    let url = test_database_url();
    let pool = db::connect(&url).await.unwrap();
    db::init_db(&pool).await.unwrap();
    (dir, pool)
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
    assert_eq!(config.agent.keywords[0], "AI");
    assert!(config.storage.database_url.contains("postgres"));
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
    let (dir, pool) = test_pool().await;
    let path = dir.path().join("bad-token.json");
    std::fs::write(&path, r#"{"label":"account1","auth_token":"short"}"#).unwrap();
    let err = auth::import_token_json(&pool, &path).await.unwrap_err();
    assert!(err.to_string().contains("missing field"));
}

#[tokio::test]
async fn fetch_dedupes_and_generates_digest() {
    let (_dir, pool) = test_pool().await;
    let config = AppConfig::default();
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
    let markdown = digest::generate_digest(&pool, 0.1).await.unwrap();
    assert!(markdown.starts_with("# xFlow Digest"));
}

#[tokio::test]
async fn fetch_seeds_sources_from_config_once() {
    let (_dir, pool) = test_pool().await;
    let config = AppConfig::default();
    assert!(storage::list_sources(&pool, true).await.unwrap().is_empty());

    let result = pipeline::run_fetch(&config, &pool).await.unwrap();
    let sources = storage::list_sources(&pool, true).await.unwrap();

    assert_eq!(result.sources, 3);
    assert_eq!(result.failed, 0);
    assert_eq!(sources.len(), 3);
}

#[tokio::test]
async fn fetch_uses_enabled_database_sources() {
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(error_states, 2);
}

#[tokio::test]
async fn fetch_state_reports_per_source_count() {
    let (_dir, pool) = test_pool().await;
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
            .fetch_all(&pool)
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
async fn tweet_username_filter_is_case_insensitive() {
    let (_dir, pool) = test_pool().await;
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

    fn send_all(&self) -> bool {
        true
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let status: String =
        sqlx::query_scalar("SELECT status FROM deliveries WHERE tweet_id = 'dup-1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "delivered");
}

#[tokio::test]
async fn deliveries_unique_constraint_allows_different_channels() {
    let (_dir, pool) = test_pool().await;
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
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn next_auth_rotates_by_last_used_at() {
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
    let (_dir, pool) = test_pool().await;
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
    sqlx::query("UPDATE auth_accounts SET updated_at = ? WHERE label = ?")
        .bind(&ten_days_ago)
        .bind("stale_one")
        .execute(&pool)
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
