use chrono::Utc;
use tempfile::tempdir;
use xflow::config::{load_config, AppConfig};
use xflow::db;
use xflow::digest;
use xflow::models::{SourceType, Tweet};
use xflow::pipeline;
use xflow::rss_feed;
use xflow::storage::{self, TokenImport, TweetFilter};

async fn test_pool() -> (tempfile::TempDir, sqlx::SqlitePool) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("xflow.db");
    let pool = db::connect(&db_path).await.unwrap();
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
  database: data/xflow.db
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
    assert!(config.storage.database.ends_with("data/xflow.db"));
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
    assert!(storage::delete_auth_account(&pool, "account1")
        .await
        .unwrap());
    assert!(storage::list_auth_accounts(&pool).await.unwrap().is_empty());
}

#[tokio::test]
async fn fetch_dedupes_and_generates_digest() {
    let (_dir, pool) = test_pool().await;
    let config = AppConfig::default();
    let first = pipeline::run_fetch(&config, &pool).await.unwrap();
    let second = pipeline::run_fetch(&config, &pool).await.unwrap();
    assert_eq!(first.sources, 3);
    assert_eq!(second.fetched, first.fetched);
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
async fn rss_generation_contains_items() {
    let (_dir, pool) = test_pool().await;
    storage::upsert_tweet(
        &pool,
        &Tweet {
            tweet_id: "1".to_string(),
            source_type: SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "openai".to_string(),
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
