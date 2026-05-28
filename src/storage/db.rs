use crate::utils::ensure_parent;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::path::Path;
use std::str::FromStr;

pub const SCHEMA: &str = r#"
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS auth_accounts (
    label TEXT PRIMARY KEY,
    domain TEXT NOT NULL DEFAULT 'x.com',
    auth_token TEXT NOT NULL,
    ct0 TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
    limited_until TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_used_at TEXT,
    exported_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_rate_limits (
    auth_label TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    remaining INTEGER,
    reset_at TEXT,
    limit_value INTEGER,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(auth_label, endpoint),
    FOREIGN KEY(auth_label) REFERENCES auth_accounts(label) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    value TEXT NOT NULL,
    label TEXT,
    fetch_limit INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source_type, value)
);

CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_value TEXT NOT NULL,
    author_username TEXT NOT NULL,
    author_name TEXT NOT NULL,
    text TEXT NOT NULL,
    url TEXT NOT NULL,
    created_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS fetch_state (
    source_type TEXT NOT NULL,
    source_value TEXT NOT NULL,
    last_fetch_at TEXT NOT NULL,
    last_status TEXT NOT NULL,
    message TEXT,
    PRIMARY KEY(source_type, source_value)
);

CREATE TABLE IF NOT EXISTS tweet_analysis (
    tweet_id TEXT PRIMARY KEY,
    relevance REAL NOT NULL,
    importance_score REAL NOT NULL,
    category TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    chinese_summary TEXT NOT NULL,
    reason TEXT NOT NULL,
    should_push INTEGER NOT NULL,
    analyzed_at TEXT NOT NULL,
    FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tweet_id TEXT,
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE SET NULL,
    UNIQUE(tweet_id, channel)
);

CREATE TABLE IF NOT EXISTS spam_keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"#;

pub async fn connect(db_path: &Path) -> anyhow::Result<SqlitePool> {
    ensure_parent(db_path)?;
    let url = format!("sqlite://{}", db_path.display());
    let options = SqliteConnectOptions::from_str(&url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await?;
    tracing::info!(path = %db_path.display(), "connected to database");
    Ok(pool)
}

pub async fn init_db(pool: &SqlitePool) -> anyhow::Result<()> {
    for statement in SCHEMA.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(pool).await?;
        }
    }
    migrate_sources(pool).await?;
    migrate_auth_accounts(pool).await?;
    migrate_deliveries(pool).await?;
    tracing::debug!("database schema initialized");
    Ok(())
}

async fn table_columns(pool: &SqlitePool, table: &str) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| sqlx::Row::get::<String, _>(row, "name"))
        .collect())
}

async fn migrate_sources(pool: &SqlitePool) -> anyhow::Result<()> {
    let columns = table_columns(pool, "sources").await?;
    let has_column = |name: &str| columns.iter().any(|column| column == name);
    if !has_column("fetch_limit") {
        sqlx::query("ALTER TABLE sources ADD COLUMN fetch_limit INTEGER")
            .execute(pool)
            .await?;
    }
    if !has_column("enabled") {
        sqlx::query("ALTER TABLE sources ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
            .execute(pool)
            .await?;
    }
    if !has_column("updated_at") {
        sqlx::query("ALTER TABLE sources ADD COLUMN updated_at TEXT")
            .execute(pool)
            .await?;
        sqlx::query("UPDATE sources SET updated_at = created_at WHERE updated_at IS NULL")
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn migrate_auth_accounts(pool: &SqlitePool) -> anyhow::Result<()> {
    let columns = table_columns(pool, "auth_accounts").await?;
    let has_column = |name: &str| columns.iter().any(|column| column == name);
    if !has_column("limited_until") {
        sqlx::query("ALTER TABLE auth_accounts ADD COLUMN limited_until TEXT")
            .execute(pool)
            .await?;
    }
    if !has_column("consecutive_failures") {
        sqlx::query(
            "ALTER TABLE auth_accounts ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0",
        )
        .execute(pool)
        .await?;
    }
    if !has_column("last_used_at") {
        sqlx::query("ALTER TABLE auth_accounts ADD COLUMN last_used_at TEXT")
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn has_unique_on_deliveries(pool: &SqlitePool) -> anyhow::Result<bool> {
    let rows = sqlx::query("PRAGMA index_list(deliveries)")
        .fetch_all(pool)
        .await?;
    for row in &rows {
        let unique: i64 = sqlx::Row::get(row, "unique");
        if unique == 1 {
            let idx_name: String = sqlx::Row::get(row, "name");
            let cols = sqlx::query(&format!("PRAGMA index_info({idx_name})"))
                .fetch_all(pool)
                .await?;
            let col_names: Vec<String> = cols
                .iter()
                .map(|r| sqlx::Row::get::<String, _>(r, "name"))
                .collect();
            if col_names.len() == 2
                && col_names.contains(&"tweet_id".to_string())
                && col_names.contains(&"channel".to_string())
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn migrate_deliveries(pool: &SqlitePool) -> anyhow::Result<()> {
    if has_unique_on_deliveries(pool).await? {
        return Ok(());
    }
    tracing::info!("migrating deliveries: adding UNIQUE(tweet_id, channel)");
    sqlx::query(
        r#"
        CREATE TABLE deliveries_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id TEXT,
            channel TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            delivered_at TEXT,
            FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE SET NULL,
            UNIQUE(tweet_id, channel)
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO deliveries_new (id, tweet_id, channel, status, payload_json, created_at, delivered_at)
        SELECT id, tweet_id, channel, status, payload_json, created_at, delivered_at
        FROM deliveries
        ORDER BY id
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP TABLE deliveries").execute(pool).await?;
    sqlx::query("ALTER TABLE deliveries_new RENAME TO deliveries")
        .execute(pool)
        .await?;
    Ok(())
}
