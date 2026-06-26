use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS auth_accounts (
    label TEXT PRIMARY KEY,
    domain TEXT NOT NULL DEFAULT 'x.com',
    auth_token TEXT NOT NULL,
    ct0 TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
    limited_until TEXT,
    consecutive_failures BIGINT NOT NULL DEFAULT 0,
    last_used_at TEXT,
    exported_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_rate_limits (
    auth_label TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    remaining BIGINT,
    reset_at TEXT,
    limit_value BIGINT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(auth_label, endpoint),
    FOREIGN KEY(auth_label) REFERENCES auth_accounts(label) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sources (
    id BIGSERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,
    value TEXT NOT NULL,
    label TEXT,
    fetch_limit BIGINT,
    enabled BIGINT NOT NULL DEFAULT 1,
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

CREATE TABLE IF NOT EXISTS deliveries (
    id BIGSERIAL PRIMARY KEY,
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
    id BIGSERIAL PRIMARY KEY,
    keyword TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_digest_runs (
    id BIGSERIAL PRIMARY KEY,
    digest_date TEXT NOT NULL,
    channel TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    status TEXT NOT NULL,
    retry_count BIGINT NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    UNIQUE(digest_date, channel)
);
"#;

pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;
    tracing::info!("connected to database");
    Ok(pool)
}

pub async fn init_db(pool: &PgPool) -> anyhow::Result<()> {
    for statement in SCHEMA.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(pool).await?;
        }
    }
    migrate_sources(pool).await?;
    migrate_auth_accounts(pool).await?;
    migrate_deliveries(pool).await?;
    migrate_deliveries_retry_count(pool).await?;
    migrate_daily_digest_runs(pool).await?;
    tracing::debug!("database schema initialized");
    Ok(())
}

async fn table_columns(pool: &PgPool, table: &str) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<String, _>("column_name"))
        .collect())
}

async fn migrate_sources(pool: &PgPool) -> anyhow::Result<()> {
    let columns = table_columns(pool, "sources").await?;
    let has_column = |name: &str| columns.iter().any(|column| column == name);
    if !has_column("fetch_limit") {
        sqlx::query("ALTER TABLE sources ADD COLUMN fetch_limit BIGINT")
            .execute(pool)
            .await?;
    }
    if !has_column("enabled") {
        sqlx::query("ALTER TABLE sources ADD COLUMN enabled BIGINT NOT NULL DEFAULT 1")
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

async fn migrate_auth_accounts(pool: &PgPool) -> anyhow::Result<()> {
    let columns = table_columns(pool, "auth_accounts").await?;
    let has_column = |name: &str| columns.iter().any(|column| column == name);
    if !has_column("limited_until") {
        sqlx::query("ALTER TABLE auth_accounts ADD COLUMN limited_until TEXT")
            .execute(pool)
            .await?;
    }
    if !has_column("consecutive_failures") {
        sqlx::query(
            "ALTER TABLE auth_accounts ADD COLUMN consecutive_failures BIGINT NOT NULL DEFAULT 0",
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

async fn has_unique_on_deliveries(pool: &PgPool) -> anyhow::Result<bool> {
    let row = sqlx::query(
        "SELECT COUNT(*) > 0 FROM pg_indexes WHERE tablename = 'deliveries' AND indexdef LIKE '%tweet_id%channel%'",
    )
    .fetch_one(pool)
    .await?;
    Ok(row.get::<bool, _>(0))
}

async fn migrate_deliveries(pool: &PgPool) -> anyhow::Result<()> {
    if has_unique_on_deliveries(pool).await? {
        return Ok(());
    }
    tracing::info!("migrating deliveries: adding UNIQUE(tweet_id, channel)");
    sqlx::query(
        r#"
        CREATE TABLE deliveries_new (
            id BIGSERIAL PRIMARY KEY,
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
        INSERT INTO deliveries_new (id, tweet_id, channel, status, payload_json, created_at, delivered_at)
        SELECT id, tweet_id, channel, status, payload_json, created_at, delivered_at
        FROM deliveries
        ORDER BY id
        ON CONFLICT DO NOTHING
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

async fn migrate_deliveries_retry_count(pool: &PgPool) -> anyhow::Result<()> {
    let columns = table_columns(pool, "deliveries").await?;
    if !columns.iter().any(|c| c == "retry_count") {
        tracing::info!("migrating deliveries: adding retry_count column");
        sqlx::query("ALTER TABLE deliveries ADD COLUMN retry_count BIGINT NOT NULL DEFAULT 0")
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn migrate_daily_digest_runs(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS daily_digest_runs (
            id BIGSERIAL PRIMARY KEY,
            digest_date TEXT NOT NULL,
            channel TEXT NOT NULL,
            window_start TEXT NOT NULL,
            window_end TEXT NOT NULL,
            status TEXT NOT NULL,
            retry_count BIGINT NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            error TEXT,
            created_at TEXT NOT NULL,
            delivered_at TEXT,
            UNIQUE(digest_date, channel)
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}
