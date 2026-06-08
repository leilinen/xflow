use anyhow::{Context, Result};
use clap::Parser;
use rusqlite::params;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "migrate-sqlite-to-pg", about = "Migrate xflow data from SQLite to PostgreSQL")]
struct Args {
    /// Path to the source SQLite database file
    #[arg(long)]
    from: PathBuf,
    /// Target PostgreSQL database URL (e.g. postgres://user:pass@localhost/xflow)
    #[arg(long)]
    to: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if !args.from.exists() {
        anyhow::bail!("SQLite file not found: {}", args.from.display());
    }

    println!("Opening SQLite database: {}", args.from.display());
    let sqlite = rusqlite::Connection::open_with_flags(
        &args.from,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .context("failed to open SQLite database")?;

    println!("Connecting to PostgreSQL: {}", mask_pg_url(&args.to));
    let pg = PgPoolOptions::new()
        .max_connections(5)
        .connect(&args.to)
        .await
        .context("failed to connect to PostgreSQL")?;

    init_schema(&pg).await.context("failed to initialize PG schema")?;
    println!("Schema initialized.");

    let tables = [
        "auth_accounts",
        "auth_rate_limits",
        "sources",
        "tweets",
        "tweet_analysis",
        "fetch_state",
        "deliveries",
        "spam_keywords",
    ];

    let mut total_rows = 0u64;
    for table in &tables {
        let count = migrate_table(&sqlite, &pg, table)
            .await
            .with_context(|| format!("failed to migrate table {table}"))?;
        total_rows += count;
        println!("  {table}: {count} rows migrated");
    }

    pg.close().await;
    println!("Done. Total rows migrated: {total_rows}");
    Ok(())
}

fn mask_pg_url(url: &str) -> String {
    if let Some(at) = url.rfind('@') {
        if let Some(colon) = url[..at].rfind(':') {
            return format!("{}:****{}", &url[..colon], &url[at..]);
        }
    }
    url.to_string()
}

async fn init_schema(pg: &PgPool) -> Result<()> {
    let statements = [
        r#"CREATE TABLE IF NOT EXISTS auth_accounts (
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
        )"#,
        r#"CREATE TABLE IF NOT EXISTS auth_rate_limits (
            auth_label TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            remaining BIGINT,
            reset_at TEXT,
            limit_value BIGINT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(auth_label, endpoint),
            FOREIGN KEY(auth_label) REFERENCES auth_accounts(label) ON DELETE CASCADE
        )"#,
        r#"CREATE TABLE IF NOT EXISTS sources (
            id BIGSERIAL PRIMARY KEY,
            source_type TEXT NOT NULL,
            value TEXT NOT NULL,
            label TEXT,
            fetch_limit BIGINT,
            enabled BIGINT NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_type, value)
        )"#,
        r#"CREATE TABLE IF NOT EXISTS tweets (
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
        )"#,
        r#"CREATE TABLE IF NOT EXISTS fetch_state (
            source_type TEXT NOT NULL,
            source_value TEXT NOT NULL,
            last_fetch_at TEXT NOT NULL,
            last_status TEXT NOT NULL,
            message TEXT,
            PRIMARY KEY(source_type, source_value)
        )"#,
        r#"CREATE TABLE IF NOT EXISTS tweet_analysis (
            tweet_id TEXT PRIMARY KEY,
            relevance DOUBLE PRECISION NOT NULL,
            importance_score DOUBLE PRECISION NOT NULL,
            category TEXT NOT NULL,
            tags_json TEXT NOT NULL,
            chinese_summary TEXT NOT NULL,
            reason TEXT NOT NULL,
            should_push BIGINT NOT NULL,
            analyzed_at TEXT NOT NULL,
            FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE CASCADE
        )"#,
        r#"CREATE TABLE IF NOT EXISTS deliveries (
            id BIGSERIAL PRIMARY KEY,
            tweet_id TEXT,
            channel TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            delivered_at TEXT,
            FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE SET NULL,
            UNIQUE(tweet_id, channel)
        )"#,
        r#"CREATE TABLE IF NOT EXISTS spam_keywords (
            id BIGSERIAL PRIMARY KEY,
            keyword TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )"#,
    ];

    for sql in &statements {
        sqlx::query(sql).execute(pg).await?;
    }
    Ok(())
}

async fn migrate_table(sqlite: &rusqlite::Connection, pg: &PgPool, table: &str) -> Result<u64> {
    // Get column names from SQLite
    let col_stmt = sqlite.prepare(&format!("SELECT * FROM {table} LIMIT 0"))?;
    let columns: Vec<String> = col_stmt.column_names().iter().map(|s| s.to_string()).collect();
    drop(col_stmt);

    let col_list = columns.join(", ");
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
    let placeholder_list = placeholders.join(", ");

    let insert_sql = format!(
        "INSERT INTO {table} ({col_list}) VALUES ({placeholder_list}) ON CONFLICT DO NOTHING"
    );

    // Read all rows from SQLite
    let select_sql = format!("SELECT {col_list} FROM {table}");
    let mut stmt = sqlite.prepare(&select_sql)?;
    let col_count = columns.len();

    let mut rows = stmt.query(params![])?;
    let mut total = 0u64;

    while let Some(row) = rows.next()? {
        // Read all values as Option<String> from SQLite
        let values: Vec<Option<String>> = (0..col_count)
            .map(|i| row.get::<_, Option<String>>(i))
            .collect::<Result<Vec<_>, _>>()?;

        let mut query = sqlx::query(&insert_sql);
        for val in &values {
            query = query.bind(val);
        }
        let result = query.execute(pg).await?;
        total += result.rows_affected();
    }

    Ok(total)
}
