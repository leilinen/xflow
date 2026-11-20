pub mod db;

use crate::models::{FavoriteCursor, Source, SourceType, StoredTweet, TelegramFavorite, Tweet};
use crate::utils::{mask_token, to_json_value};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::{PgConnection, PgQueryResult, PgRow};
use sqlx::{Executor, PgPool, Postgres, QueryBuilder, Row};
use std::collections::{HashMap, HashSet};

pub const TWEET_UPSERT_BATCH_SIZE: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthAccount {
    pub label: String,
    pub domain: String,
    pub auth_token_masked: String,
    pub ct0_masked: String,
    pub status: String,
    pub exported_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct AuthAccountSecret {
    pub label: String,
    pub auth_token: String,
    pub ct0: String,
}

#[derive(Debug, Clone)]
pub struct AuthRateLimitUpdate {
    pub auth_label: String,
    pub endpoint: String,
    pub remaining: Option<i64>,
    pub reset_at: Option<String>,
    pub limit_value: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthRateLimit {
    pub auth_label: String,
    pub endpoint: String,
    pub remaining: Option<i64>,
    pub reset_at: Option<String>,
    pub limit_value: Option<i64>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenImport {
    pub label: String,
    #[serde(default = "default_domain")]
    pub domain: String,
    pub auth_token: String,
    pub ct0: String,
    pub exported_at: Option<String>,
}

fn default_domain() -> String {
    "x.com".to_string()
}

#[derive(Debug, Clone, Default)]
pub struct TweetFilter {
    pub username: Option<String>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyDigestRun {
    pub digest_date: String,
    pub channel: String,
    pub window_start: String,
    pub window_end: String,
    pub status: String,
    pub retry_count: i64,
    pub payload: Value,
    pub error: Option<String>,
    pub delivered_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DailyDigestRunUpdate<'a> {
    pub digest_date: &'a str,
    pub channel: &'a str,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: &'a str,
    pub payload: &'a Value,
    pub error: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TweetUpsertSummary {
    pub total: usize,
    pub new: usize,
    pub duplicate: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FavoriteWriteResult {
    pub tweet_inserted: bool,
    pub favorite_inserted: bool,
}

pub async fn upsert_source(pool: &PgPool, source: &Source) -> anyhow::Result<()> {
    upsert_source_enabled(pool, source, true).await
}

pub async fn upsert_source_enabled(
    pool: &PgPool,
    source: &Source,
    enabled: bool,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    let value = source.value.trim_start_matches('@');
    sqlx::query(
        r#"
        INSERT INTO sources (source_type, value, label, fetch_limit, enabled, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT(source_type, value) DO UPDATE SET
            label=excluded.label,
            fetch_limit=excluded.fetch_limit,
            enabled=excluded.enabled,
            updated_at=excluded.updated_at
        "#,
    )
    .bind(source.source_type.as_str())
    .bind(value)
    .bind(&source.label)
    .bind(source.limit)
    .bind(i64::from(enabled))
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn disable_source(
    pool: &PgPool,
    source_type: SourceType,
    value: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE sources
        SET enabled = 0, updated_at = $1
        WHERE source_type = $2 AND value = $3
        "#,
    )
    .bind(Utc::now().to_rfc3339())
    .bind(source_type.as_str())
    .bind(value.trim_start_matches('@'))
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn delete_source(
    pool: &PgPool,
    source_type: SourceType,
    value: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM sources WHERE source_type = $1 AND value = $2")
        .bind(source_type.as_str())
        .bind(value.trim_start_matches('@'))
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn list_sources(pool: &PgPool, enabled_only: bool) -> anyhow::Result<Vec<Source>> {
    let sql = if enabled_only {
        "SELECT source_type, value, label, fetch_limit FROM sources WHERE enabled = 1 ORDER BY source_type, value"
    } else {
        "SELECT source_type, value, label, fetch_limit FROM sources ORDER BY source_type, value"
    };
    let rows = sqlx::query(sql).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            Ok(Source {
                source_type: SourceType::try_from(row.get::<String, _>("source_type").as_str())?,
                value: row.get("value"),
                label: row.get("label"),
                limit: row.get("fetch_limit"),
            })
        })
        .collect()
}

/// Insert or refresh a tweet, returning `true` only when the row was newly
/// inserted. Conflicting rows are updated and return `false`.
pub async fn upsert_tweet(pool: &PgPool, tweet: &Tweet) -> anyhow::Result<bool> {
    let mut connection = pool.acquire().await?;
    upsert_tweet_with(&mut connection, tweet).await
}

async fn upsert_tweet_with(connection: &mut PgConnection, tweet: &Tweet) -> anyhow::Result<bool> {
    let tweets = [tweet];
    if !insert_new_tweets(connection, &tweets).await?.is_empty() {
        return Ok(true);
    }
    update_existing_tweets(connection, &tweets).await?;
    Ok(false)
}

/// Persist a backfill batch using the same insert/update distinction as
/// [`upsert_tweet`].
pub async fn upsert_tweets(pool: &PgPool, tweets: &[Tweet]) -> anyhow::Result<TweetUpsertSummary> {
    // Keep the last occurrence so duplicate ids retain the same final payload
    // as the previous sequential implementation. Sorting gives concurrent
    // batches a consistent unique-index lock order and avoids deadlocks.
    let mut by_id = HashMap::with_capacity(tweets.len());
    for tweet in tweets {
        by_id.insert(tweet.tweet_id.as_str(), tweet);
    }
    let mut unique: Vec<&Tweet> = by_id.into_values().collect();
    unique.sort_unstable_by(|left, right| left.tweet_id.cmp(&right.tweet_id));

    let mut new = 0;
    for batch in unique.chunks(TWEET_UPSERT_BATCH_SIZE) {
        let mut transaction = pool.begin().await?;
        let inserted = insert_new_tweets(&mut transaction, batch).await?;
        new += inserted.len();

        if inserted.len() != batch.len() {
            let inserted: HashSet<&str> = inserted.iter().map(String::as_str).collect();
            let existing: Vec<&Tweet> = batch
                .iter()
                .copied()
                .filter(|tweet| !inserted.contains(tweet.tweet_id.as_str()))
                .collect();
            update_existing_tweets(&mut transaction, &existing).await?;
        }
        transaction.commit().await?;
    }
    Ok(TweetUpsertSummary {
        total: tweets.len(),
        new,
        duplicate: tweets.len() - new,
    })
}

async fn insert_new_tweets(
    connection: &mut PgConnection,
    tweets: &[&Tweet],
) -> anyhow::Result<Vec<String>> {
    if tweets.is_empty() {
        return Ok(Vec::new());
    }

    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO tweets (
            tweet_id, source_type, source_value, author_username, author_name,
            text, url, created_at, fetched_at, raw_json
        )
        "#,
    );
    query.push_values(tweets, |mut row, tweet| {
        row.push_bind(&tweet.tweet_id)
            .push_bind(tweet.source_type.as_str())
            .push_bind(&tweet.source_value)
            .push_bind(&tweet.author_username)
            .push_bind(&tweet.author_name)
            .push_bind(&tweet.text)
            .push_bind(&tweet.url)
            .push_bind(tweet.created_at.to_rfc3339())
            .push_bind(tweet.fetched_at.to_rfc3339())
            .push_bind(tweet.raw.to_string());
    });
    query.push(" ON CONFLICT(tweet_id) DO NOTHING RETURNING tweet_id");
    Ok(query
        .build_query_scalar::<String>()
        .fetch_all(&mut *connection)
        .await?)
}

async fn update_existing_tweets(
    connection: &mut PgConnection,
    tweets: &[&Tweet],
) -> anyhow::Result<()> {
    if tweets.is_empty() {
        return Ok(());
    }

    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        UPDATE tweets AS stored
        SET fetched_at = incoming.fetched_at,
            raw_json = incoming.raw_json
        FROM (
        "#,
    );
    query.push_values(tweets, |mut row, tweet| {
        row.push_bind(&tweet.tweet_id)
            .push_bind(tweet.fetched_at.to_rfc3339())
            .push_bind(tweet.raw.to_string());
    });
    query.push(
        r#"
        ) AS incoming(tweet_id, fetched_at, raw_json)
        WHERE stored.tweet_id = incoming.tweet_id
        "#,
    );
    let result = query.build().execute(&mut *connection).await?;
    anyhow::ensure!(
        result.rows_affected() == tweets.len() as u64,
        "tweet disappeared while resolving an upsert conflict"
    );
    Ok(())
}

/// Add a favorite for a chat id that has already been authorized by the Bot
/// boundary. This repository deliberately performs no Telegram authorization.
pub async fn add_favorite(pool: &PgPool, chat_id: i64, tweet_id: &str) -> anyhow::Result<bool> {
    add_favorite_with(pool, chat_id, tweet_id).await
}

async fn add_favorite_with<'e, E>(executor: E, chat_id: i64, tweet_id: &str) -> anyhow::Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let result: PgQueryResult = sqlx::query(
        r#"
        INSERT INTO telegram_favorites (chat_id, tweet_id)
        VALUES ($1, $2)
        ON CONFLICT (chat_id, tweet_id) DO NOTHING
        "#,
    )
    .bind(chat_id)
    .bind(tweet_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// List favorites for a chat id already authorized by the Bot boundary.
/// Pagination is keyset-based over `(created_at DESC, tweet_id DESC)`.
pub async fn list_favorites(
    pool: &PgPool,
    chat_id: i64,
    limit: i64,
    cursor: Option<&FavoriteCursor>,
) -> anyhow::Result<Vec<TelegramFavorite>> {
    let limit = limit.clamp(1, 100);
    let rows = if let Some(cursor) = cursor {
        sqlx::query(
            r#"
            SELECT t.*, f.chat_id AS favorite_chat_id, f.created_at AS favorite_created_at
            FROM telegram_favorites f
            JOIN tweets t ON t.tweet_id = f.tweet_id
            WHERE f.chat_id = $1
              AND (f.created_at, f.tweet_id) < ($2, $3)
            ORDER BY f.created_at DESC, f.tweet_id DESC
            LIMIT $4
            "#,
        )
        .bind(chat_id)
        .bind(cursor.created_at)
        .bind(&cursor.tweet_id)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT t.*, f.chat_id AS favorite_chat_id, f.created_at AS favorite_created_at
            FROM telegram_favorites f
            JOIN tweets t ON t.tweet_id = f.tweet_id
            WHERE f.chat_id = $1
            ORDER BY f.created_at DESC, f.tweet_id DESC
            LIMIT $2
            "#,
        )
        .bind(chat_id)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };
    rows.into_iter().map(row_to_favorite).collect()
}

/// Atomically save a fetched tweet and favorite it for an already-authorized
/// chat. External X and Telegram calls must happen outside this transaction.
pub async fn save_tweet_and_favorite(
    pool: &PgPool,
    chat_id: i64,
    tweet: &Tweet,
) -> anyhow::Result<FavoriteWriteResult> {
    let mut transaction = pool.begin().await?;
    let tweet_inserted = upsert_tweet_with(&mut transaction, tweet).await?;
    let favorite_inserted = add_favorite_with(&mut *transaction, chat_id, &tweet.tweet_id).await?;
    transaction.commit().await?;
    Ok(FavoriteWriteResult {
        tweet_inserted,
        favorite_inserted,
    })
}

pub async fn save_fetch_state(
    pool: &PgPool,
    source: &Source,
    status: &str,
    message: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO fetch_state (source_type, source_value, last_fetch_at, last_status, message)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT(source_type, source_value) DO UPDATE SET
            last_fetch_at=excluded.last_fetch_at,
            last_status=excluded.last_status,
            message=excluded.message
        "#,
    )
    .bind(source.source_type.as_str())
    .bind(&source.value)
    .bind(Utc::now().to_rfc3339())
    .bind(status)
    .bind(message)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn count_tweets(pool: &PgPool, filter: &TweetFilter) -> anyhow::Result<i64> {
    let mut sql = String::from("SELECT COUNT(*) FROM tweets t");
    let mut where_parts: Vec<String> = Vec::new();
    if filter.username.is_some() {
        where_parts.push("LOWER(t.author_username) = LOWER($1)".to_string());
    }
    if !where_parts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
    }
    let mut query = sqlx::query_scalar::<_, i64>(&sql);
    if let Some(ref username) = filter.username {
        query = query.bind(username.trim_start_matches('@').to_string());
    }
    let count = query.fetch_one(pool).await?;
    Ok(count)
}

pub async fn list_tweets(pool: &PgPool, filter: TweetFilter) -> anyhow::Result<Vec<StoredTweet>> {
    let mut sql = String::from("SELECT t.* FROM tweets t");
    let mut where_parts: Vec<String> = Vec::new();
    let mut param_idx = 1u32;
    if filter.username.is_some() {
        where_parts.push(format!("LOWER(t.author_username) = LOWER(${param_idx})"));
        param_idx += 1;
    }
    if !where_parts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
    }
    let limit_idx = param_idx;
    param_idx += 1;
    let offset_idx = param_idx;
    sql.push_str(&format!(
        " ORDER BY t.created_at DESC LIMIT ${limit_idx} OFFSET ${offset_idx}"
    ));
    let mut query = sqlx::query(&sql);
    if let Some(username) = filter.username {
        query = query.bind(username.trim_start_matches('@').to_string());
    }
    query = query.bind(if filter.limit > 0 { filter.limit } else { 100 });
    query = query.bind(if filter.offset > 0 { filter.offset } else { 0 });
    let rows = query.fetch_all(pool).await?;
    rows.into_iter().map(row_to_tweet).collect()
}

pub async fn list_account_tweets_for_window(
    pool: &PgPool,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    limit: i64,
) -> anyhow::Result<Vec<StoredTweet>> {
    let rows = sqlx::query(
        r#"
        SELECT t.*
        FROM tweets t
        WHERE t.source_type = 'account'
          AND t.created_at >= $1
          AND t.created_at < $2
          AND EXISTS (
              SELECT 1
              FROM sources s
              WHERE s.source_type = 'account'
                AND s.enabled = 1
                AND LOWER(s.value) = LOWER(t.source_value)
          )
        ORDER BY LOWER(t.author_username) ASC, t.created_at ASC
        LIMIT $3
        "#,
    )
    .bind(window_start.to_rfc3339())
    .bind(window_end.to_rfc3339())
    .bind(if limit > 0 { limit } else { 1000 })
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_tweet).collect()
}

pub async fn get_daily_digest_run(
    pool: &PgPool,
    digest_date: &str,
    channel: &str,
) -> anyhow::Result<Option<DailyDigestRun>> {
    let row = sqlx::query(
        r#"
        SELECT digest_date, channel, window_start, window_end, status, retry_count,
               payload_json, error, delivered_at
        FROM daily_digest_runs
        WHERE digest_date = $1 AND channel = $2
        LIMIT 1
        "#,
    )
    .bind(digest_date)
    .bind(channel)
    .fetch_optional(pool)
    .await?;
    row.map(|row| {
        let payload = serde_json::from_str(row.get::<String, _>("payload_json").as_str())
            .unwrap_or(Value::Object(Default::default()));
        Ok(DailyDigestRun {
            digest_date: row.get("digest_date"),
            channel: row.get("channel"),
            window_start: row.get("window_start"),
            window_end: row.get("window_end"),
            status: row.get("status"),
            retry_count: row.get("retry_count"),
            payload,
            error: row.get("error"),
            delivered_at: row.get("delivered_at"),
        })
    })
    .transpose()
}

pub async fn daily_digest_delivered(
    pool: &PgPool,
    digest_date: &str,
    channel: &str,
) -> anyhow::Result<bool> {
    let delivered = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT COUNT(*) > 0
        FROM daily_digest_runs
        WHERE digest_date = $1 AND channel = $2 AND status = 'delivered'
        "#,
    )
    .bind(digest_date)
    .bind(channel)
    .fetch_one(pool)
    .await?;
    Ok(delivered)
}

pub async fn save_daily_digest_run(
    pool: &PgPool,
    update: DailyDigestRunUpdate<'_>,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    let delivered_at = if update.status == "delivered" {
        Some(now.clone())
    } else {
        None
    };
    sqlx::query(
        r#"
        INSERT INTO daily_digest_runs (
            digest_date, channel, window_start, window_end, status,
            retry_count, payload_json, error, created_at, delivered_at
        )
        VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9)
        ON CONFLICT(digest_date, channel) DO UPDATE SET
            window_start=excluded.window_start,
            window_end=excluded.window_end,
            status=excluded.status,
            retry_count = CASE
                WHEN excluded.status = 'delivered' THEN daily_digest_runs.retry_count
                ELSE daily_digest_runs.retry_count + 1
            END,
            payload_json=excluded.payload_json,
            error=excluded.error,
            delivered_at=excluded.delivered_at
        "#,
    )
    .bind(update.digest_date)
    .bind(update.channel)
    .bind(update.window_start.to_rfc3339())
    .bind(update.window_end.to_rfc3339())
    .bind(update.status)
    .bind(update.payload.to_string())
    .bind(update.error)
    .bind(now)
    .bind(delivered_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_undelivered_tweets(
    pool: &PgPool,
    channel: &str,
    limit: i64,
    max_retries: i64,
) -> anyhow::Result<Vec<StoredTweet>> {
    let rows = sqlx::query(
        r#"
        SELECT t.*
        FROM tweets t
        LEFT JOIN deliveries d
            ON d.tweet_id = t.tweet_id
            AND d.channel = $1
            AND (d.status = 'delivered'
                 OR d.status = 'dead'
                 OR d.retry_count >= $3)
        WHERE d.id IS NULL
        ORDER BY t.created_at ASC
        LIMIT $2
        "#,
    )
    .bind(channel)
    .bind(limit)
    .bind(max_retries)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_tweet).collect()
}

pub async fn save_delivery(
    pool: &PgPool,
    tweet_id: &str,
    channel: &str,
    status: &str,
    payload: &Value,
    delivered: bool,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    let delivered_at = if delivered { Some(now.clone()) } else { None };
    let final_status = if status == "dead" { "dead" } else { status };
    sqlx::query(
        r#"
        INSERT INTO deliveries (tweet_id, channel, status, retry_count, payload_json, created_at, delivered_at)
        VALUES ($1, $2, $3, 0, $4, $5, $6)
        ON CONFLICT(tweet_id, channel) DO UPDATE SET
            status=excluded.status,
            retry_count = deliveries.retry_count + 1,
            payload_json=excluded.payload_json,
            created_at=excluded.created_at,
            delivered_at=excluded.delivered_at
        "#,
    )
    .bind(tweet_id)
    .bind(channel)
    .bind(final_status)
    .bind(payload.to_string())
    .bind(now)
    .bind(delivered_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn import_auth_account(pool: &PgPool, token: &TokenImport) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
        INSERT INTO auth_accounts (label, domain, auth_token, ct0, status, exported_at, created_at, updated_at)
        VALUES ($1, $2, $3, $4, 'unknown', $5, $6, $7)
        ON CONFLICT(label) DO UPDATE SET
            domain=excluded.domain,
            auth_token=excluded.auth_token,
            ct0=excluded.ct0,
            status='unknown',
            exported_at=excluded.exported_at,
            updated_at=excluded.updated_at
        "#,
    )
    .bind(&token.label)
    .bind(&token.domain)
    .bind(&token.auth_token)
    .bind(&token.ct0)
    .bind(&token.exported_at)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;
    tracing::debug!(label = %token.label, "upserted auth account");
    Ok(())
}

pub async fn list_auth_accounts(pool: &PgPool) -> anyhow::Result<Vec<AuthAccount>> {
    let rows = sqlx::query(
        "SELECT label, domain, auth_token, ct0, status, exported_at, updated_at FROM auth_accounts ORDER BY label",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| AuthAccount {
            label: row.get("label"),
            domain: row.get("domain"),
            auth_token_masked: mask_token(row.get::<String, _>("auth_token").as_str()),
            ct0_masked: mask_token(row.get::<String, _>("ct0").as_str()),
            status: row.get("status"),
            exported_at: row.get("exported_at"),
            updated_at: row.get("updated_at"),
        })
        .collect())
}

pub async fn get_auth_account(pool: &PgPool, label: &str) -> anyhow::Result<Option<AuthAccount>> {
    let row = sqlx::query(
        "SELECT label, domain, auth_token, ct0, status, exported_at, updated_at FROM auth_accounts WHERE label = $1",
    )
    .bind(label)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| AuthAccount {
        label: row.get("label"),
        domain: row.get("domain"),
        auth_token_masked: mask_token(row.get::<String, _>("auth_token").as_str()),
        ct0_masked: mask_token(row.get::<String, _>("ct0").as_str()),
        status: row.get("status"),
        exported_at: row.get("exported_at"),
        updated_at: row.get("updated_at"),
    }))
}

pub async fn first_auth_account_secret(pool: &PgPool) -> anyhow::Result<Option<AuthAccountSecret>> {
    let row = sqlx::query(
        r#"
        SELECT label, auth_token, ct0
        FROM auth_accounts
        WHERE status NOT IN ('rejected', 'deleted')
          AND (limited_until IS NULL OR limited_until <= $1)
        ORDER BY label
        LIMIT 1
        "#,
    )
    .bind(Utc::now().to_rfc3339())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| AuthAccountSecret {
        label: row.get("label"),
        auth_token: row.get("auth_token"),
        ct0: row.get("ct0"),
    }))
}

/// Select the least-recently-used auth account for round-robin rotation.
pub async fn next_auth_account_secret(pool: &PgPool) -> anyhow::Result<Option<AuthAccountSecret>> {
    let row = sqlx::query(
        r#"
        SELECT label, auth_token, ct0
        FROM auth_accounts
        WHERE status NOT IN ('rejected', 'deleted')
          AND (limited_until IS NULL OR limited_until <= $1)
        ORDER BY CASE WHEN last_used_at IS NULL THEN 0 ELSE 1 END, last_used_at ASC, label ASC
        LIMIT 1
        "#,
    )
    .bind(Utc::now().to_rfc3339())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| AuthAccountSecret {
        label: row.get("label"),
        auth_token: row.get("auth_token"),
        ct0: row.get("ct0"),
    }))
}

pub async fn mark_auth_used(pool: &PgPool, label: &str) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE auth_accounts
        SET status = 'active',
            last_used_at = $1,
            consecutive_failures = 0,
            updated_at = $2
        WHERE label = $3
        "#,
    )
    .bind(Utc::now().to_rfc3339())
    .bind(Utc::now().to_rfc3339())
    .bind(label)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_auth_rejected(pool: &PgPool, label: &str, status: &str) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE auth_accounts
        SET status = $1,
            consecutive_failures = consecutive_failures + 1,
            updated_at = $2
        WHERE label = $3
        "#,
    )
    .bind(status)
    .bind(Utc::now().to_rfc3339())
    .bind(label)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_auth_limited(
    pool: &PgPool,
    label: &str,
    limited_until: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE auth_accounts
        SET status = 'limited',
            limited_until = $1,
            updated_at = $2
        WHERE label = $3
        "#,
    )
    .bind(limited_until)
    .bind(Utc::now().to_rfc3339())
    .bind(label)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn save_auth_rate_limit(
    pool: &PgPool,
    update: &AuthRateLimitUpdate,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO auth_rate_limits (auth_label, endpoint, remaining, reset_at, limit_value, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT(auth_label, endpoint) DO UPDATE SET
            remaining=excluded.remaining,
            reset_at=excluded.reset_at,
            limit_value=excluded.limit_value,
            updated_at=excluded.updated_at
        "#,
    )
    .bind(&update.auth_label)
    .bind(&update.endpoint)
    .bind(update.remaining)
    .bind(&update.reset_at)
    .bind(update.limit_value)
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_auth_rate_limit(
    pool: &PgPool,
    auth_label: &str,
    endpoint: &str,
) -> anyhow::Result<Option<AuthRateLimit>> {
    let row = sqlx::query(
        r#"
        SELECT auth_label, endpoint, remaining, reset_at, limit_value, updated_at
        FROM auth_rate_limits
        WHERE auth_label = $1 AND endpoint = $2
        "#,
    )
    .bind(auth_label)
    .bind(endpoint)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| AuthRateLimit {
        auth_label: row.get("auth_label"),
        endpoint: row.get("endpoint"),
        remaining: row.get("remaining"),
        reset_at: row.get("reset_at"),
        limit_value: row.get("limit_value"),
        updated_at: row.get("updated_at"),
    }))
}

pub async fn delete_auth_account(pool: &PgPool, label: &str) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM auth_accounts WHERE label = $1")
        .bind(label)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn get_tweet(pool: &PgPool, tweet_id: &str) -> anyhow::Result<Option<StoredTweet>> {
    let row = sqlx::query("SELECT * FROM tweets WHERE tweet_id = $1 LIMIT 1")
        .bind(tweet_id)
        .fetch_optional(pool)
        .await?;
    row.map(row_to_tweet).transpose()
}

fn row_to_tweet(row: PgRow) -> anyhow::Result<StoredTweet> {
    let source_type = SourceType::try_from(row.get::<String, _>("source_type").as_str())?;
    let raw = serde_json::from_str(row.get::<String, _>("raw_json").as_str())
        .unwrap_or(Value::Object(Default::default()));
    let tweet = Tweet {
        tweet_id: row.get("tweet_id"),
        source_type,
        source_value: row.get("source_value"),
        author_username: row.get("author_username"),
        author_name: row.get("author_name"),
        text: row.get("text"),
        url: row.get("url"),
        created_at: DateTime::parse_from_rfc3339(row.get::<String, _>("created_at").as_str())?
            .with_timezone(&Utc),
        fetched_at: DateTime::parse_from_rfc3339(row.get::<String, _>("fetched_at").as_str())?
            .with_timezone(&Utc),
        raw,
    };
    Ok(StoredTweet { tweet })
}

fn row_to_favorite(row: PgRow) -> anyhow::Result<TelegramFavorite> {
    let chat_id = row.get("favorite_chat_id");
    let created_at = row.get("favorite_created_at");
    let tweet = row_to_tweet(row)?;
    Ok(TelegramFavorite {
        chat_id,
        created_at,
        tweet,
    })
}

pub async fn check_token_freshness(
    pool: &PgPool,
    threshold_days: i64,
) -> anyhow::Result<Vec<(String, String)>> {
    let cutoff = Utc::now() - chrono::Duration::days(threshold_days);
    let rows = sqlx::query(
        r#"
        SELECT label, updated_at
        FROM auth_accounts
        WHERE status NOT IN ('rejected', 'deleted')
          AND updated_at < $1
        "#,
    )
    .bind(cutoff.to_rfc3339())
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("label"),
                row.get::<String, _>("updated_at"),
            )
        })
        .collect())
}

pub fn delivery_payload<T: Serialize>(value: &T) -> Value {
    to_json_value(value)
}

// --- Spam keywords ---

pub async fn list_spam_keywords(pool: &PgPool) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query("SELECT keyword FROM spam_keywords ORDER BY keyword")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("keyword"))
        .collect())
}

pub async fn add_spam_keyword(pool: &PgPool, keyword: &str) -> anyhow::Result<bool> {
    let now = Utc::now().to_rfc3339();
    let keyword = keyword.trim().to_lowercase();
    if keyword.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(
        "INSERT INTO spam_keywords (keyword, created_at, updated_at) VALUES ($1, $2, $3)
         ON CONFLICT(keyword) DO NOTHING",
    )
    .bind(&keyword)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn remove_spam_keyword(pool: &PgPool, keyword: &str) -> anyhow::Result<bool> {
    let keyword = keyword.trim().to_lowercase();
    let result = sqlx::query("DELETE FROM spam_keywords WHERE keyword = $1")
        .bind(&keyword)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}
