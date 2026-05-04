use crate::models::{Source, SourceType, StoredTweet, Tweet, TweetAnalysis};
use crate::utils::{mask_token, to_json_value};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, SqlitePool};

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
    pub important_only: bool,
    pub limit: i64,
}

pub async fn upsert_source(pool: &SqlitePool, source: &Source) -> anyhow::Result<()> {
    upsert_source_enabled(pool, source, true).await
}

pub async fn upsert_source_enabled(
    pool: &SqlitePool,
    source: &Source,
    enabled: bool,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
        INSERT INTO sources (source_type, value, label, fetch_limit, enabled, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_type, value) DO UPDATE SET
            label=excluded.label,
            fetch_limit=excluded.fetch_limit,
            enabled=excluded.enabled,
            updated_at=excluded.updated_at
        "#,
    )
    .bind(source.source_type.as_str())
    .bind(&source.value)
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
    pool: &SqlitePool,
    source_type: SourceType,
    value: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE sources
        SET enabled = 0, updated_at = ?
        WHERE source_type = ? AND value = ?
        "#,
    )
    .bind(Utc::now().to_rfc3339())
    .bind(source_type.as_str())
    .bind(value.trim_start_matches('@'))
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn list_sources(pool: &SqlitePool, enabled_only: bool) -> anyhow::Result<Vec<Source>> {
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

pub async fn ensure_config_sources(pool: &SqlitePool, sources: &[Source]) -> anyhow::Result<()> {
    for source in sources {
        upsert_source(pool, source).await?;
    }
    Ok(())
}

pub async fn upsert_tweet(pool: &SqlitePool, tweet: &Tweet) -> anyhow::Result<bool> {
    let result = sqlx::query(
        r#"
        INSERT INTO tweets (
            tweet_id, source_type, source_value, author_username, author_name,
            text, url, created_at, fetched_at, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tweet_id) DO UPDATE SET
            fetched_at=excluded.fetched_at,
            raw_json=excluded.raw_json
        "#,
    )
    .bind(&tweet.tweet_id)
    .bind(tweet.source_type.as_str())
    .bind(&tweet.source_value)
    .bind(&tweet.author_username)
    .bind(&tweet.author_name)
    .bind(&tweet.text)
    .bind(&tweet.url)
    .bind(tweet.created_at.to_rfc3339())
    .bind(tweet.fetched_at.to_rfc3339())
    .bind(tweet.raw.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn save_fetch_state(
    pool: &SqlitePool,
    source: &Source,
    status: &str,
    message: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO fetch_state (source_type, source_value, last_fetch_at, last_status, message)
        VALUES (?, ?, ?, ?, ?)
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

pub async fn save_analysis(pool: &SqlitePool, analysis: &TweetAnalysis) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO tweet_analysis (
            tweet_id, relevance, importance_score, category, tags_json,
            chinese_summary, reason, should_push, analyzed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tweet_id) DO UPDATE SET
            relevance=excluded.relevance,
            importance_score=excluded.importance_score,
            category=excluded.category,
            tags_json=excluded.tags_json,
            chinese_summary=excluded.chinese_summary,
            reason=excluded.reason,
            should_push=excluded.should_push,
            analyzed_at=excluded.analyzed_at
        "#,
    )
    .bind(&analysis.tweet_id)
    .bind(analysis.relevance)
    .bind(analysis.importance_score)
    .bind(&analysis.category)
    .bind(serde_json::to_string(&analysis.tags)?)
    .bind(&analysis.chinese_summary)
    .bind(&analysis.reason)
    .bind(i64::from(analysis.should_push))
    .bind(analysis.analyzed_at.to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_tweets(
    pool: &SqlitePool,
    filter: TweetFilter,
) -> anyhow::Result<Vec<StoredTweet>> {
    let mut sql = String::from(
        r#"
        SELECT t.*, a.relevance, a.importance_score, a.category, a.tags_json,
               a.chinese_summary, a.reason, a.should_push, a.analyzed_at
        FROM tweets t
        LEFT JOIN tweet_analysis a ON a.tweet_id = t.tweet_id
        "#,
    );
    let mut where_parts = Vec::new();
    if filter.username.is_some() {
        where_parts.push("LOWER(t.author_username) = LOWER(?)");
    }
    if filter.important_only {
        where_parts.push("COALESCE(a.should_push, 0) = 1");
    }
    if !where_parts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
    }
    sql.push_str(" ORDER BY t.created_at DESC LIMIT ?");
    let mut query = sqlx::query(&sql);
    if let Some(username) = filter.username {
        query = query.bind(username.trim_start_matches('@').to_string());
    }
    query = query.bind(if filter.limit > 0 { filter.limit } else { 100 });
    let rows = query.fetch_all(pool).await?;
    rows.into_iter().map(row_to_tweet).collect()
}

pub async fn list_analyzed_for_digest(
    pool: &SqlitePool,
    threshold: f64,
    limit: i64,
) -> anyhow::Result<Vec<StoredTweet>> {
    let rows = sqlx::query(
        r#"
        SELECT t.*, a.relevance, a.importance_score, a.category, a.tags_json,
               a.chinese_summary, a.reason, a.should_push, a.analyzed_at
        FROM tweets t
        JOIN tweet_analysis a ON a.tweet_id = t.tweet_id
        WHERE a.importance_score >= ?
        ORDER BY a.category ASC, a.importance_score DESC, t.created_at DESC
        LIMIT ?
        "#,
    )
    .bind(threshold)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_tweet).collect()
}

pub async fn list_undelivered_tweets(
    pool: &SqlitePool,
    channel: &str,
    important_only: bool,
    limit: i64,
) -> anyhow::Result<Vec<StoredTweet>> {
    let important_clause = if important_only {
        "AND COALESCE(a.should_push, 0) = 1"
    } else {
        ""
    };
    let sql = format!(
        r#"
        SELECT t.*, a.relevance, a.importance_score, a.category, a.tags_json,
               a.chinese_summary, a.reason, a.should_push, a.analyzed_at
        FROM tweets t
        LEFT JOIN tweet_analysis a ON a.tweet_id = t.tweet_id
        LEFT JOIN deliveries d
            ON d.tweet_id = t.tweet_id
            AND d.channel = ?
            AND d.status = 'delivered'
        WHERE d.id IS NULL {important_clause}
        ORDER BY t.created_at ASC
        LIMIT ?
        "#
    );
    let rows = sqlx::query(&sql)
        .bind(channel)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_tweet).collect()
}

pub async fn save_delivery(
    pool: &SqlitePool,
    tweet_id: &str,
    channel: &str,
    status: &str,
    payload: &Value,
    delivered: bool,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    let delivered_at = if delivered { Some(now.clone()) } else { None };
    sqlx::query(
        "INSERT INTO deliveries (tweet_id, channel, status, payload_json, created_at, delivered_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(tweet_id)
    .bind(channel)
    .bind(status)
    .bind(payload.to_string())
    .bind(now)
    .bind(delivered_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn import_auth_account(pool: &SqlitePool, token: &TokenImport) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
        INSERT INTO auth_accounts (label, domain, auth_token, ct0, status, exported_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'unknown', ?, ?, ?)
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
    Ok(())
}

pub async fn list_auth_accounts(pool: &SqlitePool) -> anyhow::Result<Vec<AuthAccount>> {
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

pub async fn get_auth_account(
    pool: &SqlitePool,
    label: &str,
) -> anyhow::Result<Option<AuthAccount>> {
    Ok(list_auth_accounts(pool)
        .await?
        .into_iter()
        .find(|a| a.label == label))
}

pub async fn first_auth_account_secret(
    pool: &SqlitePool,
) -> anyhow::Result<Option<AuthAccountSecret>> {
    let row = sqlx::query(
        r#"
        SELECT label, auth_token, ct0
        FROM auth_accounts
        WHERE status NOT IN ('rejected', 'deleted')
          AND (limited_until IS NULL OR limited_until <= ?)
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

pub async fn mark_auth_used(pool: &SqlitePool, label: &str) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE auth_accounts
        SET status = 'active',
            last_used_at = ?,
            consecutive_failures = 0,
            updated_at = ?
        WHERE label = ?
        "#,
    )
    .bind(Utc::now().to_rfc3339())
    .bind(Utc::now().to_rfc3339())
    .bind(label)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_auth_rejected(
    pool: &SqlitePool,
    label: &str,
    status: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE auth_accounts
        SET status = ?,
            consecutive_failures = consecutive_failures + 1,
            updated_at = ?
        WHERE label = ?
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
    pool: &SqlitePool,
    label: &str,
    limited_until: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE auth_accounts
        SET status = 'limited',
            limited_until = ?,
            updated_at = ?
        WHERE label = ?
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
    pool: &SqlitePool,
    update: &AuthRateLimitUpdate,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO auth_rate_limits (auth_label, endpoint, remaining, reset_at, limit_value, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
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
    pool: &SqlitePool,
    auth_label: &str,
    endpoint: &str,
) -> anyhow::Result<Option<AuthRateLimit>> {
    let row = sqlx::query(
        r#"
        SELECT auth_label, endpoint, remaining, reset_at, limit_value, updated_at
        FROM auth_rate_limits
        WHERE auth_label = ? AND endpoint = ?
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

pub async fn delete_auth_account(pool: &SqlitePool, label: &str) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM auth_accounts WHERE label = ?")
        .bind(label)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

fn row_to_tweet(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<StoredTweet> {
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
    let importance_score: Option<f64> = row.get("importance_score");
    let analysis = if importance_score.is_some() {
        Some(TweetAnalysis {
            tweet_id: tweet.tweet_id.clone(),
            relevance: row.get("relevance"),
            importance_score: importance_score.unwrap_or_default(),
            category: row.get("category"),
            tags: serde_json::from_str(row.get::<String, _>("tags_json").as_str())
                .unwrap_or_default(),
            chinese_summary: row.get("chinese_summary"),
            reason: row.get("reason"),
            should_push: row.get::<i64, _>("should_push") == 1,
            analyzed_at: DateTime::parse_from_rfc3339(
                row.get::<String, _>("analyzed_at").as_str(),
            )?
            .with_timezone(&Utc),
        })
    } else {
        None
    };
    Ok(StoredTweet { tweet, analysis })
}

pub fn delivery_payload<T: Serialize>(value: &T) -> Value {
    to_json_value(value)
}
