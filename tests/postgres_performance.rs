mod support;

use chrono::Utc;
use serde_json::{json, Value};
use std::time::Instant;
use support::TestDatabase;
use xflow::models::{SourceType, Tweet};
use xflow::storage;

fn tweets(prefix: &str, count: usize) -> Vec<Tweet> {
    (0..count)
        .map(|index| {
            let tweet_id = format!("{prefix}-{index:04}");
            Tweet {
                tweet_id: tweet_id.clone(),
                source_type: SourceType::Account,
                source_value: "performance".to_string(),
                author_username: "performance".to_string(),
                author_name: "Performance Test".to_string(),
                text: format!("performance tweet {index}"),
                url: format!("https://x.com/performance/status/{tweet_id}"),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw: json!({"index": index}),
            }
        })
        .collect()
}

async fn sequential_upsert(pool: &sqlx::PgPool, tweets: &[Tweet]) {
    for tweet in tweets {
        sqlx::query(
            r#"
            INSERT INTO tweets (
                tweet_id, source_type, source_value, author_username, author_name,
                text, url, created_at, fetched_at, raw_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
        .await
        .unwrap();
    }
}

fn milliseconds(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.0
}

fn throughput(count: usize, milliseconds: f64) -> f64 {
    count as f64 / (milliseconds / 1_000.0)
}

async fn acquire_wait_ms(pool: &sqlx::PgPool) -> f64 {
    let start = Instant::now();
    let connection = pool.acquire().await.unwrap();
    let elapsed = milliseconds(start);
    drop(connection);
    elapsed
}

async fn measure_count(database: &TestDatabase, count: usize) -> Vec<Value> {
    let batch_tweets = tweets(&format!("batch-{count}"), count);
    let sequential_tweets = tweets(&format!("sequential-{count}"), count);
    let batches = count.div_ceil(storage::TWEET_UPSERT_BATCH_SIZE);

    let start = Instant::now();
    let batch_new = storage::upsert_tweets(database, &batch_tweets)
        .await
        .unwrap();
    let batch_new_ms = milliseconds(start);
    assert_eq!((batch_new.new, batch_new.duplicate), (count, 0));
    let batch_new_wait_ms = acquire_wait_ms(database.pg_pool()).await;

    let start = Instant::now();
    sequential_upsert(database.pg_pool(), &sequential_tweets).await;
    let sequential_new_ms = milliseconds(start);
    let sequential_new_wait_ms = acquire_wait_ms(database.pg_pool()).await;

    let start = Instant::now();
    let batch_repeated = storage::upsert_tweets(database, &batch_tweets)
        .await
        .unwrap();
    let batch_repeated_ms = milliseconds(start);
    assert_eq!((batch_repeated.new, batch_repeated.duplicate), (0, count));
    let batch_repeated_wait_ms = acquire_wait_ms(database.pg_pool()).await;

    let start = Instant::now();
    sequential_upsert(database.pg_pool(), &sequential_tweets).await;
    let sequential_repeated_ms = milliseconds(start);
    let sequential_repeated_wait_ms = acquire_wait_ms(database.pg_pool()).await;

    vec![
        json!({
            "count": count,
            "scenario": "all_new",
            "batch_ms": batch_new_ms,
            "batch_tweets_per_second": throughput(count, batch_new_ms),
            "batch_estimated_round_trips": batches * 3,
            "batch_post_run_acquire_wait_ms": batch_new_wait_ms,
            "sequential_ms": sequential_new_ms,
            "sequential_tweets_per_second": throughput(count, sequential_new_ms),
            "sequential_estimated_round_trips": count,
            "sequential_post_run_acquire_wait_ms": sequential_new_wait_ms,
        }),
        json!({
            "count": count,
            "scenario": "all_repeated",
            "batch_ms": batch_repeated_ms,
            "batch_tweets_per_second": throughput(count, batch_repeated_ms),
            "batch_estimated_round_trips": batches * 4,
            "batch_post_run_acquire_wait_ms": batch_repeated_wait_ms,
            "sequential_ms": sequential_repeated_ms,
            "sequential_tweets_per_second": throughput(count, sequential_repeated_ms),
            "sequential_estimated_round_trips": count,
            "sequential_post_run_acquire_wait_ms": sequential_repeated_wait_ms,
        }),
    ]
}

#[tokio::test]
#[ignore = "executed explicitly by the PostgreSQL performance CI job"]
async fn postgres_batch_upsert_performance() {
    // The benchmark compares SQL/network round trips, not durable WAL latency.
    // A single connection plus asynchronous commit keeps the 1,000 individual
    // baseline commits repeatable on noisy hosted-runner storage.
    let database = TestDatabase::new(1).await;
    sqlx::query("SET synchronous_commit TO OFF")
        .execute(database.pg_pool())
        .await
        .unwrap();
    let mut measurements = Vec::new();
    println!("measuring 100-Tweet PostgreSQL scenarios");
    measurements.extend(measure_count(&database, 100).await);
    println!("measuring 1,000-Tweet PostgreSQL scenarios");
    measurements.extend(measure_count(&database, 1_000).await);

    let report = json!({
        "postgres": "16",
        "batch_size": storage::TWEET_UPSERT_BATCH_SIZE,
        "synchronous_commit": "off for performance trend only",
        "round_trip_note": "Batch estimates include BEGIN, DML statements, and COMMIT; sequential baseline uses one UPSERT statement per tweet.",
        "measurements": measurements,
    });
    let output = serde_json::to_string_pretty(&report).unwrap();
    println!("STAGE3_POSTGRES_PERFORMANCE={output}");
    std::fs::create_dir_all("target").unwrap();
    std::fs::write("target/stage3-postgres-performance.json", output).unwrap();
}
