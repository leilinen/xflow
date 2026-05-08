use crate::config::AppConfig;
use crate::rss_feed;
use crate::storage::{self, TweetFilter};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use sqlx::SqlitePool;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct PaginationParams {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 {
    200
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub pool: SqlitePool,
}

pub fn router(config: AppConfig, pool: SqlitePool) -> Router {
    let state = AppState {
        config: Arc::new(config),
        pool,
    };
    Router::new()
        .route("/health", get(health))
        .route("/json/all", get(json_all))
        .route("/json/important", get(json_important))
        .route("/rss/all", get(rss_all))
        .route("/rss/account/:username", get(rss_account))
        .route("/rss/important", get(rss_important))
        .route("/api/sources", get(api_sources))
        .route("/api/fetch-state", get(api_fetch_state))
        .with_state(state)
}

pub async fn serve(config: AppConfig, pool: SqlitePool) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("serving xFlow on http://{addr}");
    axum::serve(listener, router(config, pool)).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let db_ok = sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&state.pool)
        .await
        .is_ok();
    if db_ok {
        Json(json!({"status": "ok", "db": "ok"}))
    } else {
        Json(json!({"status": "degraded", "db": "error"}))
    }
}

async fn json_all(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            limit: params.limit,
            offset: params.offset,
            ..Default::default()
        },
    )
    .await?;
    Ok(Json(json!({"tweets": tweets})))
}

async fn json_important(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            important_only: true,
            limit: params.limit,
            offset: params.offset,
            ..Default::default()
        },
    )
    .await?;
    Ok(Json(json!({"tweets": tweets})))
}

async fn rss_all(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Response, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            limit: params.limit,
            offset: params.offset,
            ..Default::default()
        },
    )
    .await?;
    rss_response(rss_feed::generate_rss(
        "xFlow All",
        "http://localhost/rss/all",
        "All cached xFlow tweets",
        &tweets,
    )?)
}

async fn rss_account(
    State(state): State<AppState>,
    Path(username): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Response, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            username: Some(username.clone()),
            limit: params.limit,
            offset: params.offset,
            ..Default::default()
        },
    )
    .await?;
    rss_response(rss_feed::generate_rss(
        &format!("xFlow @{username}"),
        &format!("http://localhost/rss/account/{username}"),
        &format!("Cached tweets from @{username}"),
        &tweets,
    )?)
}

async fn rss_important(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Response, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            important_only: true,
            limit: params.limit,
            offset: params.offset,
            ..Default::default()
        },
    )
    .await?;
    rss_response(rss_feed::generate_rss(
        "xFlow Important",
        "http://localhost/rss/important",
        "Important cached xFlow tweets",
        &tweets,
    )?)
}

async fn api_sources(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    let sources = storage::list_sources(&state.pool, false).await?;
    let fetch_states = sqlx::query(
        "SELECT source_type, source_value, last_fetch_at, last_status, message FROM fetch_state",
    )
    .fetch_all(&state.pool)
    .await?;

    use sqlx::Row;
    let mut state_map = std::collections::HashMap::new();
    for row in &fetch_states {
        let key = format!(
            "{}:{}",
            row.get::<String, _>("source_type"),
            row.get::<String, _>("source_value")
        );
        state_map.insert(
            key,
            json!({
                "last_fetch_at": row.get::<Option<String>, _>("last_fetch_at"),
                "last_status": row.get::<String, _>("last_status"),
                "message": row.get::<Option<String>, _>("message"),
            }),
        );
    }

    let sources_json: Vec<serde_json::Value> = sources
        .iter()
        .map(|s| {
            let key = format!("{}:{}", s.source_type.as_str(), s.value);
            let mut obj = json!({
                "source_type": s.source_type.as_str(),
                "value": s.value,
                "label": s.label,
                "limit": s.limit,
            });
            if let Some(state) = state_map.get(&key) {
                obj.as_object_mut()
                    .unwrap()
                    .insert("fetch_state".to_string(), state.clone());
            }
            obj
        })
        .collect();

    Ok(Json(json!({"sources": sources_json})))
}

async fn api_fetch_state(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT source_type, source_value, last_fetch_at, last_status, message \
         FROM fetch_state ORDER BY last_fetch_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;

    let states: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            json!({
                "source_type": row.get::<String, _>("source_type"),
                "source_value": row.get::<String, _>("source_value"),
                "last_fetch_at": row.get::<Option<String>, _>("last_fetch_at"),
                "last_status": row.get::<String, _>("last_status"),
                "message": row.get::<Option<String>, _>("message"),
            })
        })
        .collect();

    Ok(Json(json!({"fetch_states": states})))
}

fn rss_response(xml: String) -> Result<Response, AppError> {
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/rss+xml; charset=utf-8",
        )],
        xml,
    )
        .into_response())
}

pub struct AppError(anyhow::Error);

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!("{:?}", self.0);
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}
