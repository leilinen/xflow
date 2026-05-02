use crate::config::AppConfig;
use crate::rss_feed;
use crate::storage::{self, TweetFilter};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use sqlx::SqlitePool;
use std::net::SocketAddr;
use std::sync::Arc;

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
        .with_state(state)
}

pub async fn serve(config: AppConfig, pool: SqlitePool) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("serving xFlow on http://{addr}");
    axum::serve(listener, router(config, pool)).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

async fn json_all(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            limit: 200,
            ..Default::default()
        },
    )
    .await?;
    Ok(Json(json!({"tweets": tweets})))
}

async fn json_important(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            important_only: true,
            limit: 200,
            ..Default::default()
        },
    )
    .await?;
    Ok(Json(json!({"tweets": tweets})))
}

async fn rss_all(State(state): State<AppState>) -> Result<Response, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            limit: 200,
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
) -> Result<Response, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            username: Some(username.clone()),
            limit: 200,
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

async fn rss_important(State(state): State<AppState>) -> Result<Response, AppError> {
    let tweets = storage::list_tweets(
        &state.pool,
        TweetFilter {
            important_only: true,
            limit: 200,
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
