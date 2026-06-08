use crate::config::AppConfig;
use crate::storage::{self, AuthAccount, TokenImport};
use sqlx::PgPool;
use std::path::Path;

pub struct LiveCheckResult {
    pub ok: bool,
    pub error: Option<String>,
}

pub async fn import_token_json(pool: &PgPool, path: &Path) -> anyhow::Result<TokenImport> {
    let token: TokenImport = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    validate_token(&token)?;
    storage::import_auth_account(pool, &token).await?;
    Ok(token)
}

pub async fn import_token_values(
    pool: &PgPool,
    label: String,
    auth_token: String,
    ct0: String,
) -> anyhow::Result<TokenImport> {
    let token = TokenImport {
        label,
        domain: "x.com".to_string(),
        auth_token,
        ct0,
        exported_at: None,
    };
    validate_token(&token)?;
    storage::import_auth_account(pool, &token).await?;
    Ok(token)
}

pub fn validate_token(token: &TokenImport) -> anyhow::Result<()> {
    if token.label.trim().is_empty() {
        anyhow::bail!("token label is required");
    }
    if token.auth_token.len() < 8 {
        anyhow::bail!("auth_token is too short");
    }
    if token.ct0.len() < 4 {
        anyhow::bail!("ct0 is too short");
    }
    Ok(())
}

pub async fn check_account(pool: &PgPool, label: &str) -> anyhow::Result<AuthAccount> {
    storage::get_auth_account(pool, label)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no auth account found for {label}"))
}

pub async fn check_account_live(
    pool: &PgPool,
    config: &AppConfig,
) -> anyhow::Result<LiveCheckResult> {
    let secret = storage::first_auth_account_secret(pool).await?;
    let Some(secret) = secret else {
        return Ok(LiveCheckResult {
            ok: false,
            error: Some("no auth account available".to_string()),
        });
    };
    match crate::fetch::verify_auth(config, &secret).await {
        Ok(()) => Ok(LiveCheckResult {
            ok: true,
            error: None,
        }),
        Err(err) => Ok(LiveCheckResult {
            ok: false,
            error: Some(err.to_string()),
        }),
    }
}
