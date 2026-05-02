use serde::Serialize;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG_PATH: &str = "config.yaml";
pub const DEFAULT_DATA_DIR: &str = "data";
pub const DEFAULT_DB_PATH: &str = "data/xflow.db";

pub fn mask_token(token: &str) -> String {
    if token.len() <= 8 {
        "*".repeat(token.len())
    } else {
        format!("{}...{}", &token[..4], &token[token.len() - 4..])
    }
}

pub fn ensure_parent(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

pub fn resolve_relative(base_file: &Path, value: impl AsRef<Path>) -> PathBuf {
    let path = value.as_ref();
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_file
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(path)
    }
}

pub fn to_json_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}
