use chrono::{DateTime, FixedOffset, Utc};
use serde::Serialize;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG_PATH: &str = "config.yaml";
pub const DEFAULT_DATA_DIR: &str = "data";

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

fn utc8_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("UTC+8 is a valid offset")
}

pub fn format_utc8(dt: &DateTime<Utc>) -> String {
    dt.with_timezone(&utc8_offset()).format("%Y-%m-%d %H:%M").to_string()
}

pub fn format_utc8_full(dt: &DateTime<Utc>) -> String {
    dt.with_timezone(&utc8_offset()).format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn format_db_timestamp(s: Option<&str>) -> String {
    s.and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|dt| format!("{} UTC+8", format_utc8_full(&dt.with_timezone(&Utc))))
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn format_utc8_converts_correctly() {
        let dt = Utc.with_ymd_and_hms(2026, 5, 8, 0, 0, 0).unwrap();
        assert_eq!(format_utc8(&dt), "2026-05-08 08:00");
    }

    #[test]
    fn format_utc8_full_includes_seconds() {
        let dt = Utc.with_ymd_and_hms(2026, 5, 8, 15, 30, 45).unwrap();
        assert_eq!(format_utc8_full(&dt), "2026-05-08 23:30:45");
    }

    #[test]
    fn format_db_timestamp_parses_rfc3339() {
        let ts = Some("2026-05-08T10:30:00+00:00");
        assert_eq!(format_db_timestamp(ts), "2026-05-08 18:30:00 UTC+8");
    }

    #[test]
    fn format_db_timestamp_handles_none() {
        assert_eq!(format_db_timestamp(None), "-");
    }
}
