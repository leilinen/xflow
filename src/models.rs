use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Account,
    List,
    Search,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::List => "list",
            Self::Search => "search",
        }
    }
}

impl TryFrom<&str> for SourceType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "account" => Ok(Self::Account),
            "list" => Ok(Self::List),
            "search" => Ok(Self::Search),
            _ => anyhow::bail!("unsupported source type: {value}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub source_type: SourceType,
    pub value: String,
    pub label: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tweet {
    pub tweet_id: String,
    pub source_type: SourceType,
    pub source_value: String,
    pub author_username: String,
    pub author_name: String,
    pub text: String,
    pub url: String,
    pub created_at: DateTime<Utc>,
    pub fetched_at: DateTime<Utc>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TweetComment {
    pub tweet_id: String,
    pub author_username: String,
    pub author_name: String,
    pub text: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub media_urls: Vec<String>,
    #[serde(default)]
    pub external_links: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTweet {
    #[serde(flatten)]
    pub tweet: Tweet,
}
