use crate::models::{Source, SourceType};
use crate::utils::{resolve_relative, DEFAULT_DB_PATH};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub database: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database: PathBuf::from(DEFAULT_DB_PATH),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchConfig {
    pub interval_seconds: u64,
    pub default_limit: i64,
    pub fetcher: String,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 900,
            default_limit: 20,
            fetcher: "mock".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceConfig {
    #[serde(default)]
    pub accounts: Vec<AccountSourceConfig>,
    #[serde(default)]
    pub lists: Vec<ListSourceConfig>,
    #[serde(default)]
    pub searches: Vec<SearchSourceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSourceConfig {
    pub username: String,
    pub label: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSourceConfig {
    pub list_id: String,
    pub label: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSourceConfig {
    pub query: String,
    pub label: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub enabled: bool,
    #[serde(default = "default_keywords")]
    pub keywords: Vec<String>,
    pub importance_threshold: f64,
    pub push_threshold: f64,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            keywords: default_keywords(),
            importance_threshold: 0.45,
            push_threshold: 0.7,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramConfig {
    pub enabled: bool,
    pub bot_token_env: String,
    pub chat_id_env: String,
    pub send_all: bool,
    pub parse_mode: String,
    pub disable_web_page_preview: bool,
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token_env: "TELEGRAM_BOT_TOKEN".to_string(),
            chat_id_env: "TELEGRAM_CHAT_ID".to_string(),
            send_all: true,
            parse_mode: "HTML".to_string(),
            disable_web_page_preview: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub fetch: FetchConfig,
    #[serde(default)]
    pub sources: SourceConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub telegram: TelegramConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            fetch: FetchConfig::default(),
            sources: SourceConfig {
                accounts: vec![AccountSourceConfig {
                    username: "openai".to_string(),
                    label: None,
                    limit: Some(5),
                }],
                lists: vec![ListSourceConfig {
                    list_id: "ai-builders".to_string(),
                    label: None,
                    limit: Some(5),
                }],
                searches: vec![SearchSourceConfig {
                    query: "AI agent".to_string(),
                    label: None,
                    limit: Some(5),
                }],
            },
            agent: AgentConfig::default(),
            telegram: TelegramConfig::default(),
        }
    }
}

fn default_keywords() -> Vec<String> {
    vec![
        "AI".to_string(),
        "agent".to_string(),
        "LLM".to_string(),
        "Claude".to_string(),
        "OpenAI".to_string(),
        "Anthropic".to_string(),
        "Cursor".to_string(),
        "coding".to_string(),
        "model".to_string(),
        "paper".to_string(),
        "GitHub".to_string(),
    ]
}

pub fn write_default_config(path: &Path) -> anyhow::Result<()> {
    let text = serde_yaml::to_string(&AppConfig::default())?;
    std::fs::write(path, text)?;
    Ok(())
}

pub fn load_config(path: &Path) -> anyhow::Result<AppConfig> {
    let mut config = if path.exists() {
        serde_yaml::from_str::<AppConfig>(&std::fs::read_to_string(path)?)?
    } else {
        AppConfig::default()
    };
    config.storage.database = resolve_relative(path, &config.storage.database);
    Ok(config)
}

impl AppConfig {
    pub fn parsed_sources(&self) -> Vec<Source> {
        let mut sources = Vec::new();
        for item in &self.sources.accounts {
            sources.push(Source {
                source_type: SourceType::Account,
                value: item.username.trim_start_matches('@').to_string(),
                label: item.label.clone(),
                limit: item.limit.or(Some(self.fetch.default_limit)),
            });
        }
        for item in &self.sources.lists {
            sources.push(Source {
                source_type: SourceType::List,
                value: item.list_id.clone(),
                label: item.label.clone(),
                limit: item.limit.or(Some(self.fetch.default_limit)),
            });
        }
        for item in &self.sources.searches {
            sources.push(Source {
                source_type: SourceType::Search,
                value: item.query.clone(),
                label: item.label.clone(),
                limit: item.limit.or(Some(self.fetch.default_limit)),
            });
        }
        sources
    }
}
