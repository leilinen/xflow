use serde::{Deserialize, Serialize};
use std::path::Path;

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
    #[serde(default = "default_database_url")]
    pub database_url: String,
}

fn default_database_url() -> String {
    "postgres://localhost/xflow".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_url: default_database_url(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchConfig {
    pub interval_seconds: u64,
    pub default_limit: i64,
    pub fetcher: String,
    #[serde(default = "default_rate_limit_safety_margin")]
    pub rate_limit_safety_margin: i64,
    #[serde(default)]
    pub source_delay_min_seconds: u64,
    #[serde(default)]
    pub source_delay_max_seconds: u64,
    #[serde(default = "default_command_timeout_seconds")]
    pub command_timeout_seconds: u64,
    #[serde(default = "default_max_delivery_retries")]
    pub max_delivery_retries: i64,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 900,
            default_limit: 5,
            fetcher: "mock".to_string(),
            rate_limit_safety_margin: 10,
            source_delay_min_seconds: 0,
            source_delay_max_seconds: 0,
            command_timeout_seconds: 300,
            max_delivery_retries: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramConfig {
    pub enabled: bool,
    pub bot_token_env: String,
    pub chat_id_env: String,
    #[serde(default)]
    pub discussion_group_id_env: String,
    pub parse_mode: String,
    pub disable_web_page_preview: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranslationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    #[serde(default = "default_translation_prompt")]
    pub system_prompt: String,
}

impl Default for TranslationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key_env: default_api_key_env(),
            base_url: default_base_url(),
            model: default_model(),
            max_tokens: default_max_tokens(),
            temperature: default_temperature(),
            system_prompt: default_translation_prompt(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyDigestConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_daily_digest_send_time")]
    pub send_time: String,
    #[serde(default = "default_daily_digest_timezone_offset_hours")]
    pub timezone_offset_hours: i32,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_daily_digest_model")]
    pub model: String,
    #[serde(default = "default_daily_digest_max_tokens")]
    pub max_tokens: u32,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    #[serde(default = "default_daily_digest_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_daily_digest_max_tweets_per_account")]
    pub max_tweets_per_account: usize,
}

impl Default for DailyDigestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            send_time: default_daily_digest_send_time(),
            timezone_offset_hours: default_daily_digest_timezone_offset_hours(),
            api_key_env: default_api_key_env(),
            base_url: default_base_url(),
            model: default_daily_digest_model(),
            max_tokens: default_daily_digest_max_tokens(),
            temperature: default_temperature(),
            system_prompt: default_daily_digest_prompt(),
            max_tweets_per_account: default_daily_digest_max_tweets_per_account(),
        }
    }
}

fn default_api_key_env() -> String {
    "OPENAI_API_KEY".to_string()
}

fn default_base_url() -> String {
    "https://api.openai.com/v1".to_string()
}

fn default_model() -> String {
    "gpt-4o-mini".to_string()
}

fn default_max_tokens() -> u32 {
    1024
}

fn default_temperature() -> f32 {
    0.3
}

fn default_translation_prompt() -> String {
    "You are a professional translator. Translate the following tweet to Chinese (Simplified). Output only the translation, nothing else.".to_string()
}

fn default_daily_digest_send_time() -> String {
    "18:00".to_string()
}

fn default_daily_digest_timezone_offset_hours() -> i32 {
    8
}

fn default_daily_digest_model() -> String {
    "gpt-4o-mini".to_string()
}

fn default_daily_digest_max_tokens() -> u32 {
    1200
}

fn default_daily_digest_prompt() -> String {
    "你是一个中文信息分析助手。请按账号总结当天推文，提炼事实、产品动态、观点变化和可执行关注点。输出要简洁，避免营销语气，不要编造推文中没有的信息。".to_string()
}

fn default_daily_digest_max_tweets_per_account() -> usize {
    20
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_comment_max")]
    pub max_comments: usize,
    #[serde(default = "default_tweet_detail_query_id")]
    pub tweet_detail_query_id: String,
}

impl Default for CommentsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_comments: 20,
            tweet_detail_query_id: default_tweet_detail_query_id(),
        }
    }
}

fn default_comment_max() -> usize {
    20
}

fn default_tweet_detail_query_id() -> String {
    "zXaXQgfyR4GxE21uwYQSyA".to_string()
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token_env: "TELEGRAM_BOT_TOKEN".to_string(),
            chat_id_env: "TELEGRAM_CHAT_ID".to_string(),
            discussion_group_id_env: "TELEGRAM_DISCUSSION_GROUP_ID".to_string(),
            parse_mode: "HTML".to_string(),
            disable_web_page_preview: false,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub fetch: FetchConfig,
    #[serde(default)]
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub translation: TranslationConfig,
    #[serde(default)]
    pub daily_digest: DailyDigestConfig,
    #[serde(default)]
    pub comments: CommentsConfig,
}

fn default_command_timeout_seconds() -> u64 {
    300
}

fn default_max_delivery_retries() -> i64 {
    3
}

fn default_rate_limit_safety_margin() -> i64 {
    10
}

pub fn write_default_config(path: &Path) -> anyhow::Result<()> {
    let text = serde_yaml::to_string(&AppConfig::default())?;
    std::fs::write(path, text)?;
    Ok(())
}

pub fn load_config(path: &Path) -> anyhow::Result<AppConfig> {
    let config = if path.exists() {
        serde_yaml::from_str::<AppConfig>(&std::fs::read_to_string(path)?)?
    } else {
        AppConfig::default()
    };
    Ok(config)
}
