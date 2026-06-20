use crate::config::TranslationConfig;
use anyhow::Context;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct TranslationClient {
    http: Client,
    api_key: String,
    base_url: String,
    model: String,
    max_tokens: u32,
    temperature: f32,
    system_prompt: String,
}

impl TranslationClient {
    pub fn from_config(config: &TranslationConfig) -> anyhow::Result<Self> {
        let api_key = std::env::var(&config.api_key_env).context(format!(
            "translation API key not found in env var {}",
            config.api_key_env
        ))?;

        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client for translation")?;

        Ok(Self {
            http,
            api_key,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            model: config.model.clone(),
            max_tokens: config.max_tokens,
            temperature: config.temperature,
            system_prompt: config.system_prompt.clone(),
        })
    }

    pub async fn translate(&self, text: &str) -> anyhow::Result<String> {
        let url = format!("{}/chat/completions", self.base_url);

        let request = ChatRequest {
            model: self.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: self.system_prompt.clone(),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: text.to_string(),
                },
            ],
            max_tokens: self.max_tokens,
            temperature: self.temperature,
        };

        let response = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .context("translation API request failed")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("translation API returned status {}: {}", status, body);
        }

        let body: ChatResponse = response
            .json()
            .await
            .context("failed to parse translation API response")?;

        let content = body
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default();

        let trimmed = content.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("translation API returned empty content");
        }

        Ok(trimmed)
    }
}

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
}

#[derive(Deserialize)]
struct ChatMessageResponse {
    content: Option<String>,
}