use crate::config::AgentConfig;
use crate::models::{Tweet, TweetAnalysis};
use chrono::Utc;

pub fn analyze(tweet: &Tweet, config: &AgentConfig) -> TweetAnalysis {
    let lower = tweet.text.to_lowercase();
    let mut tags = Vec::new();
    let mut hits = 0;
    for keyword in &config.keywords {
        if lower.contains(&keyword.to_lowercase()) {
            hits += 1;
            tags.push(keyword.clone());
        }
    }
    tags.sort();
    tags.dedup();
    let importance_score = (hits as f64 / 4.0).min(1.0);
    let category = if lower.contains("paper") || lower.contains("research") {
        "research"
    } else if lower.contains("code") || lower.contains("github") || lower.contains("coding") {
        "coding"
    } else if lower.contains("model") || lower.contains("llm") {
        "model"
    } else {
        "general"
    };
    TweetAnalysis {
        tweet_id: tweet.tweet_id.clone(),
        relevance: importance_score,
        importance_score,
        category: category.to_string(),
        tags,
        chinese_summary: format!(
            "这条推文来自 @{}，内容与 {} 相关。",
            tweet.author_username, category
        ),
        reason: format!("Matched {hits} configured keywords."),
        should_push: importance_score >= config.push_threshold,
        analyzed_at: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::models::SourceType;
    use serde_json::json;

    #[test]
    fn analyzes_rule_based_keywords() {
        let config = AppConfig::default();
        let tweet = Tweet {
            tweet_id: "1".to_string(),
            source_type: SourceType::Account,
            source_value: "openai".to_string(),
            author_username: "openai".to_string(),
            author_name: "OpenAI".to_string(),
            text: "OpenAI agent coding model paper on GitHub".to_string(),
            url: "https://x.com/openai/status/1".to_string(),
            created_at: Utc::now(),
            fetched_at: Utc::now(),
            raw: json!({}),
        };
        let analysis = analyze(&tweet, &config.agent);
        assert!(analysis.should_push);
        assert!(analysis.tags.contains(&"OpenAI".to_string()));
    }
}
