use crate::storage;
use sqlx::SqlitePool;

pub async fn generate_digest(pool: &SqlitePool, threshold: f64) -> anyhow::Result<String> {
    let tweets = storage::list_analyzed_for_digest(pool, threshold, 100).await?;
    let mut markdown = String::from("# xFlow Digest\n\n");
    let mut current_category = String::new();
    for stored in tweets {
        let Some(analysis) = stored.analysis else {
            continue;
        };
        if analysis.category != current_category {
            current_category = analysis.category.clone();
            markdown.push_str(&format!("## {current_category}\n\n"));
        }
        markdown.push_str(&format!(
            "- [@{}]({}) {:.2}: {}\n",
            stored.tweet.author_username,
            stored.tweet.url,
            analysis.importance_score,
            analysis.chinese_summary
        ));
    }
    Ok(markdown)
}
