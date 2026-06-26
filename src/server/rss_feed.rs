use crate::models::StoredTweet;
use chrono::FixedOffset;
use rss::{ChannelBuilder, Item, ItemBuilder};

pub fn generate_rss(
    title: &str,
    link: &str,
    description: &str,
    tweets: &[StoredTweet],
) -> anyhow::Result<String> {
    let utc8 = FixedOffset::east_opt(8 * 3600).expect("UTC+8 is a valid offset");
    let items: Vec<Item> = tweets
        .iter()
        .map(|stored| {
            let tweet = &stored.tweet;
            let description = tweet.text.clone();
            ItemBuilder::default()
                .title(Some(format!(
                    "@{}: {}",
                    tweet.author_username,
                    tweet.text.chars().take(80).collect::<String>()
                )))
                .link(Some(tweet.url.clone()))
                .guid(Some(rss::Guid {
                    value: tweet.tweet_id.clone(),
                    permalink: false,
                }))
                .pub_date(Some(tweet.created_at.with_timezone(&utc8).to_rfc2822()))
                .description(Some(description))
                .build()
        })
        .collect();
    let channel = ChannelBuilder::default()
        .title(title.to_string())
        .link(link.to_string())
        .description(description.to_string())
        .items(items)
        .build();
    Ok(channel.to_string())
}
