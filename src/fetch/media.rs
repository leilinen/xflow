use serde_json::Value;

/// A single media item extracted from a tweet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TweetMedium {
    Photo { url: String },
    Video { url: String },
    AnimatedGif { url: String },
}

/// An external link (non-X/Twitter) from a tweet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLink {
    pub url: String,
    pub display_url: String,
}

/// Context for a reply or quote-tweet.
#[derive(Debug, Clone)]
pub struct ReplyContext {
    pub reply_to_tweet_id: Option<String>,
    pub reply_to_username: Option<String>,
    pub quoted_tweet: Option<QuotedTweet>,
}

/// A quoted (reposted with comment) tweet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotedTweet {
    pub tweet_id: String,
    pub author_username: String,
    pub text: String,
    pub url: String,
}

/// A Twitter Article (long-form content).
#[derive(Debug, Clone)]
pub struct ArticleContent {
    pub url: String,
    pub title: Option<String>,
    pub text: Option<String>,
}

/// Extract media items from the raw tweet JSON.
///
/// Walks `legacy.extended_entities.media[]` (primary) and falls back to
/// `legacy.entities.media[]`.
pub fn extract_media(raw: &Value) -> Vec<TweetMedium> {
    let legacy = match raw.get("legacy") {
        Some(l) => l,
        None => return Vec::new(),
    };
    let media_list = legacy
        .pointer("/extended_entities/media")
        .or_else(|| legacy.pointer("/entities/media"))
        .and_then(Value::as_array);
    let Some(items) = media_list else {
        return Vec::new();
    };
    items.iter().filter_map(parse_medium).collect()
}

/// Extract external (non-X/Twitter) links from the raw tweet JSON.
pub fn extract_external_links(raw: &Value) -> Vec<ExternalLink> {
    let legacy = match raw.get("legacy") {
        Some(l) => l,
        None => return Vec::new(),
    };
    let urls = legacy
        .pointer("/entities/urls")
        .and_then(Value::as_array);
    let Some(urls) = urls else {
        return Vec::new();
    };
    urls.iter()
        .filter_map(|u| {
            let expanded = u.get("expanded_url").and_then(Value::as_str)?;
            if is_internal_url(expanded) {
                return None;
            }
            let display = u
                .get("display_url")
                .and_then(Value::as_str)
                .unwrap_or(expanded);
            Some(ExternalLink {
                url: expanded.to_string(),
                display_url: display.to_string(),
            })
        })
        .collect()
}

/// Extract reply / quote context from the raw tweet JSON.
pub fn extract_reply_context(raw: &Value) -> Option<ReplyContext> {
    let legacy = raw.get("legacy")?;

    let reply_to_tweet_id = legacy
        .get("in_reply_to_status_id_str")
        .and_then(Value::as_str)
        .map(String::from);
    let reply_to_username = legacy
        .get("in_reply_to_screen_name")
        .and_then(Value::as_str)
        .map(String::from);

    let quoted_tweet = extract_quoted_tweet(raw);

    // Only return a context if there is something to show
    if reply_to_tweet_id.is_none() && reply_to_username.is_none() && quoted_tweet.is_none() {
        return None;
    }

    Some(ReplyContext {
        reply_to_tweet_id,
        reply_to_username,
        quoted_tweet,
    })
}

/// Extract a Twitter Article from the raw tweet JSON.
pub fn extract_article(raw: &Value) -> Option<ArticleContent> {
    let article = raw.get("article")?;
    let url = article
        .get("url")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| {
            // Fallback: construct from the article's own URL field
            article
                .pointer("/url/url")
                .and_then(Value::as_str)
                .map(String::from)
        })?;
    let title = article
        .get("title")
        .and_then(Value::as_str)
        .map(String::from);
    let text = article
        .pointer("/note_tweet/note_tweet_results/result/text")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| {
            // Try the article content itself
            article
                .get("content")
                .and_then(Value::as_str)
                .map(String::from)
        });
    Some(ArticleContent { url, title, text })
}

fn extract_quoted_tweet(raw: &Value) -> Option<QuotedTweet> {
    let legacy = raw.get("legacy")?;

    // Check for quoted_status_id_str as indicator
    let _quoted_id = legacy
        .get("quoted_status_id_str")
        .and_then(Value::as_str)?;

    // Try to get the full quoted tweet data
    // Path 1: legacy.quoted_status (older API shape)
    // Path 2: quoted_status_results.result (newer GraphQL shape)
    let quoted = raw
        .get("quoted_status")
        .or_else(|| raw.pointer("/quoted_status_results/result"))
        .or_else(|| legacy.get("quoted_status"))?;

    let quoted_legacy = quoted.get("legacy").unwrap_or(quoted);

    let tweet_id = quoted
        .get("rest_id")
        .or_else(|| quoted_legacy.get("id_str"))
        .and_then(Value::as_str)
        .unwrap_or(_quoted_id)
        .to_string();

    let author_username = quoted
        .pointer("/core/user_results/result/legacy/screen_name")
        .and_then(Value::as_str)
        .or_else(|| quoted_legacy.get("screen_name").and_then(Value::as_str))
        .unwrap_or("")
        .to_string();

    let text = quoted_legacy
        .get("full_text")
        .or_else(|| quoted_legacy.get("text"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let url = format!("https://x.com/{author_username}/status/{tweet_id}");

    Some(QuotedTweet {
        tweet_id,
        author_username,
        text,
        url,
    })
}

fn parse_medium(item: &Value) -> Option<TweetMedium> {
    let media_type = item.get("type").and_then(Value::as_str)?;
    match media_type {
        "photo" => {
            let url = item
                .get("media_url_https")
                .and_then(Value::as_str)
                .or_else(|| item.get("media_url").and_then(Value::as_str))?
                .to_string();
            Some(TweetMedium::Photo { url })
        }
        "video" => {
            let url = best_mp4_variant(item)?;
            Some(TweetMedium::Video { url })
        }
        "animated_gif" => {
            let url = best_mp4_variant(item)?;
            Some(TweetMedium::AnimatedGif { url })
        }
        _ => None,
    }
}

fn best_mp4_variant(media_item: &Value) -> Option<String> {
    let variants = media_item
        .pointer("/video_info/variants")
        .and_then(Value::as_array)?;
    let mut best: Option<(&Value, i64)> = None;
    for v in variants {
        let ct = v.get("content_type").and_then(Value::as_str).unwrap_or("");
        if ct != "video/mp4" {
            continue;
        }
        let bitrate = v
            .get("bitrate")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if best.map_or(true, |(_, b)| bitrate > b) {
            best = Some((v, bitrate));
        }
    }
    best.and_then(|(v, _)| v.get("url").and_then(Value::as_str).map(String::from))
}

fn is_internal_url(url: &str) -> bool {
    // URLs that are internal to Twitter/X and should not be treated as external links
    url.contains("://x.com/")
        || url.contains("://twitter.com/")
        || url.contains("://pbs.twimg.com/")
        || url.contains("://video.twimg.com/")
        || url.contains("://t.co/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_photos() {
        let raw = json!({
            "legacy": {
                "extended_entities": {
                    "media": [
                        {
                            "type": "photo",
                            "media_url_https": "https://pbs.twimg.com/media/abc.jpg"
                        },
                        {
                            "type": "photo",
                            "media_url_https": "https://pbs.twimg.com/media/def.jpg"
                        }
                    ]
                }
            }
        });
        let media = extract_media(&raw);
        assert_eq!(media.len(), 2);
        assert_eq!(
            media[0],
            TweetMedium::Photo {
                url: "https://pbs.twimg.com/media/abc.jpg".to_string()
            }
        );
    }

    #[test]
    fn extract_video_picks_highest_bitrate() {
        let raw = json!({
            "legacy": {
                "extended_entities": {
                    "media": [{
                        "type": "video",
                        "video_info": {
                            "variants": [
                                { "content_type": "video/mp4", "bitrate": 320000, "url": "https://video.twimg.com/low.mp4?tag=1" },
                                { "content_type": "application/x-mpegURL", "bitrate": 0, "url": "https://video.twimg.com/hls.m3u8" },
                                { "content_type": "video/mp4", "bitrate": 832000, "url": "https://video.twimg.com/high.mp4?tag=1" }
                            ]
                        }
                    }]
                }
            }
        });
        let media = extract_media(&raw);
        assert_eq!(media.len(), 1);
        assert_eq!(
            media[0],
            TweetMedium::Video {
                url: "https://video.twimg.com/high.mp4?tag=1".to_string()
            }
        );
    }

    #[test]
    fn extract_animated_gif() {
        let raw = json!({
            "legacy": {
                "extended_entities": {
                    "media": [{
                        "type": "animated_gif",
                        "video_info": {
                            "variants": [
                                { "content_type": "video/mp4", "bitrate": 0, "url": "https://video.twimg.com/gif.mp4" }
                            ]
                        }
                    }]
                }
            }
        });
        let media = extract_media(&raw);
        assert_eq!(media.len(), 1);
        assert!(matches!(media[0], TweetMedium::AnimatedGif { .. }));
    }

    #[test]
    fn extract_no_media() {
        let raw = json!({"legacy": {"full_text": "hello"}});
        assert!(extract_media(&raw).is_empty());
    }

    #[test]
    fn extract_external_links_filters_internal() {
        let raw = json!({
            "legacy": {
                "entities": {
                    "urls": [
                        {
                            "expanded_url": "https://x.com/openai/status/123",
                            "display_url": "x.com/openai/status/1…"
                        },
                        {
                            "expanded_url": "https://example.com/article",
                            "display_url": "example.com/article"
                        },
                        {
                            "expanded_url": "https://github.com/repo",
                            "display_url": "github.com/repo"
                        }
                    ]
                }
            }
        });
        let links = extract_external_links(&raw);
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].url, "https://example.com/article");
        assert_eq!(links[1].url, "https://github.com/repo");
    }

    #[test]
    fn extract_reply_context_with_reply() {
        let raw = json!({
            "legacy": {
                "in_reply_to_status_id_str": "999",
                "in_reply_to_screen_name": "openai",
                "full_text": "great!"
            }
        });
        let ctx = extract_reply_context(&raw).unwrap();
        assert_eq!(ctx.reply_to_tweet_id.as_deref(), Some("999"));
        assert_eq!(ctx.reply_to_username.as_deref(), Some("openai"));
        assert!(ctx.quoted_tweet.is_none());
    }

    #[test]
    fn extract_reply_context_with_quoted_tweet() {
        let raw = json!({
            "legacy": {
                "quoted_status_id_str": "555",
                "full_text": "comment"
            },
            "quoted_status_results": {
                "result": {
                    "rest_id": "555",
                    "legacy": {
                        "full_text": "original tweet text",
                        "screen_name": "openai"
                    },
                    "core": {
                        "user_results": {
                            "result": {
                                "legacy": {
                                    "screen_name": "openai"
                                }
                            }
                        }
                    }
                }
            }
        });
        let ctx = extract_reply_context(&raw).unwrap();
        let qt = ctx.quoted_tweet.unwrap();
        assert_eq!(qt.tweet_id, "555");
        assert_eq!(qt.author_username, "openai");
        assert_eq!(qt.text, "original tweet text");
    }

    #[test]
    fn extract_reply_context_none_when_no_reply() {
        let raw = json!({"legacy": {"full_text": "standalone"}});
        assert!(extract_reply_context(&raw).is_none());
    }

    #[test]
    fn extract_article_content() {
        let raw = json!({
            "article": {
                "url": "https://x.com/i/article/123",
                "title": "My Article",
                "note_tweet": {
                    "note_tweet_results": {
                        "result": {
                            "text": "Full article text here..."
                        }
                    }
                }
            }
        });
        let article = extract_article(&raw).unwrap();
        assert_eq!(article.url, "https://x.com/i/article/123");
        assert_eq!(article.title.as_deref(), Some("My Article"));
        assert_eq!(article.text.as_deref(), Some("Full article text here..."));
    }

    #[test]
    fn extract_article_none_when_absent() {
        let raw = json!({"legacy": {"full_text": "normal tweet"}});
        assert!(extract_article(&raw).is_none());
    }
}
