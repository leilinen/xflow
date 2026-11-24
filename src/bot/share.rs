use reqwest::Url;

pub mod executor;
pub mod pipeline;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedTweetUrl {
    pub(crate) tweet_id: String,
    pub(crate) canonical_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareRequest {
    pub chat_id: i64,
    pub tweet_id: String,
    /// Consumed by the Stage 4B pipeline; not read by the Stage 4A executor.
    pub canonical_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PrivateMessageRoute {
    Command,
    Share(ShareRequest),
    Other,
}

/// Parse the first valid X/Twitter status URL from arbitrary message text.
///
/// URL discovery is intentionally separate from URL validation: candidates
/// are located by their HTTPS scheme, then parsed and checked using the
/// standard URL parser. Hosts are never accepted using suffix/substring
/// matching.
pub(crate) fn parse_x_tweet_url(text: &str) -> Option<ParsedTweetUrl> {
    text.char_indices()
        .filter_map(|(start, _)| candidate_at(text, start))
        .find_map(parse_candidate)
}

pub(crate) fn route_private_message(
    chat_type: &str,
    chat_id: i64,
    allowed_chat_id: i64,
    text: &str,
) -> PrivateMessageRoute {
    if chat_type != "private" || chat_id != allowed_chat_id {
        return PrivateMessageRoute::Other;
    }

    if is_command(text) {
        return PrivateMessageRoute::Command;
    }

    match parse_x_tweet_url(text) {
        Some(parsed) => PrivateMessageRoute::Share(ShareRequest {
            chat_id,
            tweet_id: parsed.tweet_id,
            canonical_url: parsed.canonical_url,
        }),
        None => PrivateMessageRoute::Other,
    }
}

fn is_command(text: &str) -> bool {
    text.split_whitespace()
        .next()
        .is_some_and(|token| token.starts_with('/'))
}

fn candidate_at(text: &str, start: usize) -> Option<&str> {
    let rest = text.get(start..)?;
    if !rest
        .get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
    {
        return None;
    }
    let end = rest
        .char_indices()
        .find_map(|(index, ch)| (ch.is_whitespace() || ch.is_control()).then_some(index))
        .unwrap_or(rest.len());
    let candidate = rest.get(..end)?.trim_end_matches(is_trailing_punctuation);
    (!candidate.is_empty()).then_some(candidate)
}

fn is_trailing_punctuation(ch: char) -> bool {
    matches!(
        ch,
        '.' | ','
            | '!'
            | '?'
            | ';'
            | ':'
            | ')'
            | ']'
            | '}'
            | '>'
            | '"'
            | '\''
            | '，'
            | '。'
            | '！'
            | '？'
            | '；'
            | '：'
            | '）'
            | '】'
            | '》'
            | '」'
            | '』'
    )
}

fn parse_candidate(candidate: &str) -> Option<ParsedTweetUrl> {
    let url = Url::parse(candidate).ok()?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || has_explicit_port(candidate)
    {
        return None;
    }

    let host = url.host_str()?;
    if !matches!(
        host,
        "x.com" | "www.x.com" | "twitter.com" | "www.twitter.com" | "mobile.twitter.com"
    ) {
        return None;
    }

    let raw_path = after_https_scheme(candidate)?
        .split_once('/')
        .map(|(_, path)| path)
        .unwrap_or("")
        .split(['?', '#'])
        .next()
        .unwrap_or("");
    if raw_path.contains('%') || raw_path.contains('\\') {
        return None;
    }

    let segments: Vec<&str> = url.path().split('/').collect();
    let tweet_id = match segments.as_slice() {
        ["", user, "status", tweet_id] if is_valid_username_path_segment(user) => *tweet_id,
        ["", "i", "web", "status", tweet_id] => *tweet_id,
        _ => return None,
    };
    if tweet_id.is_empty()
        || tweet_id.len() > 20
        || !tweet_id.bytes().all(|byte| byte.is_ascii_digit())
        || tweet_id.parse::<u64>().is_err()
    {
        return None;
    }

    Some(ParsedTweetUrl {
        tweet_id: tweet_id.to_string(),
        canonical_url: format!("https://x.com/i/status/{tweet_id}"),
    })
}

fn is_valid_username_path_segment(user: &str) -> bool {
    !user.is_empty()
        && user != "i"
        && user.len() <= 15
        && user
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn has_explicit_port(candidate: &str) -> bool {
    let Some(after_scheme) = after_https_scheme(candidate) else {
        return true;
    };
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    authority.contains(':')
}

fn after_https_scheme(candidate: &str) -> Option<&str> {
    candidate
        .get(..8)
        .filter(|scheme| scheme.eq_ignore_ascii_case("https://"))?;
    candidate.get(8..)
}

#[cfg(test)]
mod tests {
    use super::{parse_x_tweet_url, route_private_message, PrivateMessageRoute};

    fn parsed(text: &str) -> (String, String) {
        let parsed = parse_x_tweet_url(text).expect("expected a valid tweet URL");
        (parsed.tweet_id, parsed.canonical_url)
    }

    #[test]
    fn parses_supported_hosts_and_paths() {
        for url in [
            "https://x.com/user/status/123456",
            "https://twitter.com/user/status/123456",
            "https://mobile.twitter.com/user/status/123456",
            "https://www.x.com/user/status/123456",
            "https://www.twitter.com/user/status/123456",
            "https://x.com/i/web/status/123456",
            "HTTPS://X.COM/user/status/123456",
        ] {
            assert_eq!(
                parsed(url),
                (
                    "123456".to_string(),
                    "https://x.com/i/status/123456".to_string()
                )
            );
        }
    }

    #[test]
    fn parses_share_text_query_fragment_and_trailing_punctuation() {
        assert_eq!(
            parsed("分享一条推文：https://x.com/user/status/42?s=20&t=abc#section。"),
            ("42".to_string(), "https://x.com/i/status/42".to_string())
        );
        assert_eq!(parsed("See https://x.com/user/status/43, thanks! ").0, "43");
        assert_eq!(parsed("（https://x.com/user/status/44）").0, "44");
    }

    #[test]
    fn chooses_first_valid_tweet_url() {
        let text = "https://example.com/a https://x.com.evil.test/u/status/1 https://twitter.com/u/status/2 https://x.com/u/status/3";
        assert_eq!(parsed(text).0, "2");
    }

    #[test]
    fn rejects_untrusted_hosts_and_schemes() {
        for text in [
            "https://example.com/user/status/1",
            "https://x.com.evil.test/user/status/1",
            "https://evilx.com/user/status/1",
            "https://twitter.com@evil.test/user/status/1",
            "http://x.com/user/status/1",
            "ftp://x.com/user/status/1",
        ] {
            assert!(parse_x_tweet_url(text).is_none(), "accepted {text}");
        }
    }

    #[test]
    fn rejects_userinfo_and_explicit_or_invalid_ports() {
        for text in [
            "https://user@x.com/user/status/1",
            "https://user:pass@x.com/user/status/1",
            "https://x.com:443/user/status/1",
            "https://x.com:8443/user/status/1",
            "https://x.com:99999/user/status/1",
        ] {
            assert!(parse_x_tweet_url(text).is_none(), "accepted {text}");
        }
    }

    #[test]
    fn rejects_invalid_paths_and_encoded_path_components() {
        for text in [
            "https://x.com/user/1",
            "https://x.com/status/1",
            "https://x.com/user/status/1/extra",
            "https://x.com//user/status/1",
            "https://x.com/i/status/1",
            "https://x.com/user/status/%31",
            "https://x.com/user%2Fstatus%2F1",
            "https://x.com/user\\status\\1",
        ] {
            assert!(parse_x_tweet_url(text).is_none(), "accepted {text}");
        }
    }

    #[test]
    fn rejects_invalid_overflowing_and_overlong_ids() {
        for text in [
            "https://x.com/user/status/",
            "https://x.com/user/status/-1",
            "https://x.com/user/status/1.5",
            "https://x.com/user/status/１２３",
            "https://x.com/user/status/18446744073709551616",
            "https://x.com/user/status/123456789012345678901",
        ] {
            assert!(parse_x_tweet_url(text).is_none(), "accepted {text}");
        }
    }

    #[test]
    fn authorized_private_url_routes_to_share() {
        let route = route_private_message("private", 7, 7, "https://x.com/u/status/123");
        let PrivateMessageRoute::Share(request) = route else {
            panic!("expected share route");
        };
        assert_eq!(request.chat_id, 7);
        assert_eq!(request.tweet_id, "123");
        assert_eq!(request.canonical_url, "https://x.com/i/status/123");
    }

    #[test]
    fn unauthorized_group_and_channel_messages_do_not_route_to_share() {
        for (chat_type, chat_id, allowed) in [
            ("private", 8, 7),
            ("group", 7, 7),
            ("supergroup", 7, 7),
            ("channel", 7, 7),
        ] {
            assert_eq!(
                route_private_message(chat_type, chat_id, allowed, "https://x.com/u/status/123"),
                PrivateMessageRoute::Other
            );
        }
    }

    #[test]
    fn commands_have_priority_and_plain_text_is_not_share() {
        assert_eq!(
            route_private_message("private", 7, 7, "/help https://x.com/u/status/123"),
            PrivateMessageRoute::Command
        );
        assert_eq!(
            route_private_message("private", 7, 7, "ordinary text"),
            PrivateMessageRoute::Other
        );
    }
}
