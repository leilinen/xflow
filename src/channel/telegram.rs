use super::{
    ChannelDeliveryResult, ChannelSendFuture, ChannelSendReceipt, DeliveryChannel,
};
use crate::config::{CommentsConfig, TelegramConfig};
use crate::fetch::media::{
    extract_article, extract_external_links, extract_media, extract_reply_context,
    ArticleContent, ExternalLink, QuotedTweet, ReplyContext, TweetMedium,
};
use crate::models::StoredTweet;
use crate::worker::pipeline::FetchSourceError;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct TelegramCredentials {
    pub bot_token: String,
    pub chat_id: String,
}

impl TelegramCredentials {
    pub fn channel(&self) -> String {
        format!("telegram:{}", self.chat_id)
    }
}

// --- Payload structs ---

#[derive(Debug, Clone, Serialize)]
struct SendMessagePayload {
    chat_id: String,
    text: String,
    parse_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    link_preview_options: Option<LinkPreviewOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_parameters: Option<ReplyParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[derive(Debug, Clone, Serialize)]
struct SendPhotoPayload {
    chat_id: String,
    photo: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    caption: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_parameters: Option<ReplyParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[derive(Debug, Clone, Serialize)]
struct SendVideoPayload {
    chat_id: String,
    video: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    caption: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    supports_streaming: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_parameters: Option<ReplyParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[derive(Debug, Clone, Serialize)]
struct SendMediaGroupPayload {
    chat_id: String,
    media: Vec<InputMedia>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_parameters: Option<ReplyParameters>,
}

#[derive(Debug, Clone, Serialize)]
struct InputMedia {
    #[serde(rename = "type")]
    type_: String,
    media: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    caption: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ReplyParameters {
    message_id: i64,
}

#[derive(Debug, Clone, Serialize)]
struct LinkPreviewOptions {
    is_disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefer_small_media: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    show_above_text: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InlineKeyboardButton {
    pub text: String,
    pub callback_data: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

pub fn comment_button_markup(tweet_id: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![vec![InlineKeyboardButton {
            text: "Load comments".to_string(),
            callback_data: format!("comments:{}", tweet_id),
        }]],
    }
}

pub fn comment_button_markup_with_text(text: &str, callback_data: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![vec![InlineKeyboardButton {
            text: text.to_string(),
            callback_data: callback_data.to_string(),
        }]],
    }
}

// --- Response types ---

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct TelegramApiResponse<T> {
    ok: bool,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    result: Option<T>,
}

pub type TelegramResult = ChannelDeliveryResult;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TelegramBotCommand {
    pub command: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct TelegramChannel {
    credentials: TelegramCredentials,
    send_all: bool,
    parse_mode: String,
    disable_web_page_preview: bool,
    comments_enabled: bool,
    client: Client,
}

impl TelegramChannel {
    pub fn from_config(config: &TelegramConfig, comments: &CommentsConfig) -> anyhow::Result<Self> {
        Ok(Self {
            credentials: load_credentials(config)?,
            send_all: config.send_all,
            parse_mode: config.parse_mode.clone(),
            disable_web_page_preview: config.disable_web_page_preview,
            comments_enabled: comments.enabled,
            client: Client::new(),
        })
    }

    fn effective_parse_mode(&self) -> Option<String> {
        if self.parse_mode.is_empty() {
            None
        } else {
            Some(self.parse_mode.clone())
        }
    }
}

pub fn load_credentials(config: &TelegramConfig) -> anyhow::Result<TelegramCredentials> {
    let bot_token = load_bot_token(config)?;
    let chat_id = std::env::var(&config.chat_id_env)
        .map_err(|_| anyhow::anyhow!("missing Telegram chat id env var: {}", config.chat_id_env))?;
    Ok(TelegramCredentials { bot_token, chat_id })
}

pub fn load_bot_token(config: &TelegramConfig) -> anyhow::Result<String> {
    let bot_token = std::env::var(&config.bot_token_env).map_err(|_| {
        anyhow::anyhow!(
            "missing Telegram bot token env var: {}",
            config.bot_token_env
        )
    })?;
    Ok(bot_token)
}

pub fn default_bot_commands() -> Vec<TelegramBotCommand> {
    vec![
        TelegramBotCommand {
            command: "help".to_string(),
            description: "Show available commands".to_string(),
        },
        TelegramBotCommand {
            command: "add".to_string(),
            description: "Add a source (e.g. /add @openai)".to_string(),
        },
        TelegramBotCommand {
            command: "remove".to_string(),
            description: "Remove a source".to_string(),
        },
        TelegramBotCommand {
            command: "list".to_string(),
            description: "List all sources".to_string(),
        },
        TelegramBotCommand {
            command: "status".to_string(),
            description: "Show system status".to_string(),
        },
        TelegramBotCommand {
            command: "fetch".to_string(),
            description: "Trigger immediate fetch".to_string(),
        },
        TelegramBotCommand {
            command: "latest".to_string(),
            description: "Browse tweets (e.g. /latest @openai 7d)".to_string(),
        },
        TelegramBotCommand {
            command: "digest".to_string(),
            description: "Show analyzed digest summary".to_string(),
        },
        TelegramBotCommand {
            command: "spam".to_string(),
            description: "Manage spam keywords (list/add/remove)".to_string(),
        },
    ]
}

pub async fn set_bot_commands(config: &TelegramConfig) -> anyhow::Result<Vec<TelegramBotCommand>> {
    let commands = default_bot_commands();
    set_bot_commands_to(config, commands.clone()).await?;
    Ok(commands)
}

pub async fn clear_bot_commands(config: &TelegramConfig) -> anyhow::Result<()> {
    set_bot_commands_to(config, Vec::new()).await
}

pub async fn list_bot_commands(config: &TelegramConfig) -> anyhow::Result<Vec<TelegramBotCommand>> {
    let bot_token = load_bot_token(config)?;
    get_telegram_json::<Vec<TelegramBotCommand>>(&Client::new(), &bot_token, "getMyCommands").await
}

async fn set_bot_commands_to(
    config: &TelegramConfig,
    commands: Vec<TelegramBotCommand>,
) -> anyhow::Result<()> {
    #[derive(Debug, Serialize)]
    struct SetMyCommandsPayload {
        commands: Vec<TelegramBotCommand>,
    }

    let bot_token = load_bot_token(config)?;
    post_telegram_json::<_, bool>(
        &Client::new(),
        &bot_token,
        "setMyCommands",
        &SetMyCommandsPayload { commands },
    )
    .await?;
    Ok(())
}

const TELEGRAM_MESSAGE_LIMIT: usize = 4096;
const TELEGRAM_CAPTION_LIMIT: usize = 1024;
const TRUNCATION_MARKER: &str = "\n\n…";
const MAX_RETRIES: u32 = 2;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
const QUOTED_TWEET_MAX_CHARS: usize = 200;

enum TelegramSendError {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

// --- Message formatting ---

pub fn format_tweet_message(stored: &StoredTweet) -> String {
    let mut parts = vec![
        format!(
            "<b>{}</b> (@{}) · {} UTC+8",
            html_escape(&stored.tweet.author_name),
            html_escape(&stored.tweet.author_username),
            crate::utils::format_utc8(&stored.tweet.created_at)
        ),
        html_escape(&stored.tweet.text),
    ];
    if let Some(analysis) = &stored.analysis {
        if analysis.chinese_summary != stored.tweet.text {
            parts.push(format!("<i>{}</i>", html_escape(&analysis.chinese_summary)));
        }
        if !analysis.tags.is_empty() {
            parts.push(format!("Tags: {}", html_escape(&analysis.tags.join(", "))));
        }
    }
    let footer = format!(
        "<a href=\"{}\">Open tweet</a>",
        html_escape(&stored.tweet.url)
    );
    parts.push(footer);
    let message = parts.join("\n\n");
    if message.len() <= TELEGRAM_MESSAGE_LIMIT {
        return message;
    }
    truncate_message(parts, TELEGRAM_MESSAGE_LIMIT)
}

/// Compact caption for photo/video (1024-char limit).
pub fn format_tweet_caption(stored: &StoredTweet) -> String {
    let header = format!(
        "<b>{}</b> (@{})",
        html_escape(&stored.tweet.author_name),
        html_escape(&stored.tweet.author_username)
    );
    let footer = format!(
        "<a href=\"{}\">Open tweet</a>",
        html_escape(&stored.tweet.url)
    );
    let sep = "\n\n";
    let overhead = header.len() + sep.len() + sep.len() + footer.len();
    let budget = TELEGRAM_CAPTION_LIMIT.saturating_sub(overhead);

    let mut text = html_escape(&stored.tweet.text);
    if text.len() > budget {
        text.truncate(text.floor_char_boundary(budget));
        if let Some(pos) = text.rfind('&') {
            if !text[pos..].contains(';') {
                text.truncate(pos);
            }
        }
    }

    [header, text, footer].join(sep)
}

/// Format a quoted/replied-to tweet as a block message.
pub fn format_quoted_tweet_block(quoted: &QuotedTweet) -> String {
    let text = truncate_str(&quoted.text, QUOTED_TWEET_MAX_CHARS);
    let lines: Vec<String> = text
        .lines()
        .map(|l| format!("▎ {l}"))
        .collect();
    let block = lines.join("\n");
    format!(
        "▎ <b>@{}</b>:\n{}\n▎ 🔗 {}",
        html_escape(&quoted.author_username),
        block,
        html_escape(&quoted.url)
    )
}

/// Format a reply-context message (replying to @user or quoting a tweet).
fn format_reply_context_message(reply_ctx: &ReplyContext) -> Option<String> {
    // Prefer the quoted tweet block (richer info)
    if let Some(qt) = &reply_ctx.quoted_tweet {
        return Some(format_quoted_tweet_block(qt));
    }
    // Simple reply (no quoted tweet data, just the username/id reference)
    if let Some(username) = &reply_ctx.reply_to_username {
        let mut msg = format!("Replying to <b>@{}</b>", html_escape(username));
        if let Some(id) = &reply_ctx.reply_to_tweet_id {
            msg.push_str(&format!(
                "\n▎ 🔗 https://x.com/{username}/status/{id}"
            ));
        }
        return Some(msg);
    }
    None
}

/// Format article message.
fn format_article_message(stored: &StoredTweet, article: &ArticleContent) -> String {
    let header = format!(
        "<b>{}</b> (@{}) · {} UTC+8 · [Article]",
        html_escape(&stored.tweet.author_name),
        html_escape(&stored.tweet.author_username),
        crate::utils::format_utc8(&stored.tweet.created_at)
    );

    let mut parts = vec![header];

    if let Some(title) = &article.title {
        parts.push(format!("<b>{}</b>", html_escape(title)));
    }

    // Article text (may be long)
    if let Some(text) = &article.text {
        parts.push(html_escape(text));
    } else {
        parts.push(html_escape(&stored.tweet.text));
    }

    // Footer with both article link and tweet link
    let mut footer = String::new();
    footer.push_str(&format!("<a href=\"{}\">Open article</a>", html_escape(&article.url)));
    footer.push_str(&format!(
        " · <a href=\"{}\">Open tweet</a>",
        html_escape(&stored.tweet.url)
    ));
    parts.push(footer);

    let message = parts.join("\n\n");
    if message.len() <= TELEGRAM_MESSAGE_LIMIT {
        return message;
    }
    // Truncate: keep header + truncated text + footer
    truncate_message(parts, TELEGRAM_MESSAGE_LIMIT)
}

/// Truncate by removing middle parts (summary, tags) before the footer,
/// then truncating the tweet text if still too long.
fn truncate_message(mut parts: Vec<String>, limit: usize) -> String {
    // Footer is always the last part and must be preserved.
    let footer = parts.pop().unwrap_or_default();
    // Build the skeleton: header + text + marker + footer
    // Drop middle parts (summary, tags).
    let header = parts.first().cloned().unwrap_or_default();
    let mut text = parts.get(1).cloned().unwrap_or_default();
    let marker = TRUNCATION_MARKER.to_string();
    let sep = "\n\n";
    // Calculate overhead: header + sep + marker + sep + footer + sep
    let overhead = header.len() + sep.len() + marker.len() + sep.len() + footer.len() + sep.len();
    let budget = limit.saturating_sub(overhead);
    if text.len() > budget {
        text.truncate(text.floor_char_boundary(budget));
        // Avoid splitting inside an HTML entity.
        if let Some(pos) = text.rfind('&') {
            if !text[pos..].contains(';') {
                text.truncate(pos);
            }
        }
    }
    [header, text, marker, footer].join(sep)
}

fn truncate_str(s: &str, max_chars: usize) -> String {
    if s.len() <= max_chars {
        return s.to_string();
    }
    let mut truncated = s[..s.floor_char_boundary(max_chars.saturating_sub(1))].to_string();
    truncated.push('…');
    truncated
}

// --- DeliveryChannel impl ---

impl DeliveryChannel for TelegramChannel {
    fn id(&self) -> String {
        self.credentials.channel()
    }

    fn send_all(&self) -> bool {
        self.send_all
    }

    fn send_tweet<'a>(&'a self, tweet: &'a StoredTweet) -> ChannelSendFuture<'a> {
        Box::pin(async move {
            let media = extract_media(&tweet.tweet.raw);
            let links = extract_external_links(&tweet.tweet.raw);
            let reply_ctx = extract_reply_context(&tweet.tweet.raw);
            let article = extract_article(&tweet.tweet.raw);
            let reply_markup = self.comment_markup(&tweet.tweet.tweet_id);

            // Step 1: Send reply context if present
            let reply_to_msg_id = self.send_reply_context(tweet, reply_ctx.as_ref()).await?;

            // Step 2: Route by content type
            if !media.is_empty() {
                self.send_media_tweet(tweet, &media, reply_to_msg_id, reply_markup).await
            } else if article.is_some() {
                self.send_article_tweet(tweet, article.as_ref().unwrap(), reply_to_msg_id, reply_markup).await
            } else if !links.is_empty() {
                self.send_link_tweet(tweet, &links, reply_to_msg_id, reply_markup).await
            } else {
                self.send_text_tweet(tweet, reply_to_msg_id, reply_markup).await
            }
        })
    }
}

// --- Send methods ---

impl TelegramChannel {
    /// Send the reply/quote context as a separate message, return its message_id for reply threading.
    async fn send_reply_context(
        &self,
        _tweet: &StoredTweet,
        reply_ctx: Option<&ReplyContext>,
    ) -> anyhow::Result<Option<i64>> {
        let Some(ctx) = reply_ctx else {
            return Ok(None);
        };
        let Some(text) = format_reply_context_message(ctx) else {
            return Ok(None);
        };
        let payload = SendMessagePayload {
            chat_id: self.credentials.chat_id.clone(),
            text,
            parse_mode: self.effective_parse_mode(),
            link_preview_options: Some(LinkPreviewOptions {
                is_disabled: true,
                url: None,
                prefer_small_media: None,
                show_above_text: None,
            }),
            reply_parameters: None,
            reply_markup: None,
        };
        let result = self.send_with_retry("sendMessage", &payload).await?;
        Ok(extract_message_id(&result))
    }

    /// Send a tweet with media (photos/videos).
    async fn send_media_tweet(
        &self,
        tweet: &StoredTweet,
        media: &[TweetMedium],
        reply_to_msg_id: Option<i64>,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> anyhow::Result<ChannelSendReceipt> {
        let result = match self.try_send_media(tweet, media, reply_to_msg_id, reply_markup.clone()).await {
            Ok(r) => r,
            Err(err) => {
                tracing::warn!(?err, "media send failed, falling back to text-only");
                return self.send_text_tweet(tweet, reply_to_msg_id, reply_markup).await;
            }
        };

        // If caption was truncated, send full text as a follow-up reply
        let full_msg = format_tweet_message(tweet);
        let caption = format_tweet_caption(tweet);
        if caption.len() < full_msg.len() {
            let reply_params = extract_message_id(&result).map(|id| ReplyParameters { message_id: id });
            let _ = self
                .send_text_message(&full_msg, reply_params, true, None)
                .await;
        }

        Ok(result)
    }

    async fn try_send_media(
        &self,
        tweet: &StoredTweet,
        media: &[TweetMedium],
        reply_to_msg_id: Option<i64>,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> anyhow::Result<ChannelSendReceipt> {
        let caption = format_tweet_caption(tweet);
        let reply_params = reply_to_msg_id.map(|id| ReplyParameters { message_id: id });

        // Separate photos and videos
        let photos: Vec<&TweetMedium> = media
            .iter()
            .filter(|m| matches!(m, TweetMedium::Photo { .. }))
            .collect();
        let videos: Vec<&TweetMedium> = media
            .iter()
            .filter(|m| matches!(m, TweetMedium::Video { .. } | TweetMedium::AnimatedGif { .. }))
            .collect();

        // Single photo
        if photos.len() == 1 && videos.is_empty() {
            let url = match photos[0] {
                TweetMedium::Photo { url } => url.clone(),
                _ => unreachable!(),
            };
            let payload = SendPhotoPayload {
                chat_id: self.credentials.chat_id.clone(),
                photo: url,
                caption: Some(caption),
                parse_mode: self.effective_parse_mode(),
                reply_parameters: reply_params,
                reply_markup,
            };
            return self.send_with_retry("sendPhoto", &payload).await;
        }

        // Multiple photos (album)
        if photos.len() > 1 && videos.is_empty() {
            let items: Vec<InputMedia> = photos
                .iter()
                .take(10) // Telegram limit
                .enumerate()
                .map(|(i, m)| {
                    let url = match m {
                        TweetMedium::Photo { url } => url.clone(),
                        _ => unreachable!(),
                    };
                    InputMedia {
                        type_: "photo".to_string(),
                        media: url,
                        caption: if i == 0 { Some(caption.clone()) } else { None },
                        parse_mode: if i == 0 { self.effective_parse_mode() } else { None },
                    }
                })
                .collect();
            let payload = SendMediaGroupPayload {
                chat_id: self.credentials.chat_id.clone(),
                media: items,
                reply_parameters: reply_params,
            };
            return self.send_with_retry("sendMediaGroup", &payload).await;
        }

        // Single video or GIF
        if !videos.is_empty() && photos.is_empty() {
            let url = match videos[0] {
                TweetMedium::Video { url } | TweetMedium::AnimatedGif { url } => url.clone(),
                _ => unreachable!(),
            };
            let payload = SendVideoPayload {
                chat_id: self.credentials.chat_id.clone(),
                video: url,
                caption: Some(caption),
                parse_mode: self.effective_parse_mode(),
                supports_streaming: Some(true),
                reply_parameters: reply_params,
                reply_markup,
            };
            return self.send_with_retry("sendVideo", &payload).await;
        }

        // Mixed: video first with caption, then remaining photos as album
        if !videos.is_empty() {
            let url = match videos[0] {
                TweetMedium::Video { url } | TweetMedium::AnimatedGif { url } => url.clone(),
                _ => unreachable!(),
            };
            let video_payload = SendVideoPayload {
                chat_id: self.credentials.chat_id.clone(),
                video: url,
                caption: Some(caption),
                parse_mode: self.effective_parse_mode(),
                supports_streaming: Some(true),
                reply_parameters: reply_params.clone(),
                reply_markup,
            };
            let video_result = self.send_with_retry("sendVideo", &video_payload).await?;
            let video_msg_id = extract_message_id(&video_result);

            if !photos.is_empty() {
                let items: Vec<InputMedia> = photos
                    .iter()
                    .take(10)
                    .map(|m| {
                        let url = match m {
                            TweetMedium::Photo { url } => url.clone(),
                            _ => unreachable!(),
                        };
                        InputMedia {
                            type_: "photo".to_string(),
                            media: url,
                            caption: None,
                            parse_mode: None,
                        }
                    })
                    .collect();
                let photo_reply = video_msg_id.map(|id| ReplyParameters { message_id: id });
                let photo_payload = SendMediaGroupPayload {
                    chat_id: self.credentials.chat_id.clone(),
                    media: items,
                    reply_parameters: photo_reply,
                };
                let _ = self.send_with_retry("sendMediaGroup", &photo_payload).await;
            }
            return Ok(video_result);
        }

        // Fallback to text
        self.send_text_tweet(tweet, reply_to_msg_id, reply_markup).await
    }

    /// Send an article tweet.
    async fn send_article_tweet(
        &self,
        tweet: &StoredTweet,
        article: &ArticleContent,
        reply_to_msg_id: Option<i64>,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> anyhow::Result<ChannelSendReceipt> {
        let text = format_article_message(tweet, article);
        let reply_params = reply_to_msg_id.map(|id| ReplyParameters { message_id: id });
        self.send_text_message(&text, reply_params, false, reply_markup).await
    }

    /// Send a tweet with external links (enable link preview).
    async fn send_link_tweet(
        &self,
        tweet: &StoredTweet,
        _links: &[ExternalLink],
        reply_to_msg_id: Option<i64>,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> anyhow::Result<ChannelSendReceipt> {
        let text = format_tweet_message(tweet);
        let reply_params = reply_to_msg_id.map(|id| ReplyParameters { message_id: id });
        self.send_text_message(&text, reply_params, false, reply_markup)
            .await
    }

    /// Send a plain text tweet (original behavior).
    async fn send_text_tweet(
        &self,
        tweet: &StoredTweet,
        reply_to_msg_id: Option<i64>,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> anyhow::Result<ChannelSendReceipt> {
        let text = format_tweet_message(tweet);
        let reply_params = reply_to_msg_id.map(|id| ReplyParameters { message_id: id });
        self.send_text_message(&text, reply_params, self.disable_web_page_preview, reply_markup)
            .await
    }

    /// Low-level: send a text message via sendMessage.
    async fn send_text_message(
        &self,
        text: &str,
        reply_parameters: Option<ReplyParameters>,
        disable_preview: bool,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> anyhow::Result<ChannelSendReceipt> {
        let payload = SendMessagePayload {
            chat_id: self.credentials.chat_id.clone(),
            text: text.to_string(),
            parse_mode: self.effective_parse_mode(),
            link_preview_options: Some(LinkPreviewOptions {
                is_disabled: disable_preview,
                url: None,
                prefer_small_media: None,
                show_above_text: None,
            }),
            reply_parameters,
            reply_markup,
        };
        self.send_with_retry("sendMessage", &payload).await
    }

    fn comment_markup(&self, tweet_id: &str) -> Option<InlineKeyboardMarkup> {
        if self.comments_enabled {
            Some(comment_button_markup(tweet_id))
        } else {
            None
        }
    }

    /// Generic retry wrapper for any Telegram API method.
    async fn send_with_retry<T: Serialize + Clone>(
        &self,
        method: &str,
        payload: &T,
    ) -> anyhow::Result<ChannelSendReceipt> {
        let mut last_err = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(RETRY_DELAY * attempt).await;
            }
            match self.try_api_call(method, payload).await {
                Ok(receipt) => return Ok(receipt),
                Err(TelegramSendError::Transient(err)) => {
                    tracing::warn!(attempt, method, "Telegram transient error, will retry: {err}");
                    last_err = Some(err);
                }
                Err(TelegramSendError::Permanent(err)) => return Err(err),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("max retries exceeded")))
    }

    /// Single API call attempt.
    async fn try_api_call<T: Serialize + Clone>(
        &self,
        method: &str,
        payload: &T,
    ) -> Result<ChannelSendReceipt, TelegramSendError> {
        let response = self
            .client
            .post(telegram_api_url(&self.credentials.bot_token, method))
            .json(payload)
            .send()
            .await;
        let response = match response {
            Ok(r) => r,
            Err(err) if is_transient_reqwest_error(&err) => {
                return Err(TelegramSendError::Transient(anyhow::anyhow!(
                    "Telegram connection error: {err}"
                )))
            }
            Err(err) => {
                return Err(TelegramSendError::Permanent(anyhow::anyhow!(
                    "Telegram request failed: {err}"
                )))
            }
        };
        let status = response.status();
        if status.is_server_error() || status.as_u16() == 429 {
            return Err(TelegramSendError::Transient(anyhow::anyhow!(
                "Telegram returned HTTP {status}"
            )));
        }
        let body = response
            .json::<TelegramApiResponse<serde_json::Value>>()
            .await
            .unwrap_or(TelegramApiResponse {
                ok: false,
                description: Some(format!("invalid Telegram response with status {status}")),
                result: None,
            });
        if status.is_success() && body.ok {
            Ok(ChannelSendReceipt {
                payload: serde_json::to_value(body).unwrap_or_default(),
            })
        } else {
            Err(TelegramSendError::Permanent(anyhow::anyhow!(
                "{}",
                serde_json::json!({"method": method, "response": body})
            )))
        }
    }
}

fn is_transient_reqwest_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout() || err.is_request()
}

/// Extract the `message_id` from a Telegram API response receipt.
fn extract_message_id(receipt: &ChannelSendReceipt) -> Option<i64> {
    receipt
        .payload
        .get("result")
        .and_then(|r| r.get("message_id"))
        .and_then(serde_json::Value::as_i64)
}

pub async fn send_undelivered(
    pool: &SqlitePool,
    config: &TelegramConfig,
    comments: &CommentsConfig,
    limit: i64,
) -> anyhow::Result<TelegramResult> {
    if !config.enabled {
        return Ok(TelegramResult {
            sent: 0,
            failed: 0,
            skipped: 0,
        });
    }
    super::send_undelivered(
        pool,
        &[Box::new(TelegramChannel::from_config(config, comments)?)],
        limit,
    )
    .await
}

pub(crate) fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn get_telegram_json<T: DeserializeOwned>(
    client: &Client,
    bot_token: &str,
    method: &str,
) -> anyhow::Result<T> {
    let response = client
        .get(telegram_api_url(bot_token, method))
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("Telegram API request failed for {method}"))?;
    parse_telegram_response(response, method).await
}

async fn post_telegram_json<TBody: Serialize + ?Sized, TResponse: DeserializeOwned>(
    client: &Client,
    bot_token: &str,
    method: &str,
    payload: &TBody,
) -> anyhow::Result<TResponse> {
    let response = client
        .post(telegram_api_url(bot_token, method))
        .json(payload)
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("Telegram API request failed for {method}"))?;
    parse_telegram_response(response, method).await
}

async fn parse_telegram_response<T: DeserializeOwned>(
    response: reqwest::Response,
    method: &str,
) -> anyhow::Result<T> {
    let status = response.status();
    let body = response
        .json::<TelegramApiResponse<T>>()
        .await
        .map_err(|_| {
            anyhow::anyhow!("invalid Telegram response for {method} with status {status}")
        })?;
    if status.is_success() && body.ok {
        return body
            .result
            .ok_or_else(|| anyhow::anyhow!("missing Telegram result for {method}"));
    }
    anyhow::bail!(
        "Telegram API {method} failed: {}",
        body.description
            .unwrap_or_else(|| format!("HTTP status {status}"))
    )
}

pub fn telegram_api_url(bot_token: &str, method: &str) -> String {
    format!("https://api.telegram.org/bot{bot_token}/{method}")
}

/// Send a fetch failure alert via Telegram. Best-effort: errors are logged but not propagated.
pub async fn send_fetch_alert(
    config: &TelegramConfig,
    errors: &[FetchSourceError],
) -> anyhow::Result<()> {
    if !config.enabled || errors.is_empty() {
        return Ok(());
    }
    let bot_token = match load_bot_token(config) {
        Ok(t) => t,
        Err(_) => return Ok(()),
    };
    let chat_id = match std::env::var(&config.chat_id_env) {
        Ok(id) => id,
        Err(_) => return Ok(()),
    };

    let now = crate::utils::format_utc8_full(&chrono::Utc::now());
    let mut lines = vec![
        format!("xFlow Fetch Alert ({now} UTC+8)"),
        format!("{} source(s) failed:", errors.len()),
        String::new(),
    ];
    for err in errors {
        lines.push(format!(
            "  {}:{} - {}",
            err.source_type, err.source_value, err.message
        ));
    }
    let text = lines.join("\n");
    let text = if text.len() > TELEGRAM_MESSAGE_LIMIT {
        format!("{}\n...", &text[..text.floor_char_boundary(4090)])
    } else {
        text
    };

    let client = Client::new();
    let payload = SendMessagePayload {
        chat_id,
        text,
        parse_mode: None,
        link_preview_options: Some(LinkPreviewOptions {
            is_disabled: true,
            url: None,
            prefer_small_media: None,
            show_above_text: None,
        }),
        reply_parameters: None,
        reply_markup: None,
    };

    match client
        .post(telegram_api_url(&bot_token, "sendMessage"))
        .json(&payload)
        .send()
        .await
    {
        Ok(response) if !response.status().is_success() => {
            let body = response.text().await.unwrap_or_default();
            tracing::warn!(?body, "failed to send fetch alert via Telegram");
        }
        Err(err) => {
            tracing::warn!(?err, "failed to send fetch alert via Telegram");
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{SourceType, Tweet, TweetAnalysis};
    use crate::fetch::media::{ArticleContent, QuotedTweet};
    use chrono::Utc;
    use serde_json::json;

    fn make_stored(text: &str, raw: serde_json::Value) -> StoredTweet {
        StoredTweet {
            tweet: Tweet {
                tweet_id: "1".to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: text.to_string(),
                url: "https://x.com/openai/status/1".to_string(),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw,
            },
            analysis: None,
        }
    }

    #[test]
    fn escapes_html() {
        let stored = StoredTweet {
            tweet: Tweet {
                tweet_id: "1".to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: "AI <agent> & update".to_string(),
                url: "https://x.com/openai/status/1?x=1&y=2".to_string(),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw: json!({}),
            },
            analysis: None,
        };
        let message = format_tweet_message(&stored);
        assert!(message.contains("AI &lt;agent&gt; &amp; update"));
        assert!(message.contains("x=1&amp;y=2"));
    }

    #[test]
    fn default_bot_commands_match_supported_menu() {
        let commands = default_bot_commands();
        assert_eq!(
            commands
                .iter()
                .map(|command| command.command.as_str())
                .collect::<Vec<_>>(),
            vec!["help", "add", "remove", "list", "status", "fetch", "latest", "digest", "spam"]
        );
        assert!(commands
            .iter()
            .all(|command| !command.description.trim().is_empty()));
    }

    #[test]
    fn truncates_long_message_to_telegram_limit() {
        let long_text = "A".repeat(5000);
        let stored = StoredTweet {
            tweet: Tweet {
                tweet_id: "2".to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: long_text,
                url: "https://x.com/openai/status/2".to_string(),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw: json!({}),
            },
            analysis: Some(TweetAnalysis {
                tweet_id: "2".to_string(),
                relevance: 0.9,
                importance_score: 0.8,
                category: "research".to_string(),
                tags: vec!["AI".to_string(), "LLM".to_string()],
                chinese_summary: "这是一段很长的中文摘要".to_string(),
                reason: "important".to_string(),
                should_push: true,
                analyzed_at: Utc::now(),
            }),
        };
        let message = format_tweet_message(&stored);
        assert!(message.len() <= TELEGRAM_MESSAGE_LIMIT);
        assert!(message.contains("<b>OpenAI</b> (@openai)"));
        assert!(message.contains("Open tweet"));
        assert!(message.contains(TRUNCATION_MARKER.trim()));
        // Summary and tags should be dropped when truncated.
        assert!(!message.contains("这是一段很长的中文摘要"));
    }

    #[test]
    fn short_message_is_not_truncated() {
        let stored = StoredTweet {
            tweet: Tweet {
                tweet_id: "3".to_string(),
                source_type: SourceType::Account,
                source_value: "openai".to_string(),
                author_username: "openai".to_string(),
                author_name: "OpenAI".to_string(),
                text: "Short tweet".to_string(),
                url: "https://x.com/openai/status/3".to_string(),
                created_at: Utc::now(),
                fetched_at: Utc::now(),
                raw: json!({}),
            },
            analysis: None,
        };
        let message = format_tweet_message(&stored);
        assert!(!message.contains(TRUNCATION_MARKER.trim()));
        assert!(message.contains("Short tweet"));
    }

    #[test]
    fn caption_fits_within_1024_chars() {
        let stored = make_stored(&"x".repeat(2000), json!({}));
        let caption = format_tweet_caption(&stored);
        assert!(caption.len() <= TELEGRAM_CAPTION_LIMIT);
        assert!(caption.contains("<b>OpenAI</b> (@openai)"));
        assert!(caption.contains("Open tweet"));
    }

    #[test]
    fn short_caption_not_truncated() {
        let stored = make_stored("hello world", json!({}));
        let caption = format_tweet_caption(&stored);
        assert!(caption.contains("hello world"));
        assert!(caption.len() <= TELEGRAM_CAPTION_LIMIT);
    }

    #[test]
    fn quoted_tweet_block_format() {
        let qt = QuotedTweet {
            tweet_id: "123".to_string(),
            author_username: "openai".to_string(),
            text: "We are launching GPT-5 today!".to_string(),
            url: "https://x.com/openai/status/123".to_string(),
        };
        let block = format_quoted_tweet_block(&qt);
        assert!(block.contains("▎ <b>@openai</b>:"));
        assert!(block.contains("▎ We are launching GPT-5 today!"));
        assert!(block.contains("▎ 🔗"));
    }

    #[test]
    fn quoted_tweet_block_truncates_long_text() {
        let qt = QuotedTweet {
            tweet_id: "123".to_string(),
            author_username: "openai".to_string(),
            text: "A".repeat(500),
            url: "https://x.com/openai/status/123".to_string(),
        };
        let block = format_quoted_tweet_block(&qt);
        // Should be reasonably short
        assert!(block.len() < 400);
    }

    #[test]
    fn article_message_format() {
        let stored = make_stored("check this article", json!({}));
        let article = ArticleContent {
            url: "https://x.com/i/article/123".to_string(),
            title: Some("My Article Title".to_string()),
            text: Some("Full article text here.".to_string()),
        };
        let msg = format_article_message(&stored, &article);
        assert!(msg.contains("[Article]"));
        assert!(msg.contains("My Article Title"));
        assert!(msg.contains("Full article text here."));
        assert!(msg.contains("Open article"));
        assert!(msg.contains("Open tweet"));
    }

    #[test]
    fn extract_message_id_from_receipt() {
        let receipt = ChannelSendReceipt {
            payload: serde_json::json!({
                "ok": true,
                "result": {
                    "message_id": 42,
                    "date": 1234567890
                }
            }),
        };
        assert_eq!(extract_message_id(&receipt), Some(42));
    }

    #[test]
    fn extract_message_id_returns_none_when_missing() {
        let receipt = ChannelSendReceipt {
            payload: serde_json::json!({"ok": true}),
        };
        assert_eq!(extract_message_id(&receipt), None);
    }
}
