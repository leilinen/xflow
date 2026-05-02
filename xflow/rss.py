from __future__ import annotations

from email.utils import format_datetime
from html import escape
from datetime import datetime


def generate_rss(title: str, link: str, description: str, tweets: list[dict]) -> str:
    items = "\n".join(_item(tweet) for tweet in tweets)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>{escape(title)}</title>
    <link>{escape(link)}</link>
    <description>{escape(description)}</description>
{items}
  </channel>
</rss>
"""


def _item(tweet: dict) -> str:
    analysis = tweet.get("analysis") or {}
    summary = analysis.get("chinese_summary") or tweet["text"]
    tags = ", ".join(analysis.get("tags") or [])
    desc_parts = [escape(summary), f"<p>{escape(tweet['text'])}</p>"]
    if tags:
        desc_parts.append(f"<p>Tags: {escape(tags)}</p>")
    return f"""    <item>
      <title>{escape(tweet['author_username'])}: {escape(tweet['text'][:80])}</title>
      <link>{escape(tweet['url'])}</link>
      <guid>{escape(tweet['tweet_id'])}</guid>
      <pubDate>{escape(_pubdate(tweet['created_at']))}</pubDate>
      <description>{''.join(desc_parts)}</description>
    </item>"""


def _pubdate(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
        return format_datetime(parsed)
    except ValueError:
        return value
