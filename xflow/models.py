from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

SourceType = Literal["account", "list", "search"]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class Tweet:
    tweet_id: str
    source_type: SourceType
    source_value: str
    author_username: str
    author_name: str
    text: str
    url: str
    created_at: str
    fetched_at: str = field(default_factory=utc_now_iso)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TweetAnalysis:
    tweet_id: str
    relevance: float
    importance_score: float
    category: str
    tags: list[str]
    chinese_summary: str
    reason: str
    should_push: bool
    analyzed_at: str = field(default_factory=utc_now_iso)


@dataclass(frozen=True)
class Source:
    source_type: SourceType
    value: str
    label: str | None = None
    limit: int | None = None
