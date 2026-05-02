from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

from xflow.fetch.base import Fetcher
from xflow.models import Tweet


class MockFetcher(Fetcher):
    samples = [
        "OpenAI released a new agent workflow for coding with LLM tools and GitHub integrations.",
        "A new paper compares small model routing strategies for production AI systems.",
        "Cursor users are sharing patterns for Claude-assisted refactors in large Python repos.",
        "Anthropic published notes on safer agent evaluation and tool-use benchmarks.",
        "Community update: local meetup schedule and general developer news.",
    ]

    def fetch_user(self, username: str, limit: int) -> list[Tweet]:
        username = username.lstrip("@")
        return self._make("account", username, username, f"@{username}", limit)

    def fetch_list(self, list_id: str, limit: int) -> list[Tweet]:
        return self._make("list", list_id, f"list_{list_id}", f"List {list_id}", limit)

    def search(self, query: str, limit: int) -> list[Tweet]:
        author = "search_bot"
        return self._make("search", query, author, "Search Bot", limit, prefix=f"Search result for '{query}': ")

    def _make(
        self,
        source_type: str,
        source_value: str,
        author_username: str,
        author_name: str,
        limit: int,
        prefix: str = "",
    ) -> list[Tweet]:
        base_time = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        tweets: list[Tweet] = []
        for index in range(limit):
            text = prefix + self.samples[index % len(self.samples)]
            tweet_id = self._id(source_type, source_value, str(index))
            created_at = (base_time - timedelta(minutes=index * 7)).replace(microsecond=0).isoformat()
            tweets.append(
                Tweet(
                    tweet_id=tweet_id,
                    source_type=source_type,  # type: ignore[arg-type]
                    source_value=source_value,
                    author_username=author_username,
                    author_name=author_name,
                    text=text,
                    url=f"https://x.com/{author_username}/status/{tweet_id}",
                    created_at=created_at,
                    raw={"mock": True, "index": index},
                )
            )
        return tweets

    @staticmethod
    def _id(*parts: str) -> str:
        return hashlib.sha1(":".join(parts).encode("utf-8")).hexdigest()[:18]
