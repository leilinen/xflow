from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from xflow.db import connect
from xflow.models import Source, Tweet, TweetAnalysis, utc_now_iso
from xflow.utils import dumps_json, loads_json


class Storage:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def upsert_source(self, source: Source) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO sources (source_type, value, label, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_type, value) DO UPDATE SET label=excluded.label
                """,
                (source.source_type, source.value, source.label, utc_now_iso()),
            )

    def upsert_tweet(self, tweet: Tweet) -> bool:
        with connect(self.db_path) as conn:
            cur = conn.execute(
                """
                INSERT INTO tweets (
                    tweet_id, source_type, source_value, author_username, author_name,
                    text, url, created_at, fetched_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tweet_id) DO UPDATE SET
                    fetched_at=excluded.fetched_at,
                    raw_json=excluded.raw_json
                """,
                (
                    tweet.tweet_id,
                    tweet.source_type,
                    tweet.source_value,
                    tweet.author_username,
                    tweet.author_name,
                    tweet.text,
                    tweet.url,
                    tweet.created_at,
                    tweet.fetched_at,
                    dumps_json(tweet.raw),
                ),
            )
            return cur.rowcount > 0

    def upsert_tweets(self, tweets: Iterable[Tweet]) -> int:
        count = 0
        for tweet in tweets:
            if self.upsert_tweet(tweet):
                count += 1
        return count

    def save_fetch_state(self, source: Source, status: str, message: str | None = None) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO fetch_state (source_type, source_value, last_fetch_at, last_status, message)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_type, source_value) DO UPDATE SET
                    last_fetch_at=excluded.last_fetch_at,
                    last_status=excluded.last_status,
                    message=excluded.message
                """,
                (source.source_type, source.value, utc_now_iso(), status, message),
            )

    def save_analysis(self, analysis: TweetAnalysis) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO tweet_analysis (
                    tweet_id, relevance, importance_score, category, tags_json,
                    chinese_summary, reason, should_push, analyzed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tweet_id) DO UPDATE SET
                    relevance=excluded.relevance,
                    importance_score=excluded.importance_score,
                    category=excluded.category,
                    tags_json=excluded.tags_json,
                    chinese_summary=excluded.chinese_summary,
                    reason=excluded.reason,
                    should_push=excluded.should_push,
                    analyzed_at=excluded.analyzed_at
                """,
                (
                    analysis.tweet_id,
                    analysis.relevance,
                    analysis.importance_score,
                    analysis.category,
                    dumps_json(analysis.tags),
                    analysis.chinese_summary,
                    analysis.reason,
                    int(analysis.should_push),
                    analysis.analyzed_at,
                ),
            )

    def list_tweets(self, *, username: str | None = None, important_only: bool = False, limit: int = 100) -> list[dict]:
        where: list[str] = []
        params: list[object] = []
        if username:
            where.append("t.author_username = ?")
            params.append(username.lstrip("@"))
        if important_only:
            where.append("COALESCE(a.should_push, 0) = 1")
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        params.append(limit)
        with connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT t.*, a.relevance, a.importance_score, a.category, a.tags_json,
                       a.chinese_summary, a.reason, a.should_push, a.analyzed_at
                FROM tweets t
                LEFT JOIN tweet_analysis a ON a.tweet_id = t.tweet_id
                {clause}
                ORDER BY t.created_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._tweet_row_to_dict(row) for row in rows]

    def list_analyzed_for_digest(self, threshold: float, limit: int = 100) -> list[dict]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT t.*, a.relevance, a.importance_score, a.category, a.tags_json,
                       a.chinese_summary, a.reason, a.should_push, a.analyzed_at
                FROM tweets t
                JOIN tweet_analysis a ON a.tweet_id = t.tweet_id
                WHERE a.importance_score >= ?
                ORDER BY a.category ASC, a.importance_score DESC, t.created_at DESC
                LIMIT ?
                """,
                (threshold, limit),
            ).fetchall()
        return [self._tweet_row_to_dict(row) for row in rows]

    def save_auth_account(self, label: str, auth_token: str, ct0: str) -> None:
        now = utc_now_iso()
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO auth_accounts (label, auth_token, ct0, status, created_at, updated_at)
                VALUES (?, ?, ?, 'unknown', ?, ?)
                ON CONFLICT(label) DO UPDATE SET
                    auth_token=excluded.auth_token,
                    ct0=excluded.ct0,
                    status='unknown',
                    updated_at=excluded.updated_at
                """,
                (label, auth_token, ct0, now, now),
            )

    def get_auth_account(self, label: str) -> sqlite3.Row | None:
        with connect(self.db_path) as conn:
            return conn.execute("SELECT * FROM auth_accounts WHERE label = ?", (label,)).fetchone()

    def list_auth_accounts(self) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return conn.execute("SELECT * FROM auth_accounts ORDER BY label").fetchall()

    def delete_auth_account(self, label: str) -> bool:
        with connect(self.db_path) as conn:
            cur = conn.execute("DELETE FROM auth_accounts WHERE label = ?", (label,))
            return cur.rowcount > 0

    @staticmethod
    def _tweet_row_to_dict(row: sqlite3.Row) -> dict:
        item = dict(row)
        item["raw"] = loads_json(item.pop("raw_json", None), {})
        tags = loads_json(item.pop("tags_json", None), [])
        if item.get("importance_score") is not None:
            item["analysis"] = {
                "relevance": item.pop("relevance"),
                "importance_score": item.pop("importance_score"),
                "category": item.pop("category"),
                "tags": tags,
                "chinese_summary": item.pop("chinese_summary"),
                "reason": item.pop("reason"),
                "should_push": bool(item.pop("should_push")),
                "analyzed_at": item.pop("analyzed_at"),
            }
        else:
            for key in ["relevance", "importance_score", "category", "chinese_summary", "reason", "should_push", "analyzed_at"]:
                item.pop(key, None)
            item["analysis"] = None
        return item
