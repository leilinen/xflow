from __future__ import annotations

import sqlite3
from pathlib import Path

from xflow.utils import ensure_parent


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS auth_accounts (
    label TEXT PRIMARY KEY,
    auth_token TEXT NOT NULL,
    ct0 TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    value TEXT NOT NULL,
    label TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(source_type, value)
);

CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_value TEXT NOT NULL,
    author_username TEXT NOT NULL,
    author_name TEXT NOT NULL,
    text TEXT NOT NULL,
    url TEXT NOT NULL,
    created_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS fetch_state (
    source_type TEXT NOT NULL,
    source_value TEXT NOT NULL,
    last_fetch_at TEXT NOT NULL,
    last_status TEXT NOT NULL,
    message TEXT,
    PRIMARY KEY(source_type, source_value)
);

CREATE TABLE IF NOT EXISTS tweet_analysis (
    tweet_id TEXT PRIMARY KEY,
    relevance REAL NOT NULL,
    importance_score REAL NOT NULL,
    category TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    chinese_summary TEXT NOT NULL,
    reason TEXT NOT NULL,
    should_push INTEGER NOT NULL,
    analyzed_at TEXT NOT NULL,
    FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tweet_id TEXT,
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id) ON DELETE SET NULL
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    ensure_parent(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
