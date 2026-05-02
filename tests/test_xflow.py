from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from xflow.agent.rule_based import RuleBasedAgent
from xflow.auth.agent import check_stored_auth
from xflow.config import DEFAULT_CONFIG, load_config
from xflow.db import init_db
from xflow.digest import generate_digest
from xflow.fetch.mock_fetcher import MockFetcher
from xflow.models import Tweet
from xflow.pipeline import run_fetch
from xflow.rss import generate_rss
from xflow.server import create_app
from xflow.storage import Storage
from xflow.utils import mask_token


def write_config(tmp_path: Path) -> Path:
    import yaml

    db_path = tmp_path / "xflow.db"
    profile_dir = tmp_path / "profiles"
    data = DEFAULT_CONFIG.copy()
    data["storage"] = {"database": str(db_path), "profile_dir": str(profile_dir)}
    data["sources"] = {"accounts": [{"username": "openai", "limit": 2}], "lists": [], "searches": []}
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return config_path


def test_database_init(tmp_path: Path) -> None:
    db_path = tmp_path / "xflow.db"
    init_db(db_path)
    storage = Storage(db_path)
    assert storage.list_tweets() == []


def test_token_masking_and_auth_check() -> None:
    assert mask_token("abcd1234efgh") == "abcd...efgh"
    assert mask_token("short") == "*****"
    status = check_stored_auth("account1", "abcd1234efgh", "ct0value123")
    assert status.status == "unknown"
    assert "abcd1234efgh" not in status.auth_token_masked


def test_mock_fetcher() -> None:
    fetcher = MockFetcher()
    tweets = fetcher.fetch_user("openai", 3)
    assert len(tweets) == 3
    assert tweets[0].author_username == "openai"
    assert fetcher.fetch_user("openai", 1)[0].tweet_id == tweets[0].tweet_id


def test_tweet_dedupe(tmp_path: Path) -> None:
    db_path = tmp_path / "xflow.db"
    init_db(db_path)
    storage = Storage(db_path)
    tweet = Tweet(
        tweet_id="1",
        source_type="account",
        source_value="openai",
        author_username="openai",
        author_name="OpenAI",
        text="AI agent update",
        url="https://x.com/openai/status/1",
        created_at="2026-01-01T00:00:00+00:00",
    )
    storage.upsert_tweet(tweet)
    storage.upsert_tweet(tweet)
    assert len(storage.list_tweets()) == 1


def test_rule_based_agent() -> None:
    tweet = Tweet(
        tweet_id="1",
        source_type="account",
        source_value="openai",
        author_username="openai",
        author_name="OpenAI",
        text="OpenAI agent coding model paper on GitHub",
        url="https://x.com/openai/status/1",
        created_at="2026-01-01T00:00:00+00:00",
    )
    analysis = RuleBasedAgent(push_threshold=0.7).analyze(tweet)
    assert analysis.should_push is True
    assert analysis.category in {"research", "coding"}
    assert "OpenAI" in analysis.tags
    assert analysis.chinese_summary.startswith("这条推文")


def test_rss_generation() -> None:
    xml = generate_rss(
        "Test",
        "http://localhost/rss/all",
        "desc",
        [
            {
                "tweet_id": "1",
                "author_username": "openai",
                "text": "AI update",
                "url": "https://x.com/openai/status/1",
                "created_at": "2026-01-01T00:00:00+00:00",
                "analysis": {"tags": ["AI"], "chinese_summary": "AI 摘要"},
            }
        ],
    )
    assert "<rss version=\"2.0\">" in xml
    assert "<guid>1</guid>" in xml


def test_basic_api_endpoints(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    init_db(config.storage.database)
    run_fetch(config, Storage(config.storage.database))

    client = TestClient(create_app(config_path))
    assert client.get("/health").json() == {"status": "ok"}
    assert len(client.get("/json/all").json()["tweets"]) == 2
    assert client.get("/rss/all").text.startswith("<?xml")
    assert client.get("/rss/account/openai").status_code == 200


def test_digest_generation(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    config = load_config(config_path)
    init_db(config.storage.database)
    run_fetch(config, Storage(config.storage.database))
    digest = generate_digest(Storage(config.storage.database), threshold=0.1)
    assert digest.startswith("# xFlow Digest")
    assert "##" in digest
    assert "openai" in digest
