from __future__ import annotations

from xflow.agent.rule_based import RuleBasedAgent
from xflow.config import AppConfig
from xflow.fetch import Fetcher, MockFetcher
from xflow.models import Source, Tweet
from xflow.storage import Storage


def build_fetcher(config: AppConfig) -> Fetcher:
    if config.fetch.fetcher != "mock":
        raise ValueError(f"Unsupported fetcher '{config.fetch.fetcher}'. MVP supports only 'mock'.")
    return MockFetcher()


def fetch_source(fetcher: Fetcher, source: Source, default_limit: int) -> list[Tweet]:
    limit = source.limit or default_limit
    if source.source_type == "account":
        return fetcher.fetch_user(source.value, limit)
    if source.source_type == "list":
        return fetcher.fetch_list(source.value, limit)
    if source.source_type == "search":
        return fetcher.search(source.value, limit)
    raise ValueError(f"Unsupported source type: {source.source_type}")


def run_fetch(config: AppConfig, storage: Storage) -> dict[str, int]:
    fetcher = build_fetcher(config)
    agent = RuleBasedAgent(config.agent.keywords, config.agent.push_threshold) if config.agent.enabled else None
    fetched = 0
    analyzed = 0

    for source in config.sources:
        storage.upsert_source(source)
        try:
            tweets = fetch_source(fetcher, source, config.fetch.default_limit)
            for tweet in tweets:
                storage.upsert_tweet(tweet)
                fetched += 1
                if agent:
                    storage.save_analysis(agent.analyze(tweet))
                    analyzed += 1
            storage.save_fetch_state(source, "ok", f"Fetched {len(tweets)} tweets.")
        except Exception as exc:
            storage.save_fetch_state(source, "error", str(exc))
            raise
    return {"fetched": fetched, "analyzed": analyzed, "sources": len(config.sources)}
