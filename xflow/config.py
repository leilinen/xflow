from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from xflow.models import Source
from xflow.utils import DEFAULT_CONFIG_PATH, DEFAULT_DB_PATH, DEFAULT_PROFILE_DIR


DEFAULT_KEYWORDS = [
    "AI",
    "agent",
    "LLM",
    "Claude",
    "OpenAI",
    "Anthropic",
    "Cursor",
    "coding",
    "model",
    "paper",
    "GitHub",
]


DEFAULT_CONFIG: dict[str, Any] = {
    "server": {"host": "127.0.0.1", "port": 8000},
    "storage": {"database": str(DEFAULT_DB_PATH), "profile_dir": str(DEFAULT_PROFILE_DIR)},
    "fetch": {"interval_seconds": 900, "default_limit": 20, "fetcher": "mock"},
    "sources": {
        "accounts": [{"username": "openai", "limit": 5}],
        "lists": [{"list_id": "ai-builders", "limit": 5}],
        "searches": [{"query": "AI agent", "limit": 5}],
    },
    "agent": {
        "enabled": True,
        "keywords": DEFAULT_KEYWORDS,
        "importance_threshold": 0.45,
        "push_threshold": 0.7,
    },
}


@dataclass(frozen=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass(frozen=True)
class StorageConfig:
    database: Path = DEFAULT_DB_PATH
    profile_dir: Path = DEFAULT_PROFILE_DIR


@dataclass(frozen=True)
class FetchConfig:
    interval_seconds: int = 900
    default_limit: int = 20
    fetcher: str = "mock"


@dataclass(frozen=True)
class AgentConfig:
    enabled: bool = True
    keywords: list[str] = field(default_factory=lambda: list(DEFAULT_KEYWORDS))
    importance_threshold: float = 0.45
    push_threshold: float = 0.7


@dataclass(frozen=True)
class AppConfig:
    server: ServerConfig
    storage: StorageConfig
    fetch: FetchConfig
    agent: AgentConfig
    sources: list[Source]


def write_default_config(path: Path = DEFAULT_CONFIG_PATH) -> None:
    path.write_text(yaml.safe_dump(DEFAULT_CONFIG, sort_keys=False, allow_unicode=True), encoding="utf-8")


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    data = DEFAULT_CONFIG.copy()
    if path.exists():
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        data = _deep_merge(data, loaded)

    sources = _parse_sources(data.get("sources", {}), data["fetch"]["default_limit"])
    database = Path(data["storage"]["database"])
    profile_dir = Path(data["storage"]["profile_dir"])
    if not database.is_absolute():
        database = path.parent / database
    if not profile_dir.is_absolute():
        profile_dir = path.parent / profile_dir

    return AppConfig(
        server=ServerConfig(**data["server"]),
        storage=StorageConfig(database=database, profile_dir=profile_dir),
        fetch=FetchConfig(**data["fetch"]),
        agent=AgentConfig(**data["agent"]),
        sources=sources,
    )


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = {**base}
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _parse_sources(data: dict[str, Any], default_limit: int) -> list[Source]:
    sources: list[Source] = []
    for item in data.get("accounts", []) or []:
        username = str(item["username"]).lstrip("@")
        sources.append(Source("account", username, item.get("label"), item.get("limit", default_limit)))
    for item in data.get("lists", []) or []:
        sources.append(Source("list", str(item["list_id"]), item.get("label"), item.get("limit", default_limit)))
    for item in data.get("searches", []) or []:
        sources.append(Source("search", str(item["query"]), item.get("label"), item.get("limit", default_limit)))
    return sources
