from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path("config.yaml")
DEFAULT_DATA_DIR = Path("data")
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "xflow.db"
DEFAULT_PROFILE_DIR = DEFAULT_DATA_DIR / "x_profiles"


def mask_token(token: str | None) -> str:
    if not token:
        return "<missing>"
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}...{token[-4:]}"


def dumps_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def loads_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
