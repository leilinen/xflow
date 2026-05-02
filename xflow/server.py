from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Response

from xflow.config import load_config
from xflow.rss import generate_rss
from xflow.storage import Storage


def create_app(config_path: Path = Path("config.yaml")) -> FastAPI:
    config = load_config(config_path)
    storage = Storage(config.storage.database)
    app = FastAPI(title="xFlow", version="0.1.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/json/all")
    def json_all() -> dict:
        return {"tweets": storage.list_tweets(limit=200)}

    @app.get("/json/important")
    def json_important() -> dict:
        return {"tweets": storage.list_tweets(important_only=True, limit=200)}

    @app.get("/rss/all")
    def rss_all() -> Response:
        tweets = storage.list_tweets(limit=200)
        xml = generate_rss("xFlow All", "http://localhost/rss/all", "All cached xFlow tweets", tweets)
        return Response(content=xml, media_type="application/rss+xml")

    @app.get("/rss/account/{username}")
    def rss_account(username: str) -> Response:
        tweets = storage.list_tweets(username=username, limit=200)
        xml = generate_rss(f"xFlow @{username}", f"http://localhost/rss/account/{username}", f"Cached tweets from @{username}", tweets)
        return Response(content=xml, media_type="application/rss+xml")

    @app.get("/rss/important")
    def rss_important() -> Response:
        tweets = storage.list_tweets(important_only=True, limit=200)
        xml = generate_rss("xFlow Important", "http://localhost/rss/important", "Important cached xFlow tweets", tweets)
        return Response(content=xml, media_type="application/rss+xml")

    return app
