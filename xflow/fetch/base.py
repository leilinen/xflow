from __future__ import annotations

from abc import ABC, abstractmethod

from xflow.models import Tweet


class Fetcher(ABC):
    @abstractmethod
    def fetch_user(self, username: str, limit: int) -> list[Tweet]:
        raise NotImplementedError

    @abstractmethod
    def fetch_list(self, list_id: str, limit: int) -> list[Tweet]:
        raise NotImplementedError

    @abstractmethod
    def search(self, query: str, limit: int) -> list[Tweet]:
        raise NotImplementedError
