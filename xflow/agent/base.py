from __future__ import annotations

from abc import ABC, abstractmethod

from xflow.models import Tweet, TweetAnalysis


class AnalysisAgent(ABC):
    @abstractmethod
    def analyze(self, tweet: Tweet) -> TweetAnalysis:
        raise NotImplementedError
