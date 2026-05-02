from __future__ import annotations

from xflow.agent.base import AnalysisAgent
from xflow.models import Tweet, TweetAnalysis


class LLMAnalysisAgent(AnalysisAgent):
    """Future LLM-backed analyzer. It must receive tweet content only, never auth cookies."""

    def analyze(self, tweet: Tweet) -> TweetAnalysis:
        raise NotImplementedError("LLM analysis is not implemented in v1.")
