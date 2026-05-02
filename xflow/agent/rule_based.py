from __future__ import annotations

from xflow.agent.base import AnalysisAgent
from xflow.config import DEFAULT_KEYWORDS
from xflow.models import Tweet, TweetAnalysis


class RuleBasedAgent(AnalysisAgent):
    def __init__(self, keywords: list[str] | None = None, push_threshold: float = 0.7):
        self.keywords = keywords or list(DEFAULT_KEYWORDS)
        self.push_threshold = push_threshold

    def analyze(self, tweet: Tweet) -> TweetAnalysis:
        lower = tweet.text.lower()
        tags = [keyword for keyword in self.keywords if keyword.lower() in lower]
        score = min(1.0, len(tags) / 4)
        category = self._category(tags, lower)
        reason = "Matched keywords: " + ", ".join(tags) if tags else "No configured AI keywords matched."
        return TweetAnalysis(
            tweet_id=tweet.tweet_id,
            relevance=score,
            importance_score=score,
            category=category,
            tags=tags,
            chinese_summary=self._summary(tweet.text, tags),
            reason=reason,
            should_push=score >= self.push_threshold,
        )

    @staticmethod
    def _category(tags: list[str], text: str) -> str:
        lowered = {tag.lower() for tag in tags}
        if {"paper", "model"} & lowered:
            return "research"
        if {"github", "cursor", "coding"} & lowered:
            return "coding"
        if {"agent", "llm", "claude", "openai", "anthropic"} & lowered or "ai" in lowered:
            return "ai"
        if "release" in text or "published" in text:
            return "news"
        return "general"

    @staticmethod
    def _summary(text: str, tags: list[str]) -> str:
        trimmed = text.strip()
        if len(trimmed) > 90:
            trimmed = trimmed[:87].rstrip() + "..."
        if tags:
            return f"这条推文提到 {', '.join(tags)}：{trimmed}"
        return f"这条推文暂无明显 AI 相关信号：{trimmed}"
