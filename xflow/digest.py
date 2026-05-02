from __future__ import annotations

from collections import defaultdict

from xflow.storage import Storage


def generate_digest(storage: Storage, threshold: float, limit: int = 100) -> str:
    tweets = storage.list_analyzed_for_digest(threshold, limit)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for tweet in tweets:
        grouped[(tweet.get("analysis") or {}).get("category", "general")].append(tweet)

    lines = ["# xFlow Digest", ""]
    if not grouped:
        lines.extend(["No analyzed tweets met the digest threshold.", ""])
        return "\n".join(lines)

    for category in sorted(grouped):
        lines.extend([f"## {category}", ""])
        for tweet in grouped[category]:
            analysis = tweet["analysis"] or {}
            score = analysis.get("importance_score", 0)
            summary = analysis.get("chinese_summary") or tweet["text"]
            lines.append(f"- **{tweet['author_username']}** ({score:.2f}): {summary} [{tweet['url']}]")
        lines.append("")
    return "\n".join(lines)
