"""
Example: enrich a draft tweet with Market Memory historical context.

Use either the library directly (recommended for twitter-bot) or HTTP calls
when running market-memory as a sidecar service.
"""

from __future__ import annotations

from datetime import datetime

import httpx

from market_memory import EventDB
from market_memory.models import SimilarityQuery


def enrich_tweet_with_memory(
    draft: str,
    *,
    data_dir: str = "data",
    event_type: str,
    asset: str | None = None,
    indicator_type: str | None = None,
    direction: str | None = None,
    current_value: float | None = None,
    since: str = "2021-01-01",
) -> str:
    """Append tweet-ready historical context to a draft post."""
    db = EventDB(data_dir=data_dir)
    try:
        query = SimilarityQuery(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            direction=direction,
            since=datetime.fromisoformat(since),
        )
        ctx = db.tweet_context(query, current_value=current_value)
        if ctx.occurrences == 0:
            return draft
        return f"{draft}\n\n{ctx.tweet_context}"
    finally:
        db.close()


def enrich_tweet_via_http(
    draft: str,
    *,
    base_url: str = "http://127.0.0.1:8788",
    event_type: str,
    asset: str | None = None,
    indicator_type: str | None = None,
    direction: str | None = None,
    current_value: float | None = None,
    since: str = "2021-01-01",
) -> str:
    params = {
        "event_type": event_type,
        "since": since,
    }
    if asset:
        params["asset"] = asset
    if indicator_type:
        params["indicator_type"] = indicator_type
    if direction:
        params["direction"] = direction
    if current_value is not None:
        params["current_value"] = current_value

    resp = httpx.get(f"{base_url}/tweet-context", params=params, timeout=5.0)
    resp.raise_for_status()
    ctx = resp.json()
    if ctx.get("occurrences", 0) == 0:
        return draft
    return f"{draft}\n\n{ctx['tweet_context']}"


if __name__ == "__main__":
    draft = (
        "BTC 24h liquidations spike to $461.8M.\n"
        "Long-side wipeout after yen carry unwind."
    )
    enriched = enrich_tweet_with_memory(
        draft,
        data_dir="../data",
        event_type="market_surge",
        asset="BTC",
        indicator_type="liquidations",
        direction="spike",
        current_value=461_800_000,
        since="2021-01-01",
    )
    print(enriched)