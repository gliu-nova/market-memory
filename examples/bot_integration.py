"""
Enrich twitter-bot draft posts with Market Memory historical context (HTTP).

Set in twitter-bot/.env:
  MARKET_MEMORY_API_URL=https://market-memory.pages.dev
"""

from __future__ import annotations

import os

import httpx


def enrich_tweet_via_http(
    draft: str,
    *,
    base_url: str | None = None,
    event_type: str,
    asset: str | None = None,
    indicator_type: str | None = None,
    direction: str | None = None,
    current_value: float | None = None,
    since: str = "2021-01-01",
) -> str:
    url = base_url or os.environ.get("MARKET_MEMORY_API_URL", "https://market-memory.pages.dev")
    params: dict[str, str | float] = {
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

    resp = httpx.get(f"{url.rstrip('/')}/tweet-context", params=params, timeout=10.0)
    resp.raise_for_status()
    ctx = resp.json()
    if ctx.get("occurrences", 0) == 0:
        return draft
    return f"{draft}\n\n{ctx['tweet_context']}"


def record_event(
    event: dict,
    *,
    base_url: str | None = None,
    ingest_secret: str | None = None,
) -> None:
    """Write a new event back to Market Memory after posting."""
    url = base_url or os.environ.get("MARKET_MEMORY_API_URL", "https://market-memory.pages.dev")
    headers: dict[str, str] = {"Content-Type": "application/json"}
    secret = ingest_secret or os.environ.get("MARKET_MEMORY_INGEST_SECRET")
    if secret:
        headers["Authorization"] = f"Bearer {secret}"

    httpx.post(
        f"{url.rstrip('/')}/ingest",
        json={"events": [event]},
        headers=headers,
        timeout=10.0,
    ).raise_for_status()