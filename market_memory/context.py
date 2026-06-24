from __future__ import annotations

from datetime import datetime
from typing import Optional

from market_memory.models import Event, TweetContextResponse


def ordinal_percentile(value: float) -> str:
    rounded = int(round(value))
    if 11 <= (rounded % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(rounded % 10, "th")
    return f"{rounded}{suffix}"


def _label(asset: Optional[str], indicator_type: Optional[str], direction: Optional[str]) -> str:
    parts = [p for p in (asset, indicator_type, direction) if p]
    return " ".join(parts) if parts else "similar events"


def build_tweet_context(
    *,
    event_type: str,
    asset: Optional[str] = None,
    indicator_type: Optional[str] = None,
    direction: Optional[str] = None,
    current_value: Optional[float] = None,
    since: Optional[datetime] = None,
    occurrences: int = 0,
    percentile: Optional[float] = None,
    last_seen: Optional[datetime] = None,
    top_analogs: Optional[list[Event]] = None,
) -> TweetContextResponse:
    since_label = since.date().isoformat() if since else None
    label = _label(asset, indicator_type, direction)
    since_phrase = f" since {since_label}" if since_label else ""

    suffix = "occurrences" if occurrences != 1 else "occurrence"
    parts = [f"Similar {label} events{since_phrase}: {occurrences} {suffix}."]

    if percentile is not None:
        parts.append(
            f"Current reading ranks in the {ordinal_percentile(percentile)} percentile."
        )

    if last_seen is not None:
        parts.append(f"Last seen {last_seen.date().isoformat()}.")

    return TweetContextResponse(
        asset=asset,
        indicator_type=indicator_type,
        event_type=event_type,
        current_value=current_value,
        similar_events_since=since_label,
        occurrences=occurrences,
        percentile=percentile,
        last_seen=last_seen.date().isoformat() if last_seen else None,
        tweet_context=" ".join(parts),
        top_analogs=top_analogs or [],
    )