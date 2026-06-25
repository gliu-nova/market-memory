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


def _short_subject(
    *,
    asset: Optional[str],
    indicator_type: Optional[str],
    event_type: str,
) -> str:
    if event_type == "fed_announcement":
        return "Fed moves"
    if indicator_type == "liquidations":
        return f"{asset} liquidation spikes" if asset else "liquidation spikes"
    if indicator_type == "funding":
        return f"{asset} funding moves" if asset else "funding moves"
    if indicator_type == "basis":
        return f"{asset} basis moves" if asset else "basis moves"
    if indicator_type == "exchange_spread":
        return f"{asset} exchange spreads" if asset else "exchange spreads"
    if indicator_type == "fear_greed":
        return "Fear & Greed extremes"
    _LABELS = {
        "sp500": "S&P 500 moves",
        "nasdaq100": "NASDAQ moves",
        "vix": "VIX spikes",
        "dxy": "DXY moves",
        "gold": "gold moves",
        "silver": "silver moves",
        "move": "MOVE index moves",
        "oil": "oil moves",
        "hy_spread": "HY spread moves",
        "treasury_10y": "10Y yield moves",
        "yield_curve": "yield curve moves",
        "jobless_claims": "claims spikes",
        "unemployment": "unemployment moves",
        "cpi_yoy": "CPI moves",
        "m2": "M2 moves",
        "mortgage_30y": "mortgage rate moves",
        "consumer_sentiment": "sentiment moves",
        "case_shiller": "home price moves",
        "pmi_manufacturing": "manufacturing moves",
        "ism_services": "services activity moves",
        "btc": "BTC moves",
        "eth": "ETH moves",
        "sol": "SOL moves",
    }
    if indicator_type in _LABELS:
        return _LABELS[indicator_type]
    parts = [p for p in (asset, indicator_type) if p]
    return " ".join(parts) if parts else "similar moves"


def _concise_tweet_line(
    *,
    subject: str,
    since: Optional[datetime],
    occurrences: int,
    percentile: Optional[float],
    last_seen: Optional[datetime],
) -> str:
    """One short context line aligned with twitter-bot compose guidelines."""
    since_year = since.year if since else None

    if percentile is not None and percentile >= 90:
        return f"{ordinal_percentile(percentile)} percentile historically."
    if percentile is not None and percentile <= 10:
        return f"{ordinal_percentile(percentile)} percentile — unusually low."

    if occurrences >= 8 and since_year:
        return f"{occurrences} similar {subject} since {since_year}."
    if occurrences >= 3 and last_seen is not None:
        return f"Biggest similar {subject} since {last_seen.strftime('%b %Y')}."
    if occurrences >= 3 and since_year:
        return f"{occurrences} similar {subject} since {since_year}."

    return f"Rare {subject} vs recent history."


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
    subject = _short_subject(asset=asset, indicator_type=indicator_type, event_type=event_type)
    tweet_context = _concise_tweet_line(
        subject=subject,
        since=since,
        occurrences=occurrences,
        percentile=percentile,
        last_seen=last_seen,
    )

    return TweetContextResponse(
        asset=asset,
        indicator_type=indicator_type,
        event_type=event_type,
        current_value=current_value,
        similar_events_since=since_label,
        occurrences=occurrences,
        percentile=percentile,
        last_seen=last_seen.date().isoformat() if last_seen else None,
        tweet_context=tweet_context,
        top_analogs=top_analogs or [],
    )