"""Detect historical alert-worthy moves from indicator time series."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from market_memory.indicators import CrossRule, IndicatorSpec
from market_memory.models import EventCreate


def _pct_change(curr: float, prev: float) -> float | None:
    if prev == 0:
        return None
    return (curr - prev) / abs(prev) * 100


def _direction_from_change(change: float, *, alert_unit: str) -> str:
    if alert_unit == "percent":
        return "up" if change > 0 else "down"
    return "up" if change > 0 else "down"


def _crossed(prev: float | None, curr: float, rule: CrossRule) -> bool:
    if prev is None:
        return False
    if rule.rule_type == "crosses_above":
        return prev < rule.value <= curr
    if rule.rule_type == "crosses_below":
        return prev > rule.value >= curr
    return False


def detect_series_events(
    spec: IndicatorSpec,
    rows: list[tuple[str, float]],
    *,
    source: str,
    since_date: str | None = None,
) -> list[EventCreate]:
    """Build events from (date, value) rows using twitter-bot alert thresholds."""
    if len(rows) < 2:
        return []

    events: list[EventCreate] = []
    seen_ids: set[str] = set()

    def _emit(
        day: str,
        value: float,
        *,
        direction: str,
        change: float | None,
        trigger: str,
        extra_meta: dict[str, Any] | None = None,
    ) -> None:
        event_id = f"{spec.key}-{trigger}-{day}"
        if event_id in seen_ids:
            return
        if since_date and day < since_date:
            return
        seen_ids.add(event_id)
        meta: dict[str, Any] = {"trigger": trigger, "verified_sources": [source]}
        if extra_meta:
            meta.update(extra_meta)
        events.append(
            EventCreate(
                id=event_id,
                timestamp=datetime.fromisoformat(f"{day}T16:00:00+00:00"),
                event_type=spec.event_type,
                asset=spec.asset,
                indicator_type=spec.key,
                timeframe="1d",
                value=value,
                percent_change=change,
                direction=direction,
                source=source,
                tags=list(spec.tags),
                metadata=meta,
            )
        )

    prev_day, prev_val = rows[0]
    for day, val in rows[1:]:
        change = val - prev_val if spec.alert_unit == "absolute" else _pct_change(val, prev_val)
        if spec.detect_moves and change is not None and abs(change) >= spec.normal_alert:
            _emit(
                day,
                val,
                direction=_direction_from_change(change, alert_unit=spec.alert_unit),
                change=change,
                trigger="move",
                extra_meta={"prev_value": prev_val},
            )

        for rule in spec.cross_rules:
            if _crossed(prev_val, val, rule):
                cross_dir = "above" if rule.rule_type == "crosses_above" else "below"
                _emit(
                    day,
                    val,
                    direction=cross_dir,
                    change=change,
                    trigger=f"cross_{cross_dir}_{rule.value}",
                    extra_meta={"cross_level": rule.value, "prev_value": prev_val},
                )

        prev_day, prev_val = day, val

    return events


def detect_fed_funds_events(rows: list[dict[str, Any]]) -> list[EventCreate]:
    events: list[EventCreate] = []
    for row in rows:
        change = row["change_bps"]
        direction = "drop" if change < 0 else "positive"
        events.append(
            EventCreate(
                id=f"fed-funds-{row['date']}",
                timestamp=datetime.fromisoformat(f"{row['date']}T18:00:00+00:00"),
                event_type="fed_announcement",
                indicator_type="fed_funds",
                value=row["value"],
                percent_change=change,
                direction=direction,
                source="fred",
                tags=["macro"],
                metadata={"prev_value": row["prev"], "verified_sources": ["fred"]},
            )
        )
    return events


def detect_exchange_spread_event(
    asset: str,
    spread_bps: float,
    *,
    threshold_bps: float = 6.0,
    source: str = "coinbase+kraken",
) -> EventCreate | None:
    if spread_bps < threshold_bps:
        return None
    now = datetime.now(timezone.utc)
    day = now.date().isoformat()
    return EventCreate(
        id=f"{asset.lower()}-exchange-spread-{day}",
        timestamp=now,
        event_type="market_surge",
        asset=asset,
        indicator_type="exchange_spread",
        timeframe="snapshot",
        value=spread_bps,
        direction="wide",
        source=source,
        tags=["crypto"],
        metadata={"verified_sources": [source]},
    )