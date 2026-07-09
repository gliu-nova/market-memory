"""Incremental market-memory updates for scheduled / poll-driven sync."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from market_memory.backfill import SINCE_DEFAULT
from market_memory.collect import (
    collect_crypto_derivative_events,
    collect_exchange_spread_events,
    collect_liquidation_bundle,
    collect_series_indicator_events,
)
from market_memory.detectors import detect_fed_funds_events
from market_memory.indicators import ALL_SERIES_INDICATORS
from market_memory.models import EventCreate
from market_memory.sources import (
    fetch_fred_fed_funds_changes,
    load_fred_api_key,
)

STATE_FILE = "sync_state.json"
OVERLAP = timedelta(days=7)


def _state_path(data_dir: str | Path) -> Path:
    return Path(data_dir) / STATE_FILE


def _load_state(data_dir: str | Path) -> dict[str, Any]:
    path = _state_path(data_dir)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(data_dir: str | Path, state: dict[str, Any]) -> None:
    path = _state_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2)
    fd, tmp_name = tempfile.mkstemp(prefix=".sync_state_", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _should_run(data_dir: str, interval_minutes: int) -> bool:
    state = _load_state(data_dir)
    last = _parse_dt(state.get("last_sync_at"))
    if last is None:
        return True
    return datetime.now(timezone.utc) - last >= timedelta(minutes=interval_minutes)


def _db_watermark(
    db: Any,
    *,
    asset: str | None,
    indicator_type: str | None,
    event_type: str | None = None,
) -> datetime | None:
    return db.watermark(asset=asset, indicator_type=indicator_type, event_type=event_type)


def _effective_since(
    db: Any,
    *,
    asset: str | None,
    indicator_type: str | None,
    default: datetime,
    event_type: str | None = None,
) -> datetime:
    mark = _db_watermark(db, asset=asset, indicator_type=indicator_type, event_type=event_type)
    if mark is None:
        return default
    if default.tzinfo is None:
        default = default.replace(tzinfo=timezone.utc)
    return max(default, mark - OVERLAP)


def _liquidation_warnings(liq_meta: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    mode = liq_meta.get("mode")
    if mode == "coinglass_dual":
        return warnings
    if not liq_meta.get("coinalyze"):
        warnings.append(
            "No COINALYZE_API_KEY — liquidations use OKX/Binance hourly buckets only. "
            "Add COINALYZE_API_KEY to twitter-bot/.env for cross-verified hourly history."
        )
    if not liq_meta.get("binance_available"):
        warnings.append(
            "Binance liquidation API unavailable from this network — using OKX hourly buckets only. "
            "History builds forward from each sync (OKX exposes ~14h of fills per poll)."
        )
    return warnings


def collect_incremental_events(
    db: Any,
    *,
    since: datetime | None = None,
    include_verified_liquidations: bool = False,
) -> tuple[list[EventCreate], dict[str, Any]]:
    since = since or SINCE_DEFAULT
    report: dict[str, Any] = {"mode": "incremental", "since": since.date().isoformat(), "sources": {}, "warnings": []}

    events: list[EventCreate] = []
    with httpx.Client() as client:
        since_by_asset = {
            asset: int(
                _effective_since(db, asset=asset, indicator_type="funding", default=since).timestamp()
                * 1000
            )
            for asset in ("BTC", "ETH", "SOL")
        }
        deriv_events, deriv_report = collect_crypto_derivative_events(
            client,
            since_ms_by_asset=since_by_asset,
        )
        events.extend(deriv_events)
        report["sources"].update(deriv_report)

        liq_since = min(
            _effective_since(db, asset=asset, indicator_type="liquidations", default=since)
            for asset in ("BTC", "ETH", "SOL")
        )
        liq_events, liq_meta = collect_liquidation_bundle(
            client,
            since=liq_since,
            include_verified_episodes=include_verified_liquidations
            and _db_watermark(db, asset="BTC", indicator_type="liquidations") is None,
        )
        events.extend(liq_events)
        report["sources"]["liquidations_mode"] = liq_meta.get("mode", "okx")
        report["sources"]["liquidations_events"] = len(liq_events)
        for key, val in liq_meta.items():
            if key in ("BTC", "ETH", "SOL"):
                report["sources"][f"{key}_liquidations"] = val
        report["warnings"].extend(_liquidation_warnings(liq_meta))

        fred_key = load_fred_api_key()
        if fred_key:
            fed_since = _effective_since(
                db,
                asset=None,
                indicator_type="fed_funds",
                event_type="fed_announcement",
                default=since,
            )
            fed_rows = fetch_fred_fed_funds_changes(client, fred_key, since=fed_since.date().isoformat())
            fed_events = detect_fed_funds_events(fed_rows)
            events.extend(fed_events)
            report["sources"]["fed_funds"] = {"events": len(fed_events)}
        else:
            report["warnings"].append("No FRED_API_KEY — skipped FRED macro series.")

        for spec in ALL_SERIES_INDICATORS:
            if spec.key == "fed_funds":
                continue
            spec_since = _effective_since(db, asset=spec.asset, indicator_type=spec.key, default=since)
            spec_events, spec_report = collect_series_indicator_events(
                client,
                since=spec_since,
                fred_key=fred_key,
                specs=(spec,),
                since_date=spec_since.date().isoformat(),
            )
            events.extend(spec_events)
            report["sources"][spec.key] = spec_report.get(spec.key, {"events": len(spec_events)})

        spread_events, spread_report = collect_exchange_spread_events(client)
        events.extend(spread_events)
        report["sources"].update(spread_report)

    report["total_events"] = len(events)
    return events, report


def sync_database(
    data_dir: str = "data",
    *,
    since: datetime | None = None,
    interval_minutes: int = 0,
    force: bool = False,
    seed_verified_liquidations: bool = False,
) -> dict[str, Any]:
    """Append new events since last watermark. Respects interval unless force=True."""
    from market_memory.db import EventDB

    if interval_minutes > 0 and not force and not _should_run(data_dir, interval_minutes):
        return {"mode": "incremental", "skipped": True, "reason": f"interval {interval_minutes}m not elapsed"}

    db = EventDB(data_dir=data_dir)
    try:
        events, report = collect_incremental_events(
            db,
            since=since,
            include_verified_liquidations=seed_verified_liquidations,
        )
        ingested = db.ingest_events(events)
        report["ingested"] = ingested
        report["skipped"] = False
        report["db_stats"] = db.stats().model_dump(mode="json")
    finally:
        db.close()

    state = _load_state(data_dir)
    state["last_sync_at"] = datetime.now(timezone.utc).isoformat()
    state["last_report"] = {k: v for k, v in report.items() if k != "db_stats"}
    _save_state(data_dir, state)
    return report
