"""Incremental market-memory updates for scheduled / poll-driven sync."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from market_memory.backfill import (
    SINCE_DEFAULT,
    collect_liquidation_events,
    detect_basis_events,
    detect_fed_events,
    detect_funding_events,
    detect_liquidation_events_from_coinglass,
    verified_liquidation_fallback_events,
)
from market_memory.models import EventCreate
from market_memory.sources import (
    fetch_coinglass_hourly_liquidations,
    fetch_fred_fed_funds_changes,
    fetch_hl_daily_basis,
    fetch_hl_funding_history,
    fetch_okx_daily_basis,
    fetch_okx_funding_history,
    load_coinalyze_api_key,
    load_coinglass_api_key,
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
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


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


def _db_watermark(db: Any, *, asset: str | None, indicator_type: str | None) -> datetime | None:
    clauses = ["indicator_type = ?"]
    params: list[Any] = [indicator_type]
    if asset:
        clauses.append("asset = ?")
        params.append(asset)
    row = db._conn.execute(
        f"SELECT MAX(timestamp) FROM events WHERE {' AND '.join(clauses)}",
        params,
    ).fetchone()
    if not row or row[0] is None:
        return None
    ts = row[0]
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(ts)).replace(tzinfo=timezone.utc)


def _effective_since(db: Any, *, asset: str | None, indicator_type: str | None, default: datetime) -> datetime:
    mark = _db_watermark(db, asset=asset, indicator_type=indicator_type)
    if mark is None:
        return default
    if default.tzinfo is None:
        default = default.replace(tzinfo=timezone.utc)
    return max(default, mark - OVERLAP)


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
        for asset in ("BTC", "ETH", "SOL"):
            funding_since = _effective_since(db, asset=asset, indicator_type="funding", default=since)
            since_ms = int(funding_since.timestamp() * 1000)
            okx_funding = fetch_okx_funding_history(client, asset, since_ms=since_ms)
            hl_funding = fetch_hl_funding_history(client, asset, since_ms=since_ms)
            funding_events = detect_funding_events(asset, okx_funding, hl_funding)
            events.extend(funding_events)
            report["sources"][f"{asset}_funding"] = {"events": len(funding_events), "since": funding_since.date().isoformat()}

            basis_since = _effective_since(db, asset=asset, indicator_type="basis", default=since)
            basis_ms = int(basis_since.timestamp() * 1000)
            okx_basis = fetch_okx_daily_basis(client, asset, since_ms=basis_ms)
            hl_basis = fetch_hl_daily_basis(client, asset, since_ms=basis_ms)
            basis_events = detect_basis_events(asset, okx_basis, hl_basis)
            events.extend(basis_events)
            report["sources"][f"{asset}_basis"] = {"events": len(basis_events), "since": basis_since.date().isoformat()}

        cg_key = load_coinglass_api_key()
        if cg_key:
            liq_events: list[EventCreate] = []
            liq_since = _effective_since(db, asset="BTC", indicator_type="liquidations", default=since)
            liq_ms = int(liq_since.timestamp() * 1000)
            for asset in ("BTC", "ETH", "SOL"):
                agg = fetch_coinglass_hourly_liquidations(client, asset, api_key=cg_key, exchange_scope="aggregated", since_ms=liq_ms)
                okx_only = fetch_coinglass_hourly_liquidations(client, asset, api_key=cg_key, exchange_scope="okx", since_ms=liq_ms)
                liq_events.extend(detect_liquidation_events_from_coinglass(asset, agg, okx_only))
            events.extend(liq_events)
            report["sources"]["liquidations_mode"] = "coinglass_dual"
            report["sources"]["liquidations_events"] = len(liq_events)
        else:
            liq_since = _effective_since(db, asset="BTC", indicator_type="liquidations", default=since)
            cz_key = load_coinalyze_api_key()
            liq_events, liq_meta = collect_liquidation_events(
                client,
                since=liq_since,
                coinalyze_key=cz_key,
                include_verified_episodes=False,
            )
            if include_verified_liquidations and _db_watermark(db, asset="BTC", indicator_type="liquidations") is None:
                fallback = verified_liquidation_fallback_events(client, since=since)
                events.extend(fallback)
                report["sources"]["verified_liquidation_episodes"] = len(fallback)
            events.extend(liq_events)
            report["sources"]["liquidations_mode"] = liq_meta.get("mode", "okx")
            report["sources"]["liquidations_events"] = len(liq_events)
            for asset in ("BTC", "ETH", "SOL"):
                if asset in liq_meta:
                    report["sources"][f"{asset}_liquidations"] = liq_meta[asset]
            if not liq_meta.get("coinalyze"):
                report["warnings"].append(
                    "No COINALYZE_API_KEY — liquidations use OKX/Binance hourly buckets only. "
                    "Add COINALYZE_API_KEY to twitter-bot/.env for cross-verified hourly history."
                )
            if not liq_meta.get("binance_available"):
                report["warnings"].append(
                    "Binance liquidation API unavailable from this network — using OKX hourly buckets only. "
                    "History builds forward from each sync (OKX exposes ~14h of fills per poll)."
                )

        fred_key = load_fred_api_key()
        if fred_key:
            fed_mark = db._conn.execute(
                "SELECT MAX(timestamp) FROM events WHERE event_type = 'fed_announcement'"
            ).fetchone()
            fed_default = since if since.tzinfo else since.replace(tzinfo=timezone.utc)
            fed_since = fed_default
            if fed_mark and fed_mark[0] is not None:
                ts = fed_mark[0]
                if not isinstance(ts, datetime):
                    ts = datetime.fromisoformat(str(ts))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                fed_since = max(fed_default, ts - OVERLAP)
            fed_rows = fetch_fred_fed_funds_changes(client, fred_key, since=fed_since.date().isoformat())
            fed_events = detect_fed_events(fed_rows)
            events.extend(fed_events)
            report["sources"]["fed_funds"] = {"events": len(fed_events)}

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