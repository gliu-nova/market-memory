"""Collect historical events for all twitter-bot indicators."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from market_memory.backfill import (
    SINCE_DEFAULT,
    collect_liquidation_events,
    detect_basis_events,
    detect_funding_events,
    detect_liquidation_events_from_coinglass,
    verified_liquidation_fallback_events,
)
from market_memory.detectors import (
    detect_exchange_spread_event,
    detect_fed_funds_events,
    detect_series_events,
)
from market_memory.indicators import ALL_SERIES_INDICATORS, IndicatorSpec
from market_memory.models import EventCreate
from market_memory.sources import (
    fetch_coinglass_hourly_liquidations,
    fetch_exchange_spread_bps,
    fetch_fear_greed_history,
    fetch_finra_dark_pool_history,
    fetch_fred_cpi_yoy_series,
    fetch_fred_fed_funds_changes,
    fetch_fred_series,
    fetch_hl_daily_basis,
    fetch_hl_funding_history,
    fetch_okx_daily_basis,
    fetch_okx_funding_history,
    fetch_yahoo_daily_history,
    load_coinalyze_api_key,
    load_coinglass_api_key,
    load_fred_api_key,
)

_FINRA_DARK_POOL_CACHE: dict[str, tuple[list[tuple[str, float]], list[tuple[str, float]]]] = {}

EXCHANGE_SPREAD_THRESHOLDS = {"BTC": 6.0, "ETH": 8.0, "SOL": 10.0}


def _finra_dark_pool_rows(
    client: httpx.Client,
    spec: IndicatorSpec,
    *,
    since: datetime,
) -> list[tuple[str, float]]:
    symbol = spec.symbol or "SPY"
    cache_key = f"{symbol}:{since.date().isoformat()}"
    if cache_key not in _FINRA_DARK_POOL_CACHE:
        _FINRA_DARK_POOL_CACHE[cache_key] = fetch_finra_dark_pool_history(
            client,
            since=since,
            symbol=symbol,
        )
    volume_rows, pct_rows = _FINRA_DARK_POOL_CACHE[cache_key]
    if spec.source == "finra_dark_pool_volume":
        return volume_rows
    if spec.source == "finra_dark_pool_pct":
        return pct_rows
    return []


def _fetch_series_rows(
    client: httpx.Client,
    spec: IndicatorSpec,
    *,
    since: datetime,
    fred_key: str | None,
) -> list[tuple[str, float]]:
    since_date = since.date().isoformat()
    if spec.source == "yahoo":
        return fetch_yahoo_daily_history(client, spec.symbol or "", since=since)
    if spec.source == "fred":
        if not fred_key:
            return []
        return fetch_fred_series(client, fred_key, spec.series or "", since=since_date)
    if spec.source == "fred_cpi_yoy":
        if not fred_key:
            return []
        return fetch_fred_cpi_yoy_series(client, fred_key, spec.series or "", since=since_date)
    if spec.source == "fear_greed":
        return fetch_fear_greed_history(client, since=since)
    if spec.source in ("finra_dark_pool_volume", "finra_dark_pool_pct"):
        return _finra_dark_pool_rows(client, spec, since=since)
    return []


def collect_series_indicator_events(
    client: httpx.Client,
    *,
    since: datetime,
    fred_key: str | None,
    specs: tuple[IndicatorSpec, ...] = ALL_SERIES_INDICATORS,
    since_date: str | None = None,
) -> tuple[list[EventCreate], dict[str, Any]]:
    events: list[EventCreate] = []
    report: dict[str, Any] = {}
    since_label = since_date or since.date().isoformat()

    for spec in specs:
        if spec.key == "fed_funds":
            continue
        try:
            rows = _fetch_series_rows(client, spec, since=since, fred_key=fred_key)
        except (httpx.HTTPError, RuntimeError, OSError, ValueError, KeyError, TypeError) as exc:
            report[spec.key] = {"error": str(exc), "events": 0}
            continue
        source = spec.source if spec.source != "fred_cpi_yoy" else "fred"
        spec_events = detect_series_events(spec, rows, source=source, since_date=since_label)
        events.extend(spec_events)
        report[spec.key] = {"points": len(rows), "events": len(spec_events)}

    return events, report


def collect_crypto_derivative_events(
    client: httpx.Client,
    *,
    since_ms: int | None = None,
    since_ms_by_asset: dict[str, int] | None = None,
) -> tuple[list[EventCreate], dict[str, Any]]:
    events: list[EventCreate] = []
    report: dict[str, Any] = {}

    for asset in ("BTC", "ETH", "SOL"):
        asset_since = (
            since_ms_by_asset[asset]
            if since_ms_by_asset is not None and asset in since_ms_by_asset
            else since_ms
        )
        if asset_since is None:
            raise ValueError("since_ms or since_ms_by_asset is required")
        okx_funding = fetch_okx_funding_history(client, asset, since_ms=asset_since)
        hl_funding = fetch_hl_funding_history(client, asset, since_ms=asset_since)
        funding_events = detect_funding_events(asset, okx_funding, hl_funding)
        events.extend(funding_events)
        report[f"{asset}_funding"] = {
            "okx_points": len(okx_funding),
            "hl_points": len(hl_funding),
            "events": len(funding_events),
            "since_ms": asset_since,
        }

        okx_basis = fetch_okx_daily_basis(client, asset, since_ms=asset_since)
        hl_basis = fetch_hl_daily_basis(client, asset, since_ms=asset_since)
        basis_events = detect_basis_events(asset, okx_basis, hl_basis)
        events.extend(basis_events)
        report[f"{asset}_basis"] = {
            "okx_points": len(okx_basis),
            "hl_points": len(hl_basis),
            "events": len(basis_events),
            "since_ms": asset_since,
        }

    return events, report


def collect_exchange_spread_events(client: httpx.Client) -> tuple[list[EventCreate], dict[str, Any]]:
    events: list[EventCreate] = []
    report: dict[str, Any] = {}
    for asset, threshold in EXCHANGE_SPREAD_THRESHOLDS.items():
        try:
            spread = fetch_exchange_spread_bps(client, asset)
            event = detect_exchange_spread_event(asset, spread, threshold_bps=threshold)
            if event:
                events.append(event)
            report[f"{asset}_exchange_spread"] = {"spread_bps": spread, "events": 1 if event else 0}
        except (httpx.HTTPError, RuntimeError, OSError, ValueError, KeyError, TypeError) as exc:
            report[f"{asset}_exchange_spread"] = {"error": str(exc), "events": 0}
    return events, report


def collect_liquidation_bundle(
    client: httpx.Client,
    *,
    since: datetime,
    include_verified_episodes: bool,
) -> tuple[list[EventCreate], dict[str, Any]]:
    cg_key = load_coinglass_api_key()
    if cg_key:
        since_ms = int(since.timestamp() * 1000)
        liq_events: list[EventCreate] = []
        report: dict[str, Any] = {"mode": "coinglass_dual"}
        for asset in ("BTC", "ETH", "SOL"):
            agg = fetch_coinglass_hourly_liquidations(
                client, asset, api_key=cg_key, exchange_scope="aggregated", since_ms=since_ms
            )
            okx_only = fetch_coinglass_hourly_liquidations(
                client, asset, api_key=cg_key, exchange_scope="okx", since_ms=since_ms
            )
            asset_events = detect_liquidation_events_from_coinglass(asset, agg, okx_only)
            liq_events.extend(asset_events)
            report[asset] = {
                "coinglass_agg_hours": len(agg),
                "coinglass_okx_hours": len(okx_only),
                "events": len(asset_events),
            }
        report["events"] = len(liq_events)
        return liq_events, report

    cz_key = load_coinalyze_api_key()
    liq_events, liq_meta = collect_liquidation_events(
        client,
        since=since,
        coinalyze_key=cz_key,
        include_verified_episodes=include_verified_episodes,
    )
    if include_verified_episodes:
        liq_meta["verified_episodes"] = True
    return liq_events, liq_meta


def collect_all_events(
    *,
    since: datetime | None = None,
    include_verified_liquidations: bool = True,
    include_exchange_spreads: bool = True,
    since_date: str | None = None,
) -> tuple[list[EventCreate], dict[str, Any]]:
    since = since or SINCE_DEFAULT
    report: dict[str, Any] = {"since": since.date().isoformat(), "sources": {}, "warnings": []}
    events: list[EventCreate] = []
    since_ms = int(since.timestamp() * 1000)

    with httpx.Client() as client:
        deriv_events, deriv_report = collect_crypto_derivative_events(client, since_ms=since_ms)
        events.extend(deriv_events)
        report["sources"].update(deriv_report)

        liq_events, liq_report = collect_liquidation_bundle(
            client,
            since=since,
            include_verified_episodes=include_verified_liquidations,
        )
        events.extend(liq_events)
        report["sources"]["liquidations_mode"] = liq_report.get("mode", "okx")
        report["sources"]["liquidations_events"] = len(liq_events)
        for key, val in liq_report.items():
            if key not in ("mode", "events", "coinalyze", "binance_available"):
                report["sources"][f"{key}_liquidations" if key in ("BTC", "ETH", "SOL") else key] = val

        fred_key = load_fred_api_key()
        if fred_key:
            fed_rows = fetch_fred_fed_funds_changes(client, fred_key, since=since.date().isoformat())
            fed_events = detect_fed_funds_events(fed_rows)
            events.extend(fed_events)
            report["sources"]["fed_funds"] = {"changes": len(fed_rows), "events": len(fed_events)}
        else:
            report["warnings"].append("No FRED_API_KEY — skipped FRED macro series.")

        series_events, series_report = collect_series_indicator_events(
            client,
            since=since,
            fred_key=fred_key,
            since_date=since_date,
        )
        events.extend(series_events)
        report["sources"].update(series_report)

        if include_exchange_spreads:
            spread_events, spread_report = collect_exchange_spread_events(client)
            events.extend(spread_events)
            report["sources"].update(spread_report)

    report["total_events"] = len(events)
    return events, report