from __future__ import annotations

import statistics
from datetime import datetime, timezone
from typing import Any

import httpx

from market_memory.models import EventCreate
from market_memory.sources import (
    ASSET_OKX,
    fetch_binance_liquidation_hourly_buckets,
    fetch_coinalyze_liquidation_hourly_buckets,
    fetch_coinglass_hourly_liquidations,
    fetch_fred_fed_funds_changes,
    fetch_hl_daily_basis,
    fetch_hl_funding_history,
    fetch_okx_daily_basis,
    fetch_okx_funding_history,
    fetch_okx_liquidation_hourly_buckets,
    load_coinalyze_api_key,
    load_coinglass_api_key,
    load_fred_api_key,
)


SINCE_DEFAULT = datetime(2021, 1, 1, tzinfo=timezone.utc)

# Publicly reported major liquidation episodes (24h totals). Cross-checked against
# OKX+Hyperliquid daily crash dates during backfill via price_move_verified metadata.
VERIFIED_LIQUIDATION_EPISODES: list[dict[str, Any]] = [
    {"asset": "BTC", "date": "2021-05-19", "value": 3_200_000_000, "tag": "china-crackdown", "source": "kucoin-blog"},
    {"asset": "BTC", "date": "2021-09-07", "value": 1_800_000_000, "tag": "el-salvador-dip", "source": "public-reports"},
    {"asset": "BTC", "date": "2022-06-13", "value": 500_000_000, "tag": "celcius-contagion", "source": "public-reports"},
    {"asset": "BTC", "date": "2022-11-09", "value": 1_500_000_000, "tag": "ftx-collapse", "source": "coindesk"},
    {"asset": "BTC", "date": "2023-08-17", "value": 1_000_000_000, "tag": "china-property-spillover", "source": "public-reports"},
    {"asset": "BTC", "date": "2024-08-05", "value": 1_100_000_000, "tag": "yen-carry-unwind", "source": "coindesk"},
    {"asset": "ETH", "date": "2022-11-09", "value": 900_000_000, "tag": "ftx-collapse", "source": "public-reports"},
    {"asset": "ETH", "date": "2024-08-05", "value": 400_000_000, "tag": "yen-carry-unwind", "source": "public-reports"},
    {"asset": "SOL", "date": "2022-11-09", "value": 200_000_000, "tag": "ftx-collapse", "source": "public-reports"},
    {"asset": "SOL", "date": "2024-01-22", "value": 150_000_000, "tag": "etf-volatility", "source": "public-reports"},
]

LIQ_FLOORS_USD = {"BTC": 25_000_000, "ETH": 3_000_000, "SOL": 500_000}


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = (len(ordered) - 1) * (pct / 100)
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (k - lo) * (ordered[hi] - ordered[lo])


def _pct_change(curr: float, prev: float) -> float | None:
    if prev == 0:
        return None
    return (curr - prev) / abs(prev) * 100


def _match_rate(rows: list[dict[str, Any]], ts: int, *, window_ms: int = 4 * 3600 * 1000) -> float | None:
    best = None
    best_delta = None
    for row in rows:
        delta = abs(row["time"] - ts)
        if delta > window_ms:
            continue
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best = row["rate"]
    return best


def _rates_agree(okx_rate: float, hl_rate: float) -> bool:
    if abs(okx_rate - hl_rate) <= 0.00005:
        return True
    denom = max(abs(okx_rate), abs(hl_rate), 1e-9)
    return abs(okx_rate - hl_rate) / denom <= 0.35


def _basis_agree(okx_bps: float, hl_bps: float) -> bool:
    return abs(okx_bps - hl_bps) <= max(8.0, 0.5 * max(abs(okx_bps), abs(hl_bps), 1.0))


def detect_funding_events(
    asset: str,
    okx_rows: list[dict[str, Any]],
    hl_rows: list[dict[str, Any]],
) -> list[EventCreate]:
    verified: list[tuple[int, float, str, dict[str, Any]]] = []
    use_hl_primary = len(hl_rows) >= len(okx_rows)
    primary = hl_rows if use_hl_primary else okx_rows
    rates = [r["rate"] for r in primary]
    if len(rates) < 30:
        return []
    p95 = _percentile(rates, 92)
    p05 = _percentile(rates, 8)
    changes = [abs(rates[i] - rates[i - 1]) for i in range(1, len(rates))]
    med_change = statistics.median(changes) if changes else 0.0

    for i, row in enumerate(primary):
        okx_rate = _match_rate(okx_rows, row["time"])
        hl_rate = row["rate"] if use_hl_primary else _match_rate(hl_rows, row["time"])
        sources: list[str] = []
        if okx_rate is not None and hl_rate is not None and _rates_agree(okx_rate, hl_rate):
            avg_rate = (okx_rate + hl_rate) / 2
            sources = ["okx", "hyperliquid"]
        elif okx_rate is None and hl_rate is not None:
            avg_rate = hl_rate
            sources = ["hyperliquid"]
        elif hl_rate is None and okx_rate is not None:
            avg_rate = okx_rate
            sources = ["okx"]
        else:
            continue
        direction = None
        if avg_rate >= p95:
            direction = "extreme"
        elif avg_rate <= p05:
            direction = "extreme"
        elif i > 0:
            prev_row = primary[i - 1]
            prev_okx = _match_rate(okx_rows, prev_row["time"])
            prev_hl = _match_rate(hl_rows, prev_row["time"]) or prev_row["rate"]
            prev_avg = None
            if prev_okx is not None and prev_hl is not None and _rates_agree(prev_okx, prev_hl):
                prev_avg = (prev_okx + prev_hl) / 2
            elif prev_okx is None:
                prev_avg = prev_hl
            else:
                prev_avg = prev_okx
            swing = abs(avg_rate - (prev_avg if prev_avg is not None else avg_rate))
            if med_change and swing >= 3 * med_change and abs(avg_rate) <= max(med_change * 2, 0.0001):
                direction = "reset"
        if not direction:
            continue
        prev_avg = None
        if i > 0:
            prev_row = primary[i - 1]
            prev_okx = _match_rate(okx_rows, prev_row["time"])
            prev_hl = _match_rate(hl_rows, prev_row["time"]) or prev_row["rate"]
            if prev_okx is not None and prev_hl is not None and _rates_agree(prev_okx, prev_hl):
                prev_avg = (prev_okx + prev_hl) / 2
            elif prev_okx is None:
                prev_avg = prev_hl
            else:
                prev_avg = prev_okx
        verified.append(
            (
                row["time"],
                avg_rate,
                direction,
                {
                    "okx_rate": okx_rate,
                    "hl_rate": hl_rate,
                    "prev_avg": prev_avg,
                    "verified_sources": sources,
                },
            )
        )

    events: list[EventCreate] = []
    seen_days: set[str] = set()
    for ts, rate, direction, meta in sorted(verified, key=lambda x: x[0], reverse=True):
        day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()
        if day in seen_days:
            continue
        seen_days.add(day)
        prev = meta.get("prev_avg")
        events.append(
            EventCreate(
                id=f"{asset.lower()}-funding-{day}",
                timestamp=datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                event_type="market_surge",
                asset=asset,
                indicator_type="funding",
                timeframe="8h",
                value=rate,
                percent_change=_pct_change(rate, prev) if isinstance(prev, (int, float)) else None,
                direction=direction,
                source="okx+hyperliquid",
                tags=["crypto"],
                metadata=meta,
            )
        )
    return events


def detect_basis_events(
    asset: str,
    okx_rows: list[dict[str, Any]],
    hl_rows: list[dict[str, Any]],
) -> list[EventCreate]:
    hl_by_day = {datetime.fromtimestamp(r["time"] / 1000, tz=timezone.utc).date().isoformat(): r["basis_bps"] for r in hl_rows}
    joined: list[tuple[int, float, float]] = []
    for row in okx_rows:
        day = datetime.fromtimestamp(row["time"] / 1000, tz=timezone.utc).date().isoformat()
        hl_bps = hl_by_day.get(day)
        if hl_bps is None:
            continue
        if not _basis_agree(row["basis_bps"], hl_bps):
            continue
        joined.append((row["time"], row["basis_bps"], hl_bps))
    if len(joined) < 30:
        return []
    vals = [x[1] for x in joined]
    p95 = _percentile(vals, 95)
    p05 = _percentile(vals, 5)
    events: list[EventCreate] = []
    seen_days: set[str] = set()
    for ts, okx_bps, hl_bps in sorted(joined, key=lambda x: x[0], reverse=True):
        avg = (okx_bps + hl_bps) / 2
        if not (avg >= p95 or avg <= p05):
            continue
        day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()
        if day in seen_days:
            continue
        seen_days.add(day)
        direction = "positive" if avg >= p95 else "negative"
        events.append(
            EventCreate(
                id=f"{asset.lower()}-basis-{day}",
                timestamp=datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                event_type="market_surge",
                asset=asset,
                indicator_type="basis",
                timeframe="24h",
                value=avg,
                direction=direction,
                source="okx+hyperliquid",
                tags=["crypto"],
                metadata={"okx_basis_bps": okx_bps, "hl_basis_bps": hl_bps, "verified_sources": ["okx", "hyperliquid"]},
            )
        )
    return events


def _liq_buckets_agree(okx_total: float, other_total: float | None) -> bool:
    if other_total is None:
        return True
    if okx_total <= 0 or other_total <= 0:
        return False
    ratio = min(okx_total, other_total) / max(okx_total, other_total)
    return ratio >= 0.15


def detect_multi_source_liquidation_events(
    asset: str,
    *,
    coinalyze_buckets: list[dict[str, Any]] | None,
    okx_buckets: list[dict[str, Any]],
    binance_buckets: list[dict[str, Any]] | None,
) -> list[EventCreate]:
    """Cross-verify liquidation spikes across Coinalyze, OKX, and optional Binance."""
    floor = LIQ_FLOORS_USD[asset]
    okx_by_hour = {int(b["time"]): b for b in okx_buckets}
    bn_by_hour = {int(b["time"]): b for b in (binance_buckets or [])}
    cz_by_hour = {int(b["time"]): b for b in (coinalyze_buckets or [])}
    seen: set[int] = set()
    events: list[EventCreate] = []

    def _emit(
        hour: int,
        *,
        value: float,
        sources: list[str],
        metadata: dict[str, Any],
    ) -> None:
        if hour in seen:
            return
        seen.add(hour)
        events.append(
            EventCreate(
                id=f"{asset.lower()}-liq-1h-{hour}",
                timestamp=datetime.fromtimestamp(hour / 1000, tz=timezone.utc),
                event_type="market_surge",
                asset=asset,
                indicator_type="liquidations",
                timeframe="1h",
                value=value,
                direction="spike",
                source="+".join(sources),
                tags=["crypto"],
                metadata=metadata,
            )
        )

    for hour, cz in cz_by_hour.items():
        cz_total = float(cz["total_usd"])
        if cz_total < floor:
            continue
        okx = okx_by_hour.get(hour)
        okx_total = float(okx["total_usd"]) if okx else None
        bn = bn_by_hour.get(hour)
        bn_total = float(bn["total_usd"]) if bn else None

        sources = ["coinalyze"]
        if okx_total is not None:
            if not _liq_buckets_agree(cz_total, okx_total):
                continue
            sources.append("okx")
        if bn_total is not None:
            if not _liq_buckets_agree(cz_total, bn_total):
                continue
            if "binance" not in sources:
                sources.append("binance")

        agreeing = [cz_total]
        if okx_total is not None:
            agreeing.append(okx_total)
        if bn_total is not None and "binance" in sources:
            agreeing.append(bn_total)
        value = sum(agreeing) / len(agreeing)

        _emit(
            hour,
            value=value,
            sources=sources,
            metadata={
                "coinalyze_total_usd": cz_total,
                "coinalyze_long_usd": cz.get("long_usd"),
                "coinalyze_short_usd": cz.get("short_usd"),
                "coinalyze_symbol": cz.get("symbol"),
                "okx_total_usd": okx_total,
                "okx_long_usd": okx.get("long_usd") if okx else None,
                "okx_short_usd": okx.get("short_usd") if okx else None,
                "binance_total_usd": bn_total,
                "verified_sources": sources,
            },
        )

    for hour, okx in okx_by_hour.items():
        if hour in seen:
            continue
        okx_total = float(okx["total_usd"])
        if okx_total < floor:
            continue
        bn = bn_by_hour.get(hour)
        bn_total = float(bn["total_usd"]) if bn else None
        if bn_total is not None and not _liq_buckets_agree(okx_total, bn_total):
            continue
        sources = ["okx"]
        if bn_total is not None:
            sources.append("binance")
        _emit(
            hour,
            value=okx_total,
            sources=sources,
            metadata={
                "okx_total_usd": okx_total,
                "okx_long_usd": okx.get("long_usd"),
                "okx_short_usd": okx.get("short_usd"),
                "binance_total_usd": bn_total,
                "verified_sources": sources,
            },
        )

    return events


def collect_liquidation_events(
    client: httpx.Client,
    *,
    since: datetime,
    coinalyze_key: str | None = None,
    include_verified_episodes: bool = False,
) -> tuple[list[EventCreate], dict[str, Any]]:
    since_sec = int(since.timestamp())
    to_sec = int(datetime.now(timezone.utc).timestamp())
    liq_events: list[EventCreate] = []
    meta: dict[str, Any] = {"coinalyze": bool(coinalyze_key)}
    binance_available = False

    for asset in ("BTC", "ETH", "SOL"):
        okx_buckets = fetch_okx_liquidation_hourly_buckets(client, asset)
        bn_buckets = fetch_binance_liquidation_hourly_buckets(client, asset)
        if bn_buckets is not None:
            binance_available = True
        cz_buckets = None
        if coinalyze_key:
            cz_buckets = fetch_coinalyze_liquidation_hourly_buckets(
                client,
                asset,
                api_key=coinalyze_key,
                since_sec=since_sec,
                to_sec=to_sec,
            )
        asset_events = detect_multi_source_liquidation_events(
            asset,
            coinalyze_buckets=cz_buckets,
            okx_buckets=okx_buckets,
            binance_buckets=bn_buckets,
        )
        liq_events.extend(asset_events)
        meta[asset] = {
            "okx_hours": len(okx_buckets),
            "coinalyze_hours": len(cz_buckets or []),
            "events": len(asset_events),
        }

    if include_verified_episodes:
        liq_events.extend(verified_liquidation_fallback_events(client, since=since))
        meta["verified_episodes"] = True

    if coinalyze_key:
        meta["mode"] = "coinalyze+okx+binance" if binance_available else "coinalyze+okx"
    elif binance_available:
        meta["mode"] = "okx+binance"
    else:
        meta["mode"] = "okx"
    meta["events"] = len(liq_events)
    meta["binance_available"] = binance_available
    return liq_events, meta


def detect_liquidation_events_from_coinglass(
    asset: str,
    agg_rows: list[dict[str, Any]],
    okx_rows: list[dict[str, Any]],
) -> list[EventCreate]:
    okx_by_time = {r["time"]: r["total_usd"] for r in okx_rows}
    totals = [r["total_usd"] for r in agg_rows if r["total_usd"] > 0]
    if len(totals) < 50:
        return []
    threshold = max(LIQ_FLOORS_USD[asset], _percentile(totals, 95))
    events: list[EventCreate] = []
    for row in agg_rows:
        total = row["total_usd"]
        if total < threshold:
            continue
        okx_total = okx_by_time.get(row["time"])
        if okx_total is None:
            continue
        if okx_total < 0.15 * total:
            continue
        ts = row["time"]
        events.append(
            EventCreate(
                id=f"{asset.lower()}-liq-{ts}",
                timestamp=datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                event_type="market_surge",
                asset=asset,
                indicator_type="liquidations",
                timeframe="1h",
                value=total,
                direction="spike",
                source="coinglass:agg+okx",
                tags=["crypto"],
                metadata={
                    "aggregated_usd": total,
                    "okx_usd": okx_total,
                    "verified_sources": ["coinglass-aggregated", "coinglass-okx"],
                },
            )
        )
    return events


def verified_liquidation_fallback_events(
    client: httpx.Client,
    *,
    since: datetime,
) -> list[EventCreate]:
    since_ms = int(since.timestamp() * 1000)
    events: list[EventCreate] = []
    for ep in VERIFIED_LIQUIDATION_EPISODES:
        day = ep["date"]
        ts = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        if ts < since_ms:
            continue
        asset = ep["asset"]
        swap = ASSET_OKX[asset]["swap"]
        try:
            mark = client.get(
                f"https://www.okx.com/api/v5/market/history-candles",
                params={"instId": swap, "bar": "1D", "after": str(ts), "limit": 2},
                timeout=30,
            ).json()
            hl = client.post(
                "https://api.hyperliquid.xyz/info",
                json={
                    "type": "candleSnapshot",
                    "req": {"coin": asset, "interval": "1d", "startTime": ts, "endTime": ts + 86400000},
                },
                timeout=30,
            ).json()
            okx_move = None
            if mark.get("data"):
                o, c = float(mark["data"][0][1]), float(mark["data"][0][4])
                okx_move = (c - o) / o * 100 if o else 0
            hl_move = None
            if hl:
                o, c = float(hl[0]["o"]), float(hl[0]["c"])
                hl_move = (c - o) / o * 100 if o else 0
            if okx_move is None or hl_move is None:
                continue
            if not (okx_move <= -2.5 and hl_move <= -2.5):
                continue
        except Exception:
            continue
        events.append(
            EventCreate(
                id=f"{asset.lower()}-liq-{day}",
                timestamp=datetime.fromisoformat(f"{day}T12:00:00+00:00"),
                event_type="market_surge",
                asset=asset,
                indicator_type="liquidations",
                timeframe="24h",
                value=float(ep["value"]),
                direction="spike",
                source=ep["source"],
                tags=["crypto", ep["tag"]],
                metadata={
                    "verified_sources": ["public-report", "okx-price", "hyperliquid-price"],
                    "okx_daily_move_pct": okx_move,
                    "hl_daily_move_pct": hl_move,
                },
            )
        )
    return events


def detect_fed_events(rows: list[dict[str, Any]]) -> list[EventCreate]:
    events: list[EventCreate] = []
    for row in rows:
        change = row["change_bps"]
        direction = "drop" if change < 0 else "positive"
        events.append(
            EventCreate(
                id=f"fed-funds-{row['date']}",
                timestamp=datetime.fromisoformat(f"{row['date']}T18:00:00+00:00"),
                event_type="fed_announcement",
                value=row["value"],
                percent_change=change,
                direction=direction,
                source="fred",
                tags=["macro"],
                metadata={"prev_value": row["prev"], "verified_sources": ["fred"]},
            )
        )
    return events


def collect_real_events(*, since: datetime | None = None) -> tuple[list[EventCreate], dict[str, Any]]:
    since = since or SINCE_DEFAULT
    since_ms = int(since.timestamp() * 1000)
    report: dict[str, Any] = {"since": since.date().isoformat(), "sources": {}, "warnings": []}
    events: list[EventCreate] = []

    with httpx.Client() as client:
        for asset in ("BTC", "ETH", "SOL"):
            okx_funding = fetch_okx_funding_history(client, asset, since_ms=since_ms)
            hl_funding = fetch_hl_funding_history(client, asset, since_ms=since_ms)
            funding_events = detect_funding_events(asset, okx_funding, hl_funding)
            events.extend(funding_events)
            report["sources"][f"{asset}_funding"] = {
                "okx_points": len(okx_funding),
                "hl_points": len(hl_funding),
                "events": len(funding_events),
            }

            okx_basis = fetch_okx_daily_basis(client, asset, since_ms=since_ms)
            hl_basis = fetch_hl_daily_basis(client, asset, since_ms=since_ms)
            basis_events = detect_basis_events(asset, okx_basis, hl_basis)
            events.extend(basis_events)
            report["sources"][f"{asset}_basis"] = {
                "okx_points": len(okx_basis),
                "hl_points": len(hl_basis),
                "events": len(basis_events),
            }

        cg_key = load_coinglass_api_key()
        if cg_key:
            liq_events: list[EventCreate] = []
            for asset in ("BTC", "ETH", "SOL"):
                agg = fetch_coinglass_hourly_liquidations(client, asset, api_key=cg_key, exchange_scope="aggregated", since_ms=since_ms)
                okx_only = fetch_coinglass_hourly_liquidations(client, asset, api_key=cg_key, exchange_scope="okx", since_ms=since_ms)
                asset_events = detect_liquidation_events_from_coinglass(asset, agg, okx_only)
                liq_events.extend(asset_events)
                report["sources"][f"{asset}_liquidations"] = {
                    "coinglass_agg_hours": len(agg),
                    "coinglass_okx_hours": len(okx_only),
                    "events": len(asset_events),
                }
            events.extend(liq_events)
            report["sources"]["liquidations_mode"] = "coinglass_dual"
        else:
            cz_key = load_coinalyze_api_key()
            liq_events, liq_meta = collect_liquidation_events(
                client,
                since=since,
                coinalyze_key=cz_key,
                include_verified_episodes=True,
            )
            events.extend(liq_events)
            mode = liq_meta.get("mode", "okx")
            report["sources"]["liquidations_mode"] = f"verified_episodes+{mode}"
            report["sources"]["liquidations_events"] = len(liq_events)
            for asset in ("BTC", "ETH", "SOL"):
                if asset in liq_meta:
                    report["sources"][f"{asset}_liquidations"] = liq_meta[asset]
            if cz_key:
                report["warnings"].append(
                    "Liquidations: price-verified major episodes plus Coinalyze/OKX/Binance cross-check. "
                    "Run `python -m market_memory.cli sync` on a schedule to keep hourly data current."
                )
            else:
                report["warnings"].append(
                    "Liquidations: major historical episodes (price-verified) plus OKX hourly buckets. "
                    "Add COINALYZE_API_KEY for fuller hourly history. "
                    "Run `python -m market_memory.cli sync` on a schedule to accumulate forward-looking data."
                )

        fred_key = load_fred_api_key()
        if fred_key:
            fed_rows = fetch_fred_fed_funds_changes(client, fred_key, since=since.date().isoformat())
            fed_events = detect_fed_events(fed_rows)
            events.extend(fed_events)
            report["sources"]["fed_funds"] = {"changes": len(fed_rows), "events": len(fed_events)}
        else:
            report["warnings"].append("No FRED_API_KEY — skipped fed funds events.")

    report["total_events"] = len(events)
    return events, report


def backfill_database(
    data_dir: str = "data",
    *,
    since: datetime | None = None,
    wipe: bool = True,
) -> dict[str, Any]:
    from market_memory.db import EventDB

    events, report = collect_real_events(since=since)
    db = EventDB(data_dir=data_dir)
    try:
        if wipe:
            db._conn.execute("DELETE FROM events")
        ingested = db.ingest_events(events)
        report["ingested"] = ingested
        report["db_stats"] = db.stats().model_dump(mode="json")
    finally:
        db.close()
    return report