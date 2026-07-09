from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

OKX_BASE = "https://www.okx.com/api/v5"
HL_INFO = "https://api.hyperliquid.xyz/info"
COINGLASS_BASE = "https://open-api-v4.coinglass.com"
COINALYZE_BASE = "https://api.coinalyze.net/v1"
FRED_BASE = "https://api.stlouisfed.org/fred"
YAHOO_CHART_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
FEAR_GREED_BASE = "https://api.alternative.me/fng"
FINRA_REGSHO_CDN = "https://cdn.finra.org/equity/regsho/daily"
KRAKEN_BASE = "https://api.kraken.com/0/public/Ticker"
COINBASE_BASE = "https://api.coinbase.com/v2/prices"

EXCHANGE_SPREAD_ASSETS: dict[str, dict[str, str]] = {
    "BTC": {"kraken": "XBTUSD", "coinbase": "BTC-USD"},
    "ETH": {"kraken": "ETHUSD", "coinbase": "ETH-USD"},
    "SOL": {"kraken": "SOLUSD", "coinbase": "SOL-USD"},
}

ASSET_OKX: dict[str, dict[str, str]] = {
    "BTC": {"swap": "BTC-USDT-SWAP", "index": "BTC-USDT", "uly": "BTC-USDT"},
    "ETH": {"swap": "ETH-USDT-SWAP", "index": "ETH-USDT", "uly": "ETH-USDT"},
    "SOL": {"swap": "SOL-USDT-SWAP", "index": "SOL-USDT", "uly": "SOL-USDT"},
}

ASSET_HL = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}

ASSET_COINGLASS_PAIR = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT"}
ASSET_BINANCE = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT"}
BINANCE_FAPI = "https://fapi.binance.com"


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


_RETRYABLE_STATUS = {429, 502, 503, 504}


def _request_with_retry(client: httpx.Client, method: str, url: str, **kwargs: Any) -> httpx.Response:
    last: httpx.Response | None = None
    last_exc: Exception | None = None
    for attempt in range(8):
        try:
            resp = client.request(method, url, timeout=60.0, **kwargs)
        except httpx.TransportError as exc:
            last_exc = exc
            time.sleep(min(2 ** attempt, 30))
            continue
        last = resp
        if resp.status_code in _RETRYABLE_STATUS:
            time.sleep(min(2 ** attempt, 30))
            continue
        resp.raise_for_status()
        return resp
    if last is not None:
        last.raise_for_status()
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"request failed for {url}")


def _get_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    merged = {"User-Agent": "market-memory/0.1"}
    if headers:
        merged.update(headers)
    return _request_with_retry(client, "GET", url, params=params, headers=merged).json()


def _post_json(client: httpx.Client, url: str, payload: dict[str, Any]) -> Any:
    return _request_with_retry(client, "POST", url, json=payload).json()


def fetch_okx_funding_history(client: httpx.Client, asset: str, *, since_ms: int) -> list[dict[str, Any]]:
    inst = ASSET_OKX[asset]["swap"]
    rows: list[dict[str, Any]] = []
    after: int | None = None
    for _ in range(500):
        params: dict[str, Any] = {"instId": inst, "limit": 100}
        if after is not None:
            params["after"] = str(after)
        body = _get_json(client, f"{OKX_BASE}/public/funding-rate-history", params=params)
        if body.get("code") != "0":
            raise RuntimeError(f"OKX funding error: {body}")
        batch = body.get("data") or []
        if not batch:
            break
        oldest = min(int(row["fundingTime"]) for row in batch)
        for row in batch:
            ts = int(row["fundingTime"])
            if ts >= since_ms:
                rows.append({"time": ts, "rate": float(row["fundingRate"])})
        if oldest <= since_ms:
            break
        if oldest == after:
            break
        after = oldest
        if len(batch) < 100:
            break
        time.sleep(0.2)
    return sorted(rows, key=lambda r: r["time"])


def fetch_hl_funding_history(client: httpx.Client, asset: str, *, since_ms: int) -> list[dict[str, Any]]:
    coin = ASSET_HL[asset]
    rows: list[dict[str, Any]] = []
    start = since_ms
    for _ in range(300):
        payload = {"type": "fundingHistory", "coin": coin, "startTime": start}
        batch = _post_json(client, HL_INFO, payload)
        if not batch:
            break
        if not isinstance(batch, list):
            raise RuntimeError(f"Hyperliquid fundingHistory error: {batch}")
        for row in batch:
            ts = int(row["time"])
            rows.append(
                {
                    "time": ts,
                    "rate": float(row["fundingRate"]),
                    "premium": float(row.get("premium") or 0.0),
                }
            )
        if len(batch) < 500:
            break
        start = int(batch[-1]["time"]) + 1
        time.sleep(0.35)
    dedup = {r["time"]: r for r in rows if r["time"] >= since_ms}
    return sorted(dedup.values(), key=lambda r: r["time"])


def _paginate_okx_candles(
    client: httpx.Client,
    endpoint: str,
    inst_id: str,
    *,
    since_ms: int,
    bar: str = "1D",
) -> list[list[str]]:
    rows: list[list[str]] = []
    after: str | None = None
    for _ in range(300):
        params: dict[str, Any] = {"instId": inst_id, "bar": bar, "limit": 100}
        if after:
            params["after"] = after
        body = _get_json(client, f"{OKX_BASE}/market/{endpoint}", params=params)
        if body.get("code") != "0":
            raise RuntimeError(f"OKX candles error: {body}")
        batch = body.get("data") or []
        if not batch:
            break
        stop = False
        for candle in batch:
            ts = int(candle[0])
            if ts < since_ms:
                stop = True
                break
            rows.append(candle)
        if stop or len(batch) < 100:
            break
        after = batch[-1][0]
        time.sleep(0.2)
    return sorted(rows, key=lambda c: int(c[0]))


def fetch_okx_daily_basis(client: httpx.Client, asset: str, *, since_ms: int) -> list[dict[str, Any]]:
    swap = ASSET_OKX[asset]["swap"]
    index = ASSET_OKX[asset]["index"]
    marks = {int(c[0]): float(c[4]) for c in _paginate_okx_candles(client, "history-mark-price-candles", swap, since_ms=since_ms)}
    indices = {int(c[0]): float(c[4]) for c in _paginate_okx_candles(client, "history-index-candles", index, since_ms=since_ms)}
    rows: list[dict[str, Any]] = []
    for ts, mark in marks.items():
        idx = indices.get(ts)
        if not idx or idx == 0:
            continue
        rows.append({"time": ts, "basis_bps": (mark - idx) / idx * 10000})
    return rows


def fetch_hl_daily_basis(client: httpx.Client, asset: str, *, since_ms: int) -> list[dict[str, Any]]:
    """Daily perp premium (basis proxy) from Hyperliquid funding history."""
    funding = fetch_hl_funding_history(client, asset, since_ms=since_ms)
    by_day: dict[str, list[float]] = {}
    for row in funding:
        day = datetime.fromtimestamp(row["time"] / 1000, tz=timezone.utc).date().isoformat()
        by_day.setdefault(day, []).append(float(row.get("premium") or 0.0) * 10000)
    rows: list[dict[str, Any]] = []
    for day, premiums in sorted(by_day.items()):
        ts = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        rows.append({"time": ts, "basis_bps": max(premiums, key=abs)})
    return rows


def fetch_coinglass_hourly_liquidations(
    client: httpx.Client,
    asset: str,
    *,
    api_key: str,
    exchange_scope: str,
    since_ms: int,
) -> list[dict[str, Any]]:
    symbol = asset
    pair = ASSET_COINGLASS_PAIR[asset]
    headers = {"CG-API-KEY": api_key, "accept": "application/json"}
    rows: list[dict[str, Any]] = []
    end_ms = _ms(datetime.now(timezone.utc))
    cursor_end = end_ms
    for _ in range(50):
        params = {
            "symbol": symbol,
            "interval": "1h",
            "limit": 1000,
            "end_time": cursor_end,
        }
        if exchange_scope == "okx":
            url = f"{COINGLASS_BASE}/api/futures/liquidation/history"
            params["exchange"] = "OKX"
            params["symbol"] = pair
        else:
            url = f"{COINGLASS_BASE}/api/futures/liquidation/aggregated-history"
            params["exchange_list"] = "Binance,OKX,Bybit"
        body = _get_json(client, url, params=params, headers=headers)
        if not isinstance(body, dict) or str(body.get("code")) != "0":
            raise RuntimeError(f"Coinglass error ({exchange_scope}): {body}")
        batch = body.get("data") or []
        if not batch:
            break
        oldest = None
        for row in batch:
            ts = int(row["time"])
            oldest = ts if oldest is None else min(oldest, ts)
            if ts < since_ms:
                continue
            if exchange_scope == "okx":
                total = float(row.get("longLiquidationUsd") or row.get("long_liquidation_usd") or 0) + float(
                    row.get("shortLiquidationUsd") or row.get("short_liquidation_usd") or 0
                )
            else:
                total = float(row.get("aggregated_long_liquidation_usd") or 0) + float(
                    row.get("aggregated_short_liquidation_usd") or 0
                )
            rows.append({"time": ts, "total_usd": total})
        if oldest is None or oldest <= since_ms or len(batch) < 1000:
            break
        cursor_end = oldest - 1
        time.sleep(0.25)
    dedup = {r["time"]: r for r in rows}
    return sorted(dedup.values(), key=lambda r: r["time"])


def fetch_yahoo_daily_history(
    client: httpx.Client,
    symbol: str,
    *,
    since: datetime,
) -> list[tuple[str, float]]:
    """Daily (date, close) rows from Yahoo Finance chart API."""
    period1 = int(since.timestamp())
    period2 = int(datetime.now(timezone.utc).timestamp())
    body = _get_json(
        client,
        f"{YAHOO_CHART_BASE}/{symbol}",
        params={"period1": period1, "period2": period2, "interval": "1d"},
        headers={"User-Agent": "market-memory/0.1"},
    )
    result = (body.get("chart") or {}).get("result") or []
    if not result:
        return []
    timestamps = result[0].get("timestamp") or []
    closes = (result[0].get("indicators") or {}).get("quote", [{}])[0].get("close") or []
    rows: list[tuple[str, float]] = []
    for ts, close in zip(timestamps, closes):
        if ts is None or close is None:
            continue
        day = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
        rows.append((day, float(close)))
    return rows


def fetch_fred_series(
    client: httpx.Client,
    api_key: str,
    series_id: str,
    *,
    since: str = "2021-01-01",
) -> list[tuple[str, float]]:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": since,
        "sort_order": "asc",
    }
    body = _get_json(client, f"{FRED_BASE}/series/observations", params=params)
    rows: list[tuple[str, float]] = []
    for item in body.get("observations") or []:
        raw = item.get("value")
        if raw in (None, ".", ""):
            continue
        rows.append((item["date"], float(raw)))
    return rows


def fetch_fred_cpi_yoy_series(
    client: httpx.Client,
    api_key: str,
    series_id: str,
    *,
    since: str = "2021-01-01",
) -> list[tuple[str, float]]:
    raw = fetch_fred_series(client, api_key, series_id, since=since)
    out: list[tuple[str, float]] = []
    for i, (day, val) in enumerate(raw):
        if i < 12:
            continue
        prev = raw[i - 12][1]
        if prev == 0:
            continue
        yoy = ((val / prev) - 1) * 100
        out.append((day, yoy))
    return out


def fetch_fear_greed_history(
    client: httpx.Client,
    *,
    since: datetime,
) -> list[tuple[str, float]]:
    since_ts = int(since.timestamp())
    body = _get_json(client, f"{FEAR_GREED_BASE}/", params={"limit": 0})
    rows: list[tuple[str, float]] = []
    for entry in body.get("data") or []:
        ts = entry.get("timestamp")
        if not ts:
            continue
        if int(ts) < since_ts:
            continue
        day = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
        rows.append((day, float(entry["value"])))
    return sorted(rows, key=lambda r: r[0])


def fetch_kraken_price(client: httpx.Client, pair: str) -> float:
    body = _get_json(client, KRAKEN_BASE, params={"pair": pair})
    result = body.get("result") or {}
    if not result:
        raise RuntimeError(f"No Kraken data for {pair}")
    ticker = next(iter(result.values()))
    return float(ticker["c"][0])


def fetch_coinbase_price(client: httpx.Client, product: str) -> float:
    body = _get_json(client, f"{COINBASE_BASE}/{product}/spot")
    data = body.get("data") or {}
    if "amount" not in data:
        raise RuntimeError(f"No Coinbase price for {product}")
    return float(data["amount"])


def fetch_exchange_spread_bps(client: httpx.Client, asset: str) -> float:
    cfg = EXCHANGE_SPREAD_ASSETS[asset]
    kraken_px = fetch_kraken_price(client, cfg["kraken"])
    coinbase_px = fetch_coinbase_price(client, cfg["coinbase"])
    mid = (kraken_px + coinbase_px) / 2
    if mid == 0:
        raise RuntimeError("exchange spread mid is zero")
    return abs(kraken_px - coinbase_px) / mid * 10000


def fetch_fred_fed_funds_changes(client: httpx.Client, api_key: str, *, since: str = "2021-01-01") -> list[dict[str, Any]]:
    params = {
        "series_id": "DFF",
        "api_key": api_key,
        "file_type": "json",
        "observation_start": since,
        "sort_order": "asc",
    }
    body = _get_json(client, f"{FRED_BASE}/series/observations", params=params)
    obs = body.get("observations") or []
    rows: list[dict[str, Any]] = []
    prev: float | None = None
    for item in obs:
        raw = item.get("value")
        if raw in (None, ".", ""):
            continue
        val = float(raw)
        if prev is not None and abs(val - prev) > 1e-9:
            rows.append(
                {
                    "date": item["date"],
                    "value": val,
                    "prev": prev,
                    "change_bps": (val - prev) * 100,
                }
            )
        prev = val
    return rows


def _load_env_key(name: str) -> str | None:
    key = os.environ.get(name, "").strip()
    if key:
        return key
    for path in (
        os.environ.get("TWITTER_BOT_ENV"),
        os.path.expanduser("~/projects/twitter-bot/.env"),
        os.path.join(os.getcwd(), ".env"),
    ):
        if not path or not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if line.startswith("export "):
                    line = line[len("export ") :]
                if not line.startswith(f"{name}="):
                    continue
                val = line.split("=", 1)[1].strip()
                if " #" in val:
                    val = val.split(" #", 1)[0].rstrip()
                val = val.strip().strip('"').strip("'")
                if val:
                    return val
    return None


def load_coinalyze_api_key() -> str | None:
    return _load_env_key("COINALYZE_API_KEY")


_coinalyze_markets_cache: list[dict[str, Any]] | None = None


def _coinalyze_headers(api_key: str) -> dict[str, str]:
    return {"api_key": api_key}


def fetch_coinalyze_future_markets(client: httpx.Client, api_key: str) -> list[dict[str, Any]]:
    global _coinalyze_markets_cache
    if _coinalyze_markets_cache is not None:
        return _coinalyze_markets_cache
    body = _get_json(client, f"{COINALYZE_BASE}/future-markets", headers=_coinalyze_headers(api_key))
    if not isinstance(body, list):
        raise RuntimeError(f"Coinalyze future-markets error: {body}")
    _coinalyze_markets_cache = body
    return body


def resolve_coinalyze_okx_symbol(client: httpx.Client, api_key: str, asset: str) -> str | None:
    """Find Coinalyze perpetual symbol for asset on OKX (exchange code O)."""
    markets = fetch_coinalyze_future_markets(client, api_key)
    candidates = [
        m
        for m in markets
        if m.get("base_asset") == asset
        and m.get("is_perpetual")
        and m.get("exchange") == "O"
        and m.get("quote_asset") in ("USDT", "USD")
    ]
    if not candidates:
        return None
    usdt = [m for m in candidates if m.get("quote_asset") == "USDT"]
    pick = usdt[0] if usdt else candidates[0]
    return str(pick["symbol"])


def fetch_coinalyze_liquidation_hourly_buckets(
    client: httpx.Client,
    asset: str,
    *,
    api_key: str,
    since_sec: int,
    to_sec: int,
) -> list[dict[str, Any]] | None:
    """Hourly OKX liquidation buckets from Coinalyze (USD). Returns None if symbol missing."""
    symbol = resolve_coinalyze_okx_symbol(client, api_key, asset)
    if not symbol:
        return None
    rows: list[dict[str, Any]] = []
    chunk_seconds = 7 * 24 * 3600
    cursor = since_sec
    while cursor < to_sec:
        chunk_to = min(to_sec, cursor + chunk_seconds)
        body = _get_json(
            client,
            f"{COINALYZE_BASE}/liquidation-history",
            params={
                "symbols": symbol,
                "interval": "1hour",
                "from": cursor,
                "to": chunk_to,
                "convert_to_usd": "true",
            },
            headers=_coinalyze_headers(api_key),
        )
        if not isinstance(body, list) or not body:
            cursor = chunk_to + 1
            time.sleep(1.5)
            continue
        history = body[0].get("history") or []
        for point in history:
            ts = int(point["t"])
            long_usd = float(point.get("l") or 0)
            short_usd = float(point.get("s") or 0)
            rows.append(
                {
                    "time": ts * 1000,
                    "total_usd": long_usd + short_usd,
                    "long_usd": long_usd,
                    "short_usd": short_usd,
                    "symbol": symbol,
                }
            )
        cursor = chunk_to + 1
        time.sleep(1.5)
    dedup = {r["time"]: r for r in rows if r["time"] >= since_sec * 1000}
    return sorted(dedup.values(), key=lambda r: r["time"])


def load_coinglass_api_key() -> str | None:
    return _load_env_key("COINGLASS_API_KEY")


def _hour_start_ms(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    return _ms(dt)


def _liq_fill_usd(size: float, price: float) -> float:
    return float(size) * float(price)


def fetch_okx_liquidation_hourly_buckets(client: httpx.Client, asset: str) -> list[dict[str, Any]]:
    """Aggregate recent OKX liquidation fills into UTC hourly buckets."""
    uly = ASSET_OKX[asset]["uly"]
    body = _get_json(
        client,
        f"{OKX_BASE}/public/liquidation-orders",
        params={"instType": "SWAP", "uly": uly, "state": "filled", "limit": 100},
    )
    if body.get("code") != "0":
        raise RuntimeError(f"OKX liquidations error: {body}")
    buckets: dict[int, dict[str, float]] = {}
    for block in body.get("data", []):
        for detail in block.get("details", []):
            ts = int(detail.get("time") or detail.get("ts") or 0)
            if not ts:
                continue
            hour = _hour_start_ms(ts)
            usd = _liq_fill_usd(detail["sz"], detail["bkPx"])
            bucket = buckets.setdefault(hour, {"total_usd": 0.0, "long_usd": 0.0, "short_usd": 0.0})
            bucket["total_usd"] += usd
            pos = (detail.get("posSide") or "").lower()
            if pos == "long" or (detail.get("side") or "").lower() == "sell":
                bucket["long_usd"] += usd
            elif pos == "short" or (detail.get("side") or "").lower() == "buy":
                bucket["short_usd"] += usd
            else:
                bucket["long_usd"] += usd
    return [{"time": hour, **vals} for hour, vals in sorted(buckets.items())]


def fetch_binance_liquidation_hourly_buckets(
    client: httpx.Client,
    asset: str,
    *,
    lookback_hours: int = 24,
) -> list[dict[str, Any]] | None:
    """Optional Binance liquidation buckets; returns None when geo-blocked or unavailable."""
    symbol = ASSET_BINANCE[asset]
    try:
        body = _get_json(
            client,
            f"{BINANCE_FAPI}/fapi/v1/allForceOrders",
            params={"symbol": symbol, "limit": 100},
        )
    except httpx.HTTPError:
        return None
    if isinstance(body, dict) and body.get("code") not in (None, 0):
        return None
    if not isinstance(body, list):
        return None
    cutoff = _ms(datetime.now(timezone.utc)) - lookback_hours * 3600 * 1000
    buckets: dict[int, dict[str, float]] = {}
    for row in body:
        ts = int(row.get("time") or 0)
        if ts < cutoff:
            continue
        hour = _hour_start_ms(ts)
        px = float(row.get("price") or row.get("avgPrice") or 0)
        qty = float(row.get("origQty") or row.get("executedQty") or 0)
        usd = px * qty
        bucket = buckets.setdefault(hour, {"total_usd": 0.0, "long_usd": 0.0, "short_usd": 0.0})
        bucket["total_usd"] += usd
        side = (row.get("side") or "").upper()
        if side == "SELL":
            bucket["long_usd"] += usd
        else:
            bucket["short_usd"] += usd
    return [{"time": hour, **vals} for hour, vals in sorted(buckets.items())]


def _parse_finra_regsho_day(text: str, symbol: str) -> tuple[float, float] | None:
    """Return (total_shares, short_shares) for symbol from a FINRA Reg SHO daily file."""
    total = 0.0
    short = 0.0
    found = False
    for line in text.splitlines():
        if not line or line.startswith("Date|"):
            continue
        parts = line.split("|")
        if len(parts) < 5 or parts[1] != symbol:
            continue
        short += float(parts[2])
        total += float(parts[4])
        found = True
    if not found or total <= 0:
        return None
    return total, short


def fetch_finra_dark_pool_history(
    client: httpx.Client,
    *,
    since: datetime,
    symbol: str = "SPY",
) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
    """Daily SPY off-exchange proxy from FINRA Reg SHO files.

    Returns:
        volume_rows: (date, total share volume in millions)
        pct_rows: (date, short volume % of total — DPI/DPL-style sentiment proxy)
    """
    start = since.date()
    end = datetime.now(timezone.utc).date()
    volume_rows: list[tuple[str, float]] = []
    pct_rows: list[tuple[str, float]] = []
    day = start
    while day <= end:
        if day.weekday() < 5:
            ymd = day.strftime("%Y%m%d")
            try:
                resp = _request_with_retry(
                    client,
                    "GET",
                    f"{FINRA_REGSHO_CDN}/CNMSshvol{ymd}.txt",
                )
                parsed = _parse_finra_regsho_day(resp.text, symbol)
            except httpx.HTTPError:
                parsed = None
            if parsed:
                total, short = parsed
                iso = day.isoformat()
                volume_rows.append((iso, total / 1_000_000))
                pct_rows.append((iso, short / total * 100))
            time.sleep(0.05)
        day += timedelta(days=1)
    return volume_rows, pct_rows


def load_fred_api_key() -> str | None:
    return _load_env_key("FRED_API_KEY")