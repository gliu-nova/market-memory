"""Indicator catalog aligned with twitter-bot config.yaml thresholds."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CrossRule:
    rule_type: str  # crosses_above | crosses_below
    value: float


@dataclass(frozen=True)
class IndicatorSpec:
    key: str
    name: str
    source: str
    tags: tuple[str, ...]
    alert_unit: str = "percent"
    normal_alert: float = 2.0
    event_type: str = "market_surge"
    symbol: str | None = None
    series: str | None = None
    asset: str | None = None
    cross_rules: tuple[CrossRule, ...] = ()
    detect_moves: bool = True
    verify_series: str | None = None
    verify_tolerance_pct: float = 1.5


YAHOO_INDICATORS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec("sp500", "S&P 500", "yahoo", ("equities",), normal_alert=2.0, symbol="^GSPC", verify_series="SP500"),
    IndicatorSpec("nasdaq100", "NASDAQ 100", "yahoo", ("equities",), normal_alert=3.0, symbol="^NDX"),
    IndicatorSpec("qqq", "QQQ ETF", "yahoo", ("equities", "etf"), normal_alert=2.5, symbol="QQQ"),
    IndicatorSpec("bond_etf_agg", "iShares Aggregate Bond ETF", "yahoo", ("equities", "etf", "bonds"), normal_alert=0.5, symbol="AGG"),
    IndicatorSpec("bond_etf_bnd", "Vanguard Total Bond ETF", "yahoo", ("equities", "etf", "bonds"), normal_alert=0.5, symbol="BND"),
    IndicatorSpec("crypto_etf_ibit", "iShares Bitcoin ETF", "yahoo", ("crypto", "etf"), normal_alert=5.0, symbol="IBIT"),
    IndicatorSpec("crypto_etf_fbtc", "Fidelity Bitcoin ETF", "yahoo", ("crypto", "etf"), normal_alert=5.0, symbol="FBTC"),
    IndicatorSpec("vix", "VIX", "yahoo", ("equities", "volatility"), normal_alert=15.0, symbol="^VIX", cross_rules=(
        CrossRule("crosses_above", 30.0),
        CrossRule("crosses_below", 15.0),
    )),
    IndicatorSpec("dxy", "US Dollar Index", "yahoo", ("macro", "fx"), normal_alert=1.5, symbol="DX-Y.NYB"),
    IndicatorSpec("gold", "Gold", "yahoo", ("commodities",), normal_alert=2.0, symbol="GC=F"),
    IndicatorSpec("silver", "Silver", "yahoo", ("commodities",), normal_alert=4.0, symbol="SI=F"),
    IndicatorSpec("move", "MOVE Index", "yahoo", ("macro", "volatility"), normal_alert=8.0, symbol="^MOVE", cross_rules=(
        CrossRule("crosses_above", 120.0),
    )),
    IndicatorSpec("btc", "Bitcoin", "yahoo", ("crypto",), normal_alert=5.0, symbol="BTC-USD", asset="BTC"),
    IndicatorSpec("eth", "Ethereum", "yahoo", ("crypto",), normal_alert=6.0, symbol="ETH-USD", asset="ETH"),
    IndicatorSpec("sol", "Solana", "yahoo", ("crypto",), normal_alert=8.0, symbol="SOL-USD", asset="SOL"),
)

FRED_INDICATORS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec("oil", "WTI Crude", "fred", ("commodities",), normal_alert=6.0, series="DCOILWTICO"),
    IndicatorSpec("hy_spread", "HY Credit Spread", "fred", ("macro", "credit"), alert_unit="absolute", normal_alert=0.20, series="BAMLH0A0HYM2", cross_rules=(
        CrossRule("crosses_above", 5.0),
    )),
    IndicatorSpec("fed_funds", "Fed Funds Rate", "fred", ("macro",), alert_unit="absolute", normal_alert=0.25, series="DFF", event_type="fed_announcement", detect_moves=False),
    IndicatorSpec("treasury_10y", "10Y Treasury", "fred", ("macro", "rates"), alert_unit="absolute", normal_alert=0.10, series="DGS10", cross_rules=(
        CrossRule("crosses_above", 4.5),
        CrossRule("crosses_above", 5.0),
        CrossRule("crosses_below", 4.0),
    )),
    IndicatorSpec("treasury_2y", "2Y Treasury", "fred", ("macro", "rates"), alert_unit="absolute", normal_alert=0.08, series="DGS2", cross_rules=(
        CrossRule("crosses_above", 4.0),
        CrossRule("crosses_below", 3.5),
    )),
    IndicatorSpec("yield_curve", "Yield Curve", "fred", ("macro", "rates"), alert_unit="absolute", normal_alert=0.20, series="T10Y2Y", cross_rules=(
        CrossRule("crosses_below", 0.0),
        CrossRule("crosses_above", 0.0),
        CrossRule("crosses_above", 0.50),
    )),
    IndicatorSpec("jobless_claims", "Initial Claims", "fred", ("macro",), normal_alert=8.0, series="ICSA"),
    IndicatorSpec("unemployment", "Unemployment Rate", "fred", ("macro",), alert_unit="absolute", normal_alert=0.10, series="UNRATE"),
    IndicatorSpec("cpi_yoy", "CPI YoY", "fred_cpi_yoy", ("macro", "inflation"), alert_unit="absolute", normal_alert=0.3, series="CPIAUCSL", cross_rules=(
        CrossRule("crosses_above", 3.0),
        CrossRule("crosses_below", 3.0),
    )),
    IndicatorSpec("m2", "M2 Money Supply", "fred", ("macro",), normal_alert=1.0, series="M2SL"),
    IndicatorSpec("mortgage_30y", "30Y Mortgage", "fred", ("macro", "housing"), alert_unit="absolute", normal_alert=0.15, series="MORTGAGE30US"),
    IndicatorSpec("consumer_sentiment", "Consumer Sentiment", "fred", ("macro",), alert_unit="absolute", normal_alert=3.0, series="UMCSENT"),
    IndicatorSpec("case_shiller", "Case-Shiller Home Prices", "fred", ("macro", "housing"), normal_alert=1.5, series="CSUSHPINSA"),
    IndicatorSpec("pmi_manufacturing", "Philly Fed Manufacturing", "fred", ("macro",), alert_unit="absolute", normal_alert=8.0, series="GACDFSA066MSFRBPHI", cross_rules=(
        CrossRule("crosses_below", 0.0),
    )),
    IndicatorSpec("ism_services", "Chicago Fed Nonmfg Activity", "fred", ("macro",), alert_unit="absolute", normal_alert=10.0, series="CFSBCACTIVITYNMFG", cross_rules=(
        CrossRule("crosses_below", 0.0),
    )),
)

SENTIMENT_INDICATORS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec("fear_greed", "Crypto Fear & Greed", "fear_greed", ("crypto", "sentiment"), alert_unit="absolute", detect_moves=False, cross_rules=(
        CrossRule("crosses_below", 25.0),
        CrossRule("crosses_above", 75.0),
    )),
)

FINRA_DARK_POOL_INDICATORS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec(
        "dark_pool_volume",
        "Dark Pool Volume (SPY)",
        "finra_dark_pool_volume",
        ("equities", "dark_pool"),
        normal_alert=12.0,
        symbol="SPY",
    ),
    IndicatorSpec(
        "dark_pool_pct",
        "Dark Pool Volume % (DPI/DPL proxy)",
        "finra_dark_pool_pct",
        ("equities", "dark_pool"),
        alert_unit="absolute",
        normal_alert=2.0,
        symbol="SPY",
        cross_rules=(
            CrossRule("crosses_above", 50.0),
            CrossRule("crosses_below", 40.0),
        ),
    ),
)

CRYPTO_DERIVATIVE_KEYS: tuple[str, ...] = (
    "btc_funding", "eth_funding", "sol_funding",
    "btc_basis", "eth_basis", "sol_basis",
    "btc_exchange_spread", "eth_exchange_spread", "sol_exchange_spread",
    "btc_liquidations", "eth_liquidations", "sol_liquidations",
)

ALL_SERIES_INDICATORS: tuple[IndicatorSpec, ...] = (
    YAHOO_INDICATORS + FRED_INDICATORS + SENTIMENT_INDICATORS + FINRA_DARK_POOL_INDICATORS
)

INDICATOR_BY_KEY: dict[str, IndicatorSpec] = {spec.key: spec for spec in ALL_SERIES_INDICATORS}


def memory_query_for_indicator(key: str) -> dict[str, Any] | None:
    """Similarity-query fields for twitter-bot bridge."""
    if key in CRYPTO_DERIVATIVE_KEYS:
        parts = key.split("_", 1)
        asset = parts[0].upper()
        kind = parts[1]
        if kind == "liquidations":
            return {"event_type": "market_surge", "asset": asset, "indicator_type": "liquidations", "direction": "spike"}
        if kind == "funding":
            return {"event_type": "market_surge", "asset": asset, "indicator_type": "funding"}
        if kind == "basis":
            return {"event_type": "market_surge", "asset": asset, "indicator_type": "basis"}
        if kind == "exchange_spread":
            return {"event_type": "market_surge", "asset": asset, "indicator_type": "exchange_spread"}
    spec = INDICATOR_BY_KEY.get(key)
    if not spec:
        return None
    return {
        "event_type": spec.event_type,
        "asset": spec.asset,
        "indicator_type": spec.key,
    }