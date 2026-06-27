from __future__ import annotations

from market_memory.indicators import INDICATOR_BY_KEY, memory_query_for_indicator


def test_all_user_indicators_mapped():
    expected = {
        "sp500", "nasdaq100", "qqq", "vix", "dxy", "gold", "silver", "move",
        "bond_etf_agg", "bond_etf_bnd", "crypto_etf_ibit", "crypto_etf_fbtc",
        "btc", "eth", "sol",
        "oil", "hy_spread", "fed_funds", "treasury_10y", "treasury_2y", "yield_curve",
        "jobless_claims", "unemployment", "cpi_yoy", "m2", "mortgage_30y",
        "consumer_sentiment", "case_shiller", "pmi_manufacturing", "ism_services",
        "fear_greed", "dark_pool_volume", "dark_pool_pct",
        "btc_funding", "eth_funding", "sol_funding",
        "btc_basis", "eth_basis", "sol_basis",
        "btc_exchange_spread", "eth_exchange_spread", "sol_exchange_spread",
        "btc_liquidations", "eth_liquidations", "sol_liquidations",
    }
    for key in expected:
        assert memory_query_for_indicator(key) is not None, key


def test_series_catalog_count():
    assert len(INDICATOR_BY_KEY) >= 32


def test_new_indicator_sources():
    assert INDICATOR_BY_KEY["treasury_2y"].series == "DGS2"
    assert INDICATOR_BY_KEY["qqq"].symbol == "QQQ"
    assert INDICATOR_BY_KEY["bond_etf_agg"].symbol == "AGG"
    assert INDICATOR_BY_KEY["bond_etf_bnd"].symbol == "BND"
    assert INDICATOR_BY_KEY["crypto_etf_ibit"].symbol == "IBIT"
    assert INDICATOR_BY_KEY["crypto_etf_fbtc"].symbol == "FBTC"
    assert INDICATOR_BY_KEY["dark_pool_volume"].source == "finra_dark_pool_volume"
    assert INDICATOR_BY_KEY["dark_pool_pct"].source == "finra_dark_pool_pct"