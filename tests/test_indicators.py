from __future__ import annotations

from market_memory.indicators import INDICATOR_BY_KEY, memory_query_for_indicator


def test_all_user_indicators_mapped():
    expected = {
        "sp500", "nasdaq100", "vix", "dxy", "gold", "silver", "move",
        "btc", "eth", "sol",
        "oil", "hy_spread", "fed_funds", "treasury_10y", "yield_curve",
        "jobless_claims", "unemployment", "cpi_yoy", "m2", "mortgage_30y",
        "consumer_sentiment", "case_shiller", "pmi_manufacturing", "ism_services",
        "fear_greed",
        "btc_funding", "eth_funding", "sol_funding",
        "btc_basis", "eth_basis", "sol_basis",
        "btc_exchange_spread", "eth_exchange_spread", "sol_exchange_spread",
        "btc_liquidations", "eth_liquidations", "sol_liquidations",
    }
    for key in expected:
        assert memory_query_for_indicator(key) is not None, key


def test_series_catalog_count():
    assert len(INDICATOR_BY_KEY) >= 24