from __future__ import annotations

from market_memory.sources import _parse_finra_regsho_day


SAMPLE_FINRA = """Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260624|SPY|7516535.579378|9466|15099322.724728|B,Q,N
20260624|AAPL|100|0|200|Q
"""


def test_parse_finra_regsho_day_sums_symbol_rows():
    parsed = _parse_finra_regsho_day(SAMPLE_FINRA, "SPY")
    assert parsed is not None
    total, short = parsed
    assert total == 15099322.724728
    assert short == 7516535.579378


def test_parse_finra_regsho_day_missing_symbol():
    assert _parse_finra_regsho_day(SAMPLE_FINRA, "QQQ") is None