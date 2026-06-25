from __future__ import annotations

from market_memory.detectors import detect_series_events
from market_memory.indicators import CrossRule, IndicatorSpec


def test_detect_pct_move_events():
    spec = IndicatorSpec("sp500", "S&P 500", "yahoo", ("equities",), normal_alert=2.0, symbol="^GSPC")
    rows = [
        ("2024-01-01", 100.0),
        ("2024-01-02", 100.5),
        ("2024-01-03", 103.5),
        ("2024-01-04", 102.0),
    ]
    events = detect_series_events(spec, rows, source="yahoo")
    assert len(events) == 1
    assert events[0].indicator_type == "sp500"
    assert events[0].direction == "up"
    assert events[0].percent_change == 2.9850746268656714


def test_detect_cross_events():
    spec = IndicatorSpec(
        "vix",
        "VIX",
        "yahoo",
        ("equities",),
        normal_alert=99.0,
        symbol="^VIX",
        cross_rules=(CrossRule("crosses_above", 30.0),),
    )
    rows = [
        ("2024-06-01", 25.0),
        ("2024-06-02", 31.0),
    ]
    events = detect_series_events(spec, rows, source="yahoo")
    assert len(events) == 1
    assert events[0].direction == "above"
    assert "cross" in events[0].metadata["trigger"]


def test_detect_fear_greed_cross():
    spec = IndicatorSpec(
        "fear_greed",
        "Fear & Greed",
        "fear_greed",
        ("crypto",),
        alert_unit="absolute",
        detect_moves=False,
        cross_rules=(CrossRule("crosses_below", 25.0),),
    )
    rows = [
        ("2024-03-01", 30.0),
        ("2024-03-02", 20.0),
    ]
    events = detect_series_events(spec, rows, source="fear_greed")
    assert len(events) == 1
    assert events[0].indicator_type == "fear_greed"