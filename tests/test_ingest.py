from __future__ import annotations

from datetime import datetime

import pytest

from market_memory.ingest import load_events_file, parse_events_json
from market_memory.models import EventCreate


def test_parse_json_events(sample_path):
    events = load_events_file(sample_path)
    assert len(events) >= 10
    assert events[0].asset == "BTC"
    assert events[0].indicator_type == "liquidations"


def test_parse_inline_json():
    payload = """[
        {
            "timestamp": "2024-01-01T00:00:00Z",
            "event_type": "market_surge",
            "asset": "BTC",
            "indicator_type": "liquidations",
            "value": 100000000,
            "direction": "spike"
        }
    ]"""
    events = parse_events_json(payload)
    assert len(events) == 1
    assert events[0].with_id().id


def test_validation_error():
    with pytest.raises(ValueError, match="Invalid events"):
        parse_events_json('[{"event_type": "x"}]')


def test_ingest_assigns_ids(temp_db):
    event = EventCreate(
        timestamp=datetime.fromisoformat("2024-06-01T00:00:00+00:00"),
        event_type="market_surge",
        asset="ETH",
        indicator_type="basis",
        value=0.03,
        direction="positive",
    )
    count = temp_db.ingest_events([event])
    assert count == 1
    stored = temp_db.get_events(asset="ETH")
    assert len(stored) == 1
    assert stored[0].id