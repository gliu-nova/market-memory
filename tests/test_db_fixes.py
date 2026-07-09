from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from market_memory.db import EventDB, _append_clause, _ensure_utc
from market_memory.models import EventCreate, SimilarityQuery


def test_append_clause_empty_where():
    assert _append_clause("", "value IS NOT NULL") == "WHERE value IS NOT NULL"
    assert _append_clause("WHERE event_type = ?", "value IS NOT NULL") == (
        "WHERE event_type = ? AND value IS NOT NULL"
    )


def test_ensure_utc_converts_aware_and_keeps_naive():
    naive = datetime(2024, 1, 1, 12, 0, 0)
    assert _ensure_utc(naive) == naive

    eastern = datetime(2024, 1, 1, 7, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    assert _ensure_utc(eastern) == datetime(2024, 1, 1, 12, 0, 0)


def test_percentile_with_empty_similarity_filters(temp_db):
    """Percentile must remain valid SQL even if filters produce an empty WHERE."""
    temp_db.ingest_events(
        [
            EventCreate(
                id="e1",
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                event_type="market_surge",
                value=10.0,
            ),
            EventCreate(
                id="e2",
                timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
                event_type="market_surge",
                value=20.0,
            ),
        ]
    )
    # Force empty WHERE by calling internals with a query that still has event_type,
    # then also exercise _append_clause path via a patched empty where.
    from market_memory import db as db_mod

    original = db_mod._similarity_filters

    def empty_filters(_query):
        return "", []

    db_mod._similarity_filters = empty_filters
    try:
        pct = temp_db.percentile(15.0, SimilarityQuery(event_type="market_surge"))
        assert pct == 50.0
        analogs = temp_db.top_analogs(15.0, SimilarityQuery(event_type="market_surge"), limit=2)
        assert len(analogs) == 2
    finally:
        db_mod._similarity_filters = original


def test_watermark_and_replace_all_events(temp_db):
    temp_db.ingest_events(
        [
            EventCreate(
                id="btc-old",
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                event_type="market_surge",
                asset="BTC",
                indicator_type="funding",
                value=0.01,
            ),
            EventCreate(
                id="eth-old",
                timestamp=datetime(2024, 6, 1, tzinfo=timezone.utc),
                event_type="market_surge",
                asset="ETH",
                indicator_type="funding",
                value=0.02,
            ),
        ]
    )
    btc_mark = temp_db.watermark(asset="BTC", indicator_type="funding")
    eth_mark = temp_db.watermark(asset="ETH", indicator_type="funding")
    assert btc_mark is not None and eth_mark is not None
    assert eth_mark > btc_mark

    with pytest.raises(ValueError):
        temp_db.replace_all_events([])

    n = temp_db.replace_all_events(
        [
            EventCreate(
                id="only",
                timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                event_type="market_surge",
                asset="BTC",
                indicator_type="funding",
                value=0.03,
            )
        ]
    )
    assert n == 1
    assert temp_db.count_all() == 1


def test_replace_all_events_rolls_back_on_failure(temp_db, monkeypatch):
    temp_db.ingest_events(
        [
            EventCreate(
                id="keep-me",
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                event_type="market_surge",
                value=1.0,
            )
        ]
    )
    assert temp_db.count_all() == 1

    def boom(_rows):
        raise RuntimeError("ingest failed")

    monkeypatch.setattr(temp_db, "_insert_rows", boom)
    with pytest.raises(RuntimeError, match="ingest failed"):
        temp_db.replace_all_events(
            [
                EventCreate(
                    id="new",
                    timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                    event_type="market_surge",
                    value=2.0,
                )
            ]
        )
    assert temp_db.count_all() == 1
    assert temp_db.get_events(event_type="market_surge")[0].id == "keep-me"


def test_eventdb_context_manager(tmp_path):
    with EventDB(data_dir=tmp_path) as db:
        assert db.count_all() == 0
