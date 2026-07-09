from __future__ import annotations

from datetime import datetime, timezone

import pytest

from market_memory.backfill import _liq_buckets_agree, backfill_database, detect_funding_events
from market_memory.models import EventCreate


def test_liq_buckets_agree_threshold():
    assert _liq_buckets_agree(100.0, None) is True
    assert _liq_buckets_agree(100.0, 50.0) is True
    assert _liq_buckets_agree(100.0, 49.0) is False
    assert _liq_buckets_agree(100.0, 15.0) is False


def test_detect_funding_events_computes_prev_once():
    # Build enough history for percentile gates; inject one extreme high rate.
    base = 0.0001
    okx = [{"time": i * 8 * 3600 * 1000, "rate": base} for i in range(40)]
    hl = [{"time": r["time"], "rate": base} for r in okx]
    okx[-1]["rate"] = 0.01
    hl[-1]["rate"] = 0.01
    events = detect_funding_events("BTC", okx, hl)
    assert events
    assert events[0].direction == "extreme"
    assert "prev_avg" in events[0].metadata


def test_backfill_refuses_wipe_on_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "market_memory.backfill.collect_real_events",
        lambda **_kwargs: ([], {"sources": {}}),
    )
    with pytest.raises(RuntimeError, match="refusing wipe"):
        backfill_database(data_dir=str(tmp_path), wipe=True)


def test_backfill_append_default(monkeypatch, tmp_path):
    from market_memory.db import EventDB

    with EventDB(data_dir=tmp_path) as db:
        db.ingest_events(
            [
                EventCreate(
                    id="existing",
                    timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                    event_type="market_surge",
                    value=1.0,
                )
            ]
        )

    monkeypatch.setattr(
        "market_memory.backfill.collect_real_events",
        lambda **_kwargs: (
            [
                EventCreate(
                    id="new-event",
                    timestamp=datetime(2024, 2, 1, tzinfo=timezone.utc),
                    event_type="market_surge",
                    value=2.0,
                )
            ],
            {"sources": {}},
        ),
    )
    report = backfill_database(data_dir=str(tmp_path), wipe=False)
    assert report["wiped"] is False
    assert report["ingested"] == 1
    with EventDB(data_dir=tmp_path) as db:
        assert db.count_all() == 2
