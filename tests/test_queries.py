from __future__ import annotations

from datetime import datetime

from market_memory.models import SimilarityQuery


def _btc_liquidation_query(since: str = "2021-01-01") -> SimilarityQuery:
    return SimilarityQuery(
        event_type="market_surge",
        asset="BTC",
        indicator_type="liquidations",
        direction="spike",
        since=datetime.fromisoformat(since),
    )


def test_count_similar_events(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    query = _btc_liquidation_query()
    assert temp_db.count_similar(query) == 12


def test_latest_similar(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    latest = temp_db.latest_similar(_btc_liquidation_query())
    assert latest is not None
    assert latest.id == "btc-liq-2026-02-05"


def test_min_value_filter(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    query = SimilarityQuery(
        event_type="market_surge",
        asset="BTC",
        indicator_type="liquidations",
        direction="spike",
        since=datetime.fromisoformat("2021-01-01"),
        min_value=400_000_000,
    )
    assert temp_db.count_similar(query) == 11


def test_percentile(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    pct = temp_db.percentile(720_000_000, _btc_liquidation_query())
    assert pct is not None
    assert pct > 80


def test_tweet_context(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    ctx = temp_db.tweet_context(
        _btc_liquidation_query(),
        current_value=461_800_000,
    )
    assert ctx.occurrences == 12
    assert ctx.percentile is not None
    assert len(ctx.tweet_context) <= 80
    assert (
        "percentile historically" in ctx.tweet_context
        or "similar BTC liquidation spikes" in ctx.tweet_context
    )


def test_stats(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    summary = temp_db.stats()
    assert summary.total_events == 17
    assert "market_surge" in summary.by_event_type
    assert summary.yearly_counts.get("2024", 0) >= 3


def test_prune_before(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    deleted = temp_db.prune_before(datetime.fromisoformat("2025-01-01"))
    assert deleted > 0
    assert temp_db.count_all() < 17


def test_tag_overlap(temp_db, sample_path):
    temp_db.ingest_file(sample_path)
    query = SimilarityQuery(
        event_type="market_surge",
        asset="BTC",
        indicator_type="liquidations",
        tags=["bearish"],
        since=datetime.fromisoformat("2021-01-01"),
    )
    assert temp_db.count_similar(query) == 3