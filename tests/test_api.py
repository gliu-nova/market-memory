from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from market_memory.app import create_app
from market_memory.config import AppConfig, ServiceConfig
from market_memory.db import EventDB


def _client(tmp_path) -> TestClient:
    cfg = AppConfig(service=ServiceConfig(data_dir=str(tmp_path)))
    sample = Path(__file__).resolve().parent / "fixtures" / "sample_events.json"
    db = EventDB(data_dir=tmp_path)
    db.ingest_file(sample)
    db.close()
    return TestClient(create_app(cfg))


def test_health_and_tweet_context(tmp_path):
    client = _client(tmp_path)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["total_events"] == 17

    resp = client.get(
        "/tweet-context",
        params={
            "event_type": "market_surge",
            "asset": "BTC",
            "indicator_type": "liquidations",
            "direction": "spike",
            "since": "2021-01-01",
            "current_value": 461800000,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["occurrences"] == 12
    assert "tweet_context" in body