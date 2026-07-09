from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from market_memory.sources import (
    _load_env_key,
    _request_with_retry,
    fetch_hl_funding_history,
)


def test_load_env_key_from_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text('export FRED_API_KEY="abc123" # comment\n', encoding="utf-8")
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setenv("TWITTER_BOT_ENV", str(env_file))
    assert _load_env_key("FRED_API_KEY") == "abc123"


def test_request_with_retry_retries_503(monkeypatch):
    client = MagicMock()
    fail = MagicMock(status_code=503)
    fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        "503", request=MagicMock(), response=fail
    )
    ok = MagicMock(status_code=200)
    ok.raise_for_status.return_value = None
    client.request.side_effect = [fail, ok]
    sleeps: list[float] = []
    monkeypatch.setattr("market_memory.sources.time.sleep", lambda s: sleeps.append(s))
    resp = _request_with_retry(client, "GET", "https://example.com")
    assert resp is ok
    assert sleeps


def test_request_with_retry_retries_transport_error(monkeypatch):
    client = MagicMock()
    ok = MagicMock(status_code=200)
    ok.raise_for_status.return_value = None
    client.request.side_effect = [httpx.ConnectError("boom"), ok]
    monkeypatch.setattr("market_memory.sources.time.sleep", lambda _s: None)
    resp = _request_with_retry(client, "GET", "https://example.com")
    assert resp is ok


def test_fetch_hl_funding_history_rejects_non_list(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(
        "market_memory.sources._post_json",
        lambda *_a, **_k: {"error": "bad request"},
    )
    with pytest.raises(RuntimeError, match="Hyperliquid fundingHistory"):
        fetch_hl_funding_history(client, "BTC", since_ms=0)
