from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from market_memory.sync import _liquidation_warnings, _save_state, _load_state


def test_liquidation_warnings_skip_coinglass_mode():
    assert _liquidation_warnings({"mode": "coinglass_dual"}) == []


def test_liquidation_warnings_for_okx_path():
    warnings = _liquidation_warnings({"mode": "okx", "coinalyze": None, "binance_available": False})
    assert any("COINALYZE_API_KEY" in w for w in warnings)
    assert any("Binance" in w for w in warnings)


def test_liquidation_warnings_quiet_when_coinalyze_present():
    warnings = _liquidation_warnings({"mode": "okx", "coinalyze": True, "binance_available": True})
    assert warnings == []


def test_save_state_atomic(tmp_path: Path):
    state = {"last_sync_at": datetime.now(timezone.utc).isoformat(), "n": 1}
    _save_state(tmp_path, state)
    loaded = _load_state(tmp_path)
    assert loaded["n"] == 1
    assert Path(tmp_path / "sync_state.json").exists()
    # No leftover temp files
    leftovers = list(tmp_path.glob(".sync_state_*"))
    assert leftovers == []
    # Valid JSON
    json.loads((tmp_path / "sync_state.json").read_text(encoding="utf-8"))
