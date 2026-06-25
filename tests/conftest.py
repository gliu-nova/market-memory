from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from market_memory.db import EventDB


@pytest.fixture
def temp_db() -> EventDB:
    with tempfile.TemporaryDirectory() as tmp:
        db = EventDB(data_dir=tmp)
        yield db
        db.close()


@pytest.fixture
def sample_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "sample_events.json"