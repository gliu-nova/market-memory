from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from market_memory.models import EventCreate


def parse_events_json(payload: str | bytes) -> list[EventCreate]:
    data = json.loads(payload)
    if isinstance(data, dict) and "events" in data:
        rows = data["events"]
    elif isinstance(data, list):
        rows = data
    else:
        raise ValueError("JSON must be a list of events or an object with an 'events' key")

    events: list[EventCreate] = []
    errors: list[str] = []
    for i, row in enumerate(rows):
        try:
            events.append(EventCreate.model_validate(row).with_id())
        except ValidationError as exc:
            errors.append(f"row {i}: {exc}")
    if errors:
        raise ValueError("Invalid events:\n" + "\n".join(errors))
    return events


def parse_events_csv(path: Path) -> list[EventCreate]:
    events: list[EventCreate] = []
    errors: list[str] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for i, row in enumerate(reader):
            cleaned = _csv_row_to_dict(row)
            try:
                events.append(EventCreate.model_validate(cleaned).with_id())
            except ValidationError as exc:
                errors.append(f"row {i}: {exc}")
    if errors:
        raise ValueError("Invalid CSV rows:\n" + "\n".join(errors))
    return events


def _csv_row_to_dict(row: dict[str, str | None]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if value is None or value == "":
            continue
        if key in {"tags", "metadata"}:
            out[key] = json.loads(value) if value.startswith(("{", "[")) else value
        elif key in {"value", "percent_change"}:
            out[key] = float(value)
        elif key == "timestamp":
            out[key] = value
        else:
            out[key] = value
    return out


def load_events_file(path: Path) -> list[EventCreate]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return parse_events_json(path.read_text(encoding="utf-8"))
    if suffix == ".csv":
        return parse_events_csv(path)
    raise ValueError(f"Unsupported file type: {suffix} (use .json or .csv)")