from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb

from market_memory.context import build_tweet_context
from market_memory.ingest import load_events_file
from market_memory.models import Event, EventCreate, EventStats, SimilarityQuery, TweetContextResponse

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id VARCHAR PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR NOT NULL,
    asset VARCHAR,
    indicator_type VARCHAR,
    timeframe VARCHAR,
    value DOUBLE,
    percent_change DOUBLE,
    direction VARCHAR,
    source VARCHAR,
    tags JSON,
    metadata JSON
);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_asset_indicator ON events(asset, indicator_type, timestamp);
"""


class EventDB:
    """File-backed DuckDB event store for historical market context.

    Timestamps are stored UTC-naive. Aware datetimes are converted to UTC;
    naive datetimes are treated as already-UTC.
    """

    def __init__(self, data_dir: str | Path = "data", db_name: str = "market_memory.duckdb") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / db_name
        self._lock = threading.RLock()
        self._conn = duckdb.connect(str(self.db_path))
        self._conn.execute(_SCHEMA)

    def __enter__(self) -> EventDB:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ingest_events(self, events: list[EventCreate]) -> int:
        if not events:
            return 0
        rows = [
            (
                event.with_id().id,
                _ensure_utc(event.timestamp),
                event.event_type,
                event.asset,
                event.indicator_type,
                event.timeframe,
                event.value,
                event.percent_change,
                event.direction,
                event.source,
                json.dumps(event.tags),
                json.dumps(event.metadata),
            )
            for event in events
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT OR REPLACE INTO events
                (id, timestamp, event_type, asset, indicator_type, timeframe,
                 value, percent_change, direction, source, tags, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def wipe(self) -> None:
        """Delete all events. Prefer replace_all_events for safe rebuilds."""
        with self._lock:
            self._conn.execute("DELETE FROM events")

    def _insert_rows(self, rows: list[tuple[Any, ...]]) -> None:
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO events
            (id, timestamp, event_type, asset, indicator_type, timeframe,
             value, percent_change, direction, source, tags, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def replace_all_events(self, events: list[EventCreate]) -> int:
        """Atomically replace all rows; rolls back if ingest fails."""
        if not events:
            raise ValueError("refusing to replace: empty event list")
        rows = [
            (
                event.with_id().id,
                _ensure_utc(event.timestamp),
                event.event_type,
                event.asset,
                event.indicator_type,
                event.timeframe,
                event.value,
                event.percent_change,
                event.direction,
                event.source,
                json.dumps(event.tags),
                json.dumps(event.metadata),
            )
            for event in events
        ]
        with self._lock:
            self._conn.execute("BEGIN TRANSACTION")
            try:
                self._conn.execute("DELETE FROM events")
                self._insert_rows(rows)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return len(rows)

    def watermark(
        self,
        *,
        asset: str | None = None,
        indicator_type: str | None = None,
        event_type: str | None = None,
    ) -> datetime | None:
        clauses: list[str] = []
        params: list[Any] = []
        if indicator_type:
            clauses.append("indicator_type = ?")
            params.append(indicator_type)
        if asset:
            clauses.append("asset = ?")
            params.append(asset)
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type)
        if not clauses:
            return None
        with self._lock:
            row = self._conn.execute(
                f"SELECT MAX(timestamp) FROM events WHERE {' AND '.join(clauses)}",
                params,
            ).fetchone()
        if not row or row[0] is None:
            return None
        ts = row[0]
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(str(ts)).replace(tzinfo=timezone.utc)

    def ingest_file(self, path: str | Path) -> int:
        return self.ingest_events(load_events_file(Path(path)))

    def get_events(
        self,
        *,
        event_type: Optional[str] = None,
        asset: Optional[str] = None,
        indicator_type: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Event]:
        where, params = _build_filters(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            since=since,
            until=until,
        )
        sql = f"""
            SELECT * FROM events
            {where}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
            columns = [d[0] for d in self._conn.description]
        return [_row_to_event(dict(zip(columns, row))) for row in rows]

    def find_similar(self, query: SimilarityQuery, *, limit: int = 50) -> list[Event]:
        where, params = _similarity_filters(query)
        sql = f"""
            SELECT * FROM events
            {where}
            ORDER BY timestamp DESC
            LIMIT ?
        """
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
            columns = [d[0] for d in self._conn.description]
        return [_row_to_event(dict(zip(columns, row))) for row in rows]

    def count_similar(self, query: SimilarityQuery) -> int:
        where, params = _similarity_filters(query)
        with self._lock:
            row = self._conn.execute(f"SELECT COUNT(*) FROM events {where}", params).fetchone()
        return int(row[0]) if row else 0

    def latest_similar(self, query: SimilarityQuery) -> Optional[Event]:
        matches = self.find_similar(query, limit=1)
        return matches[0] if matches else None

    def percentile(
        self,
        current_value: float,
        query: SimilarityQuery,
    ) -> Optional[float]:
        where, params = _similarity_filters(query)
        where = _append_clause(where, "value IS NOT NULL")
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE value < ?) AS below
                FROM events
                {where}
                """,
                [current_value, *params],
            ).fetchone()
        if not row or row[0] == 0:
            return None
        total, below = int(row[0]), int(row[1])
        return round(100.0 * below / total, 1)

    def top_analogs(
        self,
        current_value: float,
        query: SimilarityQuery,
        *,
        limit: int = 5,
    ) -> list[Event]:
        where, params = _similarity_filters(query)
        where = _append_clause(where, "value IS NOT NULL")
        sql = f"""
            SELECT * FROM events
            {where}
            ORDER BY ABS(value - ?) ASC, timestamp DESC
            LIMIT ?
        """
        params = [*params, current_value, limit]
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
            columns = [d[0] for d in self._conn.description]
        return [_row_to_event(dict(zip(columns, row))) for row in rows]

    def stats(self) -> EventStats:
        with self._lock:
            total_row = self._conn.execute(
                "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM events"
            ).fetchone()
            total = int(total_row[0]) if total_row else 0
            earliest = total_row[1] if total_row and total_row[1] else None
            latest = total_row[2] if total_row and total_row[2] else None

            by_type = {
                row[0]: int(row[1])
                for row in self._conn.execute(
                    "SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY 2 DESC"
                ).fetchall()
            }
            by_asset = {
                row[0]: int(row[1])
                for row in self._conn.execute(
                    "SELECT asset, COUNT(*) FROM events WHERE asset IS NOT NULL GROUP BY asset ORDER BY 2 DESC"
                ).fetchall()
            }
            monthly = {
                str(row[0]): int(row[1])
                for row in self._conn.execute(
                    "SELECT strftime(timestamp, '%Y-%m'), COUNT(*) FROM events GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            yearly = {
                str(row[0]): int(row[1])
                for row in self._conn.execute(
                    "SELECT strftime(timestamp, '%Y'), COUNT(*) FROM events GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
        return EventStats(
            total_events=total,
            earliest=earliest,
            latest=latest,
            by_event_type=by_type,
            by_asset=by_asset,
            monthly_counts=monthly,
            yearly_counts=yearly,
        )

    def prune_before(self, cutoff: datetime) -> int:
        before = self.count_all()
        with self._lock:
            self._conn.execute("DELETE FROM events WHERE timestamp < ?", [_ensure_utc(cutoff)])
        return before - self.count_all()

    def prune_keep_months(self, months: int) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) FROM events
                WHERE timestamp < (SELECT MAX(timestamp) FROM events) - INTERVAL ? MONTH
                """,
                [months],
            ).fetchone()
            to_delete = int(row[0]) if row else 0
            if to_delete:
                self._conn.execute(
                    """
                    DELETE FROM events
                    WHERE timestamp < (SELECT MAX(timestamp) FROM events) - INTERVAL ? MONTH
                    """,
                    [months],
                )
        return to_delete

    def tweet_context(
        self,
        query: SimilarityQuery,
        *,
        current_value: Optional[float] = None,
        analog_limit: int = 3,
    ) -> TweetContextResponse:
        occurrences = self.count_similar(query)
        latest = self.latest_similar(query)
        percentile = (
            self.percentile(current_value, query) if current_value is not None else None
        )
        analogs = (
            self.top_analogs(current_value, query, limit=analog_limit)
            if current_value is not None
            else self.find_similar(query, limit=analog_limit)
        )
        return build_tweet_context(
            event_type=query.event_type,
            asset=query.asset,
            indicator_type=query.indicator_type,
            direction=query.direction,
            current_value=current_value,
            since=query.since,
            occurrences=occurrences,
            percentile=percentile,
            last_seen=latest.timestamp if latest else None,
            top_analogs=analogs,
        )

    def health(self) -> dict[str, Any]:
        summary = self.stats()
        return {
            "status": "ok",
            "db_path": str(self.db_path),
            "total_events": summary.total_events,
            "earliest": summary.earliest.isoformat() if summary.earliest else None,
            "latest": summary.latest.isoformat() if summary.latest else None,
        }

    def count_all(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM events").fetchone()
        return int(row[0]) if row else 0


def _ensure_utc(ts: datetime) -> datetime:
    """Normalize to UTC-naive. Naive inputs are assumed to already be UTC."""
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(timezone.utc).replace(tzinfo=None)


def _append_clause(where: str, clause: str) -> str:
    if where:
        return f"{where} AND {clause}"
    return f"WHERE {clause}"


def _row_to_event(row: dict[str, Any]) -> Event:
    tags = row.get("tags")
    metadata = row.get("metadata")
    if isinstance(tags, str):
        tags = json.loads(tags)
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    return Event(
        id=row["id"],
        timestamp=row["timestamp"],
        event_type=row["event_type"],
        asset=row.get("asset"),
        indicator_type=row.get("indicator_type"),
        timeframe=row.get("timeframe"),
        value=row.get("value"),
        percent_change=row.get("percent_change"),
        direction=row.get("direction"),
        source=row.get("source"),
        tags=tags or [],
        metadata=metadata or {},
    )


def _build_filters(
    *,
    event_type: Optional[str] = None,
    asset: Optional[str] = None,
    indicator_type: Optional[str] = None,
    timeframe: Optional[str] = None,
    direction: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    tags: Optional[list[str]] = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    if asset:
        clauses.append("asset = ?")
        params.append(asset)
    if indicator_type:
        clauses.append("indicator_type = ?")
        params.append(indicator_type)
    if timeframe:
        clauses.append("timeframe = ?")
        params.append(timeframe)
    if direction:
        clauses.append("direction = ?")
        params.append(direction)
    if since:
        clauses.append("timestamp >= ?")
        params.append(_ensure_utc(since))
    if until:
        clauses.append("timestamp <= ?")
        params.append(_ensure_utc(until))
    if min_value is not None:
        clauses.append("value >= ?")
        params.append(min_value)
    if max_value is not None:
        clauses.append("value <= ?")
        params.append(max_value)
    if tags:
        tag_checks = []
        for tag in tags:
            tag_checks.append("json_contains(tags, ?)")
            params.append(json.dumps([tag]))
        clauses.append(f"({' OR '.join(tag_checks)})")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, params


def _similarity_filters(query: SimilarityQuery) -> tuple[str, list[Any]]:
    return _build_filters(
        event_type=query.event_type,
        asset=query.asset,
        indicator_type=query.indicator_type,
        timeframe=query.timeframe,
        direction=query.direction,
        since=query.since,
        until=query.until,
        min_value=query.min_value,
        max_value=query.max_value,
        tags=query.tags or None,
    )