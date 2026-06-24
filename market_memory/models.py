from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class EventCreate(BaseModel):
    """Validated input for ingesting a market event."""

    id: Optional[str] = None
    timestamp: datetime
    event_type: str
    asset: Optional[str] = None
    indicator_type: Optional[str] = None
    timeframe: Optional[str] = None
    value: Optional[float] = None
    percent_change: Optional[float] = None
    direction: Optional[str] = None
    source: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("tags", mode="before")
    @classmethod
    def _normalize_tags(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [t.strip() for t in value.split(",") if t.strip()]
        return list(value)

    def with_id(self) -> EventCreate:
        if self.id:
            return self
        return self.model_copy(update={"id": str(uuid4())})


class Event(EventCreate):
    """Stored event returned from queries."""

    id: str


class SimilarityQuery(BaseModel):
    """Filters for matching similar historical events."""

    event_type: str
    asset: Optional[str] = None
    indicator_type: Optional[str] = None
    timeframe: Optional[str] = None
    direction: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None


class EventStats(BaseModel):
    total_events: int
    earliest: Optional[datetime] = None
    latest: Optional[datetime] = None
    by_event_type: dict[str, int] = Field(default_factory=dict)
    by_asset: dict[str, int] = Field(default_factory=dict)
    monthly_counts: dict[str, int] = Field(default_factory=dict)
    yearly_counts: dict[str, int] = Field(default_factory=dict)


class TweetContextResponse(BaseModel):
    asset: Optional[str] = None
    indicator_type: Optional[str] = None
    event_type: str
    current_value: Optional[float] = None
    similar_events_since: Optional[str] = None
    occurrences: int = 0
    percentile: Optional[float] = None
    last_seen: Optional[str] = None
    tweet_context: str
    top_analogs: list[Event] = Field(default_factory=list)