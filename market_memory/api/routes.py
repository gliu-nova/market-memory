from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from market_memory.db import EventDB
from market_memory.models import EventCreate, SimilarityQuery


class IngestRequest(BaseModel):
    events: list[EventCreate] = Field(default_factory=list)


def create_router(db: EventDB) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    def health():
        return db.health()

    @router.post("/ingest")
    def ingest(body: IngestRequest):
        count = db.ingest_events([e.with_id() for e in body.events])
        return {"ingested": count, "status": "ok"}

    @router.get("/events")
    def events(
        event_type: Optional[str] = None,
        asset: Optional[str] = None,
        indicator_type: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        rows = db.get_events(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            since=since,
            until=until,
            limit=limit,
            offset=offset,
        )
        return {"events": [e.model_dump(mode="json") for e in rows], "count": len(rows)}

    @router.get("/similar")
    def similar(
        event_type: str,
        asset: Optional[str] = None,
        indicator_type: Optional[str] = None,
        timeframe: Optional[str] = None,
        direction: Optional[str] = None,
        tags: Optional[str] = Query(default=None, description="Comma-separated tags"),
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        limit: int = Query(default=20, ge=1, le=200),
    ):
        query = _query_from_params(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            timeframe=timeframe,
            direction=direction,
            tags=tags,
            since=since,
            until=until,
            min_value=min_value,
            max_value=max_value,
        )
        rows = db.find_similar(query, limit=limit)
        return {"events": [e.model_dump(mode="json") for e in rows], "count": len(rows)}

    @router.get("/count")
    def count(
        event_type: str,
        asset: Optional[str] = None,
        indicator_type: Optional[str] = None,
        timeframe: Optional[str] = None,
        direction: Optional[str] = None,
        tags: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ):
        query = _query_from_params(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            timeframe=timeframe,
            direction=direction,
            tags=tags,
            since=since,
            until=until,
            min_value=min_value,
            max_value=max_value,
        )
        return {"count": db.count_similar(query), "query": query.model_dump(mode="json")}

    @router.get("/latest")
    def latest(
        event_type: str,
        asset: Optional[str] = None,
        indicator_type: Optional[str] = None,
        timeframe: Optional[str] = None,
        direction: Optional[str] = None,
        tags: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ):
        query = _query_from_params(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            timeframe=timeframe,
            direction=direction,
            tags=tags,
            since=since,
            until=until,
            min_value=min_value,
            max_value=max_value,
        )
        event = db.latest_similar(query)
        if not event:
            raise HTTPException(status_code=404, detail="No matching event found")
        return event.model_dump(mode="json")

    @router.get("/percentile")
    def percentile(
        current_value: float,
        event_type: str,
        asset: Optional[str] = None,
        indicator_type: Optional[str] = None,
        timeframe: Optional[str] = None,
        direction: Optional[str] = None,
        tags: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ):
        query = _query_from_params(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            timeframe=timeframe,
            direction=direction,
            tags=tags,
            since=since,
            until=until,
        )
        value = db.percentile(current_value, query)
        if value is None:
            raise HTTPException(status_code=404, detail="Not enough data for percentile")
        return {"percentile": value, "current_value": current_value}

    @router.get("/tweet-context")
    def tweet_context(
        event_type: str,
        asset: Optional[str] = None,
        indicator_type: Optional[str] = None,
        timeframe: Optional[str] = None,
        direction: Optional[str] = None,
        tags: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        min_value: Optional[float] = None,
        current_value: Optional[float] = None,
        analog_limit: int = Query(default=3, ge=0, le=10),
    ):
        query = _query_from_params(
            event_type=event_type,
            asset=asset,
            indicator_type=indicator_type,
            timeframe=timeframe,
            direction=direction,
            tags=tags,
            since=since,
            until=until,
            min_value=min_value,
        )
        return db.tweet_context(
            query,
            current_value=current_value,
            analog_limit=analog_limit,
        ).model_dump(mode="json")

    @router.get("/stats")
    def stats():
        return db.stats().model_dump(mode="json")

    @router.post("/prune")
    def prune(
        before: Optional[datetime] = None,
        keep_months: Optional[int] = Query(default=None, ge=1, le=120),
    ):
        if before and keep_months:
            raise HTTPException(status_code=400, detail="Use either before or keep_months")
        if before:
            deleted = db.prune_before(before)
        elif keep_months:
            deleted = db.prune_keep_months(keep_months)
        else:
            raise HTTPException(status_code=400, detail="Provide before or keep_months")
        return {"deleted": deleted, "status": "ok"}

    return router


def _query_from_params(
    *,
    event_type: str,
    asset: Optional[str] = None,
    indicator_type: Optional[str] = None,
    timeframe: Optional[str] = None,
    direction: Optional[str] = None,
    tags: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> SimilarityQuery:
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
    return SimilarityQuery(
        event_type=event_type,
        asset=asset,
        indicator_type=indicator_type,
        timeframe=timeframe,
        direction=direction,
        tags=tag_list,
        since=since,
        until=until,
        min_value=min_value,
        max_value=max_value,
    )