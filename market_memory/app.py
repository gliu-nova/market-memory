from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from market_memory.api.routes import create_router
from market_memory.config import AppConfig, load_config
from market_memory.db import EventDB


def create_app(config: AppConfig | None = None) -> FastAPI:
    cfg = config or load_config()
    db = EventDB(data_dir=cfg.service.data_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        db.close()

    app = FastAPI(
        title="Market Memory",
        description="Historical market event store for twitter-bot context",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(create_router(db))
    app.state.db = db
    return app