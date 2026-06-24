from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field


class ServiceConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8788
    data_dir: str = "data"


class AppConfig(BaseModel):
    service: ServiceConfig = Field(default_factory=ServiceConfig)


def load_config(data_dir: str | None = None) -> AppConfig:
    cfg = AppConfig()
    if data_dir:
        cfg.service.data_dir = data_dir
    Path(cfg.service.data_dir).mkdir(parents=True, exist_ok=True)
    return cfg