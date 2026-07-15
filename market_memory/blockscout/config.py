"""Configuration for Blockscout ingestion (env + defaults)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Common Blockscout instances (API v2 base, without trailing slash)
INSTANCE_BASES: dict[str, str] = {
    "ethereum": "https://eth.blockscout.com/api/v2",
    "eth": "https://eth.blockscout.com/api/v2",
    "mainnet": "https://eth.blockscout.com/api/v2",
    "base": "https://base.blockscout.com/api/v2",
    "optimism": "https://optimism.blockscout.com/api/v2",
    "op": "https://optimism.blockscout.com/api/v2",
    "arbitrum": "https://arbitrum.blockscout.com/api/v2",
    "polygon": "https://polygon.blockscout.com/api/v2",
    "gnosis": "https://gnosis.blockscout.com/api/v2",
}


@dataclass
class BlockscoutConfig:
    """Runtime settings for the Blockscout pipeline."""

    api_key: str
    instance: str = "ethereum"
    base_url: str = "https://eth.blockscout.com/api/v2"
    chain_id: int = 1
    db_path: Path = field(default_factory=lambda: _PROJECT_ROOT / "data" / "blockscout.db")
    rate_limit_delay: float = 0.2
    request_timeout: float = 45.0
    max_retries: int = 3
    # Pagination: max pages per list endpoint in one run
    max_pages: int = 5
    page_items: int = 50
    # Whale / monitoring thresholds (native coin, e.g. ETH)
    large_transfer_eth: float = 100.0
    # Optional high-EV trader scoring floor (0–100 composite)
    high_ev_min_score: float = 70.0
    watchlist_path: Path | None = None
    json_backup_dir: Path | None = None

    def ensure_dirs(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.json_backup_dir is not None:
            self.json_backup_dir.mkdir(parents=True, exist_ok=True)
        if self.watchlist_path is not None:
            self.watchlist_path = Path(self.watchlist_path)

    def with_instance(self, instance: str) -> BlockscoutConfig:
        base = INSTANCE_BASES.get(instance.lower().strip())
        if not base:
            # Treat as full base URL if it looks like one
            if instance.startswith("http"):
                base = instance.rstrip("/")
                if not base.endswith("/api/v2"):
                    base = base.rstrip("/") + "/api/v2"
            else:
                raise ValueError(
                    f"Unknown Blockscout instance {instance!r}. "
                    f"Known: {', '.join(sorted(set(INSTANCE_BASES)))}"
                )
        chain_map = {
            "ethereum": 1,
            "eth": 1,
            "mainnet": 1,
            "base": 8453,
            "optimism": 10,
            "op": 10,
            "arbitrum": 42161,
            "polygon": 137,
            "gnosis": 100,
        }
        return BlockscoutConfig(
            api_key=self.api_key,
            instance=instance.lower().strip() if not instance.startswith("http") else self.instance,
            base_url=base,
            chain_id=chain_map.get(instance.lower().strip(), self.chain_id),
            db_path=self.db_path,
            rate_limit_delay=self.rate_limit_delay,
            request_timeout=self.request_timeout,
            max_retries=self.max_retries,
            max_pages=self.max_pages,
            page_items=self.page_items,
            large_transfer_eth=self.large_transfer_eth,
            high_ev_min_score=self.high_ev_min_score,
            watchlist_path=self.watchlist_path,
            json_backup_dir=self.json_backup_dir,
        )


def load_blockscout_config(
    *,
    env_file: str | Path | None = None,
    db_path: str | Path | None = None,
    instance: str | None = None,
    rate_limit_delay: float | None = None,
    large_transfer_eth: float | None = None,
    watchlist_path: str | Path | None = None,
    api_key: str | None = None,
) -> BlockscoutConfig:
    """Load config from environment (.env) with optional CLI overrides.

    Environment variables:
        BLOCKSCOUT_API_KEY (required)
        BLOCKSCOUT_INSTANCE (ethereum, base, ...)
        BLOCKSCOUT_BASE_URL
        BLOCKSCOUT_CHAIN_ID
        BLOCKSCOUT_DB_PATH
        BLOCKSCOUT_RATE_LIMIT_DELAY
        BLOCKSCOUT_LARGE_TRANSFER_ETH
        BLOCKSCOUT_MAX_PAGES
        BLOCKSCOUT_WATCHLIST_PATH
        BLOCKSCOUT_JSON_BACKUP_DIR
        BLOCKSCOUT_HIGH_EV_MIN_SCORE
    """
    if env_file is not None:
        load_dotenv(env_file)
    else:
        load_dotenv(_PROJECT_ROOT / ".env")
        load_dotenv()

    key = (api_key or os.getenv("BLOCKSCOUT_API_KEY") or "").strip()
    if not key:
        raise ValueError(
            "BLOCKSCOUT_API_KEY is not set. Copy .env.example to .env and add your key."
        )

    inst = (instance or os.getenv("BLOCKSCOUT_INSTANCE") or "ethereum").strip()
    base = os.getenv("BLOCKSCOUT_BASE_URL")
    if not base:
        base = INSTANCE_BASES.get(inst.lower(), INSTANCE_BASES["ethereum"])

    chain_id = int(os.getenv("BLOCKSCOUT_CHAIN_ID", "0") or "0")
    if chain_id == 0:
        chain_id = {
            "ethereum": 1,
            "eth": 1,
            "mainnet": 1,
            "base": 8453,
            "optimism": 10,
            "op": 10,
            "arbitrum": 42161,
            "polygon": 137,
            "gnosis": 100,
        }.get(inst.lower(), 1)

    backup = os.getenv("BLOCKSCOUT_JSON_BACKUP_DIR")
    wl = watchlist_path or os.getenv("BLOCKSCOUT_WATCHLIST_PATH")

    cfg = BlockscoutConfig(
        api_key=key,
        instance=inst.lower(),
        base_url=base.rstrip("/"),
        chain_id=chain_id,
        db_path=Path(os.getenv("BLOCKSCOUT_DB_PATH", str(_PROJECT_ROOT / "data" / "blockscout.db"))),
        rate_limit_delay=float(os.getenv("BLOCKSCOUT_RATE_LIMIT_DELAY", "0.2")),
        large_transfer_eth=float(os.getenv("BLOCKSCOUT_LARGE_TRANSFER_ETH", "100")),
        max_pages=int(os.getenv("BLOCKSCOUT_MAX_PAGES", "5")),
        high_ev_min_score=float(os.getenv("BLOCKSCOUT_HIGH_EV_MIN_SCORE", "70")),
        watchlist_path=Path(wl) if wl else None,
        json_backup_dir=Path(backup) if backup else None,
    )

    if db_path is not None:
        cfg.db_path = Path(db_path)
    if rate_limit_delay is not None:
        cfg.rate_limit_delay = rate_limit_delay
    if large_transfer_eth is not None:
        cfg.large_transfer_eth = large_transfer_eth

    cfg.ensure_dirs()
    return cfg
