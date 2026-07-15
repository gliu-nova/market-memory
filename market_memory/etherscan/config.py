"""Configuration for Etherscan ingestion (env + defaults)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from market_memory.etherscan.chains import resolve_chain

# Project root (market-memory/)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class EtherscanConfig:
    """Runtime settings for the Etherscan pipeline."""

    api_key: str
    chain_id: int = 1  # default chain (overridable per watchlist entry)
    chain_name: str = "ethereum"
    base_url: str = "https://api.etherscan.io/v2/api"
    db_path: Path = field(default_factory=lambda: _PROJECT_ROOT / "data" / "etherscan.db")
    # Free-tier friendly default (~5 req/s hard limit; stay well under)
    rate_limit_delay: float = 0.25
    request_timeout: float = 30.0
    max_retries: int = 3
    # Analysis defaults
    large_transfer_eth: float = 100.0
    volume_spike_zscore: float = 2.0
    # Whale alerts
    whale_alerts_enabled: bool = False
    whale_alerts_json: Path | None = None
    # Watchlist
    watchlist_path: Path | None = None
    # Ingest page size (Etherscan max is typically 10_000)
    page_size: int = 10_000
    # Optional JSON backup directory (None = disabled)
    json_backup_dir: Path | None = None

    def ensure_dirs(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.json_backup_dir is not None:
            self.json_backup_dir.mkdir(parents=True, exist_ok=True)
        if self.whale_alerts_json is not None:
            self.whale_alerts_json.parent.mkdir(parents=True, exist_ok=True)

    def with_chain(self, chain: str | int) -> EtherscanConfig:
        """Return a shallow copy bound to a different chain."""
        info = resolve_chain(chain)
        return EtherscanConfig(
            api_key=self.api_key,
            chain_id=info.chain_id,
            chain_name=info.name,
            base_url=self.base_url,
            db_path=self.db_path,
            rate_limit_delay=self.rate_limit_delay,
            request_timeout=self.request_timeout,
            max_retries=self.max_retries,
            large_transfer_eth=self.large_transfer_eth,
            volume_spike_zscore=self.volume_spike_zscore,
            whale_alerts_enabled=self.whale_alerts_enabled,
            whale_alerts_json=self.whale_alerts_json,
            watchlist_path=self.watchlist_path,
            page_size=self.page_size,
            json_backup_dir=self.json_backup_dir,
        )


def load_etherscan_config(
    *,
    env_file: str | Path | None = None,
    db_path: str | Path | None = None,
    rate_limit_delay: float | None = None,
    chain_id: int | str | None = None,
    large_transfer_eth: float | None = None,
    watchlist_path: str | Path | None = None,
    whale_alerts: bool | None = None,
    whale_alerts_json: str | Path | None = None,
) -> EtherscanConfig:
    """Load config from environment (.env) with optional CLI overrides.

    Environment variables:
        ETHERSCAN_API_KEY (required)
        ETHERSCAN_CHAIN_ID or ETHERSCAN_CHAIN (name or numeric id)
        ETHERSCAN_BASE_URL
        ETHERSCAN_DB_PATH
        ETHERSCAN_RATE_LIMIT_DELAY
        ETHERSCAN_LARGE_TRANSFER_ETH
        ETHERSCAN_JSON_BACKUP_DIR
        ETHERSCAN_WATCHLIST_PATH
        ETHERSCAN_WHALE_ALERTS (1/true/yes)
        ETHERSCAN_WHALE_ALERTS_JSON
    """
    if env_file is not None:
        load_dotenv(env_file)
    else:
        load_dotenv(_PROJECT_ROOT / ".env")
        load_dotenv()

    api_key = os.getenv("ETHERSCAN_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "ETHERSCAN_API_KEY is not set. Copy .env.example to .env and add your key."
        )

    env_chain = os.getenv("ETHERSCAN_CHAIN") or os.getenv("ETHERSCAN_CHAIN_ID", "1")
    chain = resolve_chain(chain_id if chain_id is not None else env_chain)

    backup = os.getenv("ETHERSCAN_JSON_BACKUP_DIR")
    wl = watchlist_path or os.getenv("ETHERSCAN_WATCHLIST_PATH")
    whale_json = whale_alerts_json or os.getenv("ETHERSCAN_WHALE_ALERTS_JSON")
    whale_env = os.getenv("ETHERSCAN_WHALE_ALERTS", "").lower() in {"1", "true", "yes", "on"}

    cfg = EtherscanConfig(
        api_key=api_key,
        chain_id=chain.chain_id,
        chain_name=chain.name,
        base_url=os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api"),
        db_path=Path(os.getenv("ETHERSCAN_DB_PATH", str(_PROJECT_ROOT / "data" / "etherscan.db"))),
        rate_limit_delay=float(os.getenv("ETHERSCAN_RATE_LIMIT_DELAY", "0.25")),
        large_transfer_eth=float(os.getenv("ETHERSCAN_LARGE_TRANSFER_ETH", "100")),
        whale_alerts_enabled=whale_env if whale_alerts is None else whale_alerts,
        whale_alerts_json=Path(whale_json) if whale_json else None,
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
