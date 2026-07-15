"""Etherscan on-chain data ingestion for market-memory.

Public API for twitter-bot / Streamlit / scheduled jobs:

    from market_memory.etherscan import (
        run_ingest, run_ingest_entries, EtherscanDB,
        load_watchlist, check_whale_alerts, format_whale_tweet,
    )
"""

from market_memory.etherscan.alerts import (
    WhaleAlert,
    check_whale_alerts,
    emit_whale_alerts,
    format_whale_tweet,
    run_whale_hook,
)
from market_memory.etherscan.analysis import (
    detect_large_transfers,
    detect_volume_spikes,
    summarize_address_activity,
)
from market_memory.etherscan.chains import list_chains, resolve_chain
from market_memory.etherscan.client import EtherscanAPIError, EtherscanClient
from market_memory.etherscan.config import EtherscanConfig, load_etherscan_config
from market_memory.etherscan.db import EtherscanDB
from market_memory.etherscan.pipeline import IngestResult, run_ingest, run_ingest_entries
from market_memory.etherscan.watchlist import WatchEntry, Watchlist, load_watchlist

__all__ = [
    "EtherscanAPIError",
    "EtherscanClient",
    "EtherscanConfig",
    "EtherscanDB",
    "IngestResult",
    "WatchEntry",
    "Watchlist",
    "WhaleAlert",
    "check_whale_alerts",
    "detect_large_transfers",
    "detect_volume_spikes",
    "emit_whale_alerts",
    "format_whale_tweet",
    "list_chains",
    "load_etherscan_config",
    "load_watchlist",
    "resolve_chain",
    "run_ingest",
    "run_ingest_entries",
    "run_whale_hook",
    "summarize_address_activity",
]
