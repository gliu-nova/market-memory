"""Blockscout on-chain data ingestion for market-memory.

Public API::

    from market_memory.blockscout import (
        BlockscoutClient, BlockscoutDB, run_ingest, load_blockscout_config,
    )
"""

from market_memory.blockscout.client import BlockscoutAPIError, BlockscoutClient
from market_memory.blockscout.config import BlockscoutConfig, load_blockscout_config
from market_memory.blockscout.db import BlockscoutDB
from market_memory.blockscout.pipeline import IngestResult, run_ingest, run_ingest_entries

__all__ = [
    "BlockscoutAPIError",
    "BlockscoutClient",
    "BlockscoutConfig",
    "BlockscoutDB",
    "IngestResult",
    "load_blockscout_config",
    "run_ingest",
    "run_ingest_entries",
]
