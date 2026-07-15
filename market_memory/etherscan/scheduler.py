"""Optional scheduling helpers for continuous ingestion."""

from __future__ import annotations

import logging
import time
from typing import Callable, Sequence

from market_memory.etherscan.config import EtherscanConfig, load_etherscan_config
from market_memory.etherscan.pipeline import IngestResult, run_ingest, run_ingest_entries
from market_memory.etherscan.watchlist import WatchEntry

logger = logging.getLogger(__name__)


def run_loop(
    entries: Sequence[WatchEntry] | None = None,
    *,
    addresses: Sequence[str] | None = None,
    mode: str = "recent",
    interval_seconds: float = 60.0,
    include_tokens: bool = True,
    include_gas: bool = True,
    whale_alerts: bool = False,
    max_iterations: int | None = None,
    config: EtherscanConfig | None = None,
    on_result: Callable[[IngestResult], None] | None = None,
) -> None:
    """Poll watchlist / addresses forever (or max_iterations)."""
    cfg = config or load_etherscan_config()
    resolved = _resolve_entries(entries, addresses, cfg, include_tokens=include_tokens)
    iteration = 0
    logger.info(
        "Starting ingest loop: entries=%s interval=%.1fs mode=%s whales=%s",
        len(resolved),
        interval_seconds,
        mode,
        whale_alerts,
    )
    while True:
        iteration += 1
        if not resolved and mode == "gas":
            try:
                result = run_ingest(
                    address=None,
                    mode="gas",
                    include_gas=True,
                    whale_alerts=False,
                    config=cfg,
                )
                if on_result:
                    on_result(result)
            except Exception:
                logger.exception("Scheduled gas ingest failed")
        else:
            try:
                results = run_ingest_entries(
                    resolved,
                    mode=mode,
                    include_gas=include_gas,
                    whale_alerts=whale_alerts,
                    config=cfg,
                )
                if on_result:
                    for r in results:
                        on_result(r)
            except Exception:
                logger.exception("Scheduled ingest batch failed")

        if max_iterations is not None and iteration >= max_iterations:
            logger.info("Reached max_iterations=%s; stopping loop", max_iterations)
            break
        time.sleep(interval_seconds)


def run_apscheduler(
    entries: Sequence[WatchEntry] | None = None,
    *,
    addresses: Sequence[str] | None = None,
    mode: str = "recent",
    interval_seconds: float = 60.0,
    include_tokens: bool = True,
    include_gas: bool = True,
    whale_alerts: bool = False,
    config: EtherscanConfig | None = None,
) -> None:
    """Run with APScheduler BlockingScheduler (requires apscheduler package)."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError as exc:
        raise ImportError(
            "APScheduler is not installed. pip install apscheduler  "
            "or use --schedule-loop instead of --schedule-apscheduler"
        ) from exc

    cfg = config or load_etherscan_config()
    resolved = _resolve_entries(entries, addresses, cfg, include_tokens=include_tokens)
    scheduler = BlockingScheduler()

    def _job() -> None:
        try:
            run_ingest_entries(
                resolved,
                mode=mode,
                include_gas=include_gas,
                whale_alerts=whale_alerts,
                config=cfg,
            )
        except Exception:
            logger.exception("APScheduler ingest failed")

    scheduler.add_job(_job, "interval", seconds=interval_seconds, id="etherscan_ingest")
    logger.info(
        "APScheduler started: every %.1fs for %s entries",
        interval_seconds,
        len(resolved),
    )
    _job()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("APScheduler stopped")


def _resolve_entries(
    entries: Sequence[WatchEntry] | None,
    addresses: Sequence[str] | None,
    cfg: EtherscanConfig,
    *,
    include_tokens: bool,
) -> list[WatchEntry]:
    if entries:
        return list(entries)
    out: list[WatchEntry] = []
    for addr in addresses or []:
        if not addr:
            continue
        out.append(
            WatchEntry(
                address=addr.lower(),
                chain_id=cfg.chain_id,
                chain_name=cfg.chain_name,
                include_tokens=include_tokens,
            )
        )
    return out
