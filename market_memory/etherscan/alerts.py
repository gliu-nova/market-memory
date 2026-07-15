"""Whale / large-transfer alert hooks for post-ingest processing.

Designed for:
  - CLI logging after `run_ingest`
  - Scheduled polling loops
  - twitter-bot consumption via `format_whale_tweet` / callbacks
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from market_memory.etherscan.analysis import detect_large_transfers
from market_memory.etherscan.chains import explorer_tx_url, resolve_chain
from market_memory.etherscan.db import EtherscanDB

logger = logging.getLogger(__name__)

AlertCallback = Callable[["WhaleAlert"], None]


@dataclass
class WhaleAlert:
    tx_hash: str
    chain_id: int
    chain_name: str
    value_eth: float
    from_address: str
    to_address: str | None
    watched_address: str | None
    time_stamp: int
    label: str | None = None
    threshold_eth: float = 100.0
    explorer_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def format_whale_tweet(alert: WhaleAlert, *, max_len: int = 280) -> str:
    """Compact tweet-ready whale alert line."""
    chain = alert.chain_name or f"chain-{alert.chain_id}"
    label = f" ({alert.label})" if alert.label else ""
    watched = alert.watched_address or "?"
    short_from = _short(alert.from_address)
    short_to = _short(alert.to_address)
    short_watch = _short(watched)
    url = alert.explorer_url or explorer_tx_url(alert.chain_id, alert.tx_hash)
    text = (
        f"🐋 Whale alert{label} on {chain}: {alert.value_eth:,.2f} ETH\n"
        f"{short_from} → {short_to}\n"
        f"watched {short_watch}\n"
        f"{url}"
    )
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _short(addr: str | None, n: int = 6) -> str:
    if not addr:
        return "?"
    if len(addr) < 12:
        return addr
    return f"{addr[: n + 2]}…{addr[-n:]}"


def check_whale_alerts(
    db: EtherscanDB,
    *,
    threshold_eth: float = 100.0,
    address: str | None = None,
    chain_id: int | None = None,
    label: str | None = None,
    since_ts: int | None = None,
    only_unalerted: bool = True,
    mark_alerted: bool = True,
    limit: int = 500,
) -> list[WhaleAlert]:
    """Find large transfers and optionally persist them as fired alerts.

    When only_unalerted=True, skips txs already recorded in whale_alerts
    (idempotent across re-runs).
    """
    rows = detect_large_transfers(
        db,
        threshold_eth=threshold_eth,
        address=address,
        since_ts=since_ts,
        chain_id=chain_id,
        limit=limit,
    )
    alerts: list[WhaleAlert] = []
    for t in rows:
        cid = getattr(t, "chain_id", None) or chain_id or 1
        if only_unalerted and db.has_whale_alert(t.tx_hash, cid):
            continue
        chain = resolve_chain(cid)
        alert = WhaleAlert(
            tx_hash=t.tx_hash,
            chain_id=chain.chain_id,
            chain_name=chain.name,
            value_eth=t.value_eth,
            from_address=t.from_address,
            to_address=t.to_address,
            watched_address=t.watched_address or address,
            time_stamp=t.time_stamp,
            label=label,
            threshold_eth=threshold_eth,
            explorer_url=explorer_tx_url(chain.chain_id, t.tx_hash),
        )
        alerts.append(alert)
        if mark_alerted:
            db.record_whale_alert(alert)

    logger.info(
        "whale alerts: threshold=%.2f address=%s chain=%s new=%s",
        threshold_eth,
        address,
        chain_id,
        len(alerts),
    )
    return alerts


def emit_whale_alerts(
    alerts: Sequence[WhaleAlert],
    *,
    callbacks: Sequence[AlertCallback] | None = None,
    json_path: str | Path | None = None,
    log: bool = True,
) -> list[dict[str, Any]]:
    """Dispatch alerts to log / JSON append / custom callbacks."""
    payload = [a.to_dict() for a in alerts]
    if not alerts:
        return payload

    if log:
        for a in alerts:
            logger.warning(
                "WHALE %.2f ETH chain=%s tx=%s %s -> %s",
                a.value_eth,
                a.chain_name,
                a.tx_hash,
                a.from_address,
                a.to_address,
            )
            logger.info("tweet draft: %s", format_whale_tweet(a))

    if json_path is not None:
        path = Path(json_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing: list[Any] = []
        if path.is_file():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = [existing]
            except json.JSONDecodeError:
                existing = []
        existing.extend(payload)
        path.write_text(json.dumps(existing, indent=2), encoding="utf-8")

    for cb in callbacks or []:
        for a in alerts:
            try:
                cb(a)
            except Exception:
                logger.exception("Whale alert callback failed for %s", a.tx_hash)

    return payload


def run_whale_hook(
    db: EtherscanDB,
    *,
    threshold_eth: float = 100.0,
    address: str | None = None,
    chain_id: int | None = None,
    label: str | None = None,
    since_ts: int | None = None,
    callbacks: Sequence[AlertCallback] | None = None,
    json_path: str | Path | None = None,
    only_unalerted: bool = True,
) -> list[WhaleAlert]:
    """End-to-end: detect → mark → emit. Call after successful ingest."""
    alerts = check_whale_alerts(
        db,
        threshold_eth=threshold_eth,
        address=address,
        chain_id=chain_id,
        label=label,
        since_ts=since_ts,
        only_unalerted=only_unalerted,
        mark_alerted=True,
    )
    emit_whale_alerts(alerts, callbacks=callbacks, json_path=json_path, log=True)
    return alerts


def alerts_since_iso(hours: float) -> int:
    """Unix timestamp for `now - hours` (helper for since_ts filters)."""
    return int(datetime.now(timezone.utc).timestamp() - hours * 3600)
