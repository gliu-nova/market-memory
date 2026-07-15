"""Analysis helpers: whale detection and high-EV trader scoring."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any

from market_memory.blockscout.db import BlockscoutDB

logger = logging.getLogger(__name__)


@dataclass
class WhaleTransfer:
    tx_hash: str
    value_eth: float
    from_address: str | None
    to_address: str | None
    watched_address: str | None
    chain_id: int
    timestamp: str | None = None
    status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TraderScore:
    address: str
    chain_id: int
    score: float
    tx_count: int
    success_rate: float
    volume_eth: float
    unique_counterparties: int
    avg_value_eth: float
    reasons: list[str]
    label: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def detect_large_transfers(
    db: BlockscoutDB,
    *,
    threshold_eth: float = 100.0,
    address: str | None = None,
    chain_id: int | None = None,
    limit: int = 500,
) -> list[WhaleTransfer]:
    rows = db.fetch_transactions(
        address=address,
        chain_id=chain_id,
        min_value_eth=threshold_eth,
        limit=limit,
    )
    out: list[WhaleTransfer] = []
    for r in rows:
        out.append(
            WhaleTransfer(
                tx_hash=r["tx_hash"],
                value_eth=float(r["value_eth"] or 0),
                from_address=r["from_address"],
                to_address=r["to_address"],
                watched_address=r["watched_address"],
                chain_id=int(r["chain_id"] or 1),
                timestamp=r["timestamp"],
                status=r["status"],
            )
        )
    return out


def score_trader_activity(
    db: BlockscoutDB,
    address: str,
    *,
    chain_id: int = 1,
    label: str | None = None,
    limit: int = 500,
) -> TraderScore:
    """Heuristic high-EV score (0–100) from local tx history.

    Not true PnL — uses success rate, volume, activity, and counterparty breadth
    as proxies for "interesting / successful" accounts to monitor.
    """
    rows = db.fetch_transactions(address=address, chain_id=chain_id, limit=limit)
    addr = address.lower()
    if not rows:
        return TraderScore(
            address=addr,
            chain_id=chain_id,
            score=0.0,
            tx_count=0,
            success_rate=0.0,
            volume_eth=0.0,
            unique_counterparties=0,
            avg_value_eth=0.0,
            reasons=["no transactions"],
            label=label,
        )

    success = 0
    volume = 0.0
    counterparties: set[str] = set()
    for r in rows:
        st = (r["status"] or "").lower()
        if st in {"ok", "success", "1", "true"}:
            success += 1
        elif st in {"", "none"} and r["value_eth"] is not None:
            # Blockscout sometimes omits status on older rows — count as success if mined
            success += 1
        volume += float(r["value_eth"] or 0)
        other = r["to_address"] if r["from_address"] == addr else r["from_address"]
        if other:
            counterparties.add(other)

    n = len(rows)
    success_rate = success / n if n else 0.0
    avg_value = volume / n if n else 0.0

    # Component scores 0–100
    activity = min(n / 50.0, 1.0) * 100  # 50+ txs → full
    reliability = success_rate * 100
    size = min(volume / 500.0, 1.0) * 100  # 500 ETH cumulative volume → full
    breadth = min(len(counterparties) / 20.0, 1.0) * 100

    score = 0.30 * reliability + 0.25 * activity + 0.25 * size + 0.20 * breadth
    reasons = [
        f"success_rate={success_rate:.0%}",
        f"tx_count={n}",
        f"volume_eth={volume:.2f}",
        f"counterparties={len(counterparties)}",
    ]
    if success_rate >= 0.95 and n >= 20:
        reasons.append("high_reliability")
    if volume >= 100:
        reasons.append("high_volume")
    if len(counterparties) >= 15:
        reasons.append("diverse_flow")

    result = TraderScore(
        address=addr,
        chain_id=chain_id,
        score=round(score, 1),
        tx_count=n,
        success_rate=round(success_rate, 4),
        volume_eth=round(volume, 6),
        unique_counterparties=len(counterparties),
        avg_value_eth=round(avg_value, 6),
        reasons=reasons,
        label=label,
    )
    db.upsert_trader_score(
        addr,
        chain_id=chain_id,
        score=result.score,
        tx_count=result.tx_count,
        success_rate=result.success_rate,
        volume_eth=result.volume_eth,
        unique_counterparties=result.unique_counterparties,
        avg_value_eth=result.avg_value_eth,
        reasons=result.reasons,
        label=label,
    )
    logger.info("trader score %s = %.1f (%s)", addr, result.score, ", ".join(reasons))
    return result


def record_new_whales(
    db: BlockscoutDB,
    *,
    threshold_eth: float,
    address: str | None = None,
    chain_id: int = 1,
    label: str | None = None,
) -> list[WhaleTransfer]:
    """Detect large transfers and persist first-time whale alerts."""
    found = detect_large_transfers(
        db, threshold_eth=threshold_eth, address=address, chain_id=chain_id
    )
    new: list[WhaleTransfer] = []
    for w in found:
        if db.has_whale_alert(w.tx_hash, chain_id):
            continue
        db.record_whale_alert(
            tx_hash=w.tx_hash,
            chain_id=chain_id,
            value_eth=w.value_eth,
            from_address=w.from_address,
            to_address=w.to_address,
            watched_address=w.watched_address or address,
            label=label,
            timestamp=w.timestamp,
        )
        new.append(w)
    return new
