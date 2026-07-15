"""Lightweight analysis helpers over ingested SQLite data.

Importable by twitter-bot alerts and Streamlit dashboards.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from market_memory.etherscan.db import EtherscanDB

logger = logging.getLogger(__name__)


@dataclass
class LargeTransfer:
    tx_hash: str
    time_stamp: int
    from_address: str
    to_address: str | None
    value_eth: float
    watched_address: str | None = None
    chain_id: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class VolumeSpike:
    bucket_start: int  # unix seconds (hour or day start)
    volume_eth: float
    tx_count: int
    zscore: float
    mean_volume: float
    std_volume: float
    chain_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AddressSummary:
    address: str
    tx_count: int
    total_value_eth_out: float
    total_value_eth_in: float
    unique_counterparties: int
    first_ts: int | None
    last_ts: int | None
    chain_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _rows_to_df(rows: list[Any]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def detect_large_transfers(
    db: EtherscanDB,
    *,
    threshold_eth: float = 100.0,
    address: str | None = None,
    since_ts: int | None = None,
    chain_id: int | None = None,
    limit: int = 500,
) -> list[LargeTransfer]:
    """Return ETH transfers with value_eth >= threshold."""
    rows = db.fetch_transactions(
        address=address,
        since_ts=since_ts,
        min_value_eth=threshold_eth,
        chain_id=chain_id,
        limit=limit,
    )
    out: list[LargeTransfer] = []
    for r in rows:
        out.append(
            LargeTransfer(
                tx_hash=r["tx_hash"],
                time_stamp=int(r["time_stamp"]),
                from_address=r["from_address"],
                to_address=r["to_address"],
                value_eth=float(r["value_eth"] or 0),
                watched_address=r["watched_address"],
                chain_id=int(r["chain_id"] or 1),
            )
        )
    logger.info(
        "detect_large_transfers: threshold=%.4f eth chain=%s found=%s",
        threshold_eth,
        chain_id,
        len(out),
    )
    return out


def detect_volume_spikes(
    db: EtherscanDB,
    *,
    address: str | None = None,
    since_ts: int | None = None,
    chain_id: int | None = None,
    bucket: str = "h",
    zscore_threshold: float = 2.0,
    limit: int = 50_000,
) -> list[VolumeSpike]:
    """Detect time buckets where total ETH volume is a statistical outlier.

    bucket: pandas offset alias — 'h' hourly, 'D' daily.
    """
    rows = db.fetch_transactions(
        address=address, since_ts=since_ts, chain_id=chain_id, limit=limit
    )
    df = _rows_to_df(rows)
    if df.empty:
        return []

    df["time_stamp"] = pd.to_datetime(df["time_stamp"], unit="s", utc=True)
    df["value_eth"] = pd.to_numeric(df["value_eth"], errors="coerce").fillna(0.0)

    grouped = (
        df.set_index("time_stamp")
        .resample(bucket)
        .agg(volume_eth=("value_eth", "sum"), tx_count=("tx_hash", "count"))
        .dropna(how="all")
    )
    if grouped.empty or len(grouped) < 3:
        return []

    mean = float(grouped["volume_eth"].mean())
    std = float(grouped["volume_eth"].std(ddof=0))
    if std == 0:
        return []

    grouped["zscore"] = (grouped["volume_eth"] - mean) / std
    spikes = grouped[grouped["zscore"] >= zscore_threshold].sort_values("zscore", ascending=False)

    results: list[VolumeSpike] = []
    for ts, row in spikes.iterrows():
        results.append(
            VolumeSpike(
                bucket_start=int(ts.timestamp()),
                volume_eth=float(row["volume_eth"]),
                tx_count=int(row["tx_count"]),
                zscore=float(row["zscore"]),
                mean_volume=mean,
                std_volume=std,
                chain_id=chain_id,
            )
        )
    logger.info(
        "detect_volume_spikes: bucket=%s z>=%.2f found=%s",
        bucket,
        zscore_threshold,
        len(results),
    )
    return results


def summarize_address_activity(
    db: EtherscanDB,
    address: str,
    *,
    since_ts: int | None = None,
    chain_id: int | None = None,
    limit: int = 50_000,
) -> AddressSummary:
    """Aggregate basic activity metrics for a watched address."""
    addr = address.lower()
    rows = db.fetch_transactions(
        address=addr, since_ts=since_ts, chain_id=chain_id, limit=limit
    )
    df = _rows_to_df(rows)
    if df.empty:
        return AddressSummary(
            address=addr,
            tx_count=0,
            total_value_eth_out=0.0,
            total_value_eth_in=0.0,
            unique_counterparties=0,
            first_ts=None,
            last_ts=None,
            chain_id=chain_id,
        )

    df["value_eth"] = pd.to_numeric(df["value_eth"], errors="coerce").fillna(0.0)
    out_mask = df["from_address"] == addr
    in_mask = df["to_address"] == addr
    counterparties = set(df.loc[out_mask, "to_address"].dropna()) | set(
        df.loc[in_mask, "from_address"].dropna()
    )
    return AddressSummary(
        address=addr,
        tx_count=len(df),
        total_value_eth_out=float(df.loc[out_mask, "value_eth"].sum()),
        total_value_eth_in=float(df.loc[in_mask, "value_eth"].sum()),
        unique_counterparties=len(counterparties),
        first_ts=int(df["time_stamp"].min()),
        last_ts=int(df["time_stamp"].max()),
        chain_id=chain_id,
    )


def large_transfers_dataframe(
    db: EtherscanDB,
    *,
    threshold_eth: float = 100.0,
    address: str | None = None,
    chain_id: int | None = None,
) -> pd.DataFrame:
    """Convenience for Streamlit: large transfers as a DataFrame."""
    items = detect_large_transfers(
        db, threshold_eth=threshold_eth, address=address, chain_id=chain_id
    )
    if not items:
        return pd.DataFrame()
    return pd.DataFrame([i.to_dict() for i in items])
