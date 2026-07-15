"""Ingestion orchestration for Blockscout modules."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from market_memory.blockscout.analysis import record_new_whales, score_trader_activity
from market_memory.blockscout.client import BlockscoutAPIError, BlockscoutClient
from market_memory.blockscout.config import BlockscoutConfig, load_blockscout_config
from market_memory.blockscout.db import BlockscoutDB

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    address: str | None
    mode: str
    chain_id: int = 1
    instance: str = "ethereum"
    label: str | None = None
    role: str | None = None
    address_meta: bool = False
    txs_inserted: int = 0
    transfers_inserted: int = 0
    token_balances: int = 0
    blocks_inserted: int = 0
    holders_inserted: int = 0
    contract_saved: bool = False
    stats_saved: bool = False
    trader_score: float | None = None
    whales: list[dict[str, Any]] = field(default_factory=list)
    status: str = "ok"
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "mode": self.mode,
            "chain_id": self.chain_id,
            "instance": self.instance,
            "label": self.label,
            "role": self.role,
            "address_meta": self.address_meta,
            "txs_inserted": self.txs_inserted,
            "transfers_inserted": self.transfers_inserted,
            "token_balances": self.token_balances,
            "blocks_inserted": self.blocks_inserted,
            "holders_inserted": self.holders_inserted,
            "contract_saved": self.contract_saved,
            "stats_saved": self.stats_saved,
            "trader_score": self.trader_score,
            "whales": self.whales,
            "status": self.status,
            "detail": self.detail,
        }


def _backup(cfg: BlockscoutConfig, name: str, payload: Any) -> None:
    if cfg.json_backup_dir is None:
        return
    path = Path(cfg.json_backup_dir) / name
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def run_ingest(
    *,
    address: str | None = None,
    mode: str = "account",
    token: str | None = None,
    label: str | None = None,
    role: str | None = None,
    include_txs: bool = True,
    include_tokens: bool = True,
    include_contract: bool = True,
    include_stats: bool = True,
    include_blocks: bool = False,
    score_trader: bool = True,
    whale_alerts: bool = True,
    config: BlockscoutConfig | None = None,
    db: BlockscoutDB | None = None,
    client: BlockscoutClient | None = None,
) -> IngestResult:
    """Run one Blockscout ingestion pass.

    Modes:
        account   — address meta + txs + token transfers/balances + optional contract + score
        stats     — network stats only
        blocks    — recent blocks
        token     — token meta + holders (+ transfers); requires --token
        contract  — smart-contract verification only
        network   — stats + recent blocks + recent txs
        full      — account + stats + blocks (for a watched address)
    """
    own_db = db is None
    own_client = client is None
    cfg = config or load_blockscout_config()
    database = db or BlockscoutDB(cfg.db_path)
    api = client or BlockscoutClient(cfg)

    result = IngestResult(
        address=address.lower() if address else None,
        mode=mode,
        chain_id=cfg.chain_id,
        instance=cfg.instance,
        label=label,
        role=role,
    )
    run_id = database.start_ingest_run(address, mode, chain_id=cfg.chain_id)

    try:
        if mode in {"stats", "network", "full"} or (include_stats and mode == "account"):
            try:
                stats = api.get_stats()
                database.insert_network_stats(stats, chain_id=cfg.chain_id)
                result.stats_saved = True
                _backup(cfg, f"stats_{cfg.instance}.json", stats)
            except BlockscoutAPIError as exc:
                logger.warning("stats fetch failed: %s", exc)

        if mode in {"blocks", "network", "full"} or include_blocks:
            try:
                blocks = api.get_blocks(max_pages=1)
                if not blocks:
                    blocks = api.get_main_page_blocks()
                result.blocks_inserted = database.upsert_blocks(blocks, chain_id=cfg.chain_id)
                _backup(cfg, f"blocks_{cfg.instance}.json", blocks[:20])
            except BlockscoutAPIError as exc:
                logger.warning("blocks fetch failed: %s", exc)

        if mode == "stats":
            database.finish_ingest_run(run_id, status="ok", detail="stats")
            return result

        if mode == "blocks":
            database.finish_ingest_run(run_id, status="ok", detail="blocks")
            return result

        if mode == "token":
            if not token:
                raise ValueError("mode=token requires token address")
            tok = api.get_token(token)
            database.upsert_token(tok, chain_id=cfg.chain_id)
            holders = api.get_token_holders(token)
            result.holders_inserted = database.upsert_token_holders(
                token, holders, chain_id=cfg.chain_id
            )
            transfers = api.get_token_transfers(token)
            result.transfers_inserted = database.upsert_token_transfers(
                transfers, chain_id=cfg.chain_id
            )
            _backup(cfg, f"token_{token}.json", {"token": tok, "holders": holders[:50]})
            database.finish_ingest_run(run_id, status="ok", detail="token")
            return result

        if mode == "network":
            try:
                txs = api.get_transactions(max_pages=1)
                result.txs_inserted = database.upsert_transactions(txs, chain_id=cfg.chain_id)
            except BlockscoutAPIError as exc:
                logger.warning("network txs failed: %s", exc)
            database.finish_ingest_run(run_id, status="ok", detail="network")
            return result

        # account / contract / full
        if not address:
            raise ValueError(f"mode={mode} requires --address")
        address = address.lower()
        result.address = address

        if mode in {"account", "full"}:
            meta = api.get_address(address)
            database.upsert_address(
                meta, chain_id=cfg.chain_id, label=label, watch_role=role or "monitor"
            )
            result.address_meta = True
            try:
                counters = api.get_address_counters(address)
                database.insert_counters(address, counters, chain_id=cfg.chain_id)
            except BlockscoutAPIError as exc:
                logger.debug("counters unavailable: %s", exc)
            _backup(cfg, f"address_{address}.json", meta)

            if include_txs:
                txs = api.get_address_transactions(address)
                result.txs_inserted = database.upsert_transactions(
                    txs, chain_id=cfg.chain_id, watched_address=address
                )
                _backup(cfg, f"txs_{address}.json", txs[:100])

            if include_tokens:
                try:
                    transfers = api.get_address_token_transfers(address)
                    result.transfers_inserted = database.upsert_token_transfers(
                        transfers, chain_id=cfg.chain_id, watched_address=address
                    )
                except BlockscoutAPIError as exc:
                    logger.warning("token transfers failed: %s", exc)
                try:
                    balances = api.get_address_tokens(address)
                    result.token_balances = database.upsert_token_balances(
                        address, balances, chain_id=cfg.chain_id
                    )
                except BlockscoutAPIError as exc:
                    logger.warning("token balances failed: %s", exc)

        if mode in {"account", "contract", "full"} and include_contract:
            try:
                contract = api.get_smart_contract(address)
                database.upsert_contract(address, contract, chain_id=cfg.chain_id)
                result.contract_saved = True
                _backup(cfg, f"contract_{address}.json", contract)
            except BlockscoutAPIError as exc:
                # Not verified / not a contract is expected often
                logger.debug("contract not available for %s: %s", address, exc)
                if mode == "contract":
                    result.detail = str(exc)

        if mode == "contract":
            database.finish_ingest_run(
                run_id, status="ok", detail="contract" if result.contract_saved else "no_contract"
            )
            return result

        if score_trader and mode in {"account", "full"}:
            scored = score_trader_activity(
                database, address, chain_id=cfg.chain_id, label=label
            )
            result.trader_score = scored.score
            if scored.score >= cfg.high_ev_min_score and role not in {"whale", "trader"}:
                from datetime import datetime, timezone

                database._conn.execute(
                    """
                    UPDATE addresses SET watch_role = 'trader', updated_at = ?
                    WHERE address = ? AND chain_id = ?
                    """,
                    (datetime.now(timezone.utc).isoformat(), address, cfg.chain_id),
                )
                database._conn.commit()

        if whale_alerts and mode in {"account", "full"}:
            whales = record_new_whales(
                database,
                threshold_eth=cfg.large_transfer_eth,
                address=address,
                chain_id=cfg.chain_id,
                label=label,
            )
            result.whales = [w.to_dict() for w in whales]

        database.finish_ingest_run(
            run_id,
            status="ok",
            detail=f"txs={result.txs_inserted} whales={len(result.whales)} score={result.trader_score}",
        )
        logger.info("Blockscout ingest complete: %s", result.to_dict())
        return result

    except Exception as exc:
        result.status = "error"
        result.detail = str(exc)
        logger.exception("Blockscout ingest failed: %s", exc)
        database.finish_ingest_run(run_id, status="error", detail=str(exc))
        raise
    finally:
        if own_client:
            api.close()
        if own_db:
            database.close()


@dataclass
class WatchTarget:
    address: str
    label: str | None = None
    role: str | None = None  # whale | trader | monitor
    token: str | None = None


def run_ingest_entries(
    entries: Sequence[WatchTarget],
    *,
    mode: str = "account",
    include_stats: bool = True,
    config: BlockscoutConfig | None = None,
    db: BlockscoutDB | None = None,
) -> list[IngestResult]:
    cfg = config or load_blockscout_config()
    own_db = db is None
    database = db or BlockscoutDB(cfg.db_path)
    client = BlockscoutClient(cfg)
    results: list[IngestResult] = []
    try:
        if include_stats and entries:
            try:
                results.append(
                    run_ingest(
                        mode="stats",
                        include_stats=True,
                        config=cfg,
                        db=database,
                        client=client,
                    )
                )
            except Exception:
                logger.exception("stats pass failed")
        for i, e in enumerate(entries):
            results.append(
                run_ingest(
                    address=e.address,
                    mode=mode,
                    token=e.token,
                    label=e.label,
                    role=e.role or "monitor",
                    include_stats=False,
                    config=cfg,
                    db=database,
                    client=client,
                )
            )
        return results
    finally:
        client.close()
        if own_db:
            database.close()
