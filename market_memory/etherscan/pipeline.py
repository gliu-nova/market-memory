"""Ingestion orchestration: fetch from Etherscan → store in SQLite."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from market_memory.etherscan.alerts import WhaleAlert, run_whale_hook
from market_memory.etherscan.client import EtherscanClient
from market_memory.etherscan.config import EtherscanConfig, load_etherscan_config
from market_memory.etherscan.db import EtherscanDB
from market_memory.etherscan.watchlist import WatchEntry

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    address: str | None
    mode: str
    chain_id: int = 1
    chain_name: str = "ethereum"
    label: str | None = None
    txs_fetched: int = 0
    txs_inserted: int = 0
    transfers_fetched: int = 0
    transfers_inserted: int = 0
    balance_wei: int | None = None
    gas_oracle: dict[str, Any] | None = None
    latest_block: int | None = None
    whale_alerts: list[dict[str, Any]] = field(default_factory=list)
    status: str = "ok"
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "mode": self.mode,
            "chain_id": self.chain_id,
            "chain_name": self.chain_name,
            "label": self.label,
            "txs_fetched": self.txs_fetched,
            "txs_inserted": self.txs_inserted,
            "transfers_fetched": self.transfers_fetched,
            "transfers_inserted": self.transfers_inserted,
            "balance_wei": self.balance_wei,
            "gas_oracle": self.gas_oracle,
            "latest_block": self.latest_block,
            "whale_alerts": self.whale_alerts,
            "status": self.status,
            "detail": self.detail,
        }


def _maybe_backup_json(
    config: EtherscanConfig,
    name: str,
    payload: Any,
) -> None:
    if config.json_backup_dir is None:
        return
    path = Path(config.json_backup_dir) / name
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.debug("Wrote JSON backup: %s", path)


def run_ingest(
    *,
    address: str | None = None,
    mode: str = "recent",
    include_tokens: bool = True,
    include_balance: bool = True,
    include_gas: bool = True,
    include_contract: bool = False,
    start_block: int | None = None,
    end_block: int | None = None,
    chain_id: int | None = None,
    label: str | None = None,
    whale_alerts: bool | None = None,
    whale_threshold_eth: float | None = None,
    config: EtherscanConfig | None = None,
    db: EtherscanDB | None = None,
    client: EtherscanClient | None = None,
) -> IngestResult:
    """Run one ingestion pass.

    Modes:
        recent   — last page of txs (desc), good for polling
        full     — from start_block (or 0) through end_block
        gas      — gas oracle only
        balance  — balance only for address
        contract — ABI/source for address
    """
    own_db = db is None
    own_client = client is None

    cfg = config or load_etherscan_config()
    if chain_id is not None and chain_id != cfg.chain_id:
        cfg = cfg.with_chain(chain_id)

    database = db or EtherscanDB(cfg.db_path)
    api = client or EtherscanClient(cfg)

    result = IngestResult(
        address=address,
        mode=mode,
        chain_id=cfg.chain_id,
        chain_name=cfg.chain_name,
        label=label,
    )
    run_id = database.start_ingest_run(address, mode, chain_id=cfg.chain_id)
    do_whales = cfg.whale_alerts_enabled if whale_alerts is None else whale_alerts
    threshold = whale_threshold_eth if whale_threshold_eth is not None else cfg.large_transfer_eth

    try:
        if mode in {"gas", "recent", "full"} and include_gas:
            gas = api.get_gas_oracle()
            database.insert_gas_oracle(gas, chain_id=cfg.chain_id)
            result.gas_oracle = gas
            _maybe_backup_json(cfg, f"gas_oracle_chain{cfg.chain_id}.json", gas)
            try:
                block = api.get_block_number()
                database.upsert_block(block, chain_id=cfg.chain_id)
                result.latest_block = block
            except Exception as exc:
                logger.warning("Could not fetch block number: %s", exc)

        if mode == "gas":
            result.status = "ok"
            database.finish_ingest_run(run_id, status="ok", detail="gas only")
            return result

        if not address:
            if mode in {"balance", "contract", "recent", "full"}:
                raise ValueError(f"--address is required for mode={mode}")
            database.finish_ingest_run(run_id, status="ok")
            return result

        address = address.lower()
        result.address = address

        if mode == "balance" or (include_balance and mode in {"recent", "full"}):
            bal = api.get_balance(address)
            database.insert_balance(address, bal, chain_id=cfg.chain_id)
            result.balance_wei = bal

        if mode == "balance":
            database.finish_ingest_run(run_id, status="ok", detail="balance only")
            return result

        if mode == "contract" or include_contract:
            abi = api.get_contract_abi(address)
            source = api.get_contract_source(address)
            database.upsert_contract(
                address,
                abi_json=abi,
                source_json=json.dumps(source),
                chain_id=cfg.chain_id,
            )
            _maybe_backup_json(
                cfg,
                f"contract_{cfg.chain_id}_{address}.json",
                {"abi": abi, "source": source},
            )
            if mode == "contract":
                database.finish_ingest_run(run_id, status="ok", detail="contract only")
                return result

        if mode == "recent":
            sblock = start_block if start_block is not None else 0
            eblock = end_block if end_block is not None else 99999999
            last = database.latest_tx_block(address, chain_id=cfg.chain_id)
            if last is not None and start_block is None:
                sblock = max(0, last - 1)
        else:
            sblock = start_block if start_block is not None else 0
            eblock = end_block if end_block is not None else 99999999

        sort = "desc" if mode == "recent" else "asc"

        txs = api.get_normal_transactions(
            address,
            start_block=sblock,
            end_block=eblock,
            sort=sort,
        )
        result.txs_fetched = len(txs)
        result.txs_inserted = database.upsert_transactions(
            txs, chain_id=cfg.chain_id, watched_address=address
        )
        _maybe_backup_json(cfg, f"txs_{cfg.chain_id}_{address}_{mode}.json", txs)

        if include_tokens:
            transfers = api.get_token_transfers(
                address,
                start_block=sblock,
                end_block=eblock,
                sort=sort,
            )
            result.transfers_fetched = len(transfers)
            result.transfers_inserted = database.upsert_token_transfers(
                transfers, chain_id=cfg.chain_id, watched_address=address
            )
            _maybe_backup_json(cfg, f"tokentx_{cfg.chain_id}_{address}_{mode}.json", transfers)

        # Whale alert hook (only for new large transfers not yet alerted)
        if do_whales and mode in {"recent", "full"}:
            alerts = run_whale_hook(
                database,
                threshold_eth=threshold,
                address=address,
                chain_id=cfg.chain_id,
                label=label,
                json_path=cfg.whale_alerts_json,
                only_unalerted=True,
            )
            result.whale_alerts = [a.to_dict() for a in alerts]

        database.finish_ingest_run(
            run_id,
            status="ok",
            txs_inserted=result.txs_inserted,
            transfers_inserted=result.transfers_inserted,
            detail=f"whales={len(result.whale_alerts)}" if result.whale_alerts else None,
        )
        logger.info("Ingest complete: %s", result.to_dict())
        return result

    except Exception as exc:
        result.status = "error"
        result.detail = str(exc)
        logger.exception("Ingest failed: %s", exc)
        database.finish_ingest_run(
            run_id,
            status="error",
            txs_inserted=result.txs_inserted,
            transfers_inserted=result.transfers_inserted,
            detail=str(exc),
        )
        raise
    finally:
        if own_client:
            api.close()
        if own_db:
            database.close()


def run_ingest_entries(
    entries: Sequence[WatchEntry],
    *,
    mode: str | None = None,
    include_gas: bool = True,
    whale_alerts: bool | None = None,
    config: EtherscanConfig | None = None,
    db: EtherscanDB | None = None,
) -> list[IngestResult]:
    """Ingest a list of watchlist / CLI entries (multi-address, multi-chain).

    Shares one DB connection; creates a client per chain for correct chainid.
    """
    cfg = config or load_etherscan_config()
    own_db = db is None
    database = db or EtherscanDB(cfg.db_path)
    results: list[IngestResult] = []
    clients: dict[int, EtherscanClient] = {}

    try:
        for i, entry in enumerate(entries):
            entry_cfg = cfg.with_chain(entry.chain_id)
            if entry.large_transfer_eth is not None:
                entry_cfg.large_transfer_eth = entry.large_transfer_eth
            if entry.chain_id not in clients:
                clients[entry.chain_id] = EtherscanClient(entry_cfg)
            client = clients[entry.chain_id]
            result = run_ingest(
                address=entry.address,
                mode=mode or entry.mode,
                include_tokens=entry.include_tokens,
                include_balance=entry.include_balance,
                include_gas=include_gas and i == 0,
                chain_id=entry.chain_id,
                label=entry.label,
                whale_alerts=whale_alerts,
                whale_threshold_eth=entry.large_transfer_eth,
                config=entry_cfg,
                db=database,
                client=client,
            )
            results.append(result)
        return results
    finally:
        for c in clients.values():
            c.close()
        if own_db:
            database.close()
