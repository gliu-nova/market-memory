"""SQLite storage for Blockscout raw + lightly processed data."""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS addresses (
    address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    is_contract INTEGER DEFAULT 0,
    is_verified INTEGER DEFAULT 0,
    name TEXT,
    ens_domain_name TEXT,
    coin_balance_wei TEXT,
    coin_balance_eth REAL,
    exchange_rate REAL,
    tx_count INTEGER,
    token_transfer_count INTEGER,
    gas_usage_count INTEGER,
    validations_count INTEGER,
    label TEXT,
    watch_role TEXT,  -- whale | trader | monitor | other
    raw_json TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (address, chain_id)
);

CREATE INDEX IF NOT EXISTS idx_bs_addr_role ON addresses(watch_role);
CREATE INDEX IF NOT EXISTS idx_bs_addr_bal ON addresses(coin_balance_eth);

CREATE TABLE IF NOT EXISTS address_counters (
    address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    transactions_count INTEGER,
    token_transfers_count INTEGER,
    gas_usage_count INTEGER,
    validations_count INTEGER,
    raw_json TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (address, chain_id, recorded_at)
);

CREATE TABLE IF NOT EXISTS transactions (
    tx_hash TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    block_number INTEGER,
    timestamp TEXT,
    from_address TEXT,
    to_address TEXT,
    value_wei TEXT,
    value_eth REAL,
    fee_wei TEXT,
    status TEXT,
    method TEXT,
    gas_used INTEGER,
    gas_price TEXT,
    watched_address TEXT,
    raw_json TEXT,
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (tx_hash, chain_id)
);

CREATE INDEX IF NOT EXISTS idx_bs_tx_from ON transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_bs_tx_to ON transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_bs_tx_block ON transactions(block_number);
CREATE INDEX IF NOT EXISTS idx_bs_tx_value ON transactions(value_eth);

CREATE TABLE IF NOT EXISTS token_transfers (
    id TEXT PRIMARY KEY,  -- chain:tx:log_index or hash of fields
    tx_hash TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    block_number INTEGER,
    timestamp TEXT,
    from_address TEXT,
    to_address TEXT,
    token_address TEXT,
    token_symbol TEXT,
    token_name TEXT,
    token_type TEXT,
    total_value TEXT,
    total_decimals INTEGER,
    value_normalized REAL,
    watched_address TEXT,
    raw_json TEXT,
    ingested_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bs_tt_token ON token_transfers(token_address);
CREATE INDEX IF NOT EXISTS idx_bs_tt_watched ON token_transfers(watched_address);

CREATE TABLE IF NOT EXISTS blocks (
    block_number INTEGER NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    hash TEXT,
    timestamp TEXT,
    tx_count INTEGER,
    miner_hash TEXT,
    size INTEGER,
    gas_used INTEGER,
    gas_limit INTEGER,
    raw_json TEXT,
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (block_number, chain_id)
);

CREATE TABLE IF NOT EXISTS tokens (
    address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    name TEXT,
    symbol TEXT,
    decimals INTEGER,
    total_supply TEXT,
    holders_count INTEGER,
    exchange_rate REAL,
    type TEXT,
    circulating_market_cap REAL,
    icon_url TEXT,
    raw_json TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (address, chain_id)
);

CREATE TABLE IF NOT EXISTS token_holders (
    token_address TEXT NOT NULL,
    holder_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    value TEXT,
    value_normalized REAL,
    rank INTEGER,
    raw_json TEXT,
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (token_address, holder_address, chain_id)
);

CREATE TABLE IF NOT EXISTS token_balances (
    owner_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    value TEXT,
    value_normalized REAL,
    token_symbol TEXT,
    token_name TEXT,
    token_type TEXT,
    raw_json TEXT,
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (owner_address, token_address, chain_id)
);

CREATE TABLE IF NOT EXISTS contracts (
    address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    name TEXT,
    language TEXT,
    compiler_version TEXT,
    optimization_enabled INTEGER,
    is_verified INTEGER DEFAULT 0,
    is_fully_verified INTEGER DEFAULT 0,
    source_code TEXT,
    abi_json TEXT,
    constructor_args TEXT,
    verified_at TEXT,
    raw_json TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (address, chain_id)
);

CREATE TABLE IF NOT EXISTS network_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id INTEGER NOT NULL DEFAULT 1,
    total_blocks INTEGER,
    total_addresses INTEGER,
    total_transactions INTEGER,
    average_block_time REAL,
    coin_price REAL,
    total_gas_used INTEGER,
    transactions_today INTEGER,
    gas_prices_json TEXT,
    raw_json TEXT,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trader_scores (
    address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    score REAL NOT NULL,
    tx_count INTEGER,
    success_rate REAL,
    volume_eth REAL,
    unique_counterparties INTEGER,
    avg_value_eth REAL,
    reasons TEXT,
    label TEXT,
    computed_at TEXT NOT NULL,
    PRIMARY KEY (address, chain_id)
);

CREATE TABLE IF NOT EXISTS whale_alerts (
    tx_hash TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    value_eth REAL NOT NULL,
    from_address TEXT,
    to_address TEXT,
    watched_address TEXT,
    label TEXT,
    timestamp TEXT,
    alerted_at TEXT NOT NULL,
    PRIMARY KEY (tx_hash, chain_id)
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT,
    chain_id INTEGER,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _wei_to_eth(value: str | int | float | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value)) / 1e18
    except (TypeError, ValueError):
        return None


def _addr_field(obj: Any, *keys: str) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj.lower()
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k]:
                return str(obj[k]).lower()
        h = obj.get("hash")
        if h:
            return str(h).lower()
    return None


class BlockscoutDB:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self.init_schema()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> BlockscoutDB:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def init_schema(self) -> None:
        self._conn.executescript(SCHEMA_SQL)
        self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        self._conn.commit()

    # --- addresses -------------------------------------------------------------

    def upsert_address(
        self,
        data: dict[str, Any],
        *,
        chain_id: int = 1,
        label: str | None = None,
        watch_role: str | None = None,
    ) -> None:
        addr = _addr_field(data, "hash", "address") or ""
        if not addr:
            return
        balance = data.get("coin_balance") or data.get("coin_balance_raw")
        counters = data.get("counters") if isinstance(data.get("counters"), dict) else {}
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO addresses (
                    address, chain_id, is_contract, is_verified, name, ens_domain_name,
                    coin_balance_wei, coin_balance_eth, exchange_rate,
                    tx_count, token_transfer_count, gas_usage_count, validations_count,
                    label, watch_role, raw_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address, chain_id) DO UPDATE SET
                    is_contract=excluded.is_contract,
                    is_verified=excluded.is_verified,
                    name=COALESCE(excluded.name, addresses.name),
                    ens_domain_name=COALESCE(excluded.ens_domain_name, addresses.ens_domain_name),
                    coin_balance_wei=excluded.coin_balance_wei,
                    coin_balance_eth=excluded.coin_balance_eth,
                    exchange_rate=excluded.exchange_rate,
                    tx_count=COALESCE(excluded.tx_count, addresses.tx_count),
                    token_transfer_count=COALESCE(excluded.token_transfer_count, addresses.token_transfer_count),
                    gas_usage_count=COALESCE(excluded.gas_usage_count, addresses.gas_usage_count),
                    validations_count=COALESCE(excluded.validations_count, addresses.validations_count),
                    label=COALESCE(excluded.label, addresses.label),
                    watch_role=COALESCE(excluded.watch_role, addresses.watch_role),
                    raw_json=excluded.raw_json,
                    updated_at=excluded.updated_at
                """,
                (
                    addr,
                    chain_id,
                    1 if data.get("is_contract") else 0,
                    1 if data.get("is_verified") else 0,
                    data.get("name"),
                    data.get("ens_domain_name"),
                    str(balance) if balance is not None else None,
                    _wei_to_eth(balance),
                    float(data["exchange_rate"]) if data.get("exchange_rate") not in (None, "") else None,
                    int(counters.get("transactions_count") or data.get("transactions_count") or 0) or None,
                    int(counters.get("token_transfers_count") or 0) or None,
                    int(counters.get("gas_usage_count") or 0) or None,
                    int(counters.get("validations_count") or 0) or None,
                    label,
                    watch_role,
                    json.dumps(data),
                    _now_iso(),
                ),
            )

    def insert_counters(self, address: str, data: dict[str, Any], *, chain_id: int = 1) -> None:
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO address_counters (
                    address, chain_id, transactions_count, token_transfers_count,
                    gas_usage_count, validations_count, raw_json, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    address.lower(),
                    chain_id,
                    int(data.get("transactions_count") or 0) or None,
                    int(data.get("token_transfers_count") or 0) or None,
                    int(data.get("gas_usage_count") or 0) or None,
                    int(data.get("validations_count") or 0) or None,
                    json.dumps(data),
                    _now_iso(),
                ),
            )

    # --- transactions ----------------------------------------------------------

    def upsert_transactions(
        self,
        rows: list[dict[str, Any]],
        *,
        chain_id: int = 1,
        watched_address: str | None = None,
    ) -> int:
        if not rows:
            return 0
        now = _now_iso()
        sql = """
            INSERT OR IGNORE INTO transactions (
                tx_hash, chain_id, block_number, timestamp, from_address, to_address,
                value_wei, value_eth, fee_wei, status, method, gas_used, gas_price,
                watched_address, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        payload = []
        for r in rows:
            tx_hash = r.get("hash") or ""
            if not tx_hash:
                continue
            value = r.get("value")
            fee = None
            if isinstance(r.get("fee"), dict):
                fee = r["fee"].get("value")
            block = r.get("block")
            block_n = None
            if isinstance(block, int):
                block_n = block
            elif isinstance(block, str) and block.isdigit():
                block_n = int(block)
            elif isinstance(r.get("block_number"), (int, str)):
                try:
                    block_n = int(r["block_number"])
                except (TypeError, ValueError):
                    pass
            status = r.get("status")
            if status is True:
                status = "ok"
            elif status is False:
                status = "error"
            payload.append(
                (
                    tx_hash,
                    chain_id,
                    block_n,
                    r.get("timestamp"),
                    _addr_field(r.get("from"), "hash"),
                    _addr_field(r.get("to"), "hash"),
                    str(value) if value is not None else None,
                    _wei_to_eth(value),
                    str(fee) if fee is not None else None,
                    str(status) if status is not None else None,
                    r.get("method"),
                    int(r["gas_used"]) if r.get("gas_used") not in (None, "") else None,
                    str(r.get("gas_price")) if r.get("gas_price") is not None else None,
                    watched_address.lower() if watched_address else None,
                    json.dumps(r),
                    now,
                )
            )
        before = self._conn.total_changes
        with self.transaction():
            self._conn.executemany(sql, payload)
        return self._conn.total_changes - before

    def upsert_token_transfers(
        self,
        rows: list[dict[str, Any]],
        *,
        chain_id: int = 1,
        watched_address: str | None = None,
    ) -> int:
        if not rows:
            return 0
        now = _now_iso()
        sql = """
            INSERT OR IGNORE INTO token_transfers (
                id, tx_hash, chain_id, block_number, timestamp, from_address, to_address,
                token_address, token_symbol, token_name, token_type,
                total_value, total_decimals, value_normalized,
                watched_address, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        payload = []
        for i, r in enumerate(rows):
            tx_hash = r.get("tx_hash") or (r.get("transaction_hash")) or ""
            if not tx_hash and isinstance(r.get("transaction"), dict):
                tx_hash = r["transaction"].get("hash") or ""
            log_index = r.get("log_index")
            if log_index is None:
                log_index = r.get("index", i)
            row_id = f"{chain_id}:{tx_hash}:{log_index}"
            token = r.get("token") if isinstance(r.get("token"), dict) else {}
            total = r.get("total") if isinstance(r.get("total"), dict) else {}
            value_raw = total.get("value")
            decimals = total.get("decimals")
            try:
                dec = int(decimals) if decimals not in (None, "") else None
                val_norm = int(value_raw) / (10**dec) if value_raw is not None and dec is not None else None
            except (TypeError, ValueError, OverflowError):
                val_norm = None
            payload.append(
                (
                    row_id,
                    tx_hash,
                    chain_id,
                    int(r["block_number"]) if r.get("block_number") not in (None, "") else None,
                    r.get("timestamp"),
                    _addr_field(r.get("from"), "hash"),
                    _addr_field(r.get("to"), "hash"),
                    _addr_field(token, "address", "hash") or _addr_field(r.get("token"), "hash"),
                    token.get("symbol"),
                    token.get("name"),
                    token.get("type"),
                    str(value_raw) if value_raw is not None else None,
                    int(decimals) if decimals not in (None, "") else None,
                    val_norm,
                    watched_address.lower() if watched_address else None,
                    json.dumps(r),
                    now,
                )
            )
        before = self._conn.total_changes
        with self.transaction():
            self._conn.executemany(sql, payload)
        return self._conn.total_changes - before

    def upsert_blocks(self, rows: list[dict[str, Any]], *, chain_id: int = 1) -> int:
        if not rows:
            return 0
        now = _now_iso()
        sql = """
            INSERT OR IGNORE INTO blocks (
                block_number, chain_id, hash, timestamp, tx_count, miner_hash,
                size, gas_used, gas_limit, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        payload = []
        for r in rows:
            height = r.get("height")
            if height is None:
                height = r.get("number")
            if height is None:
                continue
            miner = r.get("miner")
            miner_hash = _addr_field(miner, "hash") if miner else None
            payload.append(
                (
                    int(height),
                    chain_id,
                    r.get("hash"),
                    r.get("timestamp"),
                    int(r["tx_count"]) if r.get("tx_count") not in (None, "") else None,
                    miner_hash,
                    int(r["size"]) if r.get("size") not in (None, "") else None,
                    int(r["gas_used"]) if r.get("gas_used") not in (None, "") else None,
                    int(r["gas_limit"]) if r.get("gas_limit") not in (None, "") else None,
                    json.dumps(r),
                    now,
                )
            )
        before = self._conn.total_changes
        with self.transaction():
            self._conn.executemany(sql, payload)
        return self._conn.total_changes - before

    def upsert_token(self, data: dict[str, Any], *, chain_id: int = 1) -> None:
        addr = _addr_field(data, "address", "hash") or ""
        if not addr:
            return
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO tokens (
                    address, chain_id, name, symbol, decimals, total_supply,
                    holders_count, exchange_rate, type, circulating_market_cap,
                    icon_url, raw_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address, chain_id) DO UPDATE SET
                    name=excluded.name, symbol=excluded.symbol, decimals=excluded.decimals,
                    total_supply=excluded.total_supply, holders_count=excluded.holders_count,
                    exchange_rate=excluded.exchange_rate, type=excluded.type,
                    circulating_market_cap=excluded.circulating_market_cap,
                    icon_url=excluded.icon_url, raw_json=excluded.raw_json,
                    updated_at=excluded.updated_at
                """,
                (
                    addr,
                    chain_id,
                    data.get("name"),
                    data.get("symbol"),
                    int(data["decimals"]) if data.get("decimals") not in (None, "") else None,
                    str(data.get("total_supply")) if data.get("total_supply") is not None else None,
                    int(data["holders_count"]) if data.get("holders_count") not in (None, "") else None,
                    float(data["exchange_rate"]) if data.get("exchange_rate") not in (None, "") else None,
                    data.get("type"),
                    float(data["circulating_market_cap"])
                    if data.get("circulating_market_cap") not in (None, "")
                    else None,
                    data.get("icon_url"),
                    json.dumps(data),
                    _now_iso(),
                ),
            )

    def upsert_token_holders(
        self,
        token_address: str,
        rows: list[dict[str, Any]],
        *,
        chain_id: int = 1,
    ) -> int:
        if not rows:
            return 0
        now = _now_iso()
        token_address = token_address.lower()
        sql = """
            INSERT OR REPLACE INTO token_holders (
                token_address, holder_address, chain_id, value, value_normalized,
                rank, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        payload = []
        for i, r in enumerate(rows):
            holder = _addr_field(r.get("address"), "hash") or _addr_field(r, "hash")
            if not holder:
                continue
            value = r.get("value")
            try:
                val_norm = float(value) if value is not None and "." in str(value) else _wei_to_eth(value)
            except (TypeError, ValueError):
                val_norm = None
            payload.append(
                (
                    token_address,
                    holder,
                    chain_id,
                    str(value) if value is not None else None,
                    val_norm,
                    int(r["rank"]) if r.get("rank") not in (None, "") else i + 1,
                    json.dumps(r),
                    now,
                )
            )
        with self.transaction():
            self._conn.executemany(sql, payload)
        return len(payload)

    def upsert_token_balances(
        self,
        owner: str,
        rows: list[dict[str, Any]],
        *,
        chain_id: int = 1,
    ) -> int:
        if not rows:
            return 0
        now = _now_iso()
        owner = owner.lower()
        sql = """
            INSERT OR REPLACE INTO token_balances (
                owner_address, token_address, chain_id, value, value_normalized,
                token_symbol, token_name, token_type, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        payload = []
        for r in rows:
            token = r.get("token") if isinstance(r.get("token"), dict) else {}
            token_addr = _addr_field(token, "address", "hash")
            if not token_addr:
                continue
            value = r.get("value")
            try:
                val_norm = float(r["value"]) if r.get("token_instance") else _wei_to_eth(value)
            except (TypeError, ValueError):
                val_norm = _wei_to_eth(value)
            payload.append(
                (
                    owner,
                    token_addr,
                    chain_id,
                    str(value) if value is not None else None,
                    val_norm,
                    token.get("symbol"),
                    token.get("name"),
                    token.get("type") or r.get("token_type"),
                    json.dumps(r),
                    now,
                )
            )
        with self.transaction():
            self._conn.executemany(sql, payload)
        return len(payload)

    def upsert_contract(self, address: str, data: dict[str, Any], *, chain_id: int = 1) -> None:
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO contracts (
                    address, chain_id, name, language, compiler_version,
                    optimization_enabled, is_verified, is_fully_verified,
                    source_code, abi_json, constructor_args, verified_at,
                    raw_json, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address, chain_id) DO UPDATE SET
                    name=excluded.name, language=excluded.language,
                    compiler_version=excluded.compiler_version,
                    optimization_enabled=excluded.optimization_enabled,
                    is_verified=excluded.is_verified,
                    is_fully_verified=excluded.is_fully_verified,
                    source_code=excluded.source_code, abi_json=excluded.abi_json,
                    constructor_args=excluded.constructor_args,
                    verified_at=excluded.verified_at, raw_json=excluded.raw_json,
                    fetched_at=excluded.fetched_at
                """,
                (
                    address.lower(),
                    chain_id,
                    data.get("name"),
                    data.get("language"),
                    data.get("compiler_version"),
                    1 if data.get("optimization_enabled") else 0,
                    1 if data.get("is_verified") else 0,
                    1 if data.get("is_fully_verified") else 0,
                    data.get("source_code"),
                    json.dumps(data.get("abi")) if data.get("abi") is not None else None,
                    data.get("constructor_args"),
                    data.get("verified_at"),
                    json.dumps(data),
                    _now_iso(),
                ),
            )

    def insert_network_stats(self, data: dict[str, Any], *, chain_id: int = 1) -> None:
        gas_prices = data.get("gas_prices")
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO network_stats (
                    chain_id, total_blocks, total_addresses, total_transactions,
                    average_block_time, coin_price, total_gas_used, transactions_today,
                    gas_prices_json, raw_json, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chain_id,
                    int(data["total_blocks"]) if data.get("total_blocks") not in (None, "") else None,
                    int(data["total_addresses"]) if data.get("total_addresses") not in (None, "") else None,
                    int(data["total_transactions"]) if data.get("total_transactions") not in (None, "") else None,
                    float(data["average_block_time"])
                    if data.get("average_block_time") not in (None, "")
                    else None,
                    float(data["coin_price"]) if data.get("coin_price") not in (None, "") else None,
                    int(data["total_gas_used"]) if data.get("total_gas_used") not in (None, "") else None,
                    int(data["transactions_today"]) if data.get("transactions_today") not in (None, "") else None,
                    json.dumps(gas_prices) if gas_prices is not None else None,
                    json.dumps(data),
                    _now_iso(),
                ),
            )

    def upsert_trader_score(
        self,
        address: str,
        *,
        chain_id: int,
        score: float,
        tx_count: int,
        success_rate: float,
        volume_eth: float,
        unique_counterparties: int,
        avg_value_eth: float,
        reasons: list[str],
        label: str | None = None,
    ) -> None:
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO trader_scores (
                    address, chain_id, score, tx_count, success_rate, volume_eth,
                    unique_counterparties, avg_value_eth, reasons, label, computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address, chain_id) DO UPDATE SET
                    score=excluded.score, tx_count=excluded.tx_count,
                    success_rate=excluded.success_rate, volume_eth=excluded.volume_eth,
                    unique_counterparties=excluded.unique_counterparties,
                    avg_value_eth=excluded.avg_value_eth, reasons=excluded.reasons,
                    label=COALESCE(excluded.label, trader_scores.label),
                    computed_at=excluded.computed_at
                """,
                (
                    address.lower(),
                    chain_id,
                    score,
                    tx_count,
                    success_rate,
                    volume_eth,
                    unique_counterparties,
                    avg_value_eth,
                    json.dumps(reasons),
                    label,
                    _now_iso(),
                ),
            )

    def has_whale_alert(self, tx_hash: str, chain_id: int = 1) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM whale_alerts WHERE tx_hash = ? AND chain_id = ?",
            (tx_hash, chain_id),
        ).fetchone()
        return row is not None

    def record_whale_alert(
        self,
        *,
        tx_hash: str,
        chain_id: int,
        value_eth: float,
        from_address: str | None,
        to_address: str | None,
        watched_address: str | None,
        label: str | None,
        timestamp: str | None,
    ) -> None:
        with self.transaction():
            self._conn.execute(
                """
                INSERT OR IGNORE INTO whale_alerts (
                    tx_hash, chain_id, value_eth, from_address, to_address,
                    watched_address, label, timestamp, alerted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tx_hash,
                    chain_id,
                    value_eth,
                    from_address,
                    to_address,
                    watched_address,
                    label,
                    timestamp,
                    _now_iso(),
                ),
            )

    def start_ingest_run(self, address: str | None, mode: str, *, chain_id: int | None = None) -> int:
        cur = self._conn.execute(
            """
            INSERT INTO ingest_runs (address, chain_id, mode, status, started_at)
            VALUES (?, ?, ?, 'running', ?)
            """,
            (address.lower() if address else None, chain_id, mode, _now_iso()),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def finish_ingest_run(self, run_id: int, *, status: str, detail: str | None = None) -> None:
        with self.transaction():
            self._conn.execute(
                """
                UPDATE ingest_runs SET status=?, detail=?, finished_at=? WHERE id=?
                """,
                (status, detail, _now_iso(), run_id),
            )

    def fetch_transactions(
        self,
        *,
        address: str | None = None,
        chain_id: int | None = None,
        min_value_eth: float | None = None,
        limit: int = 1000,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if address:
            a = address.lower()
            clauses.append("(from_address = ? OR to_address = ? OR watched_address = ?)")
            params.extend([a, a, a])
        if chain_id is not None:
            clauses.append("chain_id = ?")
            params.append(chain_id)
        if min_value_eth is not None:
            clauses.append("value_eth >= ?")
            params.append(min_value_eth)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return list(
            self._conn.execute(
                f"SELECT * FROM transactions {where} ORDER BY ingested_at DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        )

    def fetch_high_ev_traders(
        self,
        *,
        chain_id: int | None = None,
        min_score: float = 70.0,
        limit: int = 50,
    ) -> list[sqlite3.Row]:
        if chain_id is not None:
            return list(
                self._conn.execute(
                    """
                    SELECT * FROM trader_scores
                    WHERE chain_id = ? AND score >= ?
                    ORDER BY score DESC LIMIT ?
                    """,
                    (chain_id, min_score, limit),
                ).fetchall()
            )
        return list(
            self._conn.execute(
                """
                SELECT * FROM trader_scores
                WHERE score >= ?
                ORDER BY score DESC LIMIT ?
                """,
                (min_score, limit),
            ).fetchall()
        )

    def stats(self) -> dict[str, Any]:
        def _c(table: str) -> int:
            row = self._conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            return int(row["c"]) if row else 0

        return {
            "addresses": _c("addresses"),
            "transactions": _c("transactions"),
            "token_transfers": _c("token_transfers"),
            "blocks": _c("blocks"),
            "tokens": _c("tokens"),
            "token_holders": _c("token_holders"),
            "contracts": _c("contracts"),
            "network_stats": _c("network_stats"),
            "trader_scores": _c("trader_scores"),
            "whale_alerts": _c("whale_alerts"),
        }
