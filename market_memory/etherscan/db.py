"""SQLite storage for raw + lightly processed Etherscan data.

Idempotent inserts keyed by (tx_hash, chain_id) — multi-chain ready.
Schema version 2 adds composite keys + whale_alerts table.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from market_memory.etherscan.alerts import WhaleAlert

SCHEMA_VERSION = 2

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    tx_hash TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    block_number INTEGER NOT NULL,
    time_stamp INTEGER NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT,
    value_wei TEXT NOT NULL,
    value_eth REAL NOT NULL,
    gas INTEGER,
    gas_price TEXT,
    gas_used INTEGER,
    is_error INTEGER DEFAULT 0,
    method_id TEXT,
    function_name TEXT,
    input_data TEXT,
    watched_address TEXT,
    raw_json TEXT,
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (tx_hash, chain_id)
);

CREATE INDEX IF NOT EXISTS idx_tx_block ON transactions(chain_id, block_number);
CREATE INDEX IF NOT EXISTS idx_tx_from ON transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_tx_to ON transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_tx_time ON transactions(time_stamp);
CREATE INDEX IF NOT EXISTS idx_tx_watched ON transactions(watched_address);
CREATE INDEX IF NOT EXISTS idx_tx_value ON transactions(value_eth);

CREATE TABLE IF NOT EXISTS token_transfers (
    id TEXT PRIMARY KEY,  -- {chain_id}:{tx_hash}:{log_index}
    tx_hash TEXT NOT NULL,
    log_index INTEGER,
    block_number INTEGER NOT NULL,
    time_stamp INTEGER NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    token_name TEXT,
    token_symbol TEXT,
    token_decimal INTEGER,
    value_raw TEXT NOT NULL,
    value_normalized REAL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    watched_address TEXT,
    raw_json TEXT,
    ingested_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tt_hash ON token_transfers(tx_hash);
CREATE INDEX IF NOT EXISTS idx_tt_contract ON token_transfers(contract_address);
CREATE INDEX IF NOT EXISTS idx_tt_time ON token_transfers(time_stamp);
CREATE INDEX IF NOT EXISTS idx_tt_watched ON token_transfers(watched_address);
CREATE INDEX IF NOT EXISTS idx_tt_chain ON token_transfers(chain_id);

CREATE TABLE IF NOT EXISTS balances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    balance_wei TEXT NOT NULL,
    balance_eth REAL NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    recorded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bal_addr ON balances(address, chain_id, recorded_at);

CREATE TABLE IF NOT EXISTS gas_oracle (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id INTEGER NOT NULL DEFAULT 1,
    last_block TEXT,
    safe_gas_price REAL,
    propose_gas_price REAL,
    fast_gas_price REAL,
    suggest_base_fee REAL,
    gas_used_ratio TEXT,
    raw_json TEXT,
    recorded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gas_chain ON gas_oracle(chain_id, recorded_at);

CREATE TABLE IF NOT EXISTS blocks (
    block_number INTEGER NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    noted_at TEXT NOT NULL,
    PRIMARY KEY (block_number, chain_id)
);

CREATE TABLE IF NOT EXISTS contracts (
    address TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    abi_json TEXT,
    source_json TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (address, chain_id)
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT,
    chain_id INTEGER,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    txs_inserted INTEGER DEFAULT 0,
    transfers_inserted INTEGER DEFAULT 0,
    detail TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS whale_alerts (
    tx_hash TEXT NOT NULL,
    chain_id INTEGER NOT NULL DEFAULT 1,
    value_eth REAL NOT NULL,
    from_address TEXT,
    to_address TEXT,
    watched_address TEXT,
    label TEXT,
    time_stamp INTEGER,
    explorer_url TEXT,
    alerted_at TEXT NOT NULL,
    PRIMARY KEY (tx_hash, chain_id)
);

CREATE INDEX IF NOT EXISTS idx_whale_time ON whale_alerts(alerted_at);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _wei_to_eth(value_wei: str | int) -> float:
    try:
        return int(value_wei) / 1e18
    except (TypeError, ValueError):
        return 0.0


def _token_normalized(value_raw: str, decimals: str | int | None) -> float | None:
    try:
        dec = int(decimals) if decimals is not None and str(decimals) != "" else 18
        return int(value_raw) / (10**dec)
    except (TypeError, ValueError, OverflowError):
        return None


class EtherscanDB:
    """SQLite facade with create-if-needed schema and idempotent upserts."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self.init_schema()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> EtherscanDB:
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
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version < SCHEMA_VERSION:
            self._migrate(version)
        self._conn.executescript(SCHEMA_SQL)
        self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        self._conn.commit()

    def _migrate(self, from_version: int) -> None:
        """Migrate legacy v1 single-chain tables to composite multi-chain keys."""
        if from_version >= SCHEMA_VERSION:
            return
        tables = {
            r[0]
            for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "transactions" not in tables:
            return

        pk_cols = [
            r[1]
            for r in self._conn.execute("PRAGMA table_info(transactions)").fetchall()
            if r[5]  # pk ordinal > 0
        ]
        if pk_cols == ["tx_hash"]:
            logger.info(
                "Migrating etherscan.db schema v%s → v%s (multi-chain)",
                from_version,
                SCHEMA_VERSION,
            )
            self._rebuild_legacy_tables()
        elif "whale_alerts" not in tables:
            logger.info("Upgrading schema: ensuring whale_alerts and indexes")

    def _rebuild_legacy_tables(self) -> None:
        """Copy v1 data into v2 multi-chain tables."""
        self._conn.executescript(
            """
            ALTER TABLE transactions RENAME TO transactions_v1;
            ALTER TABLE token_transfers RENAME TO token_transfers_v1;
            ALTER TABLE blocks RENAME TO blocks_v1;
            ALTER TABLE contracts RENAME TO contracts_v1;
            """
        )
        # gas_oracle / balances / ingest_runs keep autoincrement ids — add chain_id if missing
        for table in ("gas_oracle", "balances", "ingest_runs"):
            try:
                cols = {r[1] for r in self._conn.execute(f"PRAGMA table_info({table})").fetchall()}
                if "chain_id" not in cols:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN chain_id INTEGER DEFAULT 1")
            except sqlite3.Error:
                pass

        self._conn.executescript(SCHEMA_SQL)
        self._conn.execute(
            """
            INSERT OR IGNORE INTO transactions (
                tx_hash, chain_id, block_number, time_stamp, from_address, to_address,
                value_wei, value_eth, gas, gas_price, gas_used, is_error,
                method_id, function_name, input_data, watched_address, raw_json, ingested_at
            )
            SELECT
                tx_hash, COALESCE(chain_id, 1), block_number, time_stamp, from_address, to_address,
                value_wei, value_eth, gas, gas_price, gas_used, is_error,
                method_id, function_name, input_data, watched_address, raw_json, ingested_at
            FROM transactions_v1
            """
        )
        self._conn.execute(
            """
            INSERT OR IGNORE INTO token_transfers (
                id, tx_hash, log_index, block_number, time_stamp,
                from_address, to_address, contract_address,
                token_name, token_symbol, token_decimal,
                value_raw, value_normalized, chain_id, watched_address, raw_json, ingested_at
            )
            SELECT
                printf('%s:%s', COALESCE(chain_id, 1), id),
                tx_hash, log_index, block_number, time_stamp,
                from_address, to_address, contract_address,
                token_name, token_symbol, token_decimal,
                value_raw, value_normalized, COALESCE(chain_id, 1), watched_address, raw_json, ingested_at
            FROM token_transfers_v1
            """
        )
        self._conn.execute(
            """
            INSERT OR IGNORE INTO blocks (block_number, chain_id, noted_at)
            SELECT block_number, COALESCE(chain_id, 1), noted_at FROM blocks_v1
            """
        )
        self._conn.execute(
            """
            INSERT OR IGNORE INTO contracts (address, chain_id, abi_json, source_json, fetched_at)
            SELECT address, COALESCE(chain_id, 1), abi_json, source_json, fetched_at FROM contracts_v1
            """
        )
        self._conn.executescript(
            """
            DROP TABLE IF EXISTS transactions_v1;
            DROP TABLE IF EXISTS token_transfers_v1;
            DROP TABLE IF EXISTS blocks_v1;
            DROP TABLE IF EXISTS contracts_v1;
            """
        )
        self._conn.commit()
        logger.info("Schema migration complete")

    # --- inserts (idempotent) --------------------------------------------------

    def upsert_transactions(
        self,
        rows: list[dict[str, Any]],
        *,
        chain_id: int = 1,
        watched_address: str | None = None,
    ) -> int:
        """Insert normal txs; skip existing (tx_hash, chain_id). Returns new row count."""
        if not rows:
            return 0
        now = _now_iso()
        sql = """
            INSERT OR IGNORE INTO transactions (
                tx_hash, chain_id, block_number, time_stamp, from_address, to_address,
                value_wei, value_eth, gas, gas_price, gas_used, is_error,
                method_id, function_name, input_data, watched_address,
                raw_json, ingested_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?
            )
        """
        payload = []
        for r in rows:
            value_wei = r.get("value", "0")
            payload.append(
                (
                    r.get("hash"),
                    chain_id,
                    int(r.get("blockNumber") or 0),
                    int(r.get("timeStamp") or 0),
                    (r.get("from") or "").lower(),
                    (r.get("to") or "").lower() if r.get("to") else None,
                    str(value_wei),
                    _wei_to_eth(value_wei),
                    int(r["gas"]) if r.get("gas") not in (None, "") else None,
                    str(r.get("gasPrice")) if r.get("gasPrice") is not None else None,
                    int(r["gasUsed"]) if r.get("gasUsed") not in (None, "") else None,
                    int(r.get("isError") or 0),
                    r.get("methodId"),
                    r.get("functionName"),
                    r.get("input"),
                    watched_address.lower() if watched_address else None,
                    json.dumps(r),
                    now,
                )
            )
        before = self._conn.total_changes
        with self.transaction():
            self._conn.executemany(sql, payload)
        inserted = self._conn.total_changes - before
        logger.info("transactions: attempted=%s inserted=%s chain=%s", len(payload), inserted, chain_id)
        return inserted

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
                id, tx_hash, log_index, block_number, time_stamp,
                from_address, to_address, contract_address,
                token_name, token_symbol, token_decimal,
                value_raw, value_normalized, chain_id, watched_address,
                raw_json, ingested_at
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?
            )
        """
        payload = []
        for r in rows:
            tx_hash = r.get("hash") or ""
            log_index = int(r.get("logIndex") or 0)
            row_id = f"{chain_id}:{tx_hash}:{log_index}"
            value_raw = r.get("value", "0")
            decimals = r.get("tokenDecimal")
            payload.append(
                (
                    row_id,
                    tx_hash,
                    log_index,
                    int(r.get("blockNumber") or 0),
                    int(r.get("timeStamp") or 0),
                    (r.get("from") or "").lower(),
                    (r.get("to") or "").lower(),
                    (r.get("contractAddress") or "").lower(),
                    r.get("tokenName"),
                    r.get("tokenSymbol"),
                    int(decimals) if decimals not in (None, "") else None,
                    str(value_raw),
                    _token_normalized(value_raw, decimals),
                    chain_id,
                    watched_address.lower() if watched_address else None,
                    json.dumps(r),
                    now,
                )
            )
        before = self._conn.total_changes
        with self.transaction():
            self._conn.executemany(sql, payload)
        inserted = self._conn.total_changes - before
        logger.info("token_transfers: attempted=%s inserted=%s chain=%s", len(payload), inserted, chain_id)
        return inserted

    def insert_balance(
        self,
        address: str,
        balance_wei: int | str,
        *,
        chain_id: int = 1,
    ) -> None:
        now = _now_iso()
        wei = str(balance_wei)
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO balances (address, balance_wei, balance_eth, chain_id, recorded_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (address.lower(), wei, _wei_to_eth(wei), chain_id, now),
            )

    def insert_gas_oracle(self, data: dict[str, Any], *, chain_id: int = 1) -> None:
        now = _now_iso()

        def _f(key: str) -> float | None:
            v = data.get(key)
            if v is None or v == "":
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO gas_oracle (
                    chain_id, last_block, safe_gas_price, propose_gas_price, fast_gas_price,
                    suggest_base_fee, gas_used_ratio, raw_json, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chain_id,
                    data.get("LastBlock"),
                    _f("SafeGasPrice"),
                    _f("ProposeGasPrice"),
                    _f("FastGasPrice"),
                    _f("suggestBaseFee"),
                    data.get("gasUsedRatio"),
                    json.dumps(data),
                    now,
                ),
            )

    def upsert_block(self, block_number: int, *, chain_id: int = 1) -> None:
        with self.transaction():
            self._conn.execute(
                """
                INSERT OR IGNORE INTO blocks (block_number, chain_id, noted_at)
                VALUES (?, ?, ?)
                """,
                (block_number, chain_id, _now_iso()),
            )

    def upsert_contract(
        self,
        address: str,
        *,
        abi_json: str | None = None,
        source_json: str | None = None,
        chain_id: int = 1,
    ) -> None:
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO contracts (address, chain_id, abi_json, source_json, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(address, chain_id) DO UPDATE SET
                    abi_json=COALESCE(excluded.abi_json, contracts.abi_json),
                    source_json=COALESCE(excluded.source_json, contracts.source_json),
                    fetched_at=excluded.fetched_at
                """,
                (address.lower(), chain_id, abi_json, source_json, _now_iso()),
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

    def finish_ingest_run(
        self,
        run_id: int,
        *,
        status: str,
        txs_inserted: int = 0,
        transfers_inserted: int = 0,
        detail: str | None = None,
    ) -> None:
        with self.transaction():
            self._conn.execute(
                """
                UPDATE ingest_runs
                SET status=?, txs_inserted=?, transfers_inserted=?, detail=?, finished_at=?
                WHERE id=?
                """,
                (status, txs_inserted, transfers_inserted, detail, _now_iso(), run_id),
            )

    # --- whale alerts ----------------------------------------------------------

    def has_whale_alert(self, tx_hash: str, chain_id: int = 1) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM whale_alerts WHERE tx_hash = ? AND chain_id = ?",
            (tx_hash, chain_id),
        ).fetchone()
        return row is not None

    def record_whale_alert(self, alert: "WhaleAlert") -> None:
        with self.transaction():
            self._conn.execute(
                """
                INSERT OR IGNORE INTO whale_alerts (
                    tx_hash, chain_id, value_eth, from_address, to_address,
                    watched_address, label, time_stamp, explorer_url, alerted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alert.tx_hash,
                    alert.chain_id,
                    alert.value_eth,
                    alert.from_address,
                    alert.to_address,
                    alert.watched_address,
                    alert.label,
                    alert.time_stamp,
                    alert.explorer_url,
                    _now_iso(),
                ),
            )

    def fetch_whale_alerts(
        self,
        *,
        chain_id: int | None = None,
        limit: int = 100,
    ) -> list[sqlite3.Row]:
        if chain_id is not None:
            return list(
                self._conn.execute(
                    """
                    SELECT * FROM whale_alerts
                    WHERE chain_id = ?
                    ORDER BY alerted_at DESC
                    LIMIT ?
                    """,
                    (chain_id, limit),
                ).fetchall()
            )
        return list(
            self._conn.execute(
                "SELECT * FROM whale_alerts ORDER BY alerted_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        )

    # --- queries for analysis / dashboard --------------------------------------

    def fetch_transactions(
        self,
        *,
        address: str | None = None,
        since_ts: int | None = None,
        min_value_eth: float | None = None,
        chain_id: int | None = None,
        limit: int = 1000,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if address:
            addr = address.lower()
            clauses.append("(from_address = ? OR to_address = ? OR watched_address = ?)")
            params.extend([addr, addr, addr])
        if since_ts is not None:
            clauses.append("time_stamp >= ?")
            params.append(since_ts)
        if min_value_eth is not None:
            clauses.append("value_eth >= ?")
            params.append(min_value_eth)
        if chain_id is not None:
            clauses.append("chain_id = ?")
            params.append(chain_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT * FROM transactions
            {where}
            ORDER BY time_stamp DESC
            LIMIT ?
        """
        params.append(limit)
        return list(self._conn.execute(sql, params).fetchall())

    def fetch_token_transfers(
        self,
        *,
        address: str | None = None,
        since_ts: int | None = None,
        chain_id: int | None = None,
        limit: int = 1000,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if address:
            addr = address.lower()
            clauses.append("(from_address = ? OR to_address = ? OR watched_address = ?)")
            params.extend([addr, addr, addr])
        if since_ts is not None:
            clauses.append("time_stamp >= ?")
            params.append(since_ts)
        if chain_id is not None:
            clauses.append("chain_id = ?")
            params.append(chain_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT * FROM token_transfers
            {where}
            ORDER BY time_stamp DESC
            LIMIT ?
        """
        params.append(limit)
        return list(self._conn.execute(sql, params).fetchall())

    def fetch_latest_gas(self, *, chain_id: int | None = None) -> sqlite3.Row | None:
        if chain_id is not None:
            return self._conn.execute(
                """
                SELECT * FROM gas_oracle WHERE chain_id = ?
                ORDER BY id DESC LIMIT 1
                """,
                (chain_id,),
            ).fetchone()
        return self._conn.execute(
            "SELECT * FROM gas_oracle ORDER BY id DESC LIMIT 1"
        ).fetchone()

    def fetch_latest_balances(self, *, chain_id: int | None = None, limit: int = 50) -> list[sqlite3.Row]:
        if chain_id is not None:
            return list(
                self._conn.execute(
                    """
                    SELECT b.* FROM balances b
                    INNER JOIN (
                        SELECT address, chain_id, MAX(id) AS max_id
                        FROM balances WHERE chain_id = ?
                        GROUP BY address, chain_id
                    ) t ON b.id = t.max_id
                    ORDER BY b.balance_eth DESC
                    LIMIT ?
                    """,
                    (chain_id, limit),
                ).fetchall()
            )
        return list(
            self._conn.execute(
                """
                SELECT b.* FROM balances b
                INNER JOIN (
                    SELECT address, chain_id, MAX(id) AS max_id
                    FROM balances
                    GROUP BY address, chain_id
                ) t ON b.id = t.max_id
                ORDER BY b.balance_eth DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        )

    def stats(self) -> dict[str, Any]:
        def _c(table: str) -> int:
            row = self._conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            return int(row["c"]) if row else 0

        chains = [
            r[0]
            for r in self._conn.execute(
                "SELECT DISTINCT chain_id FROM transactions ORDER BY chain_id"
            ).fetchall()
        ]
        return {
            "transactions": _c("transactions"),
            "token_transfers": _c("token_transfers"),
            "balances": _c("balances"),
            "gas_oracle": _c("gas_oracle"),
            "whale_alerts": _c("whale_alerts"),
            "ingest_runs": _c("ingest_runs"),
            "chains": chains,
        }

    def count_transactions(self, address: str | None = None, *, chain_id: int | None = None) -> int:
        clauses: list[str] = []
        params: list[Any] = []
        if address:
            addr = address.lower()
            clauses.append("(from_address = ? OR to_address = ? OR watched_address = ?)")
            params.extend([addr, addr, addr])
        if chain_id is not None:
            clauses.append("chain_id = ?")
            params.append(chain_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        row = self._conn.execute(f"SELECT COUNT(*) AS c FROM transactions {where}", params).fetchone()
        return int(row["c"]) if row else 0

    def latest_tx_block(self, address: str, *, chain_id: int | None = None) -> int | None:
        addr = address.lower()
        if chain_id is not None:
            row = self._conn.execute(
                """
                SELECT MAX(block_number) AS b FROM transactions
                WHERE (from_address = ? OR to_address = ? OR watched_address = ?)
                  AND chain_id = ?
                """,
                (addr, addr, addr, chain_id),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT MAX(block_number) AS b FROM transactions
                WHERE from_address = ? OR to_address = ? OR watched_address = ?
                """,
                (addr, addr, addr),
            ).fetchone()
        return int(row["b"]) if row and row["b"] is not None else None

    def watched_addresses(self) -> list[str]:
        rows = self._conn.execute(
            """
            SELECT DISTINCT watched_address FROM transactions
            WHERE watched_address IS NOT NULL
            ORDER BY watched_address
            """
        ).fetchall()
        return [r[0] for r in rows if r[0]]
