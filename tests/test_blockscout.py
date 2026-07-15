"""Unit tests for Blockscout ingestion (mocked network)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from market_memory.blockscout.analysis import detect_large_transfers, score_trader_activity
from market_memory.blockscout.client import BlockscoutAPIError, BlockscoutClient
from market_memory.blockscout.config import BlockscoutConfig
from market_memory.blockscout.db import BlockscoutDB
from market_memory.blockscout.pipeline import run_ingest


SAMPLE_ADDR = {
    "hash": "0xfromaddr",
    "is_contract": False,
    "is_verified": False,
    "coin_balance": str(10 * 10**18),
    "name": None,
    "ens_domain_name": None,
    "exchange_rate": "3000",
}

SAMPLE_TX = {
    "hash": "0xtxwhale",
    "block_number": 19000000,
    "timestamp": "2024-01-01T00:00:00.000000Z",
    "from": {"hash": "0xfromaddr"},
    "to": {"hash": "0xtoaddr"},
    "value": str(200 * 10**18),
    "status": "ok",
    "method": None,
    "gas_used": "21000",
    "fee": {"value": "100000000000000"},
}

SAMPLE_TX_SMALL = {
    **SAMPLE_TX,
    "hash": "0xtxsmall",
    "value": str(1 * 10**18),
}

SAMPLE_STATS = {
    "total_blocks": "19000000",
    "total_addresses": "200000000",
    "total_transactions": "2500000000",
    "average_block_time": 12.1,
    "coin_price": "3000.5",
    "transactions_today": "1000000",
    "gas_prices": {"slow": 10, "average": 12, "fast": 15},
}


@pytest.fixture
def cfg(tmp_path: Path) -> BlockscoutConfig:
    return BlockscoutConfig(
        api_key="test-key",
        db_path=tmp_path / "blockscout.db",
        rate_limit_delay=0.0,
        large_transfer_eth=100.0,
        high_ev_min_score=50.0,
    )


@pytest.fixture
def db(cfg: BlockscoutConfig) -> BlockscoutDB:
    database = BlockscoutDB(cfg.db_path)
    yield database
    database.close()


def test_schema(db: BlockscoutDB) -> None:
    tables = {
        r[0]
        for r in db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    for t in (
        "addresses",
        "transactions",
        "token_transfers",
        "blocks",
        "tokens",
        "token_holders",
        "contracts",
        "network_stats",
        "trader_scores",
        "whale_alerts",
    ):
        assert t in tables


def test_upsert_tx_idempotent(db: BlockscoutDB) -> None:
    n1 = db.upsert_transactions([SAMPLE_TX, SAMPLE_TX_SMALL], watched_address="0xfromaddr")
    n2 = db.upsert_transactions([SAMPLE_TX], watched_address="0xfromaddr")
    assert n1 == 2
    assert n2 == 0


def test_detect_whales(db: BlockscoutDB) -> None:
    db.upsert_transactions([SAMPLE_TX, SAMPLE_TX_SMALL], watched_address="0xfromaddr")
    whales = detect_large_transfers(db, threshold_eth=100.0)
    assert len(whales) == 1
    assert whales[0].tx_hash == "0xtxwhale"
    assert whales[0].value_eth == pytest.approx(200.0)


def test_trader_score(db: BlockscoutDB) -> None:
    rows = []
    for i in range(30):
        rows.append(
            {
                **SAMPLE_TX_SMALL,
                "hash": f"0x{i:064x}",
                "value": str(5 * 10**18),
                "status": "ok",
                "from": {"hash": "0xtrader"},
                "to": {"hash": f"0xcp{i:040x}"[-42:] if False else f"0x{'%040x' % i}"},
            }
        )
    db.upsert_transactions(rows, watched_address="0xtrader")
    score = score_trader_activity(db, "0xtrader", chain_id=1)
    assert score.tx_count == 30
    assert score.success_rate >= 0.9
    assert score.score > 0
    traders = db.fetch_high_ev_traders(min_score=0)
    assert any(t["address"] == "0xtrader" for t in traders)


def test_run_ingest_account(cfg: BlockscoutConfig, db: BlockscoutDB) -> None:
    client = MagicMock(spec=BlockscoutClient)
    client.get_stats.return_value = SAMPLE_STATS
    client.get_address.return_value = SAMPLE_ADDR
    client.get_address_counters.return_value = {
        "transactions_count": "10",
        "token_transfers_count": "2",
        "gas_usage_count": "100000",
        "validations_count": "0",
    }
    client.get_address_transactions.return_value = [SAMPLE_TX, SAMPLE_TX_SMALL]
    client.get_address_token_transfers.return_value = []
    client.get_address_tokens.return_value = []
    client.get_smart_contract.side_effect = BlockscoutAPIError("Not found")

    result = run_ingest(
        address="0xfromaddr",
        mode="account",
        label="test-whale",
        role="whale",
        config=cfg,
        db=db,
        client=client,
    )
    assert result.status == "ok"
    assert result.txs_inserted == 2
    assert result.stats_saved
    assert result.trader_score is not None
    assert len(result.whales) == 1


def test_run_ingest_stats(cfg: BlockscoutConfig, db: BlockscoutDB) -> None:
    client = MagicMock(spec=BlockscoutClient)
    client.get_stats.return_value = SAMPLE_STATS
    result = run_ingest(mode="stats", config=cfg, db=db, client=client)
    assert result.status == "ok"
    assert result.stats_saved
    assert db.stats()["network_stats"] == 1


def test_client_paginate() -> None:
    cfg = BlockscoutConfig(api_key="k", rate_limit_delay=0.0, max_pages=3)
    client = BlockscoutClient(cfg)
    pages = [
        {"items": [{"hash": "a"}], "next_page_params": {"page": 2}},
        {"items": [{"hash": "b"}], "next_page_params": None},
    ]
    with patch.object(client, "_get_json", side_effect=pages):
        items = client._paginate("/transactions")
    assert len(items) == 2
    client.close()


def test_cli_help() -> None:
    from market_memory.blockscout.cli import build_parser

    help_text = build_parser().format_help()
    assert "--mode" in help_text
    assert "account" in help_text
