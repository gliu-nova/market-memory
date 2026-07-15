"""Unit tests for Etherscan ingestion (mocked network)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from market_memory.etherscan.alerts import (
    check_whale_alerts,
    format_whale_tweet,
    run_whale_hook,
)
from market_memory.etherscan.analysis import (
    detect_large_transfers,
    detect_volume_spikes,
    summarize_address_activity,
)
from market_memory.etherscan.chains import resolve_chain
from market_memory.etherscan.client import EtherscanAPIError, EtherscanClient
from market_memory.etherscan.config import EtherscanConfig
from market_memory.etherscan.db import EtherscanDB
from market_memory.etherscan.pipeline import run_ingest, run_ingest_entries
from market_memory.etherscan.watchlist import WatchEntry, load_watchlist, merge_cli_addresses


SAMPLE_TX = {
    "hash": "0xaaa111",
    "blockNumber": "19000000",
    "timeStamp": "1700000000",
    "from": "0xFromAddr",
    "to": "0xToAddr",
    "value": str(150 * 10**18),  # 150 ETH
    "gas": "21000",
    "gasPrice": "20000000000",
    "gasUsed": "21000",
    "isError": "0",
    "methodId": "0x",
    "functionName": "",
    "input": "0x",
}

SAMPLE_TX_SMALL = {
    **SAMPLE_TX,
    "hash": "0xbbb222",
    "value": str(1 * 10**18),
    "timeStamp": "1700003600",
}

SAMPLE_TOKEN = {
    "hash": "0xccc333",
    "logIndex": "5",
    "blockNumber": "19000001",
    "timeStamp": "1700007200",
    "from": "0xFromAddr",
    "to": "0xToAddr",
    "contractAddress": "0xToken",
    "tokenName": "USD Coin",
    "tokenSymbol": "USDC",
    "tokenDecimal": "6",
    "value": "1000000",  # 1 USDC
}


@pytest.fixture
def cfg(tmp_path: Path) -> EtherscanConfig:
    return EtherscanConfig(
        api_key="test-key",
        db_path=tmp_path / "test_etherscan.db",
        rate_limit_delay=0.0,
        large_transfer_eth=100.0,
    )


@pytest.fixture
def db(cfg: EtherscanConfig) -> EtherscanDB:
    database = EtherscanDB(cfg.db_path)
    yield database
    database.close()


def test_schema_created(db: EtherscanDB) -> None:
    tables = {
        r[0]
        for r in db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "transactions" in tables
    assert "token_transfers" in tables
    assert "balances" in tables
    assert "gas_oracle" in tables
    assert "whale_alerts" in tables


def test_upsert_transactions_idempotent(db: EtherscanDB) -> None:
    n1 = db.upsert_transactions([SAMPLE_TX, SAMPLE_TX_SMALL], watched_address="0xfromaddr")
    n2 = db.upsert_transactions([SAMPLE_TX], watched_address="0xfromaddr")
    assert n1 == 2
    assert n2 == 0
    assert db.count_transactions() == 2


def test_multichain_same_hash_allowed(db: EtherscanDB) -> None:
    """Same tx hash on different chains is stored separately."""
    n1 = db.upsert_transactions([SAMPLE_TX], chain_id=1, watched_address="0xfromaddr")
    n2 = db.upsert_transactions([SAMPLE_TX], chain_id=8453, watched_address="0xfromaddr")
    assert n1 == 1
    assert n2 == 1
    assert db.count_transactions(chain_id=1) == 1
    assert db.count_transactions(chain_id=8453) == 1


def test_upsert_token_transfers_idempotent(db: EtherscanDB) -> None:
    n1 = db.upsert_token_transfers([SAMPLE_TOKEN], watched_address="0xfromaddr")
    n2 = db.upsert_token_transfers([SAMPLE_TOKEN], watched_address="0xfromaddr")
    assert n1 == 1
    assert n2 == 0
    rows = db.fetch_token_transfers(address="0xfromaddr")
    assert len(rows) == 1
    assert rows[0]["value_normalized"] == pytest.approx(1.0)
    assert rows[0]["id"].startswith("1:")


def test_detect_large_transfers(db: EtherscanDB) -> None:
    db.upsert_transactions([SAMPLE_TX, SAMPLE_TX_SMALL], watched_address="0xfromaddr")
    large = detect_large_transfers(db, threshold_eth=100.0)
    assert len(large) == 1
    assert large[0].tx_hash == "0xaaa111"
    assert large[0].value_eth == pytest.approx(150.0)
    assert large[0].chain_id == 1


def test_whale_alerts_idempotent(db: EtherscanDB) -> None:
    db.upsert_transactions([SAMPLE_TX, SAMPLE_TX_SMALL], watched_address="0xfromaddr")
    a1 = run_whale_hook(db, threshold_eth=100.0, address="0xfromaddr", chain_id=1)
    a2 = run_whale_hook(db, threshold_eth=100.0, address="0xfromaddr", chain_id=1)
    assert len(a1) == 1
    assert len(a2) == 0
    assert db.has_whale_alert("0xaaa111", 1)
    tweet = format_whale_tweet(a1[0])
    assert "150.00" in tweet or "150" in tweet
    assert "Whale" in tweet or "🐋" in tweet


def test_summarize_and_volume_spikes(db: EtherscanDB) -> None:
    rows = []
    for i in range(10):
        rows.append(
            {
                **SAMPLE_TX_SMALL,
                "hash": f"0x{i:064x}",
                "timeStamp": str(1700000000 + i * 3600),
                "value": str(1 * 10**18),
                "from": "0xfromaddr",
                "to": "0xtoaddr",
            }
        )
    rows.append(
        {
            **SAMPLE_TX,
            "hash": "0xbig",
            "timeStamp": str(1700000000 + 5 * 3600),
            "value": str(500 * 10**18),
            "from": "0xfromaddr",
            "to": "0xtoaddr",
        }
    )
    db.upsert_transactions(rows, watched_address="0xfromaddr")

    summary = summarize_address_activity(db, "0xfromaddr")
    assert summary.tx_count == 11
    assert summary.total_value_eth_out > 0

    spikes = detect_volume_spikes(db, address="0xfromaddr", zscore_threshold=1.5)
    assert any(s.volume_eth >= 500 for s in spikes)


def test_client_list_result_handles_empty() -> None:
    cfg = EtherscanConfig(api_key="k", rate_limit_delay=0.0)
    client = EtherscanClient(cfg)
    with patch.object(client, "_request", return_value="No transactions found"):
        assert client.get_normal_transactions("0xabc") == []
    client.close()


def test_client_raises_on_hard_error() -> None:
    cfg = EtherscanConfig(api_key="k", rate_limit_delay=0.0, max_retries=1)
    client = EtherscanClient(cfg)

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "status": "0",
        "message": "NOTOK",
        "result": "Invalid API Key",
    }

    with patch.object(client._session, "get", return_value=mock_resp):
        with pytest.raises(EtherscanAPIError):
            client.get_balance("0xabc")
    client.close()


def test_run_ingest_with_mocks(cfg: EtherscanConfig, db: EtherscanDB) -> None:
    client = MagicMock(spec=EtherscanClient)
    client.get_gas_oracle.return_value = {
        "LastBlock": "19000000",
        "SafeGasPrice": "10",
        "ProposeGasPrice": "12",
        "FastGasPrice": "15",
        "suggestBaseFee": "9",
        "gasUsedRatio": "0.5",
    }
    client.get_block_number.return_value = 19000000
    client.get_balance.return_value = 10**18
    client.get_normal_transactions.return_value = [SAMPLE_TX]
    client.get_token_transfers.return_value = [SAMPLE_TOKEN]

    result = run_ingest(
        address="0xFromAddr",
        mode="recent",
        whale_alerts=True,
        config=cfg,
        db=db,
        client=client,
    )
    assert result.status == "ok"
    assert result.txs_fetched == 1
    assert result.txs_inserted == 1
    assert result.transfers_inserted == 1
    assert result.balance_wei == 10**18
    assert len(result.whale_alerts) == 1
    assert db.count_transactions("0xfromaddr") == 1

    result2 = run_ingest(
        address="0xFromAddr",
        mode="recent",
        whale_alerts=True,
        config=cfg,
        db=db,
        client=client,
    )
    assert result2.txs_inserted == 0
    assert result2.whale_alerts == []  # already alerted


def test_watchlist_yaml_and_txt(tmp_path: Path) -> None:
    yaml_path = tmp_path / "wl.yaml"
    yaml_path.write_text(
        """
defaults:
  chain: ethereum
addresses:
  - address: "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    label: vitalik
  - address: "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    label: vitalik-base
    chain: base
    large_transfer_eth: 50
""",
        encoding="utf-8",
    )
    try:
        wl = load_watchlist(yaml_path)
    except ImportError:
        pytest.skip("PyYAML not installed")
    assert len(wl.entries) == 2
    assert wl.entries[0].chain_id == 1
    assert wl.entries[1].chain_id == 8453
    assert wl.entries[1].large_transfer_eth == 50

    txt_path = tmp_path / "wl.txt"
    txt_path.write_text(
        "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045,base,bridge\n",
        encoding="utf-8",
    )
    wl2 = load_watchlist(txt_path)
    assert len(wl2.entries) == 1
    assert wl2.entries[0].chain_id == 8453
    assert wl2.entries[0].label == "bridge"


def test_merge_cli_addresses() -> None:
    wl_entries = [
        WatchEntry(address="0x" + "a" * 40, chain_id=1, chain_name="ethereum", label="a"),
    ]
    from market_memory.etherscan.watchlist import Watchlist

    merged = merge_cli_addresses(
        ["0x" + "b" * 40],
        Watchlist(entries=wl_entries),
        chain_id=1,
    )
    assert len(merged) == 2


def test_resolve_chain() -> None:
    assert resolve_chain("base").chain_id == 8453
    assert resolve_chain(1).name == "ethereum"
    assert resolve_chain("8453").chain_id == 8453
    with pytest.raises(ValueError):
        resolve_chain("not-a-chain")


def test_run_ingest_entries_multichain(cfg: EtherscanConfig, db: EtherscanDB) -> None:
    client = MagicMock(spec=EtherscanClient)
    client.get_gas_oracle.return_value = {"SafeGasPrice": "1", "ProposeGasPrice": "2", "FastGasPrice": "3"}
    client.get_block_number.return_value = 1
    client.get_balance.return_value = 0
    client.get_normal_transactions.return_value = [SAMPLE_TX]
    client.get_token_transfers.return_value = []

    entries = [
        WatchEntry(address="0x" + "c" * 40, chain_id=1, chain_name="ethereum", label="main"),
        WatchEntry(address="0x" + "c" * 40, chain_id=8453, chain_name="base", label="base"),
    ]
    # Patch EtherscanClient construction inside run_ingest_entries
    with patch("market_memory.etherscan.pipeline.EtherscanClient", return_value=client):
        results = run_ingest_entries(entries, mode="recent", whale_alerts=False, config=cfg, db=db)
    assert len(results) == 2
    assert results[0].chain_id == 1
    assert results[1].chain_id == 8453


def test_cli_help() -> None:
    from market_memory.etherscan.cli import build_parser

    parser = build_parser()
    help_text = parser.format_help()
    assert "--address" in help_text
    assert "--watchlist" in help_text
    assert "--whale-alerts" in help_text
    assert "--chain" in help_text
