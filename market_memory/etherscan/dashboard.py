"""Streamlit dashboard over data/etherscan.db.

Run:
  streamlit run market_memory/etherscan/dashboard.py
  # or
  python -m market_memory.etherscan.dashboard
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is importable when launched via streamlit
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _require_streamlit():
    try:
        import streamlit as st
    except ImportError as exc:
        raise SystemExit(
            "Streamlit is required for the dashboard.\n"
            "  pip install 'market-memory[dashboard]'   # or: pip install streamlit"
        ) from exc
    return st


def main() -> None:
    st = _require_streamlit()
    import pandas as pd

    from market_memory.etherscan.analysis import (
        detect_large_transfers,
        detect_volume_spikes,
        large_transfers_dataframe,
        summarize_address_activity,
    )
    from market_memory.etherscan.chains import list_chains, resolve_chain
    from market_memory.etherscan.config import load_etherscan_config
    from market_memory.etherscan.db import EtherscanDB

    st.set_page_config(page_title="Market Memory · On-chain", layout="wide")
    st.title("On-chain explorer")
    st.caption("Reads local SQLite from the Etherscan ingestion pipeline")

    try:
        cfg = load_etherscan_config()
        default_db = str(cfg.db_path)
        default_threshold = cfg.large_transfer_eth
    except ValueError:
        default_db = str(_ROOT / "data" / "etherscan.db")
        default_threshold = 100.0

    with st.sidebar:
        st.header("Settings")
        db_path = st.text_input("SQLite path", value=default_db)
        chain_options = ["all"] + [f"{c.name} ({c.chain_id})" for c in list_chains()]
        chain_choice = st.selectbox("Chain filter", chain_options, index=0)
        threshold = st.number_input("Whale threshold (ETH)", min_value=0.0, value=float(default_threshold))
        limit = st.slider("Row limit", min_value=50, max_value=5000, value=500, step=50)

    if not Path(db_path).is_file():
        st.warning(f"Database not found: `{db_path}`\n\nRun an ingest first, e.g.\n```\npython ingest.py --watchlist data/watchlist.yaml --mode recent\n```")
        return

    chain_id = None
    if chain_choice != "all":
        # "ethereum (1)" -> resolve
        name = chain_choice.split("(")[0].strip()
        chain_id = resolve_chain(name).chain_id

    db = EtherscanDB(db_path)
    try:
        stats = db.stats()
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Transactions", stats["transactions"])
        c2.metric("Token transfers", stats["token_transfers"])
        c3.metric("Whale alerts", stats["whale_alerts"])
        c4.metric("Gas snapshots", stats["gas_oracle"])
        c5.metric("Chains in DB", len(stats["chains"]) or 0)

        gas = db.fetch_latest_gas(chain_id=chain_id)
        if gas:
            st.subheader("Latest gas oracle")
            g1, g2, g3, g4 = st.columns(4)
            g1.metric("Safe", gas["safe_gas_price"])
            g2.metric("Propose", gas["propose_gas_price"])
            g3.metric("Fast", gas["fast_gas_price"])
            g4.metric("Base fee", gas["suggest_base_fee"])

        tab_tx, tab_whales, tab_tokens, tab_addr, tab_spikes = st.tabs(
            ["Transactions", "Whale alerts", "Token transfers", "Address", "Volume spikes"]
        )

        with tab_tx:
            rows = db.fetch_transactions(chain_id=chain_id, min_value_eth=None, limit=limit)
            df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
            if df.empty:
                st.info("No transactions yet.")
            else:
                if "time_stamp" in df.columns:
                    df["time_utc"] = pd.to_datetime(df["time_stamp"], unit="s", utc=True)
                cols = [
                    c
                    for c in [
                        "time_utc",
                        "value_eth",
                        "from_address",
                        "to_address",
                        "tx_hash",
                        "chain_id",
                        "watched_address",
                        "block_number",
                    ]
                    if c in df.columns
                ]
                st.dataframe(df[cols], use_container_width=True, height=420)

        with tab_whales:
            st.write(f"Transfers ≥ **{threshold} ETH** (analysis view) and fired alert log")
            large_df = large_transfers_dataframe(db, threshold_eth=threshold, chain_id=chain_id)
            if large_df.empty:
                st.info("No large transfers above threshold.")
            else:
                st.dataframe(large_df, use_container_width=True, height=280)
            alert_rows = db.fetch_whale_alerts(chain_id=chain_id, limit=limit)
            alert_df = pd.DataFrame([dict(r) for r in alert_rows]) if alert_rows else pd.DataFrame()
            st.markdown("#### Fired whale alerts (idempotent log)")
            if alert_df.empty:
                st.info("No whale alerts recorded yet. Re-ingest with `--whale-alerts`.")
            else:
                st.dataframe(alert_df, use_container_width=True, height=280)

        with tab_tokens:
            rows = db.fetch_token_transfers(chain_id=chain_id, limit=limit)
            df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
            if df.empty:
                st.info("No token transfers yet.")
            else:
                if "time_stamp" in df.columns:
                    df["time_utc"] = pd.to_datetime(df["time_stamp"], unit="s", utc=True)
                cols = [
                    c
                    for c in [
                        "time_utc",
                        "token_symbol",
                        "value_normalized",
                        "from_address",
                        "to_address",
                        "contract_address",
                        "tx_hash",
                        "chain_id",
                    ]
                    if c in df.columns
                ]
                st.dataframe(df[cols], use_container_width=True, height=420)

        with tab_addr:
            watched = db.watched_addresses()
            addr = st.selectbox(
                "Watched address",
                options=watched or [""],
                format_func=lambda a: a or "(none ingested yet)",
            )
            if addr:
                summary = summarize_address_activity(db, addr, chain_id=chain_id)
                st.json(summary.to_dict())
                bal_rows = db.fetch_latest_balances(chain_id=chain_id)
                bal_df = pd.DataFrame([dict(r) for r in bal_rows]) if bal_rows else pd.DataFrame()
                if not bal_df.empty:
                    st.markdown("#### Latest balances")
                    st.dataframe(bal_df, use_container_width=True)

        with tab_spikes:
            watched = db.watched_addresses()
            addr = st.selectbox(
                "Address for spike detection",
                options=["(all)"] + watched,
                key="spike_addr",
            )
            z = st.slider("Z-score threshold", 1.0, 5.0, 2.0, 0.1)
            spikes = detect_volume_spikes(
                db,
                address=None if addr == "(all)" else addr,
                chain_id=chain_id,
                zscore_threshold=z,
            )
            if not spikes:
                st.info("No volume spikes detected (need enough history).")
            else:
                sdf = pd.DataFrame([s.to_dict() for s in spikes])
                sdf["bucket_utc"] = pd.to_datetime(sdf["bucket_start"], unit="s", utc=True)
                st.dataframe(sdf, use_container_width=True)
                st.bar_chart(sdf.set_index("bucket_utc")["volume_eth"])

        st.divider()
        st.caption(
            f"DB: `{db_path}` · generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )
    finally:
        db.close()


def run() -> None:
    """CLI helper: streamlit run this file."""
    import subprocess

    script = Path(__file__).resolve()
    raise SystemExit(subprocess.call(["streamlit", "run", str(script), *sys.argv[1:]]))


if __name__ == "__main__":
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        in_streamlit = get_script_run_ctx() is not None
    except Exception:
        in_streamlit = False

    if in_streamlit:
        main()
    else:
        try:
            import streamlit  # noqa: F401
        except ImportError:
            _require_streamlit()
        run()
