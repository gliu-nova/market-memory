"""CLI for Etherscan ingestion.

Examples:
  python -m market_memory.etherscan --address 0xd8dA... --mode recent
  python -m market_memory.etherscan --watchlist data/watchlist.yaml --whale-alerts
  python -m market_memory.etherscan --mode gas --chain base
  python -m market_memory.etherscan --watchlist data/watchlist.yaml --schedule-loop
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Sequence

from market_memory.etherscan.analysis import (
    detect_large_transfers,
    detect_volume_spikes,
    summarize_address_activity,
)
from market_memory.etherscan.chains import list_chains, resolve_chain
from market_memory.etherscan.config import load_etherscan_config
from market_memory.etherscan.db import EtherscanDB
from market_memory.etherscan.pipeline import run_ingest, run_ingest_entries
from market_memory.etherscan.scheduler import run_apscheduler, run_loop
from market_memory.etherscan.watchlist import load_watchlist, merge_cli_addresses


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m market_memory.etherscan",
        description="Ingest on-chain data via Etherscan into SQLite (multi-chain ready)",
    )
    p.add_argument(
        "--address",
        "-a",
        action="append",
        dest="addresses",
        help="Address to watch (repeatable)",
    )
    p.add_argument(
        "--watchlist",
        "-w",
        default=None,
        help="Path to watchlist file (.yaml / .json / .txt)",
    )
    p.add_argument(
        "--chain",
        default=None,
        help="Chain name or id for CLI addresses (default: env / ethereum). "
        f"Known: {', '.join(c.name for c in list_chains())}",
    )
    p.add_argument(
        "--mode",
        "-m",
        choices=["recent", "full", "gas", "balance", "contract"],
        default="recent",
        help="Ingestion mode (default: recent)",
    )
    p.add_argument("--start-block", type=int, default=None)
    p.add_argument("--end-block", type=int, default=None)
    p.add_argument("--db-path", default=None)
    p.add_argument("--rate-limit-delay", type=float, default=None)
    p.add_argument("--large-transfer-eth", type=float, default=None)
    p.add_argument("--no-tokens", action="store_true")
    p.add_argument("--no-balance", action="store_true")
    p.add_argument("--no-gas", action="store_true")
    p.add_argument("--contract", action="store_true")
    p.add_argument(
        "--whale-alerts",
        action="store_true",
        help="Run whale-alert hook after ingest (idempotent)",
    )
    p.add_argument(
        "--whale-alerts-json",
        default=None,
        help="Append whale alerts to this JSON file",
    )
    p.add_argument(
        "--analyze",
        action="store_true",
        help="After ingest, print large transfers / volume spikes / summary",
    )
    p.add_argument("--schedule-loop", action="store_true")
    p.add_argument("--schedule-apscheduler", action="store_true")
    p.add_argument("--interval", type=float, default=60.0)
    p.add_argument("--max-iterations", type=int, default=None)
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--json", action="store_true")
    p.add_argument(
        "--list-chains",
        action="store_true",
        help="Print known chains and exit",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    if args.list_chains:
        for c in list_chains():
            print(f"{c.chain_id:>8}  {c.name:<12}  {c.native_symbol:<5}  {c.explorer}")
        return 0

    try:
        cfg = load_etherscan_config(
            db_path=args.db_path,
            rate_limit_delay=args.rate_limit_delay,
            chain_id=args.chain,
            large_transfer_eth=args.large_transfer_eth,
            watchlist_path=args.watchlist,
            whale_alerts=args.whale_alerts or None,
            whale_alerts_json=args.whale_alerts_json,
        )
    except ValueError as exc:
        logging.error("%s", exc)
        return 2

    if args.whale_alerts:
        cfg.whale_alerts_enabled = True
    if args.whale_alerts_json:
        from pathlib import Path

        cfg.whale_alerts_json = Path(args.whale_alerts_json)

    # Resolve entries from watchlist + CLI addresses
    watchlist = None
    wl_path = args.watchlist or cfg.watchlist_path
    if wl_path:
        try:
            watchlist = load_watchlist(wl_path, default_chain=cfg.chain_id)
        except Exception as exc:
            logging.error("Failed to load watchlist: %s", exc)
            return 2

    chain = resolve_chain(args.chain if args.chain is not None else cfg.chain_id)
    entries = merge_cli_addresses(
        args.addresses,
        watchlist,
        chain_id=chain.chain_id,
        chain_name=chain.name,
    )

    if args.schedule_loop or args.schedule_apscheduler:
        if not entries and args.mode not in {"gas"}:
            parser.error("Scheduled modes need --address and/or --watchlist (except mode=gas)")
        common = dict(
            mode=args.mode,
            interval_seconds=args.interval,
            include_tokens=not args.no_tokens,
            include_gas=not args.no_gas,
            whale_alerts=cfg.whale_alerts_enabled,
            config=cfg,
        )
        if args.schedule_apscheduler:
            run_apscheduler(entries=entries, **common)  # type: ignore[arg-type]
        else:
            run_loop(
                entries=entries,
                max_iterations=args.max_iterations,
                **common,  # type: ignore[arg-type]
            )
        return 0

    results = []
    if args.mode == "gas" and not entries:
        results.append(
            run_ingest(
                address=None,
                mode="gas",
                include_gas=True,
                chain_id=cfg.chain_id,
                whale_alerts=False,
                config=cfg,
            )
        )
    elif not entries:
        parser.error(f"--address or --watchlist is required for mode={args.mode}")
    else:
        # Override mode/token flags from CLI for all entries when set
        for e in entries:
            if args.mode:
                e.mode = args.mode
            if args.no_tokens:
                e.include_tokens = False
            if args.no_balance:
                e.include_balance = False
        results = run_ingest_entries(
            entries,
            mode=args.mode,
            include_gas=not args.no_gas,
            whale_alerts=cfg.whale_alerts_enabled,
            config=cfg,
        )

    if args.analyze:
        with EtherscanDB(cfg.db_path) as database:
            analyses = []
            for e in entries:
                summary = summarize_address_activity(
                    database, e.address, chain_id=e.chain_id
                )
                large = detect_large_transfers(
                    database,
                    threshold_eth=e.large_transfer_eth or cfg.large_transfer_eth,
                    address=e.address,
                    chain_id=e.chain_id,
                )
                spikes = detect_volume_spikes(
                    database, address=e.address, chain_id=e.chain_id
                )
                analyses.append(
                    {
                        "entry": e.to_dict(),
                        "summary": summary.to_dict(),
                        "large_transfers": [x.to_dict() for x in large[:20]],
                        "volume_spikes": [x.to_dict() for x in spikes[:20]],
                    }
                )
            if args.json:
                print(
                    json.dumps(
                        {"ingest": [r.to_dict() for r in results], "analysis": analyses},
                        indent=2,
                    )
                )
            else:
                for block in analyses:
                    print(f"\n=== {block['entry'].get('label') or block['entry']['address']} "
                          f"(chain={block['entry']['chain_name']}) ===")
                    print(json.dumps(block["summary"], indent=2))
                    thr = block["entry"].get("large_transfer_eth") or cfg.large_transfer_eth
                    print(f"\nLarge transfers (>= {thr} ETH):")
                    for t in block["large_transfers"][:20]:
                        print(
                            f"  {t['value_eth']:.4f} ETH  {t['tx_hash']}  "
                            f"{t['from_address']} -> {t['to_address']}"
                        )
            return 0

    payload = [r.to_dict() for r in results]
    if args.json:
        print(json.dumps(payload if len(payload) > 1 else payload[0], indent=2))
    else:
        for r in results:
            print(
                f"[{r.status}] chain={r.chain_name}({r.chain_id}) mode={r.mode} "
                f"address={r.address} label={r.label} "
                f"txs={r.txs_fetched}/{r.txs_inserted} "
                f"transfers={r.transfers_fetched}/{r.transfers_inserted} "
                f"balance_wei={r.balance_wei} block={r.latest_block} "
                f"whales={len(r.whale_alerts)}"
            )
            if r.gas_oracle:
                print(
                    f"  gas: safe={r.gas_oracle.get('SafeGasPrice')} "
                    f"propose={r.gas_oracle.get('ProposeGasPrice')} "
                    f"fast={r.gas_oracle.get('FastGasPrice')}"
                )
            for w in r.whale_alerts[:5]:
                print(f"  🐋 {w['value_eth']:.2f} ETH  {w['tx_hash']}")
    return 0 if all(r.status == "ok" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
