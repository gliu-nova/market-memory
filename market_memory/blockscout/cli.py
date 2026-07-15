"""CLI for Blockscout ingestion.

Examples:
  python -m market_memory.blockscout --mode stats
  python -m market_memory.blockscout --address 0xd8dA... --mode account
  python -m market_memory.blockscout --watchlist data/watchlist.yaml --mode account
  python -m market_memory.blockscout --token 0xA0b8... --mode token
  python -m market_memory.blockscout --mode network
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Sequence

from market_memory.blockscout.analysis import detect_large_transfers
from market_memory.blockscout.config import INSTANCE_BASES, load_blockscout_config
from market_memory.blockscout.db import BlockscoutDB
from market_memory.blockscout.pipeline import WatchTarget, run_ingest, run_ingest_entries


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m market_memory.blockscout",
        description="Ingest on-chain data via Blockscout API v2 into SQLite",
    )
    p.add_argument("--address", "-a", action="append", dest="addresses", help="Address (repeatable)")
    p.add_argument("--watchlist", "-w", default=None, help="YAML/JSON/TXT watchlist path")
    p.add_argument(
        "--mode",
        "-m",
        choices=["account", "stats", "blocks", "token", "contract", "network", "full"],
        default="account",
    )
    p.add_argument("--token", default=None, help="Token contract (mode=token)")
    p.add_argument("--label", default=None, help="Label for single --address")
    p.add_argument(
        "--role",
        choices=["whale", "trader", "monitor", "other"],
        default="monitor",
        help="Watch role for addresses",
    )
    p.add_argument("--instance", default=None, help=f"Blockscout instance: {', '.join(sorted(set(INSTANCE_BASES)))}")
    p.add_argument("--db-path", default=None)
    p.add_argument("--rate-limit-delay", type=float, default=None)
    p.add_argument("--large-transfer-eth", type=float, default=None)
    p.add_argument("--no-txs", action="store_true")
    p.add_argument("--no-tokens", action="store_true")
    p.add_argument("--no-contract", action="store_true")
    p.add_argument("--no-stats", action="store_true")
    p.add_argument("--no-score", action="store_true", help="Skip high-EV trader scoring")
    p.add_argument("--no-whales", action="store_true")
    p.add_argument("--analyze", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--json", action="store_true")
    p.add_argument("--list-instances", action="store_true")
    return p


def _load_entries(args, cfg) -> list[WatchTarget]:
    entries: list[WatchTarget] = []
    if args.watchlist or cfg.watchlist_path:
        path = args.watchlist or cfg.watchlist_path
        try:
            from market_memory.etherscan.watchlist import load_watchlist

            wl = load_watchlist(path)
            for e in wl.entries:
                entries.append(
                    WatchTarget(
                        address=e.address,
                        label=e.label,
                        role=args.role,
                    )
                )
        except Exception as exc:
            raise SystemExit(f"Failed to load watchlist: {exc}") from exc
    for addr in args.addresses or []:
        entries.append(
            WatchTarget(address=addr.lower(), label=args.label, role=args.role)
        )
    # Dedupe
    seen: set[str] = set()
    unique: list[WatchTarget] = []
    for e in entries:
        if e.address in seen:
            continue
        seen.add(e.address)
        unique.append(e)
    return unique


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    if args.list_instances:
        for name, url in sorted(set((k, v) for k, v in INSTANCE_BASES.items())):
            print(f"{name:<12} {url}")
        return 0

    try:
        cfg = load_blockscout_config(
            db_path=args.db_path,
            instance=args.instance,
            rate_limit_delay=args.rate_limit_delay,
            large_transfer_eth=args.large_transfer_eth,
            watchlist_path=args.watchlist,
        )
    except ValueError as exc:
        logging.error("%s", exc)
        return 2

    results = []

    if args.mode in {"stats", "blocks", "network"} and not args.addresses and not args.watchlist:
        results.append(
            run_ingest(
                mode=args.mode,
                include_stats=not args.no_stats,
                include_blocks=args.mode in {"blocks", "network"},
                config=cfg,
            )
        )
    elif args.mode == "token":
        if not args.token:
            parser.error("mode=token requires --token")
        results.append(
            run_ingest(mode="token", token=args.token, config=cfg)
        )
    else:
        entries = _load_entries(args, cfg)
        if not entries and args.mode not in {"stats", "blocks", "network"}:
            parser.error("--address or --watchlist required")
        if len(entries) == 1 and args.mode != "account":
            e = entries[0]
            results.append(
                run_ingest(
                    address=e.address,
                    mode=args.mode,
                    label=e.label or args.label,
                    role=e.role or args.role,
                    include_txs=not args.no_txs,
                    include_tokens=not args.no_tokens,
                    include_contract=not args.no_contract,
                    include_stats=not args.no_stats,
                    score_trader=not args.no_score,
                    whale_alerts=not args.no_whales,
                    config=cfg,
                )
            )
        else:
            results = run_ingest_entries(
                entries,
                mode=args.mode if args.mode != "full" else "full",
                include_stats=not args.no_stats,
                config=cfg,
            )

    if args.analyze:
        with BlockscoutDB(cfg.db_path) as database:
            stats = database.stats()
            whales = detect_large_transfers(
                database, threshold_eth=cfg.large_transfer_eth, chain_id=cfg.chain_id
            )
            traders = database.fetch_high_ev_traders(
                chain_id=cfg.chain_id, min_score=cfg.high_ev_min_score
            )
            payload = {
                "ingest": [r.to_dict() for r in results],
                "db_stats": stats,
                "large_transfers": [w.to_dict() for w in whales[:20]],
                "high_ev_traders": [dict(t) for t in traders[:20]],
            }
            print(json.dumps(payload, indent=2, default=str))
            return 0

    payload = [r.to_dict() for r in results]
    if args.json:
        print(json.dumps(payload if len(payload) > 1 else payload[0], indent=2, default=str))
    else:
        for r in results:
            print(
                f"[{r.status}] mode={r.mode} instance={r.instance} address={r.address} "
                f"txs+={r.txs_inserted} transfers+={r.transfers_inserted} "
                f"blocks+={r.blocks_inserted} score={r.trader_score} "
                f"whales={len(r.whales)} stats={r.stats_saved}"
            )
            if r.detail:
                print(f"  detail: {r.detail}")
    return 0 if all(r.status == "ok" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
