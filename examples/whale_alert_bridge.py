"""Example: turn Etherscan whale alerts into tweet drafts for twitter-bot.

This is NOT wired into twitter-bot automatically. twitter-bot today integrates
with market-memory's DuckDB EventDB (historical context), not the on-chain
SQLite pipeline. Use this as a drop-in pattern when you want on-chain whales.

Usage (from market-memory repo root)::

    python examples/whale_alert_bridge.py --watchlist data/watchlist.yaml
    python examples/whale_alert_bridge.py --address 0xd8dA... --threshold 50

Integration sketch for twitter-bot::

    from market_memory.etherscan import run_ingest, format_whale_tweet

    result = run_ingest(address=addr, mode="recent", whale_alerts=True)
    for raw in result.whale_alerts:
        draft = format_whale_tweet(WhaleAlert(**raw))  # or store/queue for posting
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running without install
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_memory.etherscan import (  # noqa: E402
    format_whale_tweet,
    load_etherscan_config,
    run_ingest,
    run_ingest_entries,
)
from market_memory.etherscan.watchlist import (  # noqa: E402
    load_watchlist,
    merge_cli_addresses,
)


def main() -> int:
    p = argparse.ArgumentParser(description="Ingest + print tweet-ready whale alerts")
    p.add_argument("--address", "-a", action="append", dest="addresses")
    p.add_argument("--watchlist", "-w", default=None)
    p.add_argument("--chain", default=None)
    p.add_argument("--threshold", type=float, default=None)
    p.add_argument("--json", action="store_true")
    args = p.parse_args()

    cfg = load_etherscan_config(
        chain_id=args.chain,
        large_transfer_eth=args.threshold,
        whale_alerts=True,
    )
    cfg.whale_alerts_enabled = True

    watchlist = load_watchlist(args.watchlist) if args.watchlist else None
    entries = merge_cli_addresses(
        args.addresses,
        watchlist,
        chain_id=cfg.chain_id,
        chain_name=cfg.chain_name,
    )
    if not entries:
        print("Provide --address and/or --watchlist", file=sys.stderr)
        return 2

    results = run_ingest_entries(entries, mode="recent", whale_alerts=True, config=cfg)
    drafts = []
    for r in results:
        for raw in r.whale_alerts:
            from market_memory.etherscan.alerts import WhaleAlert

            alert = WhaleAlert(**{k: raw[k] for k in WhaleAlert.__dataclass_fields__ if k in raw})
            drafts.append(
                {
                    "chain": r.chain_name,
                    "address": r.address,
                    "label": r.label,
                    "tweet": format_whale_tweet(alert),
                    "alert": raw,
                }
            )

    if args.json:
        print(json.dumps({"results": [r.to_dict() for r in results], "drafts": drafts}, indent=2))
    else:
        if not drafts:
            print("No new whale alerts (already fired or none above threshold).")
        for d in drafts:
            print("---")
            print(d["tweet"])
            print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
