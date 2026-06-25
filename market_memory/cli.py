from __future__ import annotations

import argparse
import json
from datetime import datetime

from market_memory.config import load_config
from market_memory.db import EventDB
from market_memory.models import SimilarityQuery


def main() -> None:
    parser = argparse.ArgumentParser(description="Market Memory CLI")
    parser.add_argument("--data-dir", default=None, help="Override data directory")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingest events from JSON or CSV")
    ingest.add_argument("path", help="Path to .json or .csv file")

    sub.add_parser("stats", help="Show database stats")

    count = sub.add_parser("count", help="Count similar events")
    _add_query_args(count)
    count.add_argument("--since", help="ISO date, e.g. 2021-01-01")

    similar = sub.add_parser("similar", help="List similar events")
    _add_query_args(similar)
    similar.add_argument("--since", help="ISO date")
    similar.add_argument("--limit", type=int, default=10)

    latest = sub.add_parser("latest", help="Latest similar event")
    _add_query_args(latest)
    latest.add_argument("--since", help="ISO date")

    percentile = sub.add_parser("percentile", help="Percentile rank of a value")
    _add_query_args(percentile)
    percentile.add_argument("--value", type=float, required=True)
    percentile.add_argument("--since", help="ISO date")

    tweet = sub.add_parser("tweet-context", help="Build tweet-ready context")
    _add_query_args(tweet)
    tweet.add_argument("--since", help="ISO date")
    tweet.add_argument("--value", type=float, help="Current reading")
    tweet.add_argument("--min-value", type=float, help="Minimum event value filter")

    prune = sub.add_parser("prune", help="Delete old events")
    prune.add_argument("--before", help="Delete events before ISO date")
    prune.add_argument("--keep-months", type=int, help="Keep only last N months")

    backfill = sub.add_parser("backfill", help="Full backfill (default: replace all events)")
    backfill.add_argument("--since", default="2021-01-01", help="ISO date lower bound")
    backfill.add_argument("--no-wipe", action="store_true", help="Append instead of replacing all events")

    sync = sub.add_parser("sync", help="Incremental sync from free exchange APIs (for cron/poll)")
    sync.add_argument("--since", default="2021-01-01", help="ISO date lower bound for first run")
    sync.add_argument("--interval-minutes", type=int, default=0, help="Skip if last sync within N minutes")
    sync.add_argument("--force", action="store_true", help="Run even if interval not elapsed")
    sync.add_argument(
        "--seed-verified-liquidations",
        action="store_true",
        help="One-time: add major historical liquidation episodes if DB has none",
    )

    args = parser.parse_args()
    cfg = load_config(data_dir=args.data_dir)

    if args.command == "backfill":
        from market_memory.backfill import backfill_database

        report = backfill_database(
            data_dir=cfg.service.data_dir,
            since=datetime.fromisoformat(args.since),
            wipe=not args.no_wipe,
        )
        print(json.dumps(report, indent=2))
        return

    if args.command == "sync":
        from market_memory.sync import sync_database

        report = sync_database(
            data_dir=cfg.service.data_dir,
            since=datetime.fromisoformat(args.since),
            interval_minutes=args.interval_minutes,
            force=args.force,
            seed_verified_liquidations=args.seed_verified_liquidations,
        )
        print(json.dumps(report, indent=2))
        return

    db = EventDB(data_dir=cfg.service.data_dir)

    try:
        if args.command == "ingest":
            count_n = db.ingest_file(args.path)
            print(f"Ingested {count_n} events")
        elif args.command == "stats":
            print(json.dumps(db.stats().model_dump(mode="json"), indent=2))
        else:
            query = _query_from_args(args)
            if args.command == "count":
                print(db.count_similar(query))
            elif args.command == "similar":
                events = db.find_similar(query, limit=args.limit)
                print(json.dumps([e.model_dump(mode="json") for e in events], indent=2))
            elif args.command == "latest":
                event = db.latest_similar(query)
                print(json.dumps(event.model_dump(mode="json") if event else None, indent=2))
            elif args.command == "percentile":
                print(db.percentile(args.value, query))
            elif args.command == "tweet-context":
                print(
                    json.dumps(
                        db.tweet_context(query, current_value=args.value).model_dump(mode="json"),
                        indent=2,
                    )
                )
            elif args.command == "prune":
                if args.before and args.keep_months:
                    parser.error("Use either --before or --keep-months")
                if args.before:
                    deleted = db.prune_before(datetime.fromisoformat(args.before))
                elif args.keep_months:
                    deleted = db.prune_keep_months(args.keep_months)
                else:
                    parser.error("Provide --before or --keep-months")
                print(f"Deleted {deleted} events")
    finally:
        db.close()


def _add_query_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--event-type", required=True)
    parser.add_argument("--asset")
    parser.add_argument("--indicator-type")
    parser.add_argument("--timeframe")
    parser.add_argument("--direction")
    parser.add_argument("--tags", help="Comma-separated tags")


def _query_from_args(args: argparse.Namespace) -> SimilarityQuery:
    since = datetime.fromisoformat(args.since) if getattr(args, "since", None) else None
    tags = [t.strip() for t in args.tags.split(",") if t.strip()] if getattr(args, "tags", None) else []
    return SimilarityQuery(
        event_type=args.event_type,
        asset=getattr(args, "asset", None),
        indicator_type=getattr(args, "indicator_type", None),
        timeframe=getattr(args, "timeframe", None),
        direction=getattr(args, "direction", None),
        tags=tags,
        since=since,
        min_value=getattr(args, "min_value", None),
    )


if __name__ == "__main__":
    main()