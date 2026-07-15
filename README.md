# Market Memory

Lightweight, local-first event store for historical crypto/market context. Powers tweet enrichment for `twitter-bot` with queries like:

> Similar BTC liquidation spikes since 2021: 12 occurrences. Current reading ranks in the 42nd percentile.

## Why DuckDB?

- **Single embedded file** (`data/market_memory.duckdb`) — no server to run
- **Fast analytical SQL** — counts, filters, percentiles, time ranges
- **Native JSON** — tags and metadata without extra parsing layers
- **Simpler than partitioned Parquet** for a bot-sidecar with moderate event volume

## Quick start

```bash
cd market-memory
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Load sample data
python -m market_memory.cli ingest tests/fixtures/sample_events.json

# Query from CLI
python -m market_memory.cli count \
  --event-type market_surge --asset BTC --indicator-type liquidations \
  --direction spike --since 2021-01-01

python -m market_memory.cli tweet-context \
  --event-type market_surge --asset BTC --indicator-type liquidations \
  --direction spike --since 2021-01-01 --value 461800000

# Start HTTP API (default http://127.0.0.1:8788; use --port if 8788 is taken)
python run.py --port 8789
```

## Library API (recommended for twitter-bot)

```python
from datetime import datetime
from market_memory import EventDB
from market_memory.models import SimilarityQuery

db = EventDB(data_dir="data")
query = SimilarityQuery(
    event_type="market_surge",
    asset="BTC",
    indicator_type="liquidations",
    direction="spike",
    since=datetime.fromisoformat("2021-01-01"),
    min_value=400_000_000,
)

ctx = db.tweet_context(query, current_value=461_800_000)
print(ctx.tweet_context)

db.close()
```

See `examples/bot_integration.py` for a full `enrich_tweet_with_memory()` helper.

## HTTP API

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Service + DB stats |
| `POST /ingest` | Ingest JSON events |
| `GET /events` | List events with filters |
| `GET /similar` | Similar historical events |
| `GET /count` | Count similar events since date |
| `GET /latest` | Most recent similar event |
| `GET /percentile` | Percentile rank of a value |
| `GET /tweet-context` | Tweet-ready context bundle |
| `GET /stats` | Totals, breakdowns, monthly/yearly counts |
| `POST /prune` | Delete by `before` or `keep_months` |

### Example: tweet context

```bash
curl "http://127.0.0.1:8788/tweet-context?\
event_type=market_surge&asset=BTC&indicator_type=liquidations&\
direction=spike&since=2021-01-01&current_value=461800000"
```

## twitter-bot integration

**In-process** — import `EventDB` directly (lowest latency).

**Sidecar** — run `python run.py` and call `/tweet-context` over HTTP.

See `examples/bot_integration.py`.

## Etherscan on-chain ingestion

Multi-chain-ready pipeline (Etherscan API v2). Fetches account txs, token transfers, balances, gas oracle, and contract metadata into SQLite with idempotent inserts, free-tier throttling, whale-alert hooks, watchlists, and a Streamlit dashboard.

### Setup

```bash
cp .env.example .env
# Edit .env and set ETHERSCAN_API_KEY=...

pip install -e ".[dev]"
# optional:
# pip install -e ".[scheduler]"      # APScheduler
# pip install -e ".[dashboard]"      # Streamlit
# pip install -e ".[watchlist-yaml]" # YAML watchlists
# pip install -e ".[all]"
```

### CLI examples

```bash
# Recent txs + token transfers + balance + gas for an address
python ingest.py --address 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 --mode recent

# Multi-address watchlist (YAML / JSON / TXT) + whale alerts
cp data/watchlist.example.yaml data/watchlist.yaml
python ingest.py --watchlist data/watchlist.yaml --mode recent --whale-alerts

# Specific chain (name or id)
python ingest.py --address 0x... --chain base --mode recent
python ingest.py --list-chains

# Analyze after ingest
python -m market_memory.etherscan --watchlist data/watchlist.yaml --analyze

# Continuous polling with whales
python ingest.py --watchlist data/watchlist.yaml --schedule-loop --interval 120 --whale-alerts

# Streamlit dashboard over data/etherscan.db
streamlit run market_memory/etherscan/dashboard.py
# or: python -m market_memory.etherscan.dashboard

# Tweet-ready whale drafts (example bridge for twitter-bot)
python examples/whale_alert_bridge.py --watchlist data/watchlist.yaml
```

### Library API

```python
from market_memory.etherscan import (
    load_etherscan_config,
    load_watchlist,
    run_ingest,
    run_ingest_entries,
    EtherscanDB,
    detect_large_transfers,
    format_whale_tweet,
    check_whale_alerts,
)

cfg = load_etherscan_config()
wl = load_watchlist("data/watchlist.yaml")
results = run_ingest_entries(wl.entries, mode="recent", whale_alerts=True, config=cfg)

for r in results:
    for raw in r.whale_alerts:
        print(raw["value_eth"], raw["tx_hash"])
```

SQLite defaults to `data/etherscan.db` (multi-chain PKs on `tx_hash+chain_id`; tables include `whale_alerts`).

### twitter-bot integration status

| Surface | Status |
|---------|--------|
| **DuckDB EventDB** (historical tweet context, sync, record posted alerts) | **Integrated** via `twitter-bot/src/market_memory_bridge.py` |
| **Etherscan on-chain pipeline** (SQLite, whales, watchlist) | **Not wired into twitter-bot yet** — importable API + `examples/whale_alert_bridge.py` ready for you to hook |

The bot today uses market-memory for *indicator history / rarity context*, not on-chain whale monitoring. To post whales, call `run_ingest(..., whale_alerts=True)` or `format_whale_tweet` from a bot job (see the example script).

## Tests

```bash
pytest -q
```
