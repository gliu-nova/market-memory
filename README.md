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

## Blockscout on-chain ingestion

Blockscout API v2 (+ Pro key) pipeline for accounts, txs/blocks, tokens/holders, contracts, and network stats. Stores in `data/blockscout.db` with whale alerts and heuristic high-EV trader scores.

### Setup

```bash
# .env
BLOCKSCOUT_API_KEY=your_pro_key
BLOCKSCOUT_INSTANCE=ethereum   # or base, optimism, arbitrum, polygon, gnosis
```

### CLI examples

```bash
# Network stats
python ingest_blockscout.py --mode stats --json

# Watched account (meta + txs + tokens + contract attempt + trader score + whales)
python ingest_blockscout.py --address 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 --mode account --role whale --analyze

# Watchlist batch
python ingest_blockscout.py --watchlist data/watchlist.yaml --mode account

# Token holders
python ingest_blockscout.py --mode token --token 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48

# Recent blocks + network feed
python ingest_blockscout.py --mode network

# List instances
python -m market_memory.blockscout --list-instances
```

### Modules covered

| Area | Endpoints / tables |
|------|--------------------|
| Addresses / whales / monitoring | `addresses`, `address_counters`, roles `whale\|trader\|monitor` |
| High-EV traders | `trader_scores` (success rate, volume, activity, counterparties) |
| Transactions & blocks | `transactions`, `blocks` |
| Tokens & holders | `tokens`, `token_holders`, `token_balances`, `token_transfers` |
| Contracts & verification | `contracts` (ABI/source when verified) |
| Stats | `network_stats` |

### Library API

```python
from market_memory.blockscout import load_blockscout_config, run_ingest, BlockscoutDB

cfg = load_blockscout_config()
result = run_ingest(address="0xd8dA...", mode="account", role="whale", config=cfg)
with BlockscoutDB(cfg.db_path) as db:
    print(db.stats())
    print(db.fetch_high_ev_traders(min_score=70))
```

### twitter-bot integration status

| Surface | Status |
|---------|--------|
| **DuckDB EventDB** (historical tweet context, sync, record posted alerts) | **Integrated** via `twitter-bot/src/market_memory_bridge.py` |
| **Etherscan on-chain pipeline** (SQLite, whales, watchlist) | **Integrated** via `twitter-bot/src/etherscan_bridge.py` |

The bot poll cycle runs `process_etherscan_for_bot`: ingest watchlist → whale hook → enqueue `eth_whale` → posting engine compose/tweet. Configure under `twitter-bot/config.yaml` → `etherscan:` and `ETHERSCAN_API_KEY`. See twitter-bot README “On-chain whales”.

## Tests

```bash
pytest -q
```
