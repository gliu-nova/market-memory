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
python -m market_memory.cli ingest data/sample_events.json

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

## Tests

```bash
pytest -q
```