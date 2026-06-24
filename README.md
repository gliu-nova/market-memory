# Market Memory

Historical crypto/market event store on **Cloudflare Pages + D1**. Powers tweet enrichment for `twitter-bot` with queries like:

> Similar BTC liquidation spikes since 2021: 12 occurrences. Current reading ranks in the 42nd percentile.

## Architecture

| Component | Technology |
|-----------|------------|
| Runtime | Cloudflare Pages Functions |
| API | Hono (`src/index.ts`, `functions/`) |
| Database | Cloudflare D1 (SQLite) |
| Deploy | GitHub Actions → `wrangler pages deploy` |

```
GitHub (main push)
    → GitHub Actions deploy
        → Cloudflare Pages (public API + dashboard)
            → D1 (events table)
twitter-bot
    → GET https://market-memory.pages.dev/tweet-context
    → POST https://market-memory.pages.dev/ingest  (record new events)
```

---

## Deploy to Cloudflare — step by step

### Prerequisites

- Cloudflare account (same one as `prediction-market-divergence` / `wacta-scoring`)
- GitHub account
- Node.js 24+ (`nvm use`)

### Step 1: Create GitHub repo and push

```bash
cd market-memory
git init
git add .
git commit -m "Add Cloudflare Pages deployment"
git branch -M main
git remote add origin https://github.com/YOUR_USER/market-memory.git
git push -u origin main
```

### Step 2: Create D1 database

```bash
npm install
npx wrangler d1 create market-memory
```

Copy the `database_id` from the output into `wrangler.toml` (replace `REPLACE_WITH_YOUR_D1_DATABASE_ID`), then apply schema:

```bash
npm run db:remote
```

### Step 3: Create Cloudflare Pages project

1. [Cloudflare Dashboard](https://dash.cloudflare.com) → **Workers & Pages** → **Create** → **Pages** → **Connect to Git**
2. Select the `market-memory` repo
3. Build settings:
   - **Framework preset:** None
   - **Build command:** (empty)
   - **Build output directory:** `public`
4. **Settings → Functions** → compatibility date `2026-06-10`

### Step 4: Bind D1

Pages project → **Settings** → **Bindings**:

| Type | Name | Value |
|------|------|-------|
| D1 database | `DB` | `market-memory` |

### Step 5: GitHub Actions secrets

In the **market-memory** GitHub repo → **Settings → Secrets → Actions**, add the same secrets you use for your other Cloudflare projects:

| Secret | Value |
|--------|-------|
| `CLOUDFLARE_API_TOKEN` | API token with Pages Edit + D1 Edit |
| `CLOUDFLARE_ACCOUNT_ID` | Your Cloudflare account ID |

Push to `main` (or re-run the workflow) to deploy.

### Step 6: Optional ingest secret

Protect `POST /ingest` from public writes:

```bash
npx wrangler pages secret put INGEST_SECRET --project-name=market-memory
```

### Step 7: Seed sample data

```bash
chmod +x scripts/seed-remote.sh
./scripts/seed-remote.sh https://market-memory.pages.dev
# If INGEST_SECRET is set:
INGEST_SECRET=your-secret ./scripts/seed-remote.sh
```

### Step 8: Verify

```bash
curl -s https://market-memory.pages.dev/health | jq
curl -s "https://market-memory.pages.dev/tweet-context?event_type=market_surge&asset=BTC&indicator_type=liquidations&direction=spike&since=2021-01-01&current_value=461800000" | jq
```

---

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Service + event counts |
| `POST /ingest` | Ingest JSON events |
| `GET /events` | List events with filters |
| `GET /similar` | Similar historical events |
| `GET /count` | Count similar events since date |
| `GET /latest` | Most recent similar event |
| `GET /percentile` | Percentile rank of a value |
| `GET /tweet-context` | Tweet-ready context bundle |
| `GET /stats` | Totals, breakdowns, monthly/yearly counts |
| `POST /prune` | Delete by `before` or `keep_months` |

---

## twitter-bot integration

Add to `twitter-bot/.env`:

```env
MARKET_MEMORY_API_URL=https://market-memory.pages.dev
# Optional, if INGEST_SECRET is configured:
MARKET_MEMORY_INGEST_SECRET=your-secret
```

Use `examples/bot_integration.py`:

```python
from examples.bot_integration import enrich_tweet_via_http, record_event

final_text = enrich_tweet_via_http(
    draft,
    event_type="market_surge",
    asset="BTC",
    indicator_type="liquidations",
    direction="spike",
    current_value=461_800_000,
    since="2021-01-01",
)

# After posting, grow history:
record_event({
    "timestamp": "2026-06-24T12:00:00Z",
    "event_type": "market_surge",
    "asset": "BTC",
    "indicator_type": "liquidations",
    "timeframe": "24h",
    "value": 461_800_000,
    "direction": "spike",
    "source": "coinglass",
})
```

---

## Why D1 (not DuckDB on Cloudflare)?

Cloudflare Workers cannot run embedded DuckDB. D1 is SQLite-compatible, serverless, and matches the pattern used by `prediction-market-divergence` and `wacta-scoring` — one binding (`env.DB`), no laptop required.

---

## Repo layout

```
src/                  # Hono API + D1 storage
functions/[[path]].ts  # Pages handler
public/               # Dashboard
migrations/           # D1 schema (also applied via ensureTables)
data/sample_events.json
.github/workflows/deploy.yml
wrangler.toml
scripts/deploy.sh
scripts/seed-remote.sh
examples/bot_integration.py
```