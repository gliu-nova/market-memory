#!/usr/bin/env bash
# Seed production D1 via POST /ingest after deploy.
set -euo pipefail
BASE_URL="${1:-https://market-memory.pages.dev}"
SECRET="${INGEST_SECRET:-}"

AUTH=()
if [[ -n "$SECRET" ]]; then
  AUTH=(-H "Authorization: Bearer $SECRET")
fi

curl -s -X POST "${BASE_URL}/ingest" \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  --data-binary @"$(dirname "$0")/../data/sample_events.json" | jq