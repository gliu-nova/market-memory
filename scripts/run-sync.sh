#!/bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"
mkdir -p data
source .venv/bin/activate
python -m market_memory.cli sync --interval-minutes 60 "$@"