#!/usr/bin/env python3
"""Convenience entrypoint: python ingest_blockscout.py --mode stats"""

from market_memory.blockscout.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
