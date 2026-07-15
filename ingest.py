#!/usr/bin/env python3
"""Convenience entrypoint: python ingest.py --address 0x... --mode recent

Delegates to market_memory.etherscan.cli
"""

from market_memory.etherscan.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
