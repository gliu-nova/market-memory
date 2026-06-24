#!/usr/bin/env python3
"""Entry point for the Market Memory service."""

import argparse

import uvicorn

from market_memory.app import create_app
from market_memory.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Market Memory historical context service")
    parser.add_argument("--host", help="Override bind host")
    parser.add_argument("--port", type=int, help="Override bind port")
    parser.add_argument("--data-dir", help="Override data directory")
    args = parser.parse_args()

    config = load_config(data_dir=args.data_dir)
    if args.host:
        config.service.host = args.host
    if args.port:
        config.service.port = args.port

    app = create_app(config)
    uvicorn.run(app, host=config.service.host, port=config.service.port)


if __name__ == "__main__":
    main()