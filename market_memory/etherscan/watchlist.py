"""Multi-address watchlist loader (YAML / JSON / plain text).

Formats
-------
YAML (recommended)::

    defaults:
      chain: ethereum
      mode: recent
      large_transfer_eth: 100
    addresses:
      - address: "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        label: vitalik
      - address: "0x..."
        label: base-bridge
        chain: base

JSON: same structure as YAML.

Plain text (one address per line)::

    # comments allowed
    0xabc...
    0xdef...,base,bridge-whale
    0xghi...,8453,numeric-chain
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from market_memory.etherscan.chains import resolve_chain

logger = logging.getLogger(__name__)

_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


@dataclass
class WatchEntry:
    address: str
    chain_id: int = 1
    chain_name: str = "ethereum"
    label: str | None = None
    mode: str = "recent"
    large_transfer_eth: float | None = None
    include_tokens: bool = True
    include_balance: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Watchlist:
    entries: list[WatchEntry] = field(default_factory=list)
    defaults: dict[str, Any] = field(default_factory=dict)

    def addresses(self) -> list[str]:
        return [e.address for e in self.entries]

    def by_chain(self) -> dict[int, list[WatchEntry]]:
        out: dict[int, list[WatchEntry]] = {}
        for e in self.entries:
            out.setdefault(e.chain_id, []).append(e)
        return out


def _validate_address(addr: str) -> str:
    a = addr.strip()
    if not _ADDR_RE.match(a):
        raise ValueError(f"Invalid Ethereum address: {addr!r}")
    return a.lower()


def _entry_from_mapping(raw: dict[str, Any], defaults: dict[str, Any]) -> WatchEntry:
    if "address" not in raw:
        raise ValueError(f"Watchlist entry missing 'address': {raw!r}")
    merged = {**defaults, **raw}
    chain = resolve_chain(merged.get("chain") or merged.get("chain_id") or 1)
    return WatchEntry(
        address=_validate_address(str(merged["address"])),
        chain_id=chain.chain_id,
        chain_name=chain.name,
        label=(str(merged["label"]) if merged.get("label") else None),
        mode=str(merged.get("mode") or "recent"),
        large_transfer_eth=(
            float(merged["large_transfer_eth"])
            if merged.get("large_transfer_eth") is not None
            else None
        ),
        include_tokens=bool(merged.get("include_tokens", True)),
        include_balance=bool(merged.get("include_balance", True)),
    )


def _load_structured(data: dict[str, Any]) -> Watchlist:
    defaults = dict(data.get("defaults") or {})
    raw_entries = data.get("addresses") or data.get("watchlist") or []
    if not isinstance(raw_entries, list):
        raise ValueError("watchlist 'addresses' must be a list")
    entries = [_entry_from_mapping(item if isinstance(item, dict) else {"address": item}, defaults) for item in raw_entries]
    # Dedupe by (chain_id, address), keep first
    seen: set[tuple[int, str]] = set()
    unique: list[WatchEntry] = []
    for e in entries:
        key = (e.chain_id, e.address)
        if key in seen:
            logger.warning("Duplicate watchlist entry skipped: %s chain=%s", e.address, e.chain_id)
            continue
        seen.add(key)
        unique.append(e)
    return Watchlist(entries=unique, defaults=defaults)


def _load_text(text: str, default_chain: str | int = 1) -> Watchlist:
    default_info = resolve_chain(default_chain)
    entries: list[WatchEntry] = []
    seen: set[tuple[int, str]] = set()
    for line_no, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = [p.strip() for p in stripped.split(",")]
        addr = parts[0]
        chain_token: str | int = default_info.chain_id
        label: str | None = None
        if len(parts) >= 2 and parts[1]:
            chain_token = parts[1]
        if len(parts) >= 3 and parts[2]:
            label = parts[2]
        try:
            chain = resolve_chain(chain_token, default=default_info.chain_id)
            entry = WatchEntry(
                address=_validate_address(addr),
                chain_id=chain.chain_id,
                chain_name=chain.name,
                label=label,
            )
        except ValueError as exc:
            raise ValueError(f"watchlist line {line_no}: {exc}") from exc
        key = (entry.chain_id, entry.address)
        if key in seen:
            continue
        seen.add(key)
        entries.append(entry)
    return Watchlist(entries=entries, defaults={"chain": default_info.name})


def load_watchlist(
    path: str | Path,
    *,
    default_chain: str | int = 1,
) -> Watchlist:
    """Load a watchlist file (auto-detect YAML / JSON / text)."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Watchlist not found: {p}")
    text = p.read_text(encoding="utf-8")
    suffix = p.suffix.lower()

    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "PyYAML is required for .yaml watchlists. pip install pyyaml "
                "or use .json / .txt format."
            ) from exc
        data = yaml.safe_load(text) or {}
        if not isinstance(data, dict):
            raise ValueError("YAML watchlist must be a mapping at the top level")
        wl = _load_structured(data)
    elif suffix == ".json":
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("JSON watchlist must be an object at the top level")
        wl = _load_structured(data)
    else:
        wl = _load_text(text, default_chain=default_chain)

    logger.info("Loaded watchlist %s: %s entries", p, len(wl.entries))
    return wl


def merge_cli_addresses(
    addresses: list[str] | None,
    watchlist: Watchlist | None,
    *,
    chain_id: int = 1,
    chain_name: str = "ethereum",
) -> list[WatchEntry]:
    """Combine CLI --address flags with an optional watchlist."""
    entries: list[WatchEntry] = []
    seen: set[tuple[int, str]] = set()

    if watchlist:
        for e in watchlist.entries:
            key = (e.chain_id, e.address)
            if key not in seen:
                seen.add(key)
                entries.append(e)

    for addr in addresses or []:
        a = _validate_address(addr)
        key = (chain_id, a)
        if key in seen:
            continue
        seen.add(key)
        entries.append(
            WatchEntry(address=a, chain_id=chain_id, chain_name=chain_name)
        )

    return entries
