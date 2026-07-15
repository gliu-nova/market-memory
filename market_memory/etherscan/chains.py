"""Etherscan-supported chain registry (API v2 chainid).

One API key works across chains via the shared v2 base URL.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChainInfo:
    chain_id: int
    name: str
    native_symbol: str = "ETH"
    explorer: str = "https://etherscan.io"


# Common Etherscan family chains (v2). Extend as needed.
CHAINS: dict[str, ChainInfo] = {
    "ethereum": ChainInfo(1, "ethereum", "ETH", "https://etherscan.io"),
    "eth": ChainInfo(1, "ethereum", "ETH", "https://etherscan.io"),
    "mainnet": ChainInfo(1, "ethereum", "ETH", "https://etherscan.io"),
    "base": ChainInfo(8453, "base", "ETH", "https://basescan.org"),
    "arbitrum": ChainInfo(42161, "arbitrum", "ETH", "https://arbiscan.io"),
    "arb": ChainInfo(42161, "arbitrum", "ETH", "https://arbiscan.io"),
    "optimism": ChainInfo(10, "optimism", "ETH", "https://optimistic.etherscan.io"),
    "op": ChainInfo(10, "optimism", "ETH", "https://optimistic.etherscan.io"),
    "polygon": ChainInfo(137, "polygon", "MATIC", "https://polygonscan.com"),
    "matic": ChainInfo(137, "polygon", "MATIC", "https://polygonscan.com"),
    "bsc": ChainInfo(56, "bsc", "BNB", "https://bscscan.com"),
    "bnb": ChainInfo(56, "bsc", "BNB", "https://bscscan.com"),
    "avalanche": ChainInfo(43114, "avalanche", "AVAX", "https://snowtrace.io"),
    "avax": ChainInfo(43114, "avalanche", "AVAX", "https://snowtrace.io"),
    "linea": ChainInfo(59144, "linea", "ETH", "https://lineascan.build"),
    "scroll": ChainInfo(534352, "scroll", "ETH", "https://scrollscan.com"),
    "blast": ChainInfo(81457, "blast", "ETH", "https://blastscan.io"),
}

# Reverse lookup: chain_id -> canonical ChainInfo
_BY_ID: dict[int, ChainInfo] = {}
for _info in CHAINS.values():
    _BY_ID.setdefault(_info.chain_id, _info)


def resolve_chain(chain: str | int | None, default: int = 1) -> ChainInfo:
    """Resolve a chain name or id to ChainInfo.

    Accepts: None, int chain_id, numeric string, or known alias (ethereum, base, ...).
    """
    if chain is None:
        return _BY_ID.get(default, ChainInfo(default, f"chain-{default}"))
    if isinstance(chain, int):
        return _BY_ID.get(chain, ChainInfo(chain, f"chain-{chain}"))
    text = str(chain).strip().lower()
    if not text:
        return _BY_ID.get(default, ChainInfo(default, f"chain-{default}"))
    if text.isdigit():
        cid = int(text)
        return _BY_ID.get(cid, ChainInfo(cid, f"chain-{cid}"))
    if text in CHAINS:
        return CHAINS[text]
    raise ValueError(
        f"Unknown chain {chain!r}. Known: {', '.join(sorted(set(c.name for c in CHAINS.values())))}"
    )


def explorer_tx_url(chain_id: int, tx_hash: str) -> str:
    info = _BY_ID.get(chain_id, ChainInfo(chain_id, f"chain-{chain_id}"))
    return f"{info.explorer}/tx/{tx_hash}"


def list_chains() -> list[ChainInfo]:
    """Unique chains sorted by chain_id."""
    seen: dict[int, ChainInfo] = {}
    for info in CHAINS.values():
        seen[info.chain_id] = info
    return sorted(seen.values(), key=lambda c: c.chain_id)
