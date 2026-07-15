"""Blockscout API v2 client with throttling, retries, and pagination."""

from __future__ import annotations

import logging
import time
from typing import Any, Iterator
from urllib.parse import urljoin

import requests

from market_memory.blockscout.config import BlockscoutConfig

logger = logging.getLogger(__name__)


class BlockscoutAPIError(RuntimeError):
    """Raised when the Blockscout API returns a hard failure."""


class BlockscoutClient:
    """REST client for Blockscout API v2 (+ Pro API key auth).

    Auth: Authorization Bearer token (Pro API). Public instances also work
    without a key but are more strictly rate-limited.
    """

    def __init__(self, config: BlockscoutConfig) -> None:
        self.config = config
        self._session = requests.Session()
        headers = {
            "User-Agent": "market-memory-blockscout/0.1",
            "Accept": "application/json",
        }
        if config.api_key:
            # Pro API keys are accepted as Bearer tokens
            headers["Authorization"] = f"Bearer {config.api_key}"
        self._session.headers.update(headers)
        self._last_request_at = 0.0

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> BlockscoutClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # --- addresses / accounts --------------------------------------------------

    def get_address(self, address: str) -> dict[str, Any]:
        """Address metadata: balance, is_contract, counters, etc."""
        return self._get_json(f"/addresses/{address}")

    def get_address_counters(self, address: str) -> dict[str, Any]:
        return self._get_json(f"/addresses/{address}/counters")

    def get_address_transactions(
        self,
        address: str,
        *,
        max_pages: int | None = None,
        filter_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        return self._paginate(
            f"/addresses/{address}/transactions",
            max_pages=max_pages,
            extra_params=filter_params,
        )

    def get_address_token_transfers(
        self,
        address: str,
        *,
        max_pages: int | None = None,
        token: str | None = None,
    ) -> list[dict[str, Any]]:
        extra: dict[str, Any] = {}
        if token:
            extra["token"] = token
        return self._paginate(
            f"/addresses/{address}/token-transfers",
            max_pages=max_pages,
            extra_params=extra or None,
        )

    def get_address_tokens(self, address: str, *, max_pages: int | None = None) -> list[dict[str, Any]]:
        """Token balances held by address."""
        return self._paginate(f"/addresses/{address}/tokens", max_pages=max_pages)

    # --- transactions & blocks -------------------------------------------------

    def get_transaction(self, tx_hash: str) -> dict[str, Any]:
        return self._get_json(f"/transactions/{tx_hash}")

    def get_transactions(self, *, max_pages: int | None = None) -> list[dict[str, Any]]:
        """Recent network transactions."""
        return self._paginate("/transactions", max_pages=max_pages)

    def get_block(self, block_number_or_hash: str | int) -> dict[str, Any]:
        return self._get_json(f"/blocks/{block_number_or_hash}")

    def get_blocks(self, *, max_pages: int | None = None, type_: str | None = None) -> list[dict[str, Any]]:
        extra = {"type": type_} if type_ else None
        return self._paginate("/blocks", max_pages=max_pages, extra_params=extra)

    def get_main_page_blocks(self) -> list[dict[str, Any]]:
        """Lightweight recent blocks (home page feed)."""
        data = self._get_json("/main-page/blocks")
        if isinstance(data, list):
            return data
        return data.get("items") or []

    # --- tokens & holders ------------------------------------------------------

    def get_token(self, address: str) -> dict[str, Any]:
        return self._get_json(f"/tokens/{address}")

    def get_token_holders(
        self,
        address: str,
        *,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._paginate(f"/tokens/{address}/holders", max_pages=max_pages)

    def get_token_transfers(
        self,
        address: str,
        *,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._paginate(f"/tokens/{address}/transfers", max_pages=max_pages)

    def get_tokens(self, *, max_pages: int | None = None, q: str | None = None) -> list[dict[str, Any]]:
        extra = {"q": q} if q else None
        return self._paginate("/tokens", max_pages=max_pages, extra_params=extra)

    # --- contracts & verification ----------------------------------------------

    def get_smart_contract(self, address: str) -> dict[str, Any]:
        """Verified contract metadata / source (404 if not verified)."""
        return self._get_json(f"/smart-contracts/{address}")

    def get_smart_contract_counters(self, address: str) -> dict[str, Any]:
        return self._get_json(f"/smart-contracts/{address}/counters")

    # --- stats -----------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Network-level stats (total blocks, addresses, txns, average block time, etc.)."""
        return self._get_json("/stats")

    def get_stats_charts_transactions(self) -> dict[str, Any]:
        return self._get_json("/stats/charts/transactions")

    def get_stats_charts_market(self) -> dict[str, Any]:
        return self._get_json("/stats/charts/market")

    # --- internal --------------------------------------------------------------

    def _url(self, path: str) -> str:
        base = self.config.base_url.rstrip("/") + "/"
        return urljoin(base, path.lstrip("/"))

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.config.rate_limit_delay - elapsed
        if wait > 0:
            time.sleep(wait)

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        last_err: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            self._throttle()
            try:
                self._last_request_at = time.monotonic()
                resp = self._session.get(
                    self._url(path),
                    params=params,
                    timeout=self.config.request_timeout,
                )
                if resp.status_code == 404:
                    raise BlockscoutAPIError(f"Not found: {path}")
                if resp.status_code == 429:
                    backoff = self.config.rate_limit_delay * (2**attempt)
                    logger.warning("Blockscout 429 rate limit; sleep %.2fs", backoff)
                    time.sleep(backoff)
                    continue
                resp.raise_for_status()
                return resp.json()
            except BlockscoutAPIError:
                raise
            except (requests.RequestException, ValueError) as exc:
                last_err = exc
                backoff = self.config.rate_limit_delay * (2 ** (attempt - 1))
                logger.warning(
                    "Blockscout request failed (attempt %s/%s) %s: %s; sleep %.2fs",
                    attempt,
                    self.config.max_retries,
                    path,
                    exc,
                    backoff,
                )
                time.sleep(backoff)
        raise BlockscoutAPIError(
            f"Blockscout request failed after {self.config.max_retries} attempts: {path}: {last_err}"
        )

    def _paginate(
        self,
        path: str,
        *,
        max_pages: int | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Follow next_page_params until exhausted or max_pages hit."""
        limit = max_pages if max_pages is not None else self.config.max_pages
        items: list[dict[str, Any]] = []
        params: dict[str, Any] = dict(extra_params or {})
        page = 0
        while page < limit:
            page += 1
            data = self._get_json(path, params=params)
            if isinstance(data, list):
                items.extend(data)
                break
            batch = data.get("items") or []
            items.extend(batch)
            next_params = data.get("next_page_params")
            if not next_params or not batch:
                break
            params = {**(extra_params or {}), **next_params}
        logger.debug("%s: fetched %s items over %s page(s)", path, len(items), page)
        return items

    def iter_pages(
        self,
        path: str,
        *,
        max_pages: int | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        limit = max_pages if max_pages is not None else self.config.max_pages
        params: dict[str, Any] = dict(extra_params or {})
        page = 0
        while page < limit:
            page += 1
            data = self._get_json(path, params=params)
            if isinstance(data, list):
                yield data
                return
            batch = data.get("items") or []
            yield batch
            next_params = data.get("next_page_params")
            if not next_params or not batch:
                return
            params = {**(extra_params or {}), **next_params}
