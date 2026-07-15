"""Thin Etherscan API client with throttling and retries."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from market_memory.etherscan.config import EtherscanConfig

logger = logging.getLogger(__name__)


class EtherscanAPIError(RuntimeError):
    """Raised when the Etherscan API returns a hard failure."""


class EtherscanClient:
    """Etherscan (v2) client scoped to a single chain.

    Designed for free-tier rate limits: serial requests with a configurable delay.
    Expandable to multi-chain by constructing one client per chain_id / base_url.
    """

    def __init__(self, config: EtherscanConfig) -> None:
        self.config = config
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "market-memory-etherscan/0.1"})
        self._last_request_at = 0.0

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> EtherscanClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # --- public module helpers -------------------------------------------------

    def get_normal_transactions(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int | None = None,
        sort: str = "desc",
    ) -> list[dict[str, Any]]:
        """account.txlist — normal ETH transactions for an address."""
        return self._list_result(
            module="account",
            action="txlist",
            address=address,
            startblock=start_block,
            endblock=end_block,
            page=page,
            offset=offset or self.config.page_size,
            sort=sort,
        )

    def get_token_transfers(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int | None = None,
        sort: str = "desc",
        contract_address: str | None = None,
    ) -> list[dict[str, Any]]:
        """account.tokentx — ERC-20 token transfers for an address."""
        params: dict[str, Any] = {
            "module": "account",
            "action": "tokentx",
            "address": address,
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset or self.config.page_size,
            "sort": sort,
        }
        if contract_address:
            params["contractaddress"] = contract_address
        return self._list_result(**params)

    def get_balance(self, address: str) -> int:
        """account.balance — current ETH balance in wei."""
        result = self._request(
            module="account",
            action="balance",
            address=address,
            tag="latest",
        )
        return int(result)

    def get_gas_oracle(self) -> dict[str, Any]:
        """gastracker.gasoracle — suggested gas prices (gwei)."""
        result = self._request(module="gastracker", action="gasoracle")
        if not isinstance(result, dict):
            raise EtherscanAPIError(f"Unexpected gasoracle payload: {result!r}")
        return result

    def get_contract_abi(self, address: str) -> str:
        """contract.getabi — verified contract ABI JSON string."""
        result = self._request(module="contract", action="getabi", address=address)
        if not isinstance(result, str):
            raise EtherscanAPIError(f"Unexpected getabi payload: {result!r}")
        return result

    def get_contract_source(self, address: str) -> list[dict[str, Any]] | dict[str, Any]:
        """contract.getsourcecode — verified source metadata."""
        result = self._request(module="contract", action="getsourcecode", address=address)
        return result

    def get_block_number(self) -> int:
        """proxy.eth_blockNumber — latest block as int."""
        result = self._request(module="proxy", action="eth_blockNumber")
        return int(result, 16) if isinstance(result, str) and result.startswith("0x") else int(result)

    # --- internal --------------------------------------------------------------

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.config.rate_limit_delay - elapsed
        if wait > 0:
            time.sleep(wait)

    def _list_result(self, **params: Any) -> list[dict[str, Any]]:
        result = self._request(**params)
        if result is None or result == []:
            return []
        if isinstance(result, str):
            # Etherscan returns a string message when there are no txs
            logger.debug("Empty list result message: %s", result)
            return []
        if not isinstance(result, list):
            raise EtherscanAPIError(f"Expected list result, got: {type(result)}")
        return result

    def _request(self, **params: Any) -> Any:
        params = {
            **params,
            "chainid": self.config.chain_id,
            "apikey": self.config.api_key,
        }
        # Drop Nones so optional filters stay clean
        params = {k: v for k, v in params.items() if v is not None}

        last_err: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            self._throttle()
            try:
                self._last_request_at = time.monotonic()
                resp = self._session.get(
                    self.config.base_url,
                    params=params,
                    timeout=self.config.request_timeout,
                )
                resp.raise_for_status()
                payload = resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_err = exc
                backoff = self.config.rate_limit_delay * (2 ** (attempt - 1))
                logger.warning(
                    "Etherscan request failed (attempt %s/%s): %s; sleep %.2fs",
                    attempt,
                    self.config.max_retries,
                    exc,
                    backoff,
                )
                time.sleep(backoff)
                continue

            status = str(payload.get("status", ""))
            message = str(payload.get("message", ""))
            result = payload.get("result")

            # Success shapes vary: status "1", or proxy methods without status
            if status == "1" or ("status" not in payload and result is not None):
                return result

            # No records is not a hard failure
            if status == "0" and message.lower() in {"no transactions found", "no records found"}:
                return []

            # Rate limit / NOTOK — retry
            result_str = str(result).lower() if result is not None else ""
            if "rate limit" in result_str or "max rate" in result_str or message == "NOTOK":
                last_err = EtherscanAPIError(f"{message}: {result}")
                backoff = self.config.rate_limit_delay * (2**attempt)
                logger.warning(
                    "Etherscan NOTOK/rate-limit (attempt %s/%s): %s; sleep %.2fs",
                    attempt,
                    self.config.max_retries,
                    result,
                    backoff,
                )
                time.sleep(backoff)
                continue

            raise EtherscanAPIError(f"Etherscan error status={status} message={message} result={result!r}")

        raise EtherscanAPIError(f"Etherscan request failed after {self.config.max_retries} attempts: {last_err}")
