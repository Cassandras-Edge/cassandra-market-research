"""Async ThetaData REST client (talks to a ThetaTerminal sidecar over HTTP).

ThetaData has no cloud REST API — all requests go to a local ThetaTerminal
process (Java) that authenticates against the ThetaData account on startup
and exposes endpoints on http://127.0.0.1:25510 (or, in our k8s setup, the
theta-terminal ClusterIP service).

The client mirrors the ergonomics of clients/polygon.py: TTL cache, simple
rate limiting, and a `get_safe()` variant that returns a default on error.

Standard tier note: bulk_snapshot endpoints require a per-expiration `exp`
argument. `bulk_all_expirations()` fans out across `/v2/list/expirations` so
the same code paths work for both Standard and Pro tiers.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx


class ThetaDataError(Exception):
    """Raised when the ThetaTerminal REST API returns an error."""

    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        super().__init__(message)


class ThetaDataClient:
    """Async HTTP client for the ThetaTerminal REST API.

    All endpoints are served by a local ThetaTerminal process. In k8s the
    terminal runs as a separate Deployment + ClusterIP Service; the URL is
    configured via THETA_TERMINAL_URL.
    """

    DEFAULT_BASE_URL = "http://127.0.0.1:25510"

    # Rate limit between requests. ThetaTerminal is local so we keep this
    # tiny — the bottleneck is wire latency to the sidecar, not throughput.
    MIN_INTERVAL = float(os.environ.get("THETA_MIN_INTERVAL", "0.0"))

    # Cache TTLs (seconds)
    TTL_REALTIME = 60
    TTL_HOURLY = 3600
    TTL_6H = 21600
    TTL_DAILY = 86400

    def __init__(self, base_url: str | None = None, timeout: float = 30.0):
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._cache: dict[str, tuple[float, Any]] = {}
        self._last_call: float = 0.0
        self._lock = asyncio.Lock()

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self._timeout,
                headers={"Accept": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _cache_key(self, path: str, params: dict | None) -> str:
        sorted_params = sorted((params or {}).items())
        return f"{path}?{'&'.join(f'{k}={v}' for k, v in sorted_params)}"

    async def _rate_limit(self) -> None:
        if self.MIN_INTERVAL <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.MIN_INTERVAL:
                await asyncio.sleep(self.MIN_INTERVAL - elapsed)
            self._last_call = time.monotonic()

    async def get(
        self,
        path: str,
        params: dict | None = None,
        cache_ttl: int = 60,
    ) -> Any:
        """Make a GET request to ThetaTerminal.

        Returns the parsed JSON body. Raises ThetaDataError if the
        terminal reports an error in the response header.
        """
        key = self._cache_key(path, params)

        if cache_ttl > 0 and key in self._cache:
            cached_at, data = self._cache[key]
            if time.monotonic() - cached_at < cache_ttl:
                return data

        await self._rate_limit()

        try:
            resp = await self._get_client().get(path, params=params or {})
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise ThetaDataError(
                f"ThetaData API error {e.response.status_code}: {e.response.text[:200]}",
                status_code=e.response.status_code,
            ) from e
        except httpx.RequestError as e:
            raise ThetaDataError(f"Request to ThetaTerminal failed: {e}") from e

        data = resp.json()

        if isinstance(data, dict):
            header = data.get("header") or {}
            error_type = header.get("error_type") if isinstance(header, dict) else None
            if error_type and error_type != "null":
                raise ThetaDataError(f"ThetaData error: {error_type}")

        if cache_ttl > 0:
            self._cache[key] = (time.monotonic(), data)

        return data

    async def get_safe(
        self,
        path: str,
        params: dict | None = None,
        cache_ttl: int = 60,
        default: Any = None,
    ) -> Any:
        """Like get() but returns `default` on any error instead of raising."""
        try:
            return await self.get(path, params=params, cache_ttl=cache_ttl)
        except ThetaDataError:
            return default

    # ------------------------------------------------------------------
    # Reference / discovery
    # ------------------------------------------------------------------

    async def list_expirations(self, root: str) -> list[str]:
        """Return all known expiration dates (YYYYMMDD strings) for an underlying."""
        data = await self.get_safe(
            "/v2/list/expirations",
            params={"root": root},
            cache_ttl=self.TTL_DAILY,
        )
        if not data or not isinstance(data, dict):
            return []
        results = data.get("response") or []
        return [str(d) for d in results if d]

    # ------------------------------------------------------------------
    # Bulk snapshots (real-time, market-hours only)
    # ------------------------------------------------------------------

    async def bulk_snapshot_all_greeks(
        self,
        root: str,
        exp: int,
        cache_ttl: int = TTL_REALTIME,
    ) -> dict | None:
        """First-order Greeks + bid/ask + IV + underlying for one expiration."""
        return await self.get_safe(
            "/v2/bulk_snapshot/option/all_greeks",
            params={"root": root, "exp": exp},
            cache_ttl=cache_ttl,
        )

    async def bulk_snapshot_quote(
        self,
        root: str,
        exp: int,
        cache_ttl: int = TTL_REALTIME,
    ) -> dict | None:
        """Bid/ask sizes for one expiration (sizes are not in all_greeks)."""
        return await self.get_safe(
            "/v2/bulk_snapshot/option/quote",
            params={"root": root, "exp": exp},
            cache_ttl=cache_ttl,
        )

    async def bulk_snapshot_open_interest(
        self,
        root: str,
        exp: int,
        cache_ttl: int = TTL_HOURLY,
    ) -> dict | None:
        """Open interest per contract for one expiration."""
        return await self.get_safe(
            "/v2/bulk_snapshot/option/open_interest",
            params={"root": root, "exp": exp},
            cache_ttl=cache_ttl,
        )

    async def bulk_snapshot_ohlc(
        self,
        root: str,
        exp: int,
        cache_ttl: int = TTL_REALTIME,
    ) -> dict | None:
        """Session OHLC + volume per contract for one expiration."""
        return await self.get_safe(
            "/v2/bulk_snapshot/option/ohlc",
            params={"root": root, "exp": exp},
            cache_ttl=cache_ttl,
        )

    async def bulk_all_expirations(
        self,
        fetcher,
        root: str,
        expirations: list[str] | None = None,
        max_concurrency: int = 8,
    ) -> list[dict]:
        """Fan out a `bulk_snapshot_*` call across multiple expirations.

        ThetaData's Standard tier requires an explicit `exp` per request, so
        we list expirations and fan out. Returns a flat list of contract
        objects ({"ticks": [...], "contract": {...}}).
        """
        if expirations is None:
            expirations = await self.list_expirations(root)
        if not expirations:
            return []

        sem = asyncio.Semaphore(max_concurrency)

        async def _one(exp_str: str) -> list[dict]:
            try:
                exp_int = int(exp_str)
            except (TypeError, ValueError):
                return []
            async with sem:
                data = await fetcher(root, exp_int)
            if not data or not isinstance(data, dict):
                return []
            response = data.get("response") or []
            if not isinstance(response, list):
                return []
            return response

        results = await asyncio.gather(*[_one(e) for e in expirations])
        flat: list[dict] = []
        for batch in results:
            flat.extend(batch)
        return flat

    # ------------------------------------------------------------------
    # Historical (single contract)
    # ------------------------------------------------------------------

    async def hist_option_ohlc(
        self,
        *,
        root: str,
        exp: int,
        strike: int,
        right: str,
        start_date: int,
        end_date: int,
        ivl_ms: int = 0,
        rth: bool = True,
        cache_ttl: int = TTL_DAILY,
    ) -> dict | None:
        """Historical OHLC bars for a single option contract.

        Args:
            strike: in 1/10 of a cent ($170.00 => 170000 * 1000 = 1700000).
                ThetaData doc says "$170.00 strike price would be 170000",
                meaning their "1/10 of a cent" is actually milli-dollars.
            right: 'C' or 'P'.
            ivl_ms: bar interval in milliseconds. 0 = tick-level, 60000 =
                1min, 3600000 = 1h, 86400000 = 1d.
        """
        params: dict[str, Any] = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl_ms,
            "rth": "true" if rth else "false",
        }
        return await self.get_safe(
            "/v2/hist/option/ohlc",
            params=params,
            cache_ttl=cache_ttl,
        )
