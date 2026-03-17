"""Async Polygon.io API client with TTL caching, rate limiting, and error handling."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx


class PolygonError(Exception):
    """Raised when the Polygon API returns an error."""

    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        super().__init__(message)


class PolygonClient:
    """Async HTTP client for Polygon.io API.

    Features:
    - Simple in-memory TTL cache (mirrors FMPClient)
    - Rate limiting (configurable via POLYGON_MIN_INTERVAL env var)
    - Graceful error handling for composite tools
    - Automatic API key injection
    """

    BASE_URL = "https://api.polygon.io"

    # Rate limit: default 0.1s (~10 req/s). Paid tiers have unlimited requests;
    # Polygon recommends staying under 100 req/s. Free tier users should set
    # POLYGON_MIN_INTERVAL=12.0 in .env (5 calls/min).
    MIN_INTERVAL = float(os.environ.get("POLYGON_MIN_INTERVAL", "0.1"))

    # Cache TTLs
    TTL_REALTIME = 60
    TTL_HOURLY = 3600
    TTL_6H = 21600
    TTL_DAILY = 86400

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client: httpx.AsyncClient | None = None
        self._cache: dict[str, tuple[float, Any]] = {}
        self._last_call: float = 0.0

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                timeout=30.0,
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
        """Enforce minimum interval between API calls."""
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.MIN_INTERVAL:
            await asyncio.sleep(self.MIN_INTERVAL - elapsed)
        self._last_call = time.monotonic()

    async def get(
        self,
        path: str,
        params: dict | None = None,
        cache_ttl: int = 300,
    ) -> Any:
        """Make a GET request to the Polygon API.

        Args:
            path: API path (e.g. "/v3/snapshot/options/AAPL")
            params: Additional query parameters
            cache_ttl: Cache duration in seconds (0 to disable)

        Returns:
            Parsed JSON response

        Raises:
            PolygonError: On API errors or invalid responses
        """
        params = dict(params or {})
        params["apiKey"] = self.api_key

        key = self._cache_key(path, params)

        # Check cache
        if cache_ttl > 0 and key in self._cache:
            cached_at, data = self._cache[key]
            if time.monotonic() - cached_at < cache_ttl:
                return data

        await self._rate_limit()

        try:
            resp = await self._get_client().get(path, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise PolygonError(
                f"Polygon API error {e.response.status_code}: {e.response.text[:200]}",
                status_code=e.response.status_code,
            ) from e
        except httpx.RequestError as e:
            raise PolygonError(f"Request failed: {e}") from e

        data = resp.json()

        # Polygon error patterns
        if isinstance(data, dict):
            if data.get("status") == "ERROR":
                raise PolygonError(data.get("error", "Unknown Polygon error"))
            if data.get("status") == "NOT_ENTITLED":
                raise PolygonError("Not entitled to this data (check Polygon plan)")

        # Cache the result
        if cache_ttl > 0:
            self._cache[key] = (time.monotonic(), data)

        return data

    async def get_safe(
        self,
        path: str,
        params: dict | None = None,
        cache_ttl: int = 300,
        default: Any = None,
    ) -> Any:
        """Like get() but returns default on error instead of raising."""
        try:
            return await self.get(path, params=params, cache_ttl=cache_ttl)
        except PolygonError:
            return default
