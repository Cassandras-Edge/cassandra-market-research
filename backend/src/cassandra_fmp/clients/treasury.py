"""Async US Treasury Fiscal Data API + FRED CMT yield client.

Data sources:
- Treasury auctions: https://api.fiscaldata.treasury.gov (free, no key)
- CMT yields (WI proxy): https://api.stlouisfed.org (requires FRED_API_KEY)
"""

from __future__ import annotations

import asyncio
import time
from datetime import date, timedelta
from typing import Any

import httpx


class TreasuryError(Exception):
    """Raised when the Treasury Fiscal Data API returns an error."""

    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        super().__init__(message)


class TreasuryClient:
    """Async HTTP client for the US Treasury Fiscal Data API.

    Features:
    - Simple in-memory TTL cache (mirrors PolygonClient)
    - Rate limiting (polite default: 5 req/s)
    - Graceful error handling for composite tools
    - No API key required
    """

    BASE_URL = "https://api.fiscaldata.treasury.gov"

    # Conservative rate limit: 0.2s between calls (~5 req/s)
    MIN_INTERVAL = 0.2

    # Cache TTLs (reuse PolygonClient convention)
    TTL_REALTIME = 60
    TTL_HOURLY = 3600
    TTL_6H = 21600
    TTL_DAILY = 86400

    # FRED CMT series mapping: security_term keyword → FRED series ID
    CMT_SERIES = {
        "2-Year": "DGS2", "3-Year": "DGS3", "5-Year": "DGS5",
        "7-Year": "DGS7", "10-Year": "DGS10", "20-Year": "DGS20",
        "30-Year": "DGS30",
    }

    FRED_BASE_URL = "https://api.stlouisfed.org"

    def __init__(self, *, fred_api_key: str | None = None) -> None:
        self.fred_api_key = fred_api_key
        self._client: httpx.AsyncClient | None = None
        self._fred_client: httpx.AsyncClient | None = None
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

    def _get_fred_client(self) -> httpx.AsyncClient:
        if self._fred_client is None or self._fred_client.is_closed:
            self._fred_client = httpx.AsyncClient(
                base_url=self.FRED_BASE_URL,
                timeout=15.0,
                headers={"Accept": "application/json"},
            )
        return self._fred_client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        if self._fred_client and not self._fred_client.is_closed:
            await self._fred_client.aclose()

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
        """Make a GET request to the Treasury Fiscal Data API.

        Args:
            path: API path (e.g. "/services/api/fiscal_service/v1/accounting/od/auctions_query")
            params: Additional query parameters
            cache_ttl: Cache duration in seconds (0 to disable)

        Returns:
            Parsed JSON response

        Raises:
            TreasuryError: On API errors or invalid responses
        """
        params = dict(params or {})
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
            raise TreasuryError(
                f"Treasury API error {e.response.status_code}: {e.response.text[:200]}",
                status_code=e.response.status_code,
            ) from e
        except httpx.RequestError as e:
            raise TreasuryError(f"Request failed: {e}") from e

        data = resp.json()

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
        except TreasuryError:
            return default

    async def fetch_auctions(
        self,
        *,
        days_back: int = 30,
        security_type: str | None = None,
        security_term: str | None = None,
        page_size: int = 100,
    ) -> list[dict]:
        """Fetch Treasury auction results from the Fiscal Data API.

        Args:
            days_back: Number of days to look back
            security_type: Filter by type (e.g. "Note", "Bond", "Bill")
            security_term: Filter by term (e.g. "10-Year", "2-Year")
            page_size: Results per page (max 10000)

        Returns:
            List of auction result dicts with string values (API returns all as strings)
        """
        since = (date.today() - timedelta(days=days_back)).isoformat()

        fields = ",".join([
            "cusip", "security_type", "security_term",
            "auction_date", "issue_date",
            "high_yield", "high_discnt_rate", "high_investment_rate",
            "avg_med_yield", "bid_to_cover_ratio",
            "offering_amt", "total_tendered", "total_accepted",
            "direct_bidder_accepted", "indirect_bidder_accepted",
            "primary_dealer_accepted", "comp_accepted", "noncomp_accepted",
            "soma_accepted", "soma_holdings",
            "cash_management_bill_cmb",
        ])

        filters = [f"auction_date:gte:{since}"]
        if security_type:
            filters.append(f"security_type:eq:{security_type}")
        if security_term:
            filters.append(f"security_term:eq:{security_term}")

        all_records: list[dict] = []
        page = 1
        max_pages = 10  # Safety cap to avoid runaway pagination

        while True:
            data = await self.get(
                "/services/api/fiscal_service/v1/accounting/od/auctions_query",
                params={
                    "fields": fields,
                    "filter": ",".join(filters),
                    "sort": "-auction_date",
                    "page[size]": str(page_size),
                    "page[number]": str(page),
                    "format": "json",
                },
                cache_ttl=self.TTL_HOURLY,
            )

            records = data.get("data", [])
            all_records.extend(records)

            meta = data.get("meta", {})
            total_pages = meta.get("total-pages", 1)
            if page >= total_pages or page >= max_pages:
                break
            page += 1

        return all_records

    def _get_cmt_series(self, security_term: str) -> str | None:
        """Map a security_term to FRED CMT series ID."""
        for keyword, series in self.CMT_SERIES.items():
            if keyword in security_term:
                return series
        return None

    async def fetch_cmt_yield(
        self,
        security_term: str,
        auction_date: str,
    ) -> float | None:
        """Fetch FRED Constant Maturity Treasury yield for a given auction date.

        Looks back up to 7 calendar days to find the most recent observation.
        Returns None if FRED API key is not configured or no data found.
        """
        if not self.fred_api_key:
            return None

        series_id = self._get_cmt_series(security_term)
        if not series_id:
            return None

        cache_key = f"fred:{series_id}:{auction_date}"
        if cache_key in self._cache:
            cached_at, data = self._cache[cache_key]
            if time.monotonic() - cached_at < self.TTL_DAILY:
                return data

        try:
            obs_date = date.fromisoformat(auction_date)
        except ValueError:
            return None

        start = (obs_date - timedelta(days=7)).isoformat()

        try:
            resp = await self._get_fred_client().get(
                "/fred/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": self.fred_api_key,
                    "file_type": "json",
                    "observation_start": start,
                    "observation_end": auction_date,
                    "sort_order": "desc",
                    "limit": "5",
                },
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError:
            return None

        for obs in data.get("observations", []):
            val = obs.get("value", "").strip()
            if val and val != ".":
                try:
                    result = float(val)
                    self._cache[cache_key] = (time.monotonic(), result)
                    return result
                except ValueError:
                    continue

        return None
