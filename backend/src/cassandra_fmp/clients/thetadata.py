"""Async ThetaData v3 REST client (talks to a ThetaTerminal v3 sidecar over HTTP).

ThetaData has no cloud REST API — all requests go to a local ThetaTerminal
process (Java) that authenticates against the ThetaData account on startup
and exposes endpoints on http://127.0.0.1:25503 (or, in our k8s setup, the
theta-terminal ClusterIP service).

This module targets ThetaData v3 (the v2 API was superseded; paths, params,
and response shape all changed). v3 responses look like:

    {
      "response": [
        {
          "contract": {"symbol": "AAPL", "strike": 220.000,
                       "right": "CALL", "expiration": "2024-11-08"},
          "data": [
            {"timestamp": "...", "open": 1.2, "high": 1.3, ...},
            ...
          ]
        },
        ...
      ]
    }

The client flattens that into a plain ``list[dict]`` where each row carries
the contract metadata merged with a single data dict. Tool code never touches
the nested shape.
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
    """Async HTTP client for the ThetaTerminal v3 REST API.

    All endpoints are served by a local ThetaTerminal v3 process. In k8s the
    terminal runs as a separate Deployment + ClusterIP Service; the URL is
    configured via THETA_TERMINAL_URL.
    """

    DEFAULT_BASE_URL = "http://127.0.0.1:25503"

    MIN_INTERVAL = float(os.environ.get("THETA_MIN_INTERVAL", "0.0"))

    # Cache TTLs (seconds)
    TTL_REALTIME = 60
    TTL_HOURLY = 3600
    TTL_6H = 21600
    TTL_DAILY = 86400

    # ThetaData v3 allowed interval strings for history endpoints with an
    # ``interval`` param. Tool code is allowed to pass anything here; we
    # pass it through untouched.
    VALID_INTERVALS = {
        "tick", "10ms", "100ms", "500ms", "1s", "5s", "10s", "15s", "30s",
        "1m", "5m", "10m", "15m", "30m", "1h",
    }

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

    # ------------------------------------------------------------------
    # Low-level GET + response flattening
    # ------------------------------------------------------------------

    async def get(
        self,
        path: str,
        params: dict | None = None,
        cache_ttl: int = 60,
    ) -> Any:
        """Make a GET request to ThetaTerminal v3.

        Always forces ``format=json_new`` — the row-oriented response shape
        (accounts created before 2025-10-31 default to ``json_legacy`` which
        is columnar and would break our ``_flatten()`` parser). See
        https://docs.thetadata.us/json-guide.html.
        Returns the parsed JSON body. Raises ThetaDataError on any non-2xx.
        """
        merged_params: dict[str, Any] = {"format": "json_new"}
        if params:
            for k, v in params.items():
                if v is None:
                    continue
                merged_params[k] = v

        key = self._cache_key(path, merged_params)

        if cache_ttl > 0 and key in self._cache:
            cached_at, data = self._cache[key]
            if time.monotonic() - cached_at < cache_ttl:
                return data

        await self._rate_limit()

        try:
            resp = await self._get_client().get(path, params=merged_params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise ThetaDataError(
                f"ThetaData API error {e.response.status_code}: {e.response.text[:200]}",
                status_code=e.response.status_code,
            ) from e
        except httpx.RequestError as e:
            raise ThetaDataError(f"Request to ThetaTerminal failed: {e}") from e

        try:
            data = resp.json()
        except ValueError as e:
            raise ThetaDataError(f"ThetaData returned non-JSON body: {resp.text[:200]}") from e

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

    @staticmethod
    def _flatten(payload: Any) -> list[dict]:
        """Flatten a v3 ``{"response": [{"contract": ..., "data": [...]}, ...]}``
        payload into a flat list of dicts. Each row merges the contract metadata
        (symbol/strike/right/expiration) with a single data entry.

        Also handles flat list-of-dicts responses (used by list/expirations,
        list/strikes, etc.) by returning them as-is.
        """
        if not isinstance(payload, dict):
            return []
        response = payload.get("response")
        if not isinstance(response, list):
            return []

        out: list[dict] = []
        for entry in response:
            if not isinstance(entry, dict):
                continue
            # Flat shape (list/expirations, list/strikes, list/contracts)
            if "contract" not in entry and "data" not in entry:
                out.append(entry)
                continue
            # Nested shape with contract metadata + data rows
            contract = entry.get("contract") or {}
            data = entry.get("data") or []
            if not isinstance(data, list):
                continue
            for row in data:
                if not isinstance(row, dict):
                    continue
                merged = dict(contract)
                merged.update(row)
                out.append(merged)
        return out

    async def _get_flat(
        self,
        path: str,
        params: dict | None = None,
        cache_ttl: int = 60,
    ) -> list[dict]:
        """Shortcut: GET + flatten. Returns [] on any error."""
        payload = await self.get_safe(path, params=params, cache_ttl=cache_ttl)
        return self._flatten(payload)

    # ------------------------------------------------------------------
    # Reference / discovery
    # ------------------------------------------------------------------

    async def list_expirations(self, symbol: str) -> list[str]:
        """Return all known expiration dates (YYYYMMDD strings) for an underlying."""
        rows = await self._get_flat(
            "/v3/option/list/expirations",
            params={"symbol": symbol},
            cache_ttl=self.TTL_DAILY,
        )
        out: list[str] = []
        for r in rows:
            exp = r.get("expiration")
            if not exp:
                continue
            # v3 returns ISO dates ("2024-11-08") — normalize to YYYYMMDD
            if isinstance(exp, str):
                out.append(exp.replace("-", ""))
        return sorted(set(out))

    async def list_strikes(self, symbol: str, expiration: str) -> list[float]:
        """Return all known strikes (in dollars) for an underlying+expiration."""
        rows = await self._get_flat(
            "/v3/option/list/strikes",
            params={"symbol": symbol, "expiration": expiration},
            cache_ttl=self.TTL_DAILY,
        )
        out: list[float] = []
        for r in rows:
            s = r.get("strike")
            if isinstance(s, (int, float)):
                out.append(float(s))
        return sorted(set(out))

    async def list_contracts(
        self,
        request_type: str,
        date: str,
        symbol: str | None = None,
        cache_ttl: int = TTL_6H,
    ) -> list[dict]:
        """List all option contracts that had a trade or quote on a given date.

        request_type: "trade" or "quote".
        date: YYYYMMDD.
        symbol: optional comma-separated list of underlyings to filter by.
        """
        return await self._get_flat(
            f"/v3/option/list/contracts/{request_type}",
            params={"date": date, "symbol": symbol},
            cache_ttl=cache_ttl,
        )

    # ------------------------------------------------------------------
    # Snapshots (real-time, for options_chain)
    # ------------------------------------------------------------------

    async def snapshot_greeks_first_order(
        self,
        symbol: str,
        expiration: str = "*",
        strike: str | None = None,
        right: str | None = None,
        strike_range: int | None = None,
        max_dte: int | None = None,
        cache_ttl: int = TTL_REALTIME,
    ) -> list[dict]:
        """Real-time delta/gamma/theta/vega + IV + bid/ask across contracts.

        Passing ``expiration="*"`` returns every contract for every expiry in
        one call — this is the v3 replacement for v2's per-expiration fan-out.
        """
        return await self._get_flat(
            "/v3/option/snapshot/greeks/first_order",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "strike_range": strike_range,
                "max_dte": max_dte,
            },
            cache_ttl=cache_ttl,
        )

    async def snapshot_open_interest(
        self,
        symbol: str,
        expiration: str = "*",
        strike: str | None = None,
        right: str | None = None,
        strike_range: int | None = None,
        max_dte: int | None = None,
        cache_ttl: int = TTL_HOURLY,
    ) -> list[dict]:
        """Real-time open interest snapshot across contracts."""
        return await self._get_flat(
            "/v3/option/snapshot/open_interest",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "strike_range": strike_range,
                "max_dte": max_dte,
            },
            cache_ttl=cache_ttl,
        )

    async def snapshot_ohlc(
        self,
        symbol: str,
        expiration: str = "*",
        strike: str | None = None,
        right: str | None = None,
        strike_range: int | None = None,
        max_dte: int | None = None,
        cache_ttl: int = TTL_REALTIME,
    ) -> list[dict]:
        """Real-time session OHLCV snapshot across contracts."""
        return await self._get_flat(
            "/v3/option/snapshot/ohlc",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "strike_range": strike_range,
                "max_dte": max_dte,
            },
            cache_ttl=cache_ttl,
        )

    async def snapshot_quote(
        self,
        symbol: str,
        expiration: str = "*",
        strike: str | None = None,
        right: str | None = None,
        strike_range: int | None = None,
        max_dte: int | None = None,
        cache_ttl: int = TTL_REALTIME,
    ) -> list[dict]:
        """Real-time NBBO bid/ask + sizes snapshot across contracts."""
        return await self._get_flat(
            "/v3/option/snapshot/quote",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "strike_range": strike_range,
                "max_dte": max_dte,
            },
            cache_ttl=cache_ttl,
        )

    # ------------------------------------------------------------------
    # Historical endpoints (single-contract queries)
    # ------------------------------------------------------------------

    # ThetaData v3 caps intraday history (anything with an `interval` param)
    # at 1 month per request. We chunk client-side so callers can pass
    # arbitrary ranges without knowing the limit exists.
    CHUNK_DAYS = 28

    @staticmethod
    def _split_date_range(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
        """Split a YYYYMMDD date range into ~chunk_days windows (inclusive)."""
        try:
            from datetime import datetime, timedelta  # noqa: PLC0415

            start = datetime.strptime(start_date, "%Y%m%d").date()
            end = datetime.strptime(end_date, "%Y%m%d").date()
        except ValueError:
            return [(start_date, end_date)]

        if end < start:
            return [(start_date, end_date)]

        windows: list[tuple[str, str]] = []
        cur = start
        while cur <= end:
            chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
            windows.append((cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
            cur = chunk_end + timedelta(days=1)
        return windows

    async def _chunked_history(
        self,
        path: str,
        base_params: dict,
        start_date: str,
        end_date: str,
        cache_ttl: int,
    ) -> list[dict]:
        """Fetch an intraday history endpoint across arbitrary date ranges.

        ThetaData caps each request at 1 month. We split into ``CHUNK_DAYS``
        windows and fire them in parallel, concatenating the flattened rows.
        Single-chunk ranges take the direct path to avoid gather overhead.
        """
        windows = self._split_date_range(start_date, end_date, self.CHUNK_DAYS)
        if len(windows) <= 1:
            params = {**base_params, "start_date": start_date, "end_date": end_date}
            return await self._get_flat(path, params=params, cache_ttl=cache_ttl)

        async def _fetch(window_start: str, window_end: str) -> list[dict]:
            params = {**base_params, "start_date": window_start, "end_date": window_end}
            return await self._get_flat(path, params=params, cache_ttl=cache_ttl)

        chunks = await asyncio.gather(*[_fetch(s, e) for s, e in windows])
        out: list[dict] = []
        for chunk in chunks:
            out.extend(chunk)
        return out

    async def history_eod(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Daily EOD OHLCV + closing NBBO for a single contract across dates."""
        return await self._get_flat(
            "/v3/option/history/eod",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "start_date": start_date,
                "end_date": end_date,
            },
            cache_ttl=cache_ttl,
        )

    async def history_ohlc(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        interval: str = "1m",
        start_time: str | None = None,
        end_time: str | None = None,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Intraday aggregated OHLC bars for a single contract.

        The upstream endpoint caps each request at ~1 month of data; this
        method transparently chunks the range into 28-day windows and
        gathers them in parallel.
        """
        return await self._chunked_history(
            "/v3/option/history/ohlc",
            base_params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
            },
            start_date=start_date,
            end_date=end_date,
            cache_ttl=cache_ttl,
        )

    async def history_quote(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        interval: str = "1m",
        start_time: str | None = None,
        end_time: str | None = None,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Intraday NBBO bid/ask snapshots for a single contract.

        Auto-chunks ranges >1 month into 28-day windows.
        """
        return await self._chunked_history(
            "/v3/option/history/quote",
            base_params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
            },
            start_date=start_date,
            end_date=end_date,
            cache_ttl=cache_ttl,
        )

    async def history_open_interest(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Historical daily open interest for a single contract."""
        return await self._get_flat(
            "/v3/option/history/open_interest",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "start_date": start_date,
                "end_date": end_date,
            },
            cache_ttl=cache_ttl,
        )

    async def history_greeks_eod(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str | None = None,
        right: str | None = None,
        start_date: str,
        end_date: str,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Daily EOD greeks (delta/gamma/theta/vega/IV) for single contract or chain.

        Pass strike=None + right=None to use the default ``strike=*`` which
        returns every contract on the expiration. Pass ``expiration="*"``
        (with start_date == end_date) to get every contract on every expiration.
        """
        return await self._get_flat(
            "/v3/option/history/greeks/eod",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "start_date": start_date,
                "end_date": end_date,
            },
            cache_ttl=cache_ttl,
        )

    async def history_greeks_first_order(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        interval: str = "5m",
        start_time: str | None = None,
        end_time: str | None = None,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Intraday first-order greeks (delta/theta/vega/rho) + IV for a contract.

        Auto-chunks ranges >1 month into 28-day windows.
        """
        return await self._chunked_history(
            "/v3/option/history/greeks/first_order",
            base_params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
            },
            start_date=start_date,
            end_date=end_date,
            cache_ttl=cache_ttl,
        )

    async def history_greeks_implied_volatility(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        interval: str = "5m",
        start_time: str | None = None,
        end_time: str | None = None,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Historical implied volatility (bid/mid/ask IV) for a contract.

        Auto-chunks ranges >1 month into 28-day windows.
        """
        return await self._chunked_history(
            "/v3/option/history/greeks/implied_volatility",
            base_params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
            },
            start_date=start_date,
            end_date=end_date,
            cache_ttl=cache_ttl,
        )

    # ------------------------------------------------------------------
    # At-time endpoints (NBBO / IV at a specific second across dates)
    # ------------------------------------------------------------------

    async def at_time_quote(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Last NBBO quote at a specific wall-clock time on each date in range."""
        return await self._get_flat(
            "/v3/option/at_time/quote",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "start_date": start_date,
                "end_date": end_date,
                "time_of_day": time_of_day,
            },
            cache_ttl=cache_ttl,
        )

    async def at_time_trade(
        self,
        *,
        symbol: str,
        expiration: str,
        strike: str,
        right: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        cache_ttl: int = TTL_DAILY,
    ) -> list[dict]:
        """Last trade at a specific wall-clock time on each date in range."""
        return await self._get_flat(
            "/v3/option/at_time/trade",
            params={
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "right": right,
                "start_date": start_date,
                "end_date": end_date,
                "time_of_day": time_of_day,
            },
            cache_ttl=cache_ttl,
        )
