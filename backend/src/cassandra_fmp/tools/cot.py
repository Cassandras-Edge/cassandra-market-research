"""Commitment of Traders (COT) report tools — direct FMP /stable calls."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from fastmcp import FastMCP

FMP_BASE = "https://financialmodelingprep.com/stable"


def register(mcp: FastMCP) -> None:
    api_key = os.environ.get("FMP_API_KEY", "")

    async def _fmp_get(path: str, params: dict | None = None) -> list | dict:
        p = {"apikey": api_key, **(params or {})}
        async with httpx.AsyncClient(timeout=15) as http:
            resp = await http.get(f"{FMP_BASE}/{path}", params=p)
            resp.raise_for_status()
            return resp.json()

    @mcp.tool(
        annotations={
            "title": "COT Report",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def cot_report(
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> dict:
        """Get Commitment of Traders report for a commodity/futures symbol.

        Shows positioning data for commercials, non-commercials (speculators),
        and non-reportable traders. Essential for gauging market sentiment in
        commodities and futures.

        Common symbols: GC (gold), SI (silver), CL (crude oil), NG (nat gas),
        HG (copper), ZW (wheat), ZC (corn), ZS (soybeans), ES (S&P 500 futures),
        NQ (Nasdaq futures), 6E (Euro FX), 6J (Japanese Yen).

        Args:
            symbol: Futures symbol (e.g. 'GC' for gold, 'CL' for crude oil).
            from_date: Start date (YYYY-MM-DD). Defaults to ~2 years of history.
            to_date: End date (YYYY-MM-DD). Defaults to latest available.
        """
        params: dict = {"symbol": symbol.upper()}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        try:
            data = await _fmp_get("commitment-of-traders-report", params)
        except httpx.HTTPError as e:
            return {"error": f"COT request failed: {e}"}

        if not isinstance(data, list) or not data:
            return {"error": f"No COT data found for '{symbol}'"}

        # Summarize positioning for each report date
        reports = []
        for row in data:
            reports.append({
                "date": row.get("date"),
                "name": row.get("name"),
                "sector": row.get("sector"),
                "open_interest": row.get("openInterestAll"),
                "noncomm_long": row.get("noncommPositionsLongAll"),
                "noncomm_short": row.get("noncommPositionsShortAll"),
                "noncomm_spread": row.get("noncommPositionsSpreadAll"),
                "comm_long": row.get("commPositionsLongAll"),
                "comm_short": row.get("commPositionsShortAll"),
                "noncomm_net": (row.get("noncommPositionsLongAll") or 0)
                - (row.get("noncommPositionsShortAll") or 0),
                "comm_net": (row.get("commPositionsLongAll") or 0)
                - (row.get("commPositionsShortAll") or 0),
                "change_open_interest": row.get("changeInOpenInterestAll"),
                "change_noncomm_long": row.get("changeInNoncommLongAll"),
                "change_noncomm_short": row.get("changeInNoncommShortAll"),
                "change_comm_long": row.get("changeInCommLongAll"),
                "change_comm_short": row.get("changeInCommShortAll"),
                "pct_oi_noncomm_long": row.get("pctOfOiNoncommLongAll"),
                "pct_oi_noncomm_short": row.get("pctOfOiNoncommShortAll"),
                "pct_oi_comm_long": row.get("pctOfOiCommLongAll"),
                "pct_oi_comm_short": row.get("pctOfOiCommShortAll"),
            })

        return {
            "symbol": symbol.upper(),
            "report_count": len(reports),
            "reports": reports,
        }
