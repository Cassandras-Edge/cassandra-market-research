"""ESG (Environmental, Social, Governance) tools — direct FMP /stable calls."""

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
            "title": "ESG Ratings",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def esg_ratings(
        symbol: str,
    ) -> dict:
        """Get ESG risk ratings history for a company.

        Returns the ESG risk rating (letter grade) and industry rank across
        fiscal years. Useful for screening, compliance, and ESG-aware
        portfolio construction.

        Args:
            symbol: Stock ticker (e.g. 'AAPL', 'MSFT').
        """
        try:
            data = await _fmp_get("esg-ratings", {"symbol": symbol.upper()})
        except httpx.HTTPError as e:
            return {"error": f"ESG ratings request failed: {e}"}

        if not isinstance(data, list) or not data:
            return {"error": f"No ESG ratings found for '{symbol}'"}

        ratings = []
        for row in data:
            ratings.append({
                "fiscal_year": row.get("fiscalYear"),
                "rating": row.get("ESGRiskRating"),
                "industry": row.get("industry"),
                "industry_rank": row.get("industryRank"),
            })

        # Sort by fiscal year descending
        ratings.sort(key=lambda r: r.get("fiscal_year") or 0, reverse=True)

        return {
            "symbol": symbol.upper(),
            "company_name": data[0].get("companyName"),
            "rating_count": len(ratings),
            "ratings": ratings,
        }

    @mcp.tool(
        annotations={
            "title": "ESG Sector Benchmark",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def esg_benchmark(
        year: int = 2023,
    ) -> dict:
        """Get ESG benchmark scores by sector for a given year.

        Returns environmental, social, governance, and composite ESG scores
        for each sector. Useful for comparing a company's ESG performance
        against its sector peers.

        Args:
            year: Fiscal year for benchmark data (default: 2023).
        """
        try:
            data = await _fmp_get("esg-benchmark", {"year": year})
        except httpx.HTTPError as e:
            return {"error": f"ESG benchmark request failed: {e}"}

        if not isinstance(data, list) or not data:
            return {"error": f"No ESG benchmark data for year {year}"}

        sectors = []
        for row in data:
            sectors.append({
                "sector": row.get("sector"),
                "environmental_score": row.get("environmentalScore"),
                "social_score": row.get("socialScore"),
                "governance_score": row.get("governanceScore"),
                "esg_score": row.get("ESGScore"),
            })

        # Sort by composite ESG score descending
        sectors.sort(key=lambda s: s.get("esg_score") or 0, reverse=True)

        return {
            "year": year,
            "sector_count": len(sectors),
            "sectors": sectors,
        }
