"""Economy indicators tool via Polygon.io."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from cass_market_sdk.clients.polygon import PolygonClient

VALID_CATEGORIES = {"inflation", "labor", "rates", "all"}


def _extract_trend(results: list[dict], value_field: str, count: int = 12) -> list[dict]:
    """Extract recent trend data points from Polygon results."""
    out = []
    for r in results[:count]:
        val = r.get(value_field)
        if val is not None:
            out.append({"date": r.get("date"), "value": val})
    return out


def register(mcp: FastMCP, polygon_client: PolygonClient) -> None:
    @mcp.tool(
        annotations={
            "title": "Economy Indicators",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def economy_indicators(
        category: str = "all",
    ) -> dict:
        """Get macroeconomic indicator time series: CPI, unemployment, treasury yields.

        Returns latest values plus 12-month trend for each indicator.
        FMP only has an economic calendar (upcoming events) — this provides
        actual historical values for inflation, labor, and yield curve data.

        Args:
            category: "inflation", "labor", "rates", or "all" (default "all")
        """
        category = category.lower().strip()
        if category not in VALID_CATEGORIES:
            return {"error": f"Invalid category '{category}'. Use: {', '.join(sorted(VALID_CATEGORIES))}"}

        common_params = {"limit": 13, "sort": "date.desc"}
        result: dict = {"category": category, "source": "polygon.io"}
        _warnings: list[str] = []

        async def _fetch_inflation() -> dict | None:
            data = await polygon_client.get_safe(
                "/fed/v1/inflation",
                params=common_params,
                cache_ttl=polygon_client.TTL_6H,
            )
            if not data or not isinstance(data, dict):
                return None
            results = data.get("results", [])
            if not results:
                return None
            latest = results[0]
            return {
                "latest": {
                    "date": latest.get("date"),
                    "cpi": latest.get("cpi"),
                    "cpi_core": latest.get("cpi_core"),
                    "cpi_yoy_pct": latest.get("cpi_year_over_year"),
                    "pce": latest.get("pce"),
                    "pce_core": latest.get("pce_core"),
                },
                "cpi_yoy_trend": _extract_trend(results, "cpi_year_over_year"),
            }

        async def _fetch_inflation_expectations() -> dict | None:
            data = await polygon_client.get_safe(
                "/fed/v1/inflation-expectations",
                params=common_params,
                cache_ttl=polygon_client.TTL_6H,
            )
            if not data or not isinstance(data, dict):
                return None
            results = data.get("results", [])
            if not results:
                return None
            latest = results[0]
            return {
                "latest": {
                    "date": latest.get("date"),
                    "market_5y": latest.get("market_5_year"),
                    "market_10y": latest.get("market_10_year"),
                    "model_1y": latest.get("model_1_year"),
                    "model_5y": latest.get("model_5_year"),
                    "model_10y": latest.get("model_10_year"),
                    "forward_5y_to_10y": latest.get("forward_years_5_to_10"),
                },
                "market_5y_trend": _extract_trend(results, "market_5_year"),
            }

        async def _fetch_labor() -> dict | None:
            data = await polygon_client.get_safe(
                "/fed/v1/labor-market",
                params=common_params,
                cache_ttl=polygon_client.TTL_6H,
            )
            if not data or not isinstance(data, dict):
                return None
            results = data.get("results", [])
            if not results:
                return None
            latest = results[0]
            return {
                "latest": {
                    "date": latest.get("date"),
                    "unemployment_rate": latest.get("unemployment_rate"),
                    "labor_force_participation": latest.get("labor_force_participation_rate"),
                    "avg_hourly_earnings": latest.get("avg_hourly_earnings"),
                    "job_openings_thousands": latest.get("job_openings"),
                },
                "unemployment_trend": _extract_trend(results, "unemployment_rate"),
            }

        async def _fetch_rates() -> dict | None:
            data = await polygon_client.get_safe(
                "/fed/v1/treasury-yields",
                params=common_params,
                cache_ttl=polygon_client.TTL_6H,
            )
            if not data or not isinstance(data, dict):
                return None
            results = data.get("results", [])
            if not results:
                return None
            latest = results[0]
            return {
                "latest": {
                    "date": latest.get("date"),
                    "yield_1m": latest.get("yield_1_month"),
                    "yield_3m": latest.get("yield_3_month"),
                    "yield_6m": latest.get("yield_6_month"),
                    "yield_1y": latest.get("yield_1_year"),
                    "yield_2y": latest.get("yield_2_year"),
                    "yield_5y": latest.get("yield_5_year"),
                    "yield_10y": latest.get("yield_10_year"),
                    "yield_20y": latest.get("yield_20_year"),
                    "yield_30y": latest.get("yield_30_year"),
                },
                "yield_10y_trend": _extract_trend(results, "yield_10_year"),
                "yield_2y_trend": _extract_trend(results, "yield_2_year"),
            }

        if category == "inflation":
            inflation, expectations = await asyncio.gather(
                _fetch_inflation(), _fetch_inflation_expectations()
            )
            if inflation:
                result["inflation"] = inflation
            else:
                _warnings.append("inflation data unavailable")
            if expectations:
                result["inflation_expectations"] = expectations
            else:
                _warnings.append("inflation expectations unavailable")

        elif category == "labor":
            labor = await _fetch_labor()
            if labor:
                result["labor"] = labor
            else:
                _warnings.append("labor market data unavailable")

        elif category == "rates":
            rates = await _fetch_rates()
            if rates:
                result["treasury_yields"] = rates
            else:
                _warnings.append("treasury yields unavailable")

        else:  # "all"
            inflation, expectations, labor, rates = await asyncio.gather(
                _fetch_inflation(),
                _fetch_inflation_expectations(),
                _fetch_labor(),
                _fetch_rates(),
            )
            if inflation:
                result["inflation"] = inflation
            else:
                _warnings.append("inflation data unavailable")
            if expectations:
                result["inflation_expectations"] = expectations
            else:
                _warnings.append("inflation expectations unavailable")
            if labor:
                result["labor"] = labor
            else:
                _warnings.append("labor market data unavailable")
            if rates:
                result["treasury_yields"] = rates
            else:
                _warnings.append("treasury yields unavailable")

        if _warnings:
            result["_warnings"] = _warnings

        return result
