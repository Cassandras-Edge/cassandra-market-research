"""Financial statements, growth, and financial health tools."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from cassandra_fmp.tools._helpers import TTL_DAILY, TTL_HOURLY, _as_list, _date_only, _safe_call, _safe_first

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient


def _calc_cagr(start_val: float | None, end_val: float | None, years: int) -> float | None:
    """Calculate compound annual growth rate."""
    if not start_val or not end_val or years <= 0:
        return None
    if start_val <= 0 or end_val <= 0:
        return None
    try:
        return round(((end_val / start_val) ** (1 / years) - 1) * 100, 2)
    except (ZeroDivisionError, ValueError, OverflowError):
        return None


def _simplify_period(income: dict, balance: dict, cashflow: dict) -> dict:
    """Extract key metrics from a single period across all three statements."""
    return {
        "date": _date_only(income.get("date") or balance.get("date") or cashflow.get("date")),
        "period": income.get("period") or balance.get("period"),
        # Income statement
        "revenue": income.get("revenue"),
        "gross_profit": income.get("grossProfit"),
        "operating_income": income.get("operatingIncome"),
        "net_income": income.get("netIncome"),
        "eps": income.get("eps"),
        "eps_diluted": income.get("epsDiluted"),
        "ebitda": income.get("ebitda"),
        "gross_margin": _pct(income.get("grossProfit"), income.get("revenue")),
        "operating_margin": _pct(income.get("operatingIncome"), income.get("revenue")),
        "net_margin": _pct(income.get("netIncome"), income.get("revenue")),
        # Balance sheet
        "total_assets": balance.get("totalAssets"),
        "total_liabilities": balance.get("totalLiabilities"),
        "total_equity": balance.get("totalStockholdersEquity"),
        "total_debt": balance.get("totalDebt"),
        "cash_and_equivalents": balance.get("cashAndCashEquivalents"),
        "net_debt": balance.get("netDebt"),
        # Cash flow
        "operating_cash_flow": cashflow.get("operatingCashFlow"),
        "capex": cashflow.get("capitalExpenditure"),
        "free_cash_flow": cashflow.get("freeCashFlow"),
        "dividends_paid": cashflow.get("commonDividendsPaid"),
        "share_buybacks": cashflow.get("commonStockRepurchased"),
    }


def _pct(numerator, denominator) -> float | None:
    """Calculate percentage safely."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator * 100, 2)


def _trend_direction(values: list[float | None]) -> str | None:
    """Determine trend direction from a time series (newest first)."""
    clean = [v for v in values if v is not None and isinstance(v, (int, float))]
    if len(clean) < 2:
        return None
    # Compare recent average to earlier average
    mid = len(clean) // 2
    recent_avg = sum(clean[:mid]) / mid
    earlier_avg = sum(clean[mid:]) / (len(clean) - mid)
    if recent_avg > earlier_avg * 1.05:  # 5% threshold
        return "improving"
    elif recent_avg < earlier_avg * 0.95:
        return "deteriorating"
    return "stable"


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    @mcp.tool(
        annotations={
            "title": "Ratio History",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def ratio_history(
        symbol: str,
        period: str = "annual",
        limit: int = 10,
    ) -> dict:
        """Get historical financial ratios with trend analysis.

        Returns time series and trend direction (improving/deteriorating/stable) for
        profitability (ROIC, ROE, ROA, margins), efficiency (asset/inventory turnover,
        cash conversion cycle), leverage (D/E, interest coverage), and liquidity
        (current/quick ratios).

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            period: "annual" or "quarter" (default "annual")
            limit: Number of historical periods to fetch (default 10)
        """
        symbol = symbol.upper().strip()

        # Fetch financial ratios and key metrics
        ratios_data, metrics_data = await asyncio.gather(
            _safe_call(
                client.fundamental.get_financial_ratios,
                symbol=symbol,
                period=period,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.fundamental.get_key_metrics,
                symbol=symbol,
                period=period,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            ),
        )

        ratios_list = _as_list(ratios_data)
        metrics_list = _as_list(metrics_data)

        if not ratios_list and not metrics_list:
            return {"error": f"No ratio data found for '{symbol}'"}

        # Build indexed lookups by date
        metrics_by_date = {_date_only(m.get("date")): m for m in metrics_list} if metrics_list else {}

        # Build time series
        time_series = []

        if ratios_list:
            # Primary path: use financial-ratios with key-metrics supplementation
            for r in ratios_list:
                date = _date_only(r.get("date")) or ""
                m = metrics_by_date.get(date, {})

                entry = {
                    "date": date,
                    "period": r.get("period"),
                    # Profitability
                    "roic": m.get("roic"),
                    "roe": r.get("returnOnEquity"),
                    "roa": r.get("returnOnAssets"),
                    "gross_margin": r.get("grossProfitMargin"),
                    "operating_margin": r.get("operatingProfitMargin"),
                    "net_margin": r.get("netProfitMargin"),
                    # Efficiency
                    "asset_turnover": r.get("assetTurnover"),
                    "inventory_turnover": r.get("inventoryTurnover"),
                    "cash_conversion_cycle": r.get("cashConversionCycle"),
                    # Leverage
                    "debt_to_equity": r.get("debtEquityRatio"),
                    "interest_coverage": r.get("interestCoverage"),
                    # Liquidity
                    "current_ratio": r.get("currentRatio"),
                    "quick_ratio": r.get("quickRatio"),
                    # FCF
                    "fcf_to_revenue": _pct(m.get("freeCashFlowPerShare"), m.get("revenuePerShare")),
                }
                time_series.append(entry)
        else:
            # Fallback: build from key-metrics alone (some symbols like GOOG
            # return empty financial-ratios but populated key-metrics)
            for m in metrics_list:
                entry = {
                    "date": _date_only(m.get("date")) or "",
                    "period": m.get("period"),
                    # Profitability
                    "roic": m.get("roic"),
                    "roe": m.get("returnOnEquity"),
                    "roa": m.get("returnOnAssets"),
                    "gross_margin": None,
                    "operating_margin": None,
                    "net_margin": m.get("netIncomePerShare") and m.get("revenuePerShare") and round(m["netIncomePerShare"] / m["revenuePerShare"], 4) if m.get("netIncomePerShare") and m.get("revenuePerShare") and m["revenuePerShare"] != 0 else None,
                    # Efficiency
                    "asset_turnover": None,
                    "inventory_turnover": None,
                    "cash_conversion_cycle": None,
                    # Leverage
                    "debt_to_equity": None,
                    "interest_coverage": m.get("interestCoverage"),
                    # Liquidity
                    "current_ratio": m.get("currentRatio"),
                    "quick_ratio": None,
                    # FCF
                    "fcf_to_revenue": _pct(m.get("freeCashFlowPerShare"), m.get("revenuePerShare")),
                }
                time_series.append(entry)

        # Calculate trends for each metric
        def _extract_series(key: str) -> list[float | None]:
            return [t.get(key) for t in time_series]

        trends = {
            "profitability": {
                "roic": _trend_direction(_extract_series("roic")),
                "roe": _trend_direction(_extract_series("roe")),
                "roa": _trend_direction(_extract_series("roa")),
                "gross_margin": _trend_direction(_extract_series("gross_margin")),
                "operating_margin": _trend_direction(_extract_series("operating_margin")),
                "net_margin": _trend_direction(_extract_series("net_margin")),
            },
            "efficiency": {
                "asset_turnover": _trend_direction(_extract_series("asset_turnover")),
                "inventory_turnover": _trend_direction(_extract_series("inventory_turnover")),
                "cash_conversion_cycle": _trend_direction([
                    -v if v is not None else None for v in _extract_series("cash_conversion_cycle")
                ]),  # Lower is better, so invert
            },
            "leverage": {
                "debt_to_equity": _trend_direction([
                    -v if v is not None else None for v in _extract_series("debt_to_equity")
                ]),  # Lower is better
                "interest_coverage": _trend_direction(_extract_series("interest_coverage")),
            },
            "liquidity": {
                "current_ratio": _trend_direction(_extract_series("current_ratio")),
                "quick_ratio": _trend_direction(_extract_series("quick_ratio")),
            },
            "fcf": {
                "fcf_to_revenue": _trend_direction(_extract_series("fcf_to_revenue")),
            },
        }

        result = {
            "symbol": symbol,
            "period_type": period,
            "time_series": time_series,
            "trends": trends,
        }

        _warnings = []
        if not ratios_list:
            _warnings.append("financial ratios unavailable")
        if not metrics_list:
            _warnings.append("key metrics unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Financial Statements",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def financial_statements(
        symbol: str,
        period: str = "annual",
        limit: int = 5,
    ) -> dict:
        """Get income statement, balance sheet, and cash flow data with growth rates.

        Returns simplified per-period financials and 3-year CAGRs for key metrics.
        Use after company_overview for deeper analysis.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            period: "annual" or "quarter" (default "annual")
            limit: Number of periods to return (default 5)
        """
        symbol = symbol.upper().strip()

        income_data, balance_data, cashflow_data = await asyncio.gather(
            _safe_call(
                client.fundamental.get_income_statement,
                symbol=symbol,
                period=period,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.fundamental.get_balance_sheet,
                symbol=symbol,
                period=period,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.fundamental.get_cash_flow,
                symbol=symbol,
                period=period,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            ),
        )

        if not income_data and not balance_data and not cashflow_data:
            return {"error": f"No financial data found for '{symbol}'"}

        # Ensure lists
        income_list = _as_list(income_data)
        balance_list = _as_list(balance_data)
        cashflow_list = _as_list(cashflow_data)

        # Build indexed lookups by date
        balance_by_date = {}
        for b in balance_list:
            key = _date_only(b.get("date"))
            if key:
                balance_by_date[key] = b
        cashflow_by_date = {}
        for c in cashflow_list:
            key = _date_only(c.get("date"))
            if key:
                cashflow_by_date[key] = c

        # Build simplified periods (income-statement-led since it's most complete)
        periods = []
        for inc in income_list:
            period_date = _date_only(inc.get("date")) or ""
            bal = balance_by_date.get(period_date, {})
            cf = cashflow_by_date.get(period_date, {})
            periods.append(_simplify_period(inc, bal, cf))

        # Calculate 3-year CAGRs if we have enough data
        # Data is newest-first, so index 0 = latest, index 3 = 3 years ago
        growth = {}
        if len(periods) >= 4:
            latest = periods[0]
            three_yr = periods[3]
            growth = {
                "revenue_cagr_3y": _calc_cagr(
                    three_yr.get("revenue"), latest.get("revenue"), 3
                ),
                "net_income_cagr_3y": _calc_cagr(
                    three_yr.get("net_income"), latest.get("net_income"), 3
                ),
                "fcf_cagr_3y": _calc_cagr(
                    three_yr.get("free_cash_flow"), latest.get("free_cash_flow"), 3
                ),
                "eps_cagr_3y": _calc_cagr(
                    three_yr.get("eps_diluted"), latest.get("eps_diluted"), 3
                ),
            }

        result = {
            "symbol": symbol,
            "period_type": period,
            "periods": periods,
        }
        if growth:
            result["growth_3y_cagr"] = growth

        # Flag partial data
        errors = []
        if not income_list:
            errors.append("income statement unavailable")
        if not balance_list:
            errors.append("balance sheet unavailable")
        if not cashflow_list:
            errors.append("cash flow statement unavailable")
        if errors:
            result["_warnings"] = errors

        return result

    @mcp.tool(
        annotations={
            "title": "Revenue Segments",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def revenue_segments(symbol: str) -> dict:
        """Get revenue breakdown by product/service and geographic region.

        Returns segment % of total, identifies fastest-growing segment,
        and flags concentration risk (>50% from one segment).

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        product_data, geo_data = await asyncio.gather(
            _safe_call(
                client.company.get_product_revenue_segmentation,
                symbol=symbol.upper().strip(),
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.company.get_geographic_revenue_segmentation,
                symbol=symbol.upper().strip(),
                ttl=TTL_HOURLY,
                default=[],
            ),
        )

        product_list = _as_list(product_data)
        geo_list = _as_list(geo_data)

        if not product_list and not geo_list:
            return {"error": f"No revenue segmentation data found for '{symbol}'"}

        def _process_segments(data_list: list) -> dict:
            """Process segment data into structured output with analysis."""
            if not data_list:
                return {}

            # Each item in data_list is a dict keyed by date, containing
            # a dict of segment_name: revenue_value
            # Sort by date to get latest and prior periods
            periods = []
            for item in data_list:
                if isinstance(item.get("data"), dict):
                    periods.append({"date": item.get("date"), "segments": item.get("data")})
                    continue
                for date_key, segments in item.items():
                    if isinstance(segments, dict):
                        periods.append({"date": date_key, "segments": segments})
            periods.sort(key=lambda p: p["date"], reverse=True)

            if not periods:
                return {}

            latest = periods[0]
            total = sum(v for v in latest["segments"].values() if isinstance(v, (int, float)) and v > 0)

            segments_out = []
            for name, value in sorted(latest["segments"].items(), key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, reverse=True):
                if not isinstance(value, (int, float)) or value <= 0:
                    continue
                pct = round(value / total * 100, 2) if total > 0 else None
                segment = {"name": name, "revenue": value, "pct_of_total": pct}

                # YoY growth if prior year available
                if len(periods) >= 2:
                    prior = periods[1]
                    prior_val = prior["segments"].get(name)
                    if prior_val and isinstance(prior_val, (int, float)) and prior_val > 0:
                        segment["yoy_growth_pct"] = round((value / prior_val - 1) * 100, 2)

                segments_out.append(segment)

            # Fastest growing segment
            fastest = None
            max_growth = float("-inf")
            for s in segments_out:
                growth = s.get("yoy_growth_pct")
                if growth is not None and growth > max_growth:
                    max_growth = growth
                    fastest = s["name"]

            # Concentration risk
            concentrated = any(
                (s.get("pct_of_total") or 0) > 50 for s in segments_out
            )

            return {
                "date": latest["date"],
                "total_revenue": total,
                "segments": segments_out,
                "fastest_growing": fastest,
                "concentration_risk": concentrated,
            }

        result = {"symbol": symbol}

        product_analysis = _process_segments(product_list)
        if product_analysis:
            result["product_segments"] = product_analysis

        geo_analysis = _process_segments(geo_list)
        if geo_analysis:
            result["geographic_segments"] = geo_analysis

        _warnings = []
        if not product_list:
            _warnings.append("product segmentation unavailable")
        if not geo_list:
            _warnings.append("geographic segmentation unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Financial Health",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def financial_health(symbol: str) -> dict:
        """Get financial health scores and owner earnings.

        Returns Altman Z-Score, Piotroski F-Score, owner earnings,
        and maintenance vs growth capex breakdown.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()

        scores_data, owner_data = await asyncio.gather(
            _safe_call(
                client.company.get_financial_scores,
                symbol=symbol,
                ttl=TTL_DAILY,
                default=[],
            ),
            _safe_call(
                client.fundamental.get_owner_earnings,
                symbol=symbol,
                ttl=TTL_DAILY,
                default=[],
            ),
        )

        scores_list = _as_list(scores_data)
        owner_list = _as_list(owner_data)

        if not scores_list and not owner_list:
            return {"error": f"No financial health data found for '{symbol}'"}

        scores = _safe_first(scores_list)
        owner = _safe_first(owner_list)

        result: dict = {"symbol": symbol}

        if scores:
            result["scores"] = {
                "altman_z_score": scores.get("altmanZScore"),
                "piotroski_score": scores.get("piotroskiScore"),
            }
            result["score_inputs"] = {
                "working_capital": scores.get("workingCapital"),
                "total_assets": scores.get("totalAssets"),
                "retained_earnings": scores.get("retainedEarnings"),
                "ebit": scores.get("ebit"),
                "market_cap": scores.get("marketCap"),
                "total_liabilities": scores.get("totalLiabilities"),
                "revenue": scores.get("revenue"),
            }

        if owner:
            result["owner_earnings"] = {
                "owner_earnings": owner.get("reportedOwnerEarnings")
                or owner.get("ownerEarnings")
                or owner.get("ownersEarnings"),
                "owner_earnings_per_share": owner.get("ownerEarningsPerShare")
                or owner.get("ownersEarningsPerShare"),
                "average_ppe": owner.get("averagePPE"),
                "maintenance_capex": owner.get("maintenanceCapex"),
                "growth_capex": owner.get("growthCapex"),
            }

        _warnings = []
        if not scores_list:
            _warnings.append("financial scores unavailable")
        if not owner_list:
            _warnings.append("owner earnings unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result
