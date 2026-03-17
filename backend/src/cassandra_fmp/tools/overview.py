"""Company overview, search, executives, and SEC filings tools."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from fmp_data.company.endpoints import DELISTED_COMPANIES
from cassandra_fmp.tools._helpers import (
    TTL_DAILY,
    TTL_HOURLY,
    TTL_REALTIME,
    _as_dict,
    _as_list,
    _date_only,
    _latest_price,
    _safe_call,
    _safe_endpoint_call,
)

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient
    from cassandra_fmp.clients.schwab import SchwabClient


def _fmp_quote_result(
    symbol: str,
    q: dict,
    client: AsyncFMPDataClient,  # noqa: ARG001 — kept for signature consistency
    premarket_data: Any = None,
    afterhours_data: Any = None,
) -> dict:
    """Build quote result dict from FMP data."""
    pre_candidates = [
        item for item in _as_list(premarket_data or [])
        if (item.get("symbol") or "").upper() == symbol
        and (item.get("session") or "").lower() == "pre"
    ]

    latest = _latest_price(q, pre_candidates, _as_dict(afterhours_data) if afterhours_data else None)

    result: dict = {
        "symbol": symbol,
        "name": q.get("name"),
        "price": latest["price"],
        "change": q.get("change"),
        "change_pct": latest.get("change_pct"),
        "volume": q.get("volume"),
        "market_cap": q.get("marketCap"),
        "source": "fmp",
    }
    if latest["source"] != "quote":
        result["price_source"] = latest["source"]
        result["regular_close"] = q.get("price")
    return result


def register(mcp: FastMCP, client: AsyncFMPDataClient, *, schwab_client: SchwabClient | None = None) -> None:
    @mcp.tool(
        annotations={
            "title": "Quote",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def quote(symbol: str) -> dict:
        """Get the current price for a stock, including pre-market/after-hours.

        Returns the freshest available price across regular session,
        pre-market, and after-hours. Minimal and fast.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()

        if schwab_client is not None:
            # Fire Schwab + FMP concurrently; Schwab for real-time price, FMP for market_cap
            schwab_data, fmp_data = await asyncio.gather(
                schwab_client.get_quote(symbol),
                _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            )
            fmp_q = _as_dict(fmp_data)

            if schwab_data and isinstance(schwab_data, dict) and schwab_data.get("mark") is not None:
                ext = schwab_data.get("extended_hours") or {}
                price = schwab_data["mark"]
                source = "schwab"

                result = {
                    "symbol": symbol,
                    "name": schwab_data.get("description") or fmp_q.get("name"),
                    "price": price,
                    "change": schwab_data.get("net_change"),
                    "change_pct": schwab_data.get("net_change_pct"),
                    "volume": schwab_data.get("volume"),
                    "market_cap": fmp_q.get("marketCap"),
                    "source": source,
                }
                if ext:
                    result["extended_hours"] = ext
                return result

            # Schwab failed — fall through to FMP-only
            if fmp_q:
                return _fmp_quote_result(symbol, fmp_q, client)

        # FMP-only path (no Schwab configured or both failed)
        quote_data, premarket_data, afterhours_data = await asyncio.gather(
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            _safe_call(client.market.get_pre_post_market, ttl=TTL_REALTIME, default=[]),
            _safe_call(client.company.get_aftermarket_trade, symbol=symbol, ttl=TTL_REALTIME, default=None),
        )

        q = _as_dict(quote_data)
        if not q:
            return {"error": f"No quote data for '{symbol}'"}

        return _fmp_quote_result(symbol, q, client, premarket_data, afterhours_data)

    @mcp.tool(
        annotations={
            "title": "Company Overview",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def company_overview(symbol: str, detail: bool = False) -> dict:
        """Get company profile, price data, and financial ratios.

        Default mode returns quote + most up-to-date price (including
        pre-market/after-hours when available). Use detail=True for full
        profile with sector, industry, description, and valuation ratios.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            detail: If True, include full profile and ratios (default False)
        """
        symbol = symbol.upper().strip()

        if not detail:
            # Lean mode: quote + extended hours for freshest price
            if schwab_client is not None:
                schwab_data, fmp_data = await asyncio.gather(
                    schwab_client.get_quote(symbol),
                    _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
                )
                fmp_q = _as_dict(fmp_data)
                if schwab_data and isinstance(schwab_data, dict) and schwab_data.get("mark") is not None:
                    result = {
                        "symbol": symbol,
                        "name": schwab_data.get("description") or fmp_q.get("name"),
                        "price": schwab_data["mark"],
                        "price_source": "schwab",
                        "market_cap": fmp_q.get("marketCap"),
                        "volume": schwab_data.get("volume"),
                        "change_pct": schwab_data.get("net_change_pct"),
                        "day_range": {"low": schwab_data.get("low"), "high": schwab_data.get("high")},
                        "year_range": {"low": schwab_data.get("52wk_low"), "high": schwab_data.get("52wk_high")},
                        "sma_50": fmp_q.get("priceAvg50"),
                        "sma_200": fmp_q.get("priceAvg200"),
                    }
                    ext = schwab_data.get("extended_hours")
                    if ext:
                        result["extended_hours"] = ext
                    return result
                # Schwab failed, use FMP data if available
                if fmp_q:
                    quote = fmp_q
                else:
                    return {"error": f"No data found for symbol '{symbol}'"}
            else:
                quote_data, premarket_data, afterhours_data = await asyncio.gather(
                    _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
                    _safe_call(client.market.get_pre_post_market, ttl=TTL_REALTIME, default=[]),
                    _safe_call(client.company.get_aftermarket_trade, symbol=symbol, ttl=TTL_REALTIME, default=None),
                )
                quote = _as_dict(quote_data)
                if not quote:
                    return {"error": f"No data found for symbol '{symbol}'"}
                premarket_data_local = premarket_data
                afterhours_data_local = afterhours_data

            # FMP-only price resolution (either Schwab failed or not configured)
            if schwab_client is not None:
                # Schwab was configured but failed — we have fmp_q as 'quote' but no extended hours
                pre_candidates: list = []
                afterhours_local = None
            else:
                pre_candidates = [
                    item for item in _as_list(premarket_data_local)
                    if (item.get("symbol") or "").upper() == symbol
                    and (item.get("session") or "").lower() == "pre"
                ]
                afterhours_local = afterhours_data_local

            latest = _latest_price(quote, pre_candidates, afterhours_local)

            result = {
                "symbol": symbol,
                "name": quote.get("name"),
                "price": latest["price"],
                "price_source": latest["source"],
                "market_cap": quote.get("marketCap"),
                "volume": quote.get("volume"),
                "change_pct": latest.get("change_pct"),
                "day_range": {"low": quote.get("dayLow"), "high": quote.get("dayHigh")},
                "year_range": {"low": quote.get("yearLow"), "high": quote.get("yearHigh")},
                "sma_50": quote.get("priceAvg50"),
                "sma_200": quote.get("priceAvg200"),
            }
            if latest["source"] != "quote":
                result["regular_close"] = quote.get("price")
            return result

        # Detail mode: full profile + quote + ratios + extended hours
        # Always fetch FMP profile + ratios; use Schwab for price if available
        coros: list = [
            _safe_call(client.company.get_profile, symbol=symbol, ttl=TTL_DAILY, default=None),
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            _safe_call(client.company.get_financial_ratios_ttm, symbol=symbol, ttl=TTL_HOURLY, default=[]),
        ]
        if schwab_client is not None:
            coros.append(schwab_client.get_quote(symbol))
        else:
            coros.append(_safe_call(client.market.get_pre_post_market, ttl=TTL_REALTIME, default=[]))
            coros.append(_safe_call(client.company.get_aftermarket_trade, symbol=symbol, ttl=TTL_REALTIME, default=None))

        results_list = await asyncio.gather(*coros)
        profile = _as_dict(results_list[0])
        quote = _as_dict(results_list[1])
        ratios = _as_dict(results_list[2])

        if not profile and not quote:
            return {"error": f"No data found for symbol '{symbol}'"}

        # Determine price source
        schwab_quote = None
        if schwab_client is not None:
            schwab_quote = results_list[3] if isinstance(results_list[3], dict) else None

        if schwab_quote and schwab_quote.get("mark") is not None:
            price = schwab_quote["mark"]
            price_source = "schwab"
            day_range = {"low": schwab_quote.get("low"), "high": schwab_quote.get("high")}
            year_range = {"low": schwab_quote.get("52wk_low"), "high": schwab_quote.get("52wk_high")}
            volume = schwab_quote.get("volume")
            change_pct = schwab_quote.get("net_change_pct")
            regular_close = None
        else:
            # FMP extended hours path
            if schwab_client is not None:
                # Schwab was configured but failed; no extended hours data
                pre_candidates_detail: list = []
                afterhours_detail = None
            else:
                premarket_data_detail = results_list[3]
                afterhours_data_detail = results_list[4]
                pre_candidates_detail = [
                    item for item in _as_list(premarket_data_detail)
                    if (item.get("symbol") or "").upper() == symbol
                    and (item.get("session") or "").lower() == "pre"
                ]
                afterhours_detail = afterhours_data_detail

            latest = _latest_price(quote, pre_candidates_detail, afterhours_detail)
            price = latest["price"]
            price_source = latest["source"]
            day_range = {"low": quote.get("dayLow"), "high": quote.get("dayHigh")}
            year_range = {"low": quote.get("yearLow"), "high": quote.get("yearHigh")}
            volume = quote.get("volume")
            change_pct = latest.get("change_pct")
            regular_close = quote.get("price") if latest["source"] != "quote" else None

        result = {
            "symbol": symbol,
            "name": profile.get("companyName"),
            "sector": profile.get("sector"),
            "industry": profile.get("industry"),
            "ceo": profile.get("ceo"),
            "employees": profile.get("fullTimeEmployees"),
            "description": profile.get("description"),
            "exchange": profile.get("exchange"),
            "country": profile.get("country"),
            "website": profile.get("website"),
            "price": price,
            "price_source": price_source,
            "market_cap": quote.get("marketCap"),
            "volume": volume,
            "change_pct": change_pct,
            "day_range": day_range,
            "year_range": year_range,
            "sma_50": quote.get("priceAvg50"),
            "sma_200": quote.get("priceAvg200"),
            "ratios": {
                "pe_ttm": ratios.get("priceToEarningsRatioTTM"),
                "pb_ttm": ratios.get("priceToBookRatioTTM"),
                "ps_ttm": ratios.get("priceToSalesRatioTTM"),
                "peg_ttm": ratios.get("priceToEarningsGrowthRatioTTM"),
                "ev_ebitda_ttm": ratios.get("enterpriseValueMultipleTTM"),
                "dividend_yield_ttm": ratios.get("dividendYieldTTM"),
                "roe_ttm": ratios.get("returnOnEquityTTM"),
                "roa_ttm": ratios.get("returnOnAssetsTTM"),
                "debt_equity_ttm": ratios.get("debtToEquityRatioTTM"),
                "current_ratio_ttm": ratios.get("currentRatioTTM"),
                "gross_margin_ttm": ratios.get("grossProfitMarginTTM"),
                "operating_margin_ttm": ratios.get("operatingProfitMarginTTM"),
                "net_margin_ttm": ratios.get("netProfitMarginTTM"),
                "price_to_fcf_ttm": ratios.get("priceToFreeCashFlowRatioTTM"),
            },
        }
        if regular_close is not None:
            result["regular_close"] = regular_close
        if schwab_quote and schwab_quote.get("extended_hours"):
            result["extended_hours"] = schwab_quote["extended_hours"]

        errors = []
        if not profile:
            errors.append("profile data unavailable")
        if not quote:
            errors.append("quote data unavailable")
        if not ratios:
            errors.append("ratio data unavailable")
        if errors:
            result["_warnings"] = errors
        return result

    @mcp.tool(
        annotations={
            "title": "Stock Search",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def stock_search(
        query: str,
        exchange: str | None = None,
        sector: str | None = None,
        market_cap_min: int | None = None,
        market_cap_max: int | None = None,
        price_min: float | None = None,
        price_max: float | None = None,
        beta_min: float | None = None,
        beta_max: float | None = None,
        volume_min: int | None = None,
        dividend_yield_min: float | None = None,
        dividend_yield_max: float | None = None,
        country: str | None = None,
        is_etf: bool | None = None,
        is_actively_trading: bool | None = None,
        limit: int = 20,
    ) -> dict:
        use_screener = any(
            [
                exchange,
                sector,
                market_cap_min,
                market_cap_max,
                price_min,
                price_max,
                beta_min,
                beta_max,
                volume_min,
                dividend_yield_min,
                dividend_yield_max,
                country,
                is_etf is not None,
                is_actively_trading is not None,
            ]
        )

        if use_screener:
            data = await _safe_call(
                client.market.get_company_screener,
                market_cap_more_than=market_cap_min,
                market_cap_less_than=market_cap_max,
                price_more_than=price_min,
                price_less_than=price_max,
                beta_more_than=beta_min,
                beta_less_than=beta_max,
                volume_more_than=volume_min,
                dividend_more_than=dividend_yield_min,
                dividend_less_than=dividend_yield_max,
                is_etf=is_etf,
                is_actively_trading=is_actively_trading,
                sector=sector,
                country=country,
                exchange=exchange,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            )
        else:
            data = await _safe_call(
                client.market.search_company,
                query=query,
                limit=limit,
                exchange=exchange,
                ttl=TTL_HOURLY,
                default=[],
            )

        rows = _as_list(data)
        if not rows:
            return {"results": [], "count": 0}

        results = []
        for item in rows[:limit]:
            results.append(
                {
                    "symbol": item.get("symbol"),
                    "name": item.get("companyName") or item.get("name"),
                    "exchange": item.get("exchangeShortName") or item.get("exchange"),
                    "sector": item.get("sector"),
                    "industry": item.get("industry"),
                    "market_cap": item.get("marketCap"),
                    "price": item.get("price"),
                    "beta": item.get("beta"),
                    "volume": item.get("volume"),
                    "dividend_yield": item.get("lastAnnualDividend"),
                    "country": item.get("country"),
                    "is_etf": item.get("isEtf"),
                    "is_actively_trading": item.get("isActivelyTrading"),
                }
            )
        return {"results": results, "count": len(results)}

    @mcp.tool(
        annotations={
            "title": "Company Executives",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def company_executives(symbol: str) -> dict:
        symbol = symbol.upper().strip()
        exec_data, comp_data, benchmark_data = await asyncio.gather(
            _safe_call(client.company.get_executives, symbol=symbol, ttl=TTL_DAILY, default=[]),
            _safe_call(client.company.get_executive_compensation, symbol=symbol, ttl=TTL_DAILY, default=[]),
            _safe_call(
                client.company.get_executive_compensation_benchmark,
                year=date.today().year,
                ttl=TTL_DAILY,
                default=[],
            ),
        )
        exec_list = _as_list(exec_data)
        comp_list = _as_list(comp_data)
        benchmark_list = _as_list(benchmark_data)
        if not exec_list:
            return {"error": f"No executive data found for '{symbol}'"}

        comp_by_name: dict[str, dict] = {}
        for comp in comp_list:
            name = comp.get("nameOfExecutive")
            if name:
                comp_by_name[name] = comp

        exec_list.sort(key=lambda e: e.get("pay") or 0, reverse=True)
        executives = []
        for e in exec_list:
            name = e.get("name")
            comp = comp_by_name.get(name, {})
            exec_entry = {
                "name": name,
                "title": e.get("title"),
                "pay": e.get("pay"),
                "currency": e.get("currencyPay"),
                "year_born": e.get("yearBorn"),
                "title_since": e.get("titleSince"),
                "gender": e.get("gender"),
            }
            if comp:
                exec_entry["compensation_breakdown"] = {
                    "filing_date": comp.get("filingDate"),
                    "accepted_date": comp.get("acceptedDate"),
                    "year": comp.get("year"),
                    "salary": comp.get("salary"),
                    "bonus": comp.get("bonus"),
                    "stock_award": comp.get("stockAward"),
                    "incentive_plan_compensation": comp.get("incentivePlanCompensation"),
                    "all_other_compensation": comp.get("allOtherCompensation"),
                    "total": comp.get("total"),
                }
            executives.append(exec_entry)

        benchmarks = []
        for bench in benchmark_list:
            benchmarks.append(
                {
                    "industry": bench.get("industry") or bench.get("industryTitle"),
                    "year": bench.get("year"),
                    "salary_average": bench.get("averageSalary") or bench.get("averageCompensation"),
                    "bonus_average": bench.get("averageBonus") or bench.get("averageCashCompensation"),
                    "stock_award_average": bench.get("averageStockAward") or bench.get("averageEquityCompensation"),
                    "incentive_plan_average": bench.get("averageIncentivePlanCompensation")
                    or bench.get("averageOtherCompensation"),
                    "total_average": bench.get("averageTotal") or bench.get("averageTotalCompensation"),
                    "percentile_25": bench.get("percentile25"),
                    "percentile_50": bench.get("percentile50"),
                    "percentile_75": bench.get("percentile75"),
                }
            )

        result = {"symbol": symbol, "count": len(executives), "executives": executives}
        if benchmarks:
            result["industry_benchmarks"] = benchmarks
        warnings = []
        if not comp_list:
            warnings.append("detailed compensation data unavailable")
        if not benchmark_list:
            warnings.append("industry benchmark data unavailable")
        if warnings:
            result["_warnings"] = warnings
        return result

    @mcp.tool(
        annotations={
            "title": "Employee History",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def employee_history(symbol: str) -> dict:
        symbol = symbol.upper().strip()
        data = await _safe_call(client.company.get_employee_count, symbol=symbol, ttl=TTL_DAILY, default=[])
        history_list = _as_list(data)
        if not history_list:
            return {"error": f"No employee data found for '{symbol}'"}

        history_list.sort(key=lambda h: h.get("periodDate") or "", reverse=True)
        current_count = history_list[0].get("employeeCount") if history_list else None
        history = []
        for i, h in enumerate(history_list):
            count = h.get("employeeCount")
            entry = {
                "period_date": h.get("periodDate"),
                "filing_date": h.get("filingDate"),
                "employee_count": count,
                "source": h.get("source"),
                "form_type": h.get("formType"),
            }
            if i > 0 and count is not None:
                for prev in history_list[i:]:
                    prev_count = prev.get("employeeCount")
                    if prev_count and prev_count != count:
                        entry["yoy_change"] = count - prev_count
                        entry["yoy_change_pct"] = round((count / prev_count - 1) * 100, 2) if prev_count else None
                        break
            history.append(entry)

        def calc_cagr(years: int) -> float | None:
            if len(history_list) < 2:
                return None
            start_count = None
            for h in reversed(history_list):
                if h.get("employeeCount"):
                    start_count = h.get("employeeCount")
                    break
            if not current_count or not start_count or start_count == 0:
                return None
            actual_years = len(history_list) - 1
            years = min(years, actual_years)
            if years <= 0:
                return None
            return round((pow(current_count / start_count, 1 / years) - 1) * 100, 2)

        result = {"symbol": symbol, "current_employee_count": current_count, "count": len(history), "history": history}
        cagr_5y = calc_cagr(5)
        cagr_10y = calc_cagr(10)
        if cagr_5y is not None or cagr_10y is not None:
            result["growth_metrics"] = {}
            if cagr_5y is not None:
                result["growth_metrics"]["cagr_5y_pct"] = cagr_5y
            if cagr_10y is not None:
                result["growth_metrics"]["cagr_10y_pct"] = cagr_10y
        return result

    @mcp.tool(
        annotations={
            "title": "Delisted Companies",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def delisted_companies(
        query: str | None = None,
        limit: int = 20,
    ) -> dict:
        page = 0
        data = await _safe_endpoint_call(
            client,
            DELISTED_COMPANIES,
            page=page,
            limit=max(limit * 5, 100),
            ttl=TTL_DAILY,
            default=[],
        )
        companies_list = _as_list(data)
        if query:
            q = query.lower().strip()
            matches = [
                c
                for c in companies_list
                if q in (c.get("symbol") or "").lower() or q in (c.get("companyName") or "").lower()
            ]
            non_matches = [
                c
                for c in companies_list
                if q not in (c.get("symbol") or "").lower() and q not in (c.get("companyName") or "").lower()
            ]
            companies_list = matches + non_matches
        if not companies_list:
            msg = "No delisted companies found"
            if query:
                msg += f" matching '{query}'"
            return {"error": msg}

        companies_list.sort(key=lambda c: c.get("delistedDate") or "", reverse=True)
        companies = []
        for c in companies_list[:limit]:
            companies.append(
                {
                    "symbol": c.get("symbol"),
                    "company_name": c.get("companyName"),
                    "exchange": c.get("exchange"),
                    "delisted_date": c.get("delistedDate"),
                    "ipo_date": c.get("ipoDate"),
                }
            )
        return {"query": query, "count": len(companies), "companies": companies}

    @mcp.tool(
        annotations={
            "title": "SEC Filings",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def sec_filings(
        symbol: str,
        form_type: str | None = None,
        limit: int = 20,
    ) -> dict:
        symbol = symbol.upper().strip()
        limit = max(1, min(limit, 100))
        to_date = date.today()
        from_date = to_date - timedelta(days=365 * 2)
        data = await _safe_call(
            client.sec.search_by_symbol,
            symbol=symbol,
            page=0,
            limit=100,
            from_date=from_date,
            to_date=to_date,
            ttl=TTL_HOURLY,
            default=[],
        )
        filings_list = _as_list(data)
        if not filings_list:
            return {"error": f"No SEC filings found for '{symbol}'"}
        if form_type:
            form_type_upper = form_type.upper().strip()
            filings_list = [f for f in filings_list if (f.get("formType") or "").upper() == form_type_upper]
        filings_list.sort(
            key=lambda f: _date_only(f.get("filingDate") or f.get("filedDate") or f.get("fillingDate")) or "",
            reverse=True,
        )
        filings = []
        for f in filings_list[:limit]:
            filings.append(
                {
                    "filing_date": _date_only(f.get("filingDate") or f.get("filedDate") or f.get("fillingDate")),
                    "accepted_date": f.get("acceptedDate"),
                    "form_type": f.get("formType"),
                    "url": f.get("finalLink") or f.get("link"),
                    "cik": f.get("cik"),
                }
            )
        return {"symbol": symbol, "form_type_filter": form_type, "count": len(filings), "filings": filings}

    @mcp.tool(
        annotations={
            "title": "Symbol Lookup",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def symbol_lookup(
        query: str,
        type: str = "name",
    ) -> dict:
        type_lower = type.lower().strip()
        if type_lower == "cik":
            data = await _safe_call(client.market.search_by_cik, query=query, ttl=TTL_DAILY, default=[])
        elif type_lower == "cusip":
            data = await _safe_call(client.market.search_by_cusip, query=query, ttl=TTL_DAILY, default=[])
        else:
            data = await _safe_call(client.market.search_company, query=query, ttl=TTL_DAILY, default=[])
            if not _as_list(data):
                data = await _safe_call(client.market.search_symbol, query=query, ttl=TTL_DAILY, default=[])

        results_list = _as_list(data)
        if not results_list:
            return {"error": f"No results found for {type} '{query}'"}

        results = []
        for item in results_list:
            results.append(
                {
                    "symbol": item.get("symbol"),
                    "company_name": item.get("companyName") or item.get("name"),
                    "cik": item.get("cik"),
                    "cusip": item.get("cusip"),
                    "exchange": item.get("exchange") or item.get("exchangeShortName") or item.get("exchangeFullName"),
                    "currency": item.get("currency"),
                }
            )
        return {"query": query, "lookup_type": type_lower, "count": len(results), "results": results}
