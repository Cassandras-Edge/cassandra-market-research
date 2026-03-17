"""Macro-economic data tools: treasury rates, economic calendar, market overview, IPOs, dividends calendar, indices, sector valuation, crowdfunding."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import TYPE_CHECKING

import toon
from fmp_data.models import APIVersion, Endpoint, EndpointParam, ParamLocation, ParamType

from cassandra_fmp.tools._helpers import (
    TTL_12H,
    TTL_DAILY,
    TTL_HOURLY,
    TTL_REALTIME,
    _as_dict,
    _as_list,
    _date_only,
    _safe_call,
    _safe_endpoint_call,
    _safe_first,
    _to_date,
)

if TYPE_CHECKING:
    from fmp_data import AsyncFMPDataClient
    from fastmcp import FastMCP

# Custom endpoints not yet in fmp-data SDK
CROWDFUNDING_LATEST = Endpoint(
    name="crowdfunding_latest",
    path="crowdfunding-offerings-latest",
    version=APIVersion.STABLE,
    description="Get latest crowdfunding campaigns",
    mandatory_params=[],
    optional_params=[
        EndpointParam(
            name="page",
            location=ParamLocation.QUERY,
            param_type=ParamType.INTEGER,
            required=False,
            description="Page number",
        ),
    ],
    response_model=dict,
)

CROWDFUNDING_SEARCH = Endpoint(
    name="crowdfunding_search",
    path="crowdfunding-offerings-search",
    version=APIVersion.STABLE,
    description="Search crowdfunding campaigns by name",
    mandatory_params=[
        EndpointParam(
            name="name",
            location=ParamLocation.QUERY,
            param_type=ParamType.STRING,
            required=True,
            description="Company or campaign name",
        ),
    ],
    optional_params=[],
    response_model=dict,
)

FUNDRAISING_LATEST = Endpoint(
    name="fundraising_latest",
    path="fundraising-latest",
    version=APIVersion.STABLE,
    description="Get latest Form D exempt offering filings",
    mandatory_params=[],
    optional_params=[
        EndpointParam(
            name="page",
            location=ParamLocation.QUERY,
            param_type=ParamType.INTEGER,
            required=False,
            description="Page number",
        ),
    ],
    response_model=dict,
)

FUNDRAISING_SEARCH = Endpoint(
    name="fundraising_search",
    path="fundraising-search",
    version=APIVersion.STABLE,
    description="Search Form D filings by company/fund name",
    mandatory_params=[
        EndpointParam(
            name="name",
            location=ParamLocation.QUERY,
            param_type=ParamType.STRING,
            required=True,
            description="Company or fund name",
        ),
    ],
    optional_params=[],
    response_model=dict,
)

FUNDRAISING_BY_CIK = Endpoint(
    name="fundraising_by_cik",
    path="fundraising",
    version=APIVersion.STABLE,
    description="Get Form D filing history for a company by CIK",
    mandatory_params=[
        EndpointParam(
            name="cik",
            location=ParamLocation.QUERY,
            param_type=ParamType.STRING,
            required=True,
            description="SEC Central Index Key (CIK)",
        ),
    ],
    optional_params=[],
    response_model=dict,
)

MIN_MARKET_CAP = 1_000_000_000  # $1B floor for movers


async def _fetch_sectors(client: "AsyncFMPDataClient") -> list[dict]:
    """Fetch sector performance from NYSE + NASDAQ and average by sector.

    /stable/sector-performance-snapshot requires `date` and returns
    per-exchange data with `averageChange` (not `changesPercentage`).
    """
    today_dt = date.today()
    nyse_data, nasdaq_data = await asyncio.gather(
        _safe_call(
            client.market.get_sector_performance,
            date=today_dt,
            exchange="NYSE",
            ttl=TTL_REALTIME,
            default=[],
        ),
        _safe_call(
            client.market.get_sector_performance,
            date=today_dt,
            exchange="NASDAQ",
            ttl=TTL_REALTIME,
            default=[],
        ),
    )

    nyse_list = _as_list(nyse_data)
    nasdaq_list = _as_list(nasdaq_data)

    # Build lookup by sector and average
    sector_vals: dict[str, list[float]] = {}
    for entry in nyse_list + nasdaq_list:
        sector = entry.get("sector")
        change = entry.get("averageChange")
        if change is None:
            change = entry.get("changePercentage")
        if sector and change is not None:
            sector_vals.setdefault(sector, []).append(change)

    sectors = []
    for sector, vals in sector_vals.items():
        avg = round(sum(vals) / len(vals), 4)
        sectors.append({"sector": sector, "change_pct": avg})

    sectors.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)
    return sectors


async def _fetch_movers_with_mcap(client: "AsyncFMPDataClient") -> tuple[list, list, list]:
    """Fetch gainers/losers/actives and filter by market cap using batch-quote.

    Returns (gainers, losers, actives) lists with marketCap-enriched entries,
    filtered to MIN_MARKET_CAP.
    """
    gainers_data, losers_data, actives_data = await asyncio.gather(
        _safe_call(client.market.get_gainers, ttl=TTL_REALTIME, default=[]),
        _safe_call(client.market.get_losers, ttl=TTL_REALTIME, default=[]),
        _safe_call(client.market.get_most_active, ttl=TTL_REALTIME, default=[]),
    )

    gainers_raw = _as_list(gainers_data)
    losers_raw = _as_list(losers_data)
    actives_raw = _as_list(actives_data)

    # Collect all unique symbols for batch quote
    all_symbols = set()
    for item in gainers_raw + losers_raw + actives_raw:
        sym = item.get("symbol")
        if sym:
            all_symbols.add(sym)

    # Batch-quote to get market caps (max ~150 symbols per call is fine)
    mcap_map: dict[str, float] = {}
    if all_symbols:
        batch_data = await _safe_call(
            client.batch.get_quotes,
            symbols=sorted(all_symbols),
            ttl=TTL_REALTIME,
            default=[],
        )
        batch_list = _as_list(batch_data)
        for q in batch_list:
            sym = q.get("symbol")
            mc = q.get("marketCap")
            if sym and mc:
                mcap_map[sym] = mc

    def _enrich_and_filter(items: list[dict]) -> list[dict]:
        result = []
        for m in items:
            sym = m.get("symbol")
            mc = mcap_map.get(sym)
            if mc is not None and mc < MIN_MARKET_CAP:
                continue
            entry = {
                "symbol": sym,
                "name": m.get("name"),
                "price": m.get("price"),
                "change_pct": m.get("changesPercentage"),
            }
            if mc is not None:
                entry["market_cap"] = mc
            result.append(entry)
        return result

    return (
        _enrich_and_filter(gainers_raw),
        _enrich_and_filter(losers_raw),
        _enrich_and_filter(actives_raw),
    )


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    def _format_split_value(value) -> str:
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    @mcp.tool(
        annotations={
            "title": "Treasury Rates",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def treasury_rates() -> dict:
        """Get current US Treasury yield curve and equity risk premium.

        Returns latest yields across maturities, yield curve slope (10Y-2Y),
        inversion flag, and DCF-ready inputs (10Y rate + equity risk premium).
        """
        rates_data, erp_data = await asyncio.gather(
            _safe_call(
                client.economics.get_treasury_rates,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.economics.get_market_risk_premium,
                ttl=TTL_DAILY,
                default=[],
            ),
        )

        rates_list = _as_list(rates_data)
        latest = _safe_first(rates_list)

        if not latest:
            return {"error": "No treasury rate data available"}

        # Extract key maturities
        yields = {
            "1m": latest.get("month1"),
            "3m": latest.get("month3"),
            "6m": latest.get("month6"),
            "1y": latest.get("year1"),
            "2y": latest.get("year2"),
            "5y": latest.get("year5"),
            "10y": latest.get("year10"),
            "20y": latest.get("year20"),
            "30y": latest.get("year30"),
        }

        # Yield curve slope
        y10 = latest.get("year10")
        y2 = latest.get("year2")
        slope = None
        inverted = False
        if y10 is not None and y2 is not None:
            slope = round(y10 - y2, 3)
            inverted = slope < 0

        # DCF inputs from US market risk premium
        erp_list = _as_list(erp_data)
        us_erp = None
        for entry in erp_list:
            if entry.get("country") == "United States":
                us_erp = entry.get("totalEquityRiskPremium")
                break

        dcf_inputs = {
            "risk_free_rate": y10,
            "equity_risk_premium": us_erp,
        }
        if y10 is not None and us_erp is not None:
            dcf_inputs["implied_cost_of_equity"] = round(y10 + us_erp, 3)

        result = {
            "date": latest.get("date"),
            "yields": yields,
            "curve_slope_10y_2y": slope,
            "curve_inverted": inverted,
            "dcf_inputs": dcf_inputs,
        }

        _warnings = []
        if not erp_list:
            _warnings.append("equity risk premium unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Economic Calendar",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def economic_calendar(days_ahead: int = 14) -> dict:
        """Get upcoming high-impact macro-economic events.

        Filters for important events (Fed, CPI, NFP, GDP, etc.) and
        sorts by date. Covers the next N days (default 14).

        Args:
            days_ahead: Number of days to look ahead (default 14, max 90)
        """
        days_ahead = min(max(days_ahead, 1), 90)
        today = date.today()
        end_date = today + timedelta(days=days_ahead)

        data = await _safe_call(
            client.economics.get_economic_calendar,
            start_date=today,
            end_date=end_date,
            ttl=TTL_HOURLY,
            default=[],
        )

        events_list = _as_list(data)

        if not events_list:
            return {"events": [], "count": 0, "period": f"{today.isoformat()} to {end_date.isoformat()}"}

        # Filter for high-impact events
        high_impact_keywords = [
            "fed", "fomc", "interest rate", "federal funds",
            "cpi", "consumer price", "inflation",
            "nonfarm", "non-farm", "employment", "unemployment", "jobless",
            "gdp", "gross domestic",
            "pce", "personal consumption",
            "retail sales",
            "ism", "pmi", "manufacturing",
            "housing starts", "building permits",
            "consumer confidence", "consumer sentiment",
            "trade balance",
            "treasury",
        ]

        filtered = []
        for event in events_list:
            event_name = (event.get("event") or "").lower()
            country = (event.get("country") or "").upper()

            # Only US events
            if country != "US":
                continue

            is_high_impact = any(kw in event_name for kw in high_impact_keywords)
            if not is_high_impact:
                continue

            filtered.append({
                "date": event.get("date"),
                "event": event.get("event"),
                "estimate": event.get("estimate"),
                "actual": event.get("actual"),
                "previous": event.get("previous"),
                "change": event.get("change"),
                "impact": event.get("impact"),
            })

        # Sort by date ascending
        filtered.sort(key=lambda x: x.get("date") or "")

        return {
            "events": filtered,
            "count": len(filtered),
            "period": f"{today.isoformat()} to {end_date.isoformat()}",
            "total_unfiltered": len(events_list),
        }

    @mcp.tool(
        annotations={
            "title": "Market Overview",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def market_overview() -> dict:
        """Get today's market snapshot: sector performance, biggest movers, and most active stocks.

        Returns sector rankings, top 5 gainers/losers, and most actively traded names.
        Movers are filtered to companies with market cap > $1B to exclude micro-caps.
        """
        sectors, (gainers, losers, actives) = await asyncio.gather(
            _fetch_sectors(client),
            _fetch_movers_with_mcap(client),
        )

        if not any([sectors, gainers, losers, actives]):
            return {"error": "No market data available"}

        result = {
            "sectors": sectors,
            "top_gainers": gainers[:5],
            "top_losers": losers[:5],
            "most_active": actives[:5],
        }

        _warnings = []
        if not sectors:
            _warnings.append("sector performance unavailable")
        if not gainers:
            _warnings.append("gainers data unavailable")
        if not losers:
            _warnings.append("losers data unavailable")
        if not actives:
            _warnings.append("most active data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    SECTOR_ETFS: dict[str, str] = {
        "SPY": "S&P 500",
        "XLK": "Technology",
        "XLV": "Health Care",
        "XLF": "Financials",
        "XLY": "Consumer Discretionary",
        "XLC": "Communication Services",
        "XLI": "Industrials",
        "XLE": "Energy",
        "XLU": "Utilities",
        "XLRE": "Real Estate",
        "XLB": "Materials",
        "XLP": "Consumer Staples",
    }

    @mcp.tool(
        annotations={
            "title": "Sector Performance",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def sector_performance() -> dict:
        """Get sector performance via SPDR Select Sector ETFs.

        Batch-quotes the 11 SPDR sector ETFs plus SPY as the benchmark.
        Returns real prices, dollar change, and % change for each sector,
        sorted by performance (leaders first).
        """
        symbols = list(SECTOR_ETFS.keys())

        quote_data = await _safe_call(
            client.batch.get_quotes,
            symbols=symbols,
            ttl=TTL_REALTIME,
            default=[],
        )

        quotes = _as_list(quote_data)

        if not quotes:
            return {"error": "No sector ETF quote data available"}

        quote_map = {q.get("symbol"): q for q in quotes}

        benchmark = None
        sectors = []

        for sym, name in SECTOR_ETFS.items():
            q = quote_map.get(sym)
            if not q:
                continue

            entry = {
                "symbol": sym,
                "name": name,
                "price": q.get("price"),
                "change": q.get("change"),
                "change_pct": q.get("changesPercentage"),
            }

            if sym == "SPY":
                benchmark = entry
            else:
                sectors.append(entry)

        # Sort sectors by change_pct descending
        sectors.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)

        # Top 2 leaders / bottom 2 laggards
        leaders = [s["name"] for s in sectors[:2] if (s.get("change_pct") or 0) > 0]
        laggards = [s["name"] for s in sectors[-2:] if (s.get("change_pct") or 0) < 0]

        result: dict = {}
        if benchmark:
            result["benchmark"] = benchmark
        result["sectors"] = sectors
        if leaders:
            result["leaders"] = leaders
        if laggards:
            result["laggards"] = laggards

        return result

    @mcp.tool(
        annotations={
            "title": "IPO Calendar",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def ipo_calendar(days_ahead: int = 14) -> dict:
        """Get upcoming IPOs within a date window.

        Returns company, expected date, price range, shares offered, exchange,
        and links to prospectus/disclosure documents.

        Args:
            days_ahead: Number of days to look ahead (default 14, max 90)
        """
        days_ahead = min(max(days_ahead, 1), 90)
        today = date.today()
        end_date = today + timedelta(days=days_ahead)

        # Fetch IPO calendar, prospectus, and disclosures in parallel
        calendar_data, prospectus_data, disclosure_data = await asyncio.gather(
            _safe_call(
                client.intelligence.get_ipo_calendar,
                start_date=today,
                end_date=end_date,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.market.get_ipo_prospectus,
                from_date=today,
                to_date=end_date,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.market.get_ipo_disclosure,
                from_date=today,
                to_date=end_date,
                ttl=TTL_HOURLY,
                default=[],
            ),
        )

        ipo_list = _as_list(calendar_data)
        prospectus_list = _as_list(prospectus_data)
        disclosure_list = _as_list(disclosure_data)

        if not ipo_list:
            return {
                "ipos": [],
                "count": 0,
                "period": f"{today.isoformat()} to {end_date.isoformat()}",
            }

        # Build prospectus and disclosure maps by symbol
        prospectus_map: dict[str, list] = {}
        for p in prospectus_list:
            symbol = p.get("symbol")
            if symbol:
                prospectus_map.setdefault(symbol, []).append({
                    "url": p.get("url"),
                    "title": p.get("title"),
                    "date": p.get("date"),
                })

        disclosure_map: dict[str, list] = {}
        for d in disclosure_list:
            symbol = d.get("symbol")
            if symbol:
                disclosure_map.setdefault(symbol, []).append({
                    "url": d.get("url"),
                    "title": d.get("title"),
                    "date": d.get("date"),
                })

        # Sort by date ascending
        ipo_list.sort(key=lambda x: x.get("date") or "")

        ipos = []
        for ipo in ipo_list:
            symbol = ipo.get("symbol")
            entry = {
                "symbol": symbol,
                "company": ipo.get("company"),
                "date": ipo.get("date"),
                "exchange": ipo.get("exchange"),
                "price_range": ipo.get("priceRange"),
                "shares": ipo.get("shares"),
                "market_cap": ipo.get("marketCap"),
                "actions": ipo.get("actions"),
            }

            # Add prospectus links if available
            if symbol in prospectus_map:
                entry["prospectus"] = prospectus_map[symbol]

            # Add disclosure links if available
            if symbol in disclosure_map:
                entry["disclosures"] = disclosure_map[symbol]

            ipos.append(entry)

        result = {
            "ipos": ipos,
            "count": len(ipos),
            "period": f"{today.isoformat()} to {end_date.isoformat()}",
        }

        _warnings = []
        if not prospectus_list:
            _warnings.append("prospectus data unavailable")
        if not disclosure_list:
            _warnings.append("disclosure data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Dividends Calendar",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def dividends_calendar(
        symbol: str | None = None,
        days_ahead: int = 14,
        min_market_cap: float = 1_000_000_000,
        min_yield: float = 0.0,
        limit: int = 50,
        country: str | None = "US",
    ) -> dict:
        """Get upcoming ex-dividend dates across all stocks.

        Returns symbols going ex-dividend within the specified window.
        When no symbol is given, filters to US large-cap stocks (default $1B+)
        to keep results manageable. Results sorted by market cap descending.

        Args:
            symbol: Optional stock ticker to filter results (e.g. "AAPL")
            days_ahead: Number of days to look ahead (default 14, max 90)
            min_market_cap: Minimum market cap filter in USD when browsing
                (default 1B). Ignored when symbol is specified.
                Set to 0 to include all companies.
            min_yield: Minimum dividend yield % filter (default 0, no filter)
            limit: Max results in browsing mode (default 50, max 200).
                Ignored when symbol is specified.
            country: Country filter - "US" (default) filters to domestic tickers.
                Set to None or "" for global results.
        """
        days_ahead = min(max(days_ahead, 1), 90)
        limit = max(1, min(limit, 200))
        today = date.today()
        end_date = today + timedelta(days=days_ahead)

        data = await _safe_call(
            client.intelligence.get_dividends_calendar,
            start_date=today,
            end_date=end_date,
            ttl=TTL_HOURLY,
            default=[],
        )

        div_list = _as_list(data)

        if not div_list:
            return {
                "dividends": [],
                "count": 0,
                "period": f"{today.isoformat()} to {end_date.isoformat()}",
            }

        # Single-symbol lookup: return all entries for that symbol
        if symbol:
            symbol = symbol.upper().strip()
            div_list = [d for d in div_list if (d.get("symbol") or "").upper() == symbol]
            if not div_list:
                return {"error": f"No ex-dividend dates for '{symbol}' in the next {days_ahead} days"}
            div_list.sort(key=lambda x: x.get("date") or "")
            dividends = []
            for d in div_list:
                dividends.append({
                    "symbol": d.get("symbol"),
                    "ex_date": d.get("date"),
                    "dividend": d.get("dividend"),
                    "adj_dividend": d.get("adjDividend"),
                    "record_date": d.get("recordDate"),
                    "payment_date": d.get("paymentDate"),
                    "yield_pct": d.get("yield"),
                    "frequency": d.get("frequency"),
                })
            return {
                "dividends": dividends,
                "count": len(dividends),
                "symbol": symbol,
                "period": f"{today.isoformat()} to {end_date.isoformat()}",
            }

        # Browse mode: deduplicate by symbol (keep soonest ex-date)
        div_list.sort(key=lambda x: x.get("date") or "")
        seen: set[str] = set()
        unique = []
        for d in div_list:
            sym = (d.get("symbol") or "").upper()
            if sym and sym not in seen:
                seen.add(sym)
                unique.append(d)

        # Country filter: "US" excludes international tickers (those with exchange suffixes like .DE, .L)
        country_upper = country.upper().strip() if isinstance(country, str) and country.strip() else None
        if country_upper == "US":
            unique = [d for d in unique if "." not in (d.get("symbol") or "")]

        # Apply min_yield filter early
        if min_yield > 0:
            unique = [d for d in unique if (d.get("yield") or 0) >= min_yield]

        # Batch-quote for market caps (chunk in groups of 200)
        mcap_map: dict[str, float] = {}
        if min_market_cap > 0 and unique:
            symbols = sorted({(d.get("symbol") or "").upper() for d in unique})
            chunk_size = 200
            chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
            batch_results = await asyncio.gather(*(
                _safe_call(
                    client.batch.get_quotes,
                    symbols=chunk,
                    ttl=TTL_HOURLY,
                    default=[],
                )
                for chunk in chunks
            ))
            for batch in batch_results:
                for q in _as_list(batch):
                    sym = q.get("symbol")
                    mc = q.get("marketCap")
                    if sym and mc:
                        mcap_map[sym] = mc

        # Build output with filters
        dividends = []
        for d in unique:
            sym = (d.get("symbol") or "").upper()
            mc = mcap_map.get(sym)
            if min_market_cap > 0 and (mc is None or mc < min_market_cap):
                continue
            dividends.append({
                "s": sym,
                "ex": d.get("date"),
                "div": d.get("dividend"),
                "yld": round(d["yield"], 2) if d.get("yield") else None,
                "mc": mc,
            })

        if not dividends:
            filter_parts = [f"≥${min_market_cap/1e9:.0f}B market cap"]
            if country_upper:
                filter_parts.append(f"country={country_upper}")
            if min_yield > 0:
                filter_parts.append(f"yield≥{min_yield}%")
            return {"error": f"No dividends matching {', '.join(filter_parts)} in the next {days_ahead} days"}

        # Sort by market cap descending
        dividends.sort(key=lambda x: -(x.get("mc") or 0))

        total_matching = len(dividends)
        dividends = dividends[:limit]

        return {
            "dividends": toon.encode(dividends),
            "count": len(dividends),
            "total_matching": total_matching,
            "min_market_cap": min_market_cap,
            "country": country_upper,
            "period": f"{today.isoformat()} to {end_date.isoformat()}",
        }

    INDEX_METHODS = {
        "sp500": client.index.get_sp500_constituents,
        "nasdaq": client.index.get_nasdaq_constituents,
        "dowjones": client.index.get_dowjones_constituents,
    }

    @mcp.tool(
        annotations={
            "title": "Index Constituents",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def index_constituents(index: str) -> dict:
        """Get constituent list for a major market index.

        Returns all symbols in the index with sector classification.

        Args:
            index: Index name - "sp500", "nasdaq", or "dowjones"
        """
        index = index.lower().strip()
        if index not in INDEX_METHODS:
            return {"error": f"Invalid index '{index}'. Use: {', '.join(INDEX_METHODS.keys())}"}

        data = await _safe_call(
            INDEX_METHODS[index],
            ttl=TTL_DAILY,
            default=[],
        )

        constituents_list = _as_list(data)

        if not constituents_list:
            return {"error": f"No constituent data found for '{index}'"}

        # Sort alphabetically by symbol
        constituents_list.sort(key=lambda x: x.get("symbol") or "")

        constituents = []
        for c in constituents_list:
            constituents.append({
                "symbol": c.get("symbol"),
                "name": c.get("name"),
                "sector": c.get("sector"),
                "sub_sector": c.get("subSector"),
                "head_quarter": c.get("headQuarter"),
                "date_first_added": c.get("dateFirstAdded"),
                "founded": c.get("founded"),
            })

        # Sector breakdown
        sector_counts: dict[str, int] = {}
        for c in constituents:
            sector = c.get("sector") or "Unknown"
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        return {
            "index": index,
            "count": len(constituents),
            "constituents": constituents,
            "sector_breakdown": dict(sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)),
        }

    @mcp.tool(
        annotations={
            "title": "Index Performance",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def index_performance(
        indices: list[str] | None = None,
    ) -> dict:
        """Get current prices and performance for major market indices.

        Returns per-index current price, change, and performance across
        multiple timeframes (1d, 1w, 1m, 3m, ytd, 1y).

        Args:
            indices: List of index symbols (default: ["^GSPC", "^DJI", "^IXIC", "^RUT"])
        """
        if indices is None:
            indices = ["^GSPC", "^DJI", "^IXIC", "^RUT"]

        # Clean up symbols
        indices = [idx.upper().strip() for idx in indices]

        # Get current quotes
        quote_data = await _safe_call(
            client.batch.get_quotes,
            symbols=indices,
            ttl=TTL_REALTIME,
            default=[],
        )

        quotes = _as_list(quote_data)

        if not quotes:
            return {"error": f"No quote data found for indices: {', '.join(indices)}"}

        # Get historical data for each index
        today = date.today()
        one_year_ago = today - timedelta(days=365)

        # Fetch historical data for all indices
        historical_tasks = []
        for idx in indices:
            historical_tasks.append(
                _safe_call(
                    client.company.get_historical_prices,
                    symbol=idx,
                    from_date=one_year_ago,
                    to_date=today,
                    ttl=TTL_12H,
                    default=None,
                )
            )

        historical_results = await asyncio.gather(*historical_tasks)

        # Build quote map
        quote_map = {q.get("symbol"): q for q in quotes}

        # Calculate performance for each index
        index_data = []
        for i, idx in enumerate(indices):
            quote = quote_map.get(idx, {})
            historical_data = historical_results[i] if i < len(historical_results) else None
            historical = _as_list(historical_data, list_key="historical")

            current_price = quote.get("price")
            day_change = quote.get("changesPercentage")

            # Calculate performance across timeframes
            from cassandra_fmp.tools.market import _calc_performance
            performance = {}
            if current_price and historical:
                for period, days in [("1w", 5), ("1m", 21), ("3m", 63), ("ytd", None), ("1y", 252)]:
                    if days is None:
                        # YTD calculation
                        ytd_start = date(today.year, 1, 1)
                        ytd_history = [
                            h for h in historical if (hist_date := _to_date(h.get("date"))) and hist_date >= ytd_start
                        ]
                        if ytd_history:
                            ytd_start_price = ytd_history[-1].get("close")
                            if ytd_start_price and ytd_start_price > 0:
                                performance["ytd"] = round((current_price / ytd_start_price - 1) * 100, 2)
                    else:
                        perf = _calc_performance(current_price, historical, days)
                        if perf is not None:
                            performance[period] = perf

            index_data.append({
                "symbol": idx,
                "name": quote.get("name"),
                "price": current_price,
                "change_pct": day_change,
                "performance": performance,
            })

        return {
            "indices": index_data,
            "count": len(index_data),
        }

    @mcp.tool(
        annotations={
            "title": "Market Hours",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def market_hours(
        exchange: str = "NYSE",
    ) -> dict:
        """Get current market status and trading hours.

        Returns market open/close status, regular trading hours,
        extended hours, and upcoming holidays.

        Args:
            exchange: Exchange name (default "NYSE")
        """
        exchange = exchange.upper().strip()

        # Use /stable/exchange-market-hours with exchange param
        hours_data = await _safe_call(
            client.market.get_market_hours,
            exchange=exchange,
            ttl=TTL_HOURLY,
            default=None,
        )

        exchange_hours = _as_dict(hours_data)

        # Get upcoming holidays
        today = date.today()

        holidays_data = await _safe_call(
            client.market.get_holidays_by_exchange,
            exchange=exchange,
            ttl=TTL_DAILY,
            default=[],
        )

        holidays_list = _as_list(holidays_data)

        # Filter to future dates and sort ascending
        upcoming_holidays = []
        today_date = today
        for h in holidays_list:
            h_date = _to_date(h.get("date"))
            if h_date and h_date >= today_date:
                upcoming_holidays.append({
                    "date": h_date.isoformat(),
                    "name": h.get("name"),
                    "is_closed": h.get("isClosed"),
                })
        upcoming_holidays.sort(key=lambda h: h.get("date") or "")
        upcoming_holidays = upcoming_holidays[:5]

        result = {
            "exchange": exchange,
        }

        if exchange_hours:
            result["is_open"] = exchange_hours.get("isMarketOpen") or exchange_hours.get("isTheStockMarketOpen")
            result["name"] = exchange_hours.get("name")
            result["timezone"] = exchange_hours.get("timezone")
            result["regular_hours"] = {
                "open": exchange_hours.get("openingHour"),
                "close": exchange_hours.get("closingHour"),
            }

        result["upcoming_holidays"] = upcoming_holidays

        _warnings = []
        if not exchange_hours:
            _warnings.append(f"market hours data unavailable for {exchange}")
        if not upcoming_holidays:
            _warnings.append("holiday calendar unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Industry Performance",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def industry_performance(
        sector: str | None = None,
    ) -> dict:
        """Get industry performance rankings with valuation context.

        Returns industries ranked by performance with median P/E ratios.
        Optionally filter to a specific sector.

        Args:
            sector: Optional sector to filter by (e.g. "Technology")
        """
        today_dt = date.today()
        query_date = today_dt
        nyse_perf_list: list[dict] = []
        nasdaq_perf_list: list[dict] = []
        nyse_pe_list: list[dict] = []
        nasdaq_pe_list: list[dict] = []

        # FMP snapshots can be empty on weekends/holidays. Retry a few prior dates.
        for offset in range(0, 4):
            query_date = today_dt - timedelta(days=offset)
            nyse_perf, nasdaq_perf, nyse_pe, nasdaq_pe = await asyncio.gather(
                _safe_call(
                    client.market.get_industry_performance_snapshot,
                    date=query_date,
                    exchange="NYSE",
                    ttl=TTL_REALTIME,
                    default=[],
                ),
                _safe_call(
                    client.market.get_industry_performance_snapshot,
                    date=query_date,
                    exchange="NASDAQ",
                    ttl=TTL_REALTIME,
                    default=[],
                ),
                _safe_call(
                    client.market.get_industry_pe_snapshot,
                    date=query_date,
                    exchange="NYSE",
                    ttl=TTL_DAILY,
                    default=[],
                ),
                _safe_call(
                    client.market.get_industry_pe_snapshot,
                    date=query_date,
                    exchange="NASDAQ",
                    ttl=TTL_DAILY,
                    default=[],
                ),
            )

            nyse_perf_list = _as_list(nyse_perf)
            nasdaq_perf_list = _as_list(nasdaq_perf)
            nyse_pe_list = _as_list(nyse_pe)
            nasdaq_pe_list = _as_list(nasdaq_pe)
            if nyse_perf_list or nasdaq_perf_list:
                break

        # Build performance map (average across exchanges)
        perf_map: dict[str, list[float]] = {}
        sector_map: dict[str, str] = {}

        for entry in nyse_perf_list + nasdaq_perf_list:
            industry = entry.get("industry")
            change = entry.get("averageChange")
            if change is None:
                change = entry.get("changePercentage")
            industry_sector = entry.get("sector")
            if industry and change is not None:
                perf_map.setdefault(industry, []).append(change)
                if industry_sector:
                    sector_map[industry] = industry_sector

        # Build PE map
        pe_map: dict[str, list[float]] = {}
        for entry in nyse_pe_list + nasdaq_pe_list:
            industry = entry.get("industry")
            pe = entry.get("pe")
            if industry and pe is not None and pe > 0:
                pe_map.setdefault(industry, []).append(pe)

        # Combine into industry data
        industries = []
        for industry, changes in perf_map.items():
            avg_change = round(sum(changes) / len(changes), 4)
            industry_sector = sector_map.get(industry)

            # Filter by sector if specified
            if sector and industry_sector and industry_sector.lower() != sector.lower():
                continue

            avg_pe = None
            if industry in pe_map:
                avg_pe = round(sum(pe_map[industry]) / len(pe_map[industry]), 2)

            industries.append({
                "industry": industry,
                "sector": industry_sector,
                "change_pct": avg_change,
                "median_pe": avg_pe,
            })

        if not industries:
            msg = "No industry performance data available"
            if sector:
                msg += f" for sector '{sector}'"
            return {"error": msg}

        # Sort by performance descending
        industries.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)

        result = {
            "date": query_date.isoformat(),
            "industries": industries,
            "count": len(industries),
        }

        if sector:
            result["sector_filter"] = sector

        return result

    @mcp.tool(
        annotations={
            "title": "Splits Calendar",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def splits_calendar(days_ahead: int = 30) -> dict:
        """Get upcoming stock splits calendar.

        Returns companies with announced stock splits in the specified window.

        Args:
            days_ahead: Number of days to look ahead (default 30, max 90)
        """
        days_ahead = min(max(days_ahead, 1), 90)
        today = date.today()
        end_date = today + timedelta(days=days_ahead)

        data = await _safe_call(
            client.intelligence.get_stock_splits_calendar,
            start_date=today,
            end_date=end_date,
            ttl=TTL_HOURLY,
            default=[],
        )

        splits_list = _as_list(data)

        if not splits_list:
            return {
                "splits": [],
                "count": 0,
                "period": f"{today.isoformat()} to {end_date.isoformat()}",
            }

        # Sort by date ascending
        splits_list.sort(key=lambda s: _date_only(s.get("date")) or "")

        splits = []
        for s in splits_list:
            num = s.get("numerator")
            den = s.get("denominator")
            label = f"{_format_split_value(num)}:{_format_split_value(den)}" if num is not None and den is not None else None

            splits.append({
                "symbol": s.get("symbol"),
                "date": _date_only(s.get("date")),
                "numerator": num,
                "denominator": den,
                "label": label,
            })

        return {
            "splits": splits,
            "count": len(splits),
            "period": f"{today.isoformat()} to {end_date.isoformat()}",
        }

    @mcp.tool(
        annotations={
            "title": "Sector Valuation",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def sector_valuation() -> dict:
        """Get sector and industry P/E ratio rankings.

        Returns average P/E by sector and industry, showing relative valuation
        across the market. Fetches NYSE + NASDAQ data and averages.
        """
        today_dt = date.today()
        query_date = today_dt
        sector_nyse = sector_nasdaq = industry_nyse = industry_nasdaq = []

        # Snapshot endpoints can be empty on non-trading days. Retry a few prior dates.
        for offset in range(0, 4):
            query_date = today_dt - timedelta(days=offset)
            sector_nyse, sector_nasdaq, industry_nyse, industry_nasdaq = await asyncio.gather(
                _safe_call(
                    client.market.get_sector_pe_snapshot,
                    date=query_date,
                    exchange="NYSE",
                    ttl=TTL_DAILY,
                    default=[],
                ),
                _safe_call(
                    client.market.get_sector_pe_snapshot,
                    date=query_date,
                    exchange="NASDAQ",
                    ttl=TTL_DAILY,
                    default=[],
                ),
                _safe_call(
                    client.market.get_industry_pe_snapshot,
                    date=query_date,
                    exchange="NYSE",
                    ttl=TTL_DAILY,
                    default=[],
                ),
                _safe_call(
                    client.market.get_industry_pe_snapshot,
                    date=query_date,
                    exchange="NASDAQ",
                    ttl=TTL_DAILY,
                    default=[],
                ),
            )
            if _as_list(sector_nyse) or _as_list(sector_nasdaq) or _as_list(industry_nyse) or _as_list(industry_nasdaq):
                break

        def _avg_pe(nyse_data, nasdaq_data, key_field: str) -> list[dict]:
            """Average PE by name across NYSE and NASDAQ."""
            nyse_list = _as_list(nyse_data)
            nasdaq_list = _as_list(nasdaq_data)
            pe_vals: dict[str, list[float]] = {}
            for entry in nyse_list + nasdaq_list:
                name = entry.get(key_field)
                pe = entry.get("pe")
                if name and pe is not None and pe > 0:
                    pe_vals.setdefault(name, []).append(pe)
            result = []
            for name, vals in pe_vals.items():
                avg = round(sum(vals) / len(vals), 2)
                result.append({"name": name, "pe": avg})
            result.sort(key=lambda x: x["pe"])
            return result

        sectors = _avg_pe(sector_nyse, sector_nasdaq, "sector")
        industries = _avg_pe(industry_nyse, industry_nasdaq, "industry")

        if not sectors and not industries:
            return {"error": "No sector/industry valuation data available"}

        result: dict = {"date": query_date.isoformat()}

        if sectors:
            result["sectors"] = sectors

        if industries:
            result["top_10_cheapest"] = industries[:10]
            result["top_10_most_expensive"] = industries[-10:][::-1]
            result["total_industries"] = len(industries)

        _warnings = []
        if not sectors:
            _warnings.append("sector PE data unavailable")
        if not industries:
            _warnings.append("industry PE data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Crowdfunding Offerings",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def crowdfunding_offerings(
        query: str | None = None,
        limit: int = 20,
    ) -> dict:
        """Browse or search SEC-registered crowdfunding campaigns (Reg CF / Reg A).

        Discover early-stage companies raising capital from retail investors.
        Returns issuer name, offering amount, securities offered, and filing details.

        Args:
            query: Search by company/platform name (omit to browse latest)
            limit: Max results to return (default 20, max 100)
        """
        limit = max(1, min(limit, 100))

        if query:
            data = await _safe_endpoint_call(
                client,
                CROWDFUNDING_SEARCH,
                name=query.strip(),
                ttl=TTL_HOURLY,
                default=[],
            )
        else:
            data = await _safe_endpoint_call(
                client,
                CROWDFUNDING_LATEST,
                page=0,
                ttl=TTL_HOURLY,
                default=[],
            )

        offerings_list = _as_list(data)

        if not offerings_list:
            msg = f"No crowdfunding offerings found for '{query}'" if query else "No crowdfunding offerings available"
            return {"error": msg}

        offerings = []
        for o in offerings_list[:limit]:
            offerings.append({
                "company_name": o.get("companyName") or o.get("entityName") or o.get("name"),
                "cik": o.get("cik"),
                "offering_amount": o.get("offeringAmount") or o.get("totalOfferingAmount"),
                "securities_offered": o.get("securitiesOffered") or o.get("typeOfSecuritiesOffered"),
                "offering_date": _date_only(o.get("offeringDate") or o.get("dateOfFirstSale") or o.get("date")),
                "closing_date": _date_only(o.get("closingDate")),
                "amount_sold": o.get("totalAmountSold"),
                "investors_count": o.get("investorsCount") or o.get("numberOfInvestors"),
                "intermediary": o.get("intermediaryCompanyName") or o.get("intermediary"),
                "state": o.get("stateOrCountry") or o.get("state"),
            })

        result: dict = {
            "mode": "search" if query else "latest",
            "count": len(offerings),
            "offerings": offerings,
        }
        if query:
            result["query"] = query

        return result

    @mcp.tool(
        annotations={
            "title": "Fundraising (Form D)",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def fundraising(
        query: str | None = None,
        cik: str | None = None,
        limit: int = 25,
    ) -> dict:
        """Search SEC Form D exempt offerings — private fundraising rounds, SPVs, and feeder funds.

        This is the key tool for discovering private company capital raises (SpaceX,
        Anthropic, xAI, Stripe, etc.) and the feeder funds/SPVs that give investors
        exposure to them.

        Modes (auto-detected):
        - Search by name: finds companies/funds matching a query (e.g. "SpaceX", "Anthropic", "xAI")
        - Lookup by CIK: gets full Form D filing history with offering amounts, investors, etc.
        - Browse latest: shows most recent Form D filings

        Args:
            query: Company or fund name to search (e.g. "SpaceX", "Anthropic", "xAI")
            cik: SEC CIK number for full filing history (e.g. "0001181412" for SpaceX)
            limit: Max results (default 25, max 100)
        """
        limit = max(1, min(limit, 100))

        if cik:
            cik = cik.strip()
            data = await _safe_endpoint_call(
                client,
                FUNDRAISING_BY_CIK,
                cik=cik,
                ttl=TTL_HOURLY,
                default=[],
            )
            filings_list = _as_list(data)

            if not filings_list:
                return {"error": f"No Form D filings found for CIK '{cik}'"}

            # Sort by date descending
            filings_list.sort(key=lambda x: x.get("date") or "", reverse=True)

            company_name = filings_list[0].get("companyName") or filings_list[0].get("entityName")

            filings = []
            for f in filings_list[:limit]:
                filings.append({
                    "date": _date_only(f.get("date")),
                    "form_type": f.get("formType"),
                    "description": f.get("formSignification"),
                    "offering_amount": f.get("totalOfferingAmount"),
                    "amount_sold": f.get("totalAmountSold"),
                    "amount_remaining": f.get("totalAmountRemaining"),
                    "investors": f.get("totalNumberAlreadyInvested"),
                    "min_investment": f.get("minimumInvestmentAccepted"),
                    "entity_type": f.get("entityType"),
                    "industry": f.get("industryGroupType"),
                    "exemptions": f.get("federalExemptionsExclusions"),
                    "equity_offered": f.get("securitiesOfferedAreOfEquityType"),
                    "accredited_only": not f.get("hasNonAccreditedInvestors", True),
                    "state": f.get("issuerStateOrCountry"),
                })

            return {
                "mode": "filings",
                "cik": cik,
                "company_name": company_name,
                "filing_count": len(filings_list),
                "showing": len(filings),
                "filings": filings,
            }

        if query:
            data = await _safe_endpoint_call(
                client,
                FUNDRAISING_SEARCH,
                name=query.strip(),
                ttl=TTL_HOURLY,
                default=[],
            )
            results_list = _as_list(data)

            if not results_list:
                return {"error": f"No Form D filings found for '{query}'"}

            # Deduplicate by CIK (search returns one row per filing date)
            seen_ciks: dict[str, dict] = {}
            for r in results_list:
                cik_val = r.get("cik", "")
                name_val = r.get("name", "")
                date_val = r.get("date", "")
                if cik_val not in seen_ciks:
                    seen_ciks[cik_val] = {
                        "cik": cik_val,
                        "name": name_val,
                        "latest_filing": date_val,
                        "filing_count": 1,
                    }
                else:
                    seen_ciks[cik_val]["filing_count"] += 1

            entities = sorted(seen_ciks.values(), key=lambda x: x.get("latest_filing", ""), reverse=True)

            return {
                "mode": "search",
                "query": query,
                "count": len(entities),
                "entities": entities[:limit],
                "hint": "Use the CIK with fundraising(cik='...') to get full filing details",
            }

        # Browse latest
        data = await _safe_endpoint_call(
            client,
            FUNDRAISING_LATEST,
            page=0,
            ttl=TTL_HOURLY,
            default=[],
        )
        filings_list = _as_list(data)

        if not filings_list:
            return {"error": "No recent Form D filings available"}

        filings = []
        for f in filings_list[:limit]:
            filings.append({
                "company_name": f.get("companyName") or f.get("entityName"),
                "cik": f.get("cik"),
                "date": _date_only(f.get("date")),
                "form_type": f.get("formType"),
                "offering_amount": f.get("totalOfferingAmount"),
                "amount_sold": f.get("totalAmountSold"),
                "investors": f.get("totalNumberAlreadyInvested"),
                "industry": f.get("industryGroupType"),
                "entity_type": f.get("entityType"),
                "state": f.get("issuerStateOrCountry"),
            })

        return {
            "mode": "latest",
            "count": len(filings),
            "filings": filings,
        }
