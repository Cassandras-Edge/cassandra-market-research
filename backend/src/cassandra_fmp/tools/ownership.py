"""Insider activity and institutional ownership tools."""

from __future__ import annotations

import asyncio
import calendar
from datetime import date, timedelta
from typing import TYPE_CHECKING

import httpx
from cass_market_sdk.clients.polygon import PolygonClient as _PolygonClientRuntime
from cassandra_fmp.tools._helpers import TTL_DAILY, TTL_HOURLY, TTL_REALTIME, _as_dict, _as_list, _date_only, _safe_call

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient
    from cass_market_sdk.clients.polygon import PolygonClient

FINRA_URL = "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
polygon_TTL_HOURLY = _PolygonClientRuntime.TTL_HOURLY


def _latest_quarter() -> tuple[int, int]:
    """Return the most recently completed quarter's (year, quarter)."""
    today = date.today()
    month = today.month
    year = today.year
    if month <= 3:
        return year - 1, 4
    elif month <= 6:
        return year, 1
    elif month <= 9:
        return year, 2
    else:
        return year, 3


def _quarter_candidates(lookback_quarters: int = 6) -> list[tuple[int, int]]:
    """Return recent completed quarter candidates, newest first."""
    year, quarter = _latest_quarter()
    out: list[tuple[int, int]] = []
    for _ in range(max(1, lookback_quarters)):
        out.append((year, quarter))
        quarter -= 1
        if quarter < 1:
            quarter = 4
            year -= 1
    return out


def _quarter_end_date(year: int, quarter: int) -> date:
    """Convert (year, quarter) to quarter-end date for SDK report_date params."""
    month = quarter * 3
    day = calendar.monthrange(year, month)[1]
    return date(year, month, day)


async def _resolve_latest_symbol_institutional_period(
    client: AsyncFMPDataClient,
    symbol: str,
    lookback_quarters: int = 6,
) -> tuple[int, int, list[dict], list[dict]]:
    """Find most recent quarter with institutional ownership data for a symbol."""
    candidates = _quarter_candidates(lookback_quarters)
    default_year, default_quarter = candidates[0]

    for year, quarter in candidates:
        report_date = _quarter_end_date(year, quarter)
        summary_data, holders_data = await asyncio.gather(
            _safe_call(
                client.institutional.get_symbol_positions_summary,
                symbol=symbol,
                report_date=report_date,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.institutional.get_institutional_ownership_analytics,
                symbol=symbol,
                report_date=report_date,
                limit=20,
                ttl=TTL_HOURLY,
                default=[],
            ),
        )
        summary_list = _as_list(summary_data)
        holders_list = _as_list(holders_data)
        if summary_list or holders_list:
            return year, quarter, summary_list, holders_list

    return default_year, default_quarter, [], []


async def _resolve_latest_fund_period(
    client: AsyncFMPDataClient,
    cik: str,
    lookback_quarters: int = 8,
) -> tuple[int, int, list[dict], list[dict]]:
    """Find most recent quarter with holdings/industry data for a fund CIK."""
    candidates = _quarter_candidates(lookback_quarters)
    default_year, default_quarter = candidates[0]

    for year, quarter in candidates:
        report_date = _quarter_end_date(year, quarter)
        holdings_data, industry_data = await asyncio.gather(
            _safe_call(
                client.institutional.get_institutional_ownership_extract,
                cik=cik,
                report_date=report_date,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.institutional.get_holder_industry_breakdown,
                cik=cik,
                report_date=report_date,
                ttl=TTL_HOURLY,
                default=[],
            ),
        )
        holdings_list = _as_list(holdings_data)
        industry_list = _as_list(industry_data)
        if holdings_list or industry_list:
            return year, quarter, holdings_list, industry_list

    return default_year, default_quarter, [], []


def _short_interest_dates() -> list[str]:
    """Generate candidate FINRA settlement dates (15th + last biz day).

    FINRA publishes short interest on the 15th and last business day
    of each month. We generate ~6 candidates (current + 2 prior months),
    filter out future dates, and sort descending (newest first).
    """
    today = date.today()
    candidates: list[date] = []

    for months_back in range(3):
        # Walk back months
        month = today.month - months_back
        year = today.year
        while month < 1:
            month += 12
            year -= 1

        # 15th of the month
        candidates.append(date(year, month, 15))

        # Last business day of the month
        last_day = calendar.monthrange(year, month)[1]
        d = date(year, month, last_day)
        while d.weekday() >= 5:  # Sat=5, Sun=6
            d -= timedelta(days=1)
        candidates.append(d)

    # Filter future dates and sort newest first
    candidates = [d for d in candidates if d <= today]
    candidates.sort(reverse=True)
    return [d.isoformat() for d in candidates]


async def _fetch_finra_short_interest(symbol: str, settlement_date: str) -> dict | None:
    """Fetch FINRA consolidated short interest for one settlement date.

    Returns the first matching record, or None if no data (204 or empty).
    """
    payload = {
        "fields": [
            "settlementDate",
            "currentShortPositionQuantity",
            "previousShortPositionQuantity",
            "changePreviousNumber",
            "changePercent",
            "averageDailyVolumeQuantity",
            "daysToCoverQuantity",
        ],
        "dateRangeFilters": [
            {"fieldName": "settlementDate", "startDate": settlement_date, "endDate": settlement_date}
        ],
        "domainFilters": [
            {"fieldName": "symbolCode", "values": [symbol]}
        ],
        "limit": 1,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            resp = await http.post(FINRA_URL, json=payload)
        if resp.status_code == 204 or not resp.content:
            return None
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0]
        return None
    except (httpx.HTTPError, ValueError):
        return None


async def _fetch_polygon_short_volume(
    polygon_client: PolygonClient, symbol: str
) -> dict | None:
    """Fetch recent daily short volume from Polygon.io."""
    data = await polygon_client.get_safe(
        "/stocks/v1/short-interest",
        params={"ticker": symbol, "limit": 5, "sort": "settlement_date.desc"},
        cache_ttl=polygon_TTL_HOURLY,
    )
    if not data or not isinstance(data, dict):
        return None
    results = data.get("results", [])
    if not results:
        return None
    latest = results[0]
    return {
        "settlement_date": latest.get("settlement_date"),
        "short_interest": latest.get("short_interest"),
        "days_to_cover": latest.get("days_to_cover"),
        "avg_daily_volume": latest.get("avg_daily_volume"),
        "source": "polygon.io",
    }


def register(mcp: FastMCP, client: AsyncFMPDataClient, polygon_client: PolygonClient | None = None) -> None:
    @mcp.tool(
        annotations={
            "title": "Insider Activity",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def insider_activity(symbol: str) -> dict:
        """Get insider trading activity, statistics, and ownership context.

        Returns net buy/sell over 30/90 days, cluster buying signals,
        CEO/CFO action highlights, and insider ownership as % of float.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()

        trades_data, stats_data, float_data = await asyncio.gather(
            _safe_call(
                client.institutional.search_insider_trading,
                symbol=symbol,
                limit=100,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.institutional.get_insider_statistics,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=None,
            ),
            _safe_call(
                client.company.get_share_float,
                symbol=symbol,
                ttl=TTL_DAILY,
                default=None,
            ),
        )

        trades_list = _as_list(trades_data)
        stats = _as_dict(stats_data)
        float_info = _as_dict(float_data)

        if not trades_list and not stats:
            return {"error": f"No insider data found for '{symbol}'"}

        # Analyze trades over 30/90 day windows
        today = date.today()
        cutoff_30 = (today - timedelta(days=30)).isoformat()
        cutoff_90 = (today - timedelta(days=90)).isoformat()

        buys_30, sells_30, buys_90, sells_90 = 0, 0, 0, 0
        cluster_buyers = {}  # Track unique buyers in 30-day window
        notable_trades = []

        for trade in trades_list:
            trade_date = _date_only(trade.get("filingDate") or trade.get("transactionDate")) or ""
            tx_type = (trade.get("transactionType") or "").lower()
            shares = trade.get("securitiesTransacted") or 0
            price = trade.get("price") or 0
            name = trade.get("reportingName", "")
            title = trade.get("typeOfOwner", "")

            is_buy = "purchase" in tx_type or "p-purchase" in tx_type
            is_sell = "sale" in tx_type or "s-sale" in tx_type

            if trade_date >= cutoff_90:
                if is_buy:
                    buys_90 += shares
                elif is_sell:
                    sells_90 += shares

                if trade_date >= cutoff_30:
                    if is_buy:
                        buys_30 += shares
                        cluster_buyers[name] = cluster_buyers.get(name, 0) + 1
                    elif is_sell:
                        sells_30 += shares

            # Highlight C-suite trades
            title_lower = title.lower() if title else ""
            if any(t in title_lower for t in ["ceo", "cfo", "coo", "director", "officer"]):
                if is_buy or is_sell:
                    notable_trades.append({
                        "name": name,
                        "title": title,
                        "type": "buy" if is_buy else "sell",
                        "shares": shares,
                        "price": price,
                        "date": trade_date,
                        "value": round(shares * price, 2) if shares and price else None,
                    })

        # Cluster buying: 3+ unique insiders buying within 30 days
        cluster_buying = len([b for b in cluster_buyers.values() if b > 0]) >= 3

        # Insider ownership % of float
        shares_float = float_info.get("floatShares")
        insider_ownership_pct = None
        if shares_float and shares_float > 0:
            outstanding = float_info.get("outstandingShares")
            if outstanding and shares_float:
                insider_ownership_pct = round(
                    (1 - shares_float / outstanding) * 100, 2
                ) if outstanding > shares_float else None

        # Stats use different field names in /stable/ API
        total_acquired = stats.get("totalAcquired") or stats.get("totalBought")
        total_disposed = stats.get("totalDisposed") or stats.get("totalSold")
        acquired_count = stats.get("acquiredTransactions") or stats.get("buyCount")
        disposed_count = stats.get("disposedTransactions") or stats.get("sellCount")

        result = {
            "symbol": symbol,
            "net_activity_30d": {
                "shares_bought": buys_30,
                "shares_sold": sells_30,
                "net_shares": buys_30 - sells_30,
                "signal": "net_buying" if buys_30 > sells_30 else "net_selling" if sells_30 > buys_30 else "neutral",
            },
            "net_activity_90d": {
                "shares_bought": buys_90,
                "shares_sold": sells_90,
                "net_shares": buys_90 - sells_90,
            },
            "cluster_buying": cluster_buying,
            "statistics": {
                "total_bought": total_acquired,
                "total_sold": total_disposed,
                "buy_count": acquired_count,
                "sell_count": disposed_count,
            },
            "notable_trades": notable_trades[:10],
            "float_context": {
                "float_shares": shares_float,
                "outstanding_shares": float_info.get("outstandingShares"),
                "insider_ownership_pct": insider_ownership_pct,
            },
        }

        _warnings = []
        if not trades_list:
            _warnings.append("insider trades unavailable")
        if not stats:
            _warnings.append("insider statistics unavailable")
        if not float_info:
            _warnings.append("float data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Institutional Ownership",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def institutional_ownership(symbol: str) -> dict:
        """Get institutional ownership breakdown and position changes.

        Returns top 10 holders with % ownership, quarter-over-quarter
        position changes, and institutional vs float ratio.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()
        year, quarter, summary_list, holders_list = await _resolve_latest_symbol_institutional_period(client, symbol)
        float_data = await _safe_call(
            client.company.get_share_float,
            symbol=symbol,
            ttl=TTL_DAILY,
            default=None,
        )
        float_info = _as_dict(float_data)

        if not summary_list and not holders_list:
            return {"error": f"No institutional ownership data found for '{symbol}'"}

        # Top holders with ownership %
        outstanding = float_info.get("outstandingShares") or 0
        top_holders = []
        for h in holders_list[:10]:
            shares = h.get("sharesNumber") or h.get("shares") or 0
            pct = round(shares / outstanding * 100, 2) if outstanding > 0 else None
            change = h.get("changeInSharesNumber") or h.get("changeInShares") or 0
            top_holders.append({
                "holder": h.get("investorName") or h.get("name") or h.get("holder"),
                "shares": shares,
                "ownership_pct": pct,
                "change_in_shares": change,
                "change_type": "increased" if change > 0 else "decreased" if change < 0 else "unchanged",
                "date_reported": h.get("date") or h.get("dateReported"),
            })

        # Aggregate position changes from summary
        summary = _as_dict(summary_list)
        investors_holding = summary.get("investorsHolding", 0)
        investors_change = summary.get("investorsHoldingChange", 0)
        total_institutional_shares = summary.get("numberOf13Fshares", 0)
        ownership_pct = summary.get("ownershipPercent", 0)

        # Institutional ownership as % of float
        float_shares = float_info.get("floatShares") or 0
        institutional_pct_of_float = None
        if total_institutional_shares and float_shares > 0:
            institutional_pct_of_float = round(
                total_institutional_shares / float_shares * 100, 2
            )

        result = {
            "symbol": symbol,
            "reporting_period": f"Q{quarter} {year}",
            "top_holders": top_holders,
            "position_changes": {
                "investors_holding": investors_holding,
                "investors_change": investors_change,
                "ownership_pct": ownership_pct,
            },
            "ownership_summary": {
                "total_institutional_shares": total_institutional_shares,
                "float_shares": float_shares,
                "institutional_pct_of_float": institutional_pct_of_float,
                "outstanding_shares": outstanding,
            },
        }

        _warnings = []
        if not summary_list:
            _warnings.append("ownership summary unavailable")
        if not holders_list:
            _warnings.append("holder details unavailable")
        if not float_info:
            _warnings.append("float data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Short Interest",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def short_interest(symbol: str) -> dict:
        """Get short interest data with float context.

        Combines FINRA consolidated short interest (free, no API key)
        with FMP shares-float to compute short % of float and outstanding.
        If Polygon.io is configured, also includes more timely short interest data.

        Returns shares short, days to cover, change vs prior period,
        and short as % of float/outstanding.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()

        candidates = _short_interest_dates()
        finra_tasks = [_fetch_finra_short_interest(symbol, d) for d in candidates]
        float_task = _safe_call(
            client.company.get_share_float,
            symbol=symbol,
            ttl=TTL_DAILY,
            default=None,
        )

        # Include Polygon short volume if available
        tasks: list = [*finra_tasks, float_task]
        polygon_idx = None
        if polygon_client is not None:
            polygon_idx = len(tasks)
            tasks.append(_fetch_polygon_short_volume(polygon_client, symbol))

        all_results = await asyncio.gather(*tasks)
        finra_results = all_results[:len(finra_tasks)]
        float_data = all_results[len(finra_tasks)]
        polygon_short = all_results[polygon_idx] if polygon_idx is not None else None

        # Take first non-None FINRA result (candidates sorted newest-first)
        finra = None
        for r in finra_results:
            if r is not None:
                finra = r
                break

        float_info = _as_dict(float_data)

        if finra is None and not float_info:
            return {"error": f"No short interest or float data found for '{symbol}'"}

        result: dict = {"symbol": symbol}

        if finra is not None:
            result["settlement_date"] = finra.get("settlementDate")
            result["short_interest"] = {
                "shares_short": finra.get("currentShortPositionQuantity"),
                "previous_shares_short": finra.get("previousShortPositionQuantity"),
                "change_pct": finra.get("changePercent"),
                "avg_daily_volume": finra.get("averageDailyVolumeQuantity"),
                "days_to_cover": finra.get("daysToCoverQuantity"),
            }

        if float_info:
            float_shares = float_info.get("floatShares")
            outstanding = float_info.get("outstandingShares")
            shares_short = (finra or {}).get("currentShortPositionQuantity")

            short_pct_of_float = None
            short_pct_of_outstanding = None
            if shares_short and float_shares and float_shares > 0:
                short_pct_of_float = round(shares_short / float_shares * 100, 2)
            if shares_short and outstanding and outstanding > 0:
                short_pct_of_outstanding = round(shares_short / outstanding * 100, 2)

            result["float_context"] = {
                "float_shares": float_shares,
                "outstanding_shares": outstanding,
                "short_pct_of_float": short_pct_of_float,
                "short_pct_of_outstanding": short_pct_of_outstanding,
            }

        if polygon_short:
            result["polygon_short_interest"] = polygon_short

        _warnings = []
        if finra is None:
            _warnings.append("FINRA short interest unavailable")
        if not float_info:
            _warnings.append("float data unavailable")
        if polygon_client is not None and not polygon_short:
            _warnings.append("Polygon short interest unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Senate Trading",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def senate_trading(
        symbol: str | None = None,
        name: str | None = None,
        page: int = 0,
        limit: int = 50,
    ) -> dict:
        """Get U.S. Senate disclosed trading activity.

        Supports three modes:
        - by symbol (``symbol="LMT"``)
        - by senator name (``name="Tuberville"``)
        - latest feed (no symbol/name)
        """
        page = max(0, page)
        limit = max(1, min(limit, 100))

        if symbol and name:
            return {"error": "Provide either symbol or name, not both."}

        mode = "latest"
        if symbol:
            mode = "symbol"
            symbol = symbol.upper().strip()
            data = await _safe_call(
                client.intelligence.get_senate_trading,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=[],
            )
        elif name:
            mode = "name"
            name = name.strip()
            data = await _safe_call(
                client.intelligence.get_senate_trades_by_name,
                name=name,
                ttl=TTL_HOURLY,
                default=[],
            )
        else:
            data = await _safe_call(
                client.intelligence.get_senate_latest,
                page=page,
                limit=limit,
                ttl=TTL_REALTIME,
                default=[],
            )

        rows = _as_list(data)
        if not rows:
            scope = symbol or name or f"page {page}"
            return {"error": f"No senate trading data found for {scope}"}

        trades = []
        for row in rows[:limit]:
            trades.append(
                {
                    "symbol": row.get("symbol"),
                    "disclosure_date": _date_only(row.get("disclosureDate")),
                    "transaction_date": _date_only(row.get("transactionDate")),
                    "reporting_name": row.get("representative"),
                    "office": row.get("office"),
                    "asset_description": row.get("assetDescription"),
                    "asset_type": row.get("assetType"),
                    "transaction_type": row.get("type"),
                    "amount": row.get("amount"),
                    "owner": row.get("owner"),
                    "comment": row.get("comment"),
                }
            )

        return {
            "mode": mode,
            "symbol": symbol,
            "name": name,
            "page": page,
            "count": len(trades),
            "trades": trades,
        }

    @mcp.tool(
        annotations={
            "title": "Fund Search",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def fund_search(query: str, limit: int = 25) -> dict:
        """Map fund names to CIKs and related identifiers."""
        query = query.strip()
        if not query:
            return {"error": "query is required"}
        limit = max(1, min(limit, 100))

        disclosure_data, mutual_data = await asyncio.gather(
            _safe_call(
                client.investment.search_fund_disclosure_holders,
                name=query,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.investment.get_mutual_fund_by_name,
                name=query,
                ttl=TTL_HOURLY,
                default=[],
            ),
        )

        disclosure_list = _as_list(disclosure_data)
        mutual_list = _as_list(mutual_data)

        results: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for row in disclosure_list:
            cik = str(row.get("cik") or "").strip()
            entity_name = row.get("entityName") or row.get("seriesName") or row.get("className")
            key = (cik, str(entity_name))
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "source": "fund-disclosure",
                    "symbol": row.get("symbol"),
                    "cik": cik or None,
                    "entity_name": entity_name,
                    "series_name": row.get("seriesName"),
                    "class_name": row.get("className"),
                    "series_id": row.get("seriesId"),
                    "class_id": row.get("classId"),
                }
            )

        for row in mutual_list:
            cik = str(row.get("cik") or "").strip()
            entity_name = row.get("holder") or row.get("fundName") or row.get("name")
            key = (cik, str(entity_name))
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "source": "mutual-fund",
                    "symbol": row.get("symbol"),
                    "cik": cik or None,
                    "entity_name": entity_name,
                    "series_name": row.get("seriesName"),
                    "class_name": row.get("className"),
                    "series_id": row.get("seriesId"),
                    "class_id": row.get("classId"),
                }
            )

        if not results:
            return {"error": f"No funds found matching '{query}'"}

        result = {
            "query": query,
            "count": min(len(results), limit),
            "results": results[:limit],
        }
        warnings = []
        if not disclosure_list:
            warnings.append("fund disclosure search returned no rows")
        if not mutual_list:
            warnings.append("mutual fund search returned no rows")
        if warnings:
            result["_warnings"] = warnings
        return result

    @mcp.tool(
        annotations={
            "title": "Fund Holdings",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def fund_holdings(cik: str, year: int | str | None = None, quarter: int | str | None = None) -> dict:
        """Get institutional investor's portfolio by CIK.

        Query a fund's holdings, performance track record, and sector allocation.
        Returns top 50 holdings with position changes, fund performance metrics,
        and industry breakdown.

        Args:
            cik: Central Index Key (CIK) for the institutional investor
            year: Year for holdings (defaults to latest available quarter)
            quarter: Quarter (1-4) for holdings (defaults to latest available)
        """
        cik = cik.strip()

        # Coerce string inputs (MCP clients may send strings)
        def _to_int(v: int | str | None) -> int | None:
            if v is None:
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        year = _to_int(year)
        quarter = _to_int(quarter)

        # If period is omitted, use most recent quarter where data is available.
        holdings_list: list[dict]
        industry_list: list[dict]
        report_date: date | None = None
        if year is None or quarter is None:
            year, quarter, holdings_list, industry_list = await _resolve_latest_fund_period(client, cik)
            report_date = _quarter_end_date(year, quarter)
        else:
            report_date = _quarter_end_date(year, quarter)
            holdings_data, industry_data = await asyncio.gather(
                _safe_call(
                    client.institutional.get_institutional_ownership_extract,
                    cik=cik,
                    report_date=report_date,
                    ttl=TTL_HOURLY,
                    default=[],
                ),
                _safe_call(
                    client.institutional.get_holder_industry_breakdown,
                    cik=cik,
                    report_date=report_date,
                    ttl=TTL_HOURLY,
                    default=[],
                ),
            )
            holdings_list = _as_list(holdings_data)
            industry_list = _as_list(industry_data)

        performance_data = await _safe_call(
            client.institutional.get_holder_performance_summary,
            cik=cik,
            report_date=report_date,
            ttl=TTL_HOURLY,
            default=[],
        )
        performance_list = _as_list(performance_data)

        if not holdings_list and not performance_list and not industry_list:
            return {"error": f"No fund data found for CIK '{cik}'"}

        # Process holdings - top 50 with position changes
        top_holdings = []
        total_portfolio_value = 0
        for h in holdings_list[:50]:
            shares = h.get("shares") or 0
            value = h.get("value") or 0
            change = h.get("changeInShares") or 0
            total_portfolio_value += value

            top_holdings.append({
                "symbol": h.get("symbol"),
                "company_name": h.get("companyName"),
                "shares": shares,
                "value": value,
                "change_in_shares": change,
                "change_type": "increased" if change > 0 else "decreased" if change < 0 else "unchanged",
                "date_reported": h.get("date"),
            })

        # Calculate portfolio concentration (% held in top 10)
        top_10_value = sum(h["value"] for h in top_holdings[:10])
        concentration_pct = round(top_10_value / total_portfolio_value * 100, 2) if total_portfolio_value > 0 else None

        # Performance summary
        performance_summary = {}
        perf = _as_dict(performance_list)
        if perf:
            performance_summary = {
                "total_value": perf.get("totalValue"),
                "total_holdings": perf.get("totalHoldings"),
                "avg_return_1y": perf.get("oneYearReturn"),
                "avg_return_3y": perf.get("threeYearReturn"),
                "avg_return_5y": perf.get("fiveYearReturn"),
            }

        # Industry breakdown
        industry_breakdown = []
        for ind in industry_list:
            industry_breakdown.append({
                "industry": ind.get("industry") or ind.get("sector"),
                "value": ind.get("value"),
                "percentage": ind.get("percentage"),
                "holdings_count": ind.get("holdingsCount"),
            })

        # Sort by percentage descending
        industry_breakdown.sort(key=lambda x: x.get("percentage") or 0, reverse=True)

        result = {
            "cik": cik,
            "reporting_period": f"Q{quarter} {year}",
            "portfolio_summary": {
                "total_value": total_portfolio_value,
                "holdings_count": len(holdings_list),
                "top_10_concentration_pct": concentration_pct,
            },
            "top_holdings": top_holdings,
            "performance": performance_summary,
            "industry_allocation": industry_breakdown[:10],  # Top 10 industries
        }

        _warnings = []
        if not holdings_list:
            _warnings.append("holdings data unavailable")
        if not performance_list:
            _warnings.append("performance data unavailable")
        if not industry_list:
            _warnings.append("industry breakdown unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Ownership Structure",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def ownership_structure(symbol: str) -> dict:
        """Get comprehensive ownership structure analysis.

        Combined view of float, insider ownership, institutional ownership,
        and short interest. Returns shares outstanding breakdown, ownership
        percentages, and implied retail ownership.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()
        year, quarter, institutional_list, _holders_unused = await _resolve_latest_symbol_institutional_period(client, symbol)

        # Fetch float, insider stats, institutional summary, short interest, and Polygon short in parallel
        tasks = [
            _safe_call(
                client.company.get_share_float,
                symbol=symbol,
                ttl=TTL_DAILY,
                default=None,
            ),
            _safe_call(
                client.institutional.get_insider_statistics,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=None,
            ),
            asyncio.gather(
                *[_fetch_finra_short_interest(symbol, d) for d in _short_interest_dates()]
            ),
        ]
        if polygon_client is not None:
            tasks.append(_fetch_polygon_short_volume(polygon_client, symbol))

        all_results = await asyncio.gather(*tasks)
        float_data = all_results[0]
        insider_stats_data = all_results[1]
        finra_results = all_results[2]
        polygon_short = all_results[3] if polygon_client is not None else None

        float_info = _as_dict(float_data)
        insider_stats = _as_dict(insider_stats_data)
        institutional_summary = _as_dict(institutional_list)

        # Process FINRA results
        finra = None
        for r in finra_results:
            if r is not None:
                finra = r
                break

        if not float_info:
            return {"error": f"No ownership data found for '{symbol}'"}

        # Float metrics
        outstanding_shares = float_info.get("outstandingShares") or 0
        float_shares = float_info.get("floatShares") or 0

        # Insider ownership (derived from float vs outstanding)
        insider_shares = outstanding_shares - float_shares if outstanding_shares > float_shares else 0
        insider_pct = round(insider_shares / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0

        # Institutional ownership
        institutional_shares = institutional_summary.get("numberOf13Fshares") or 0
        institutional_pct = round(institutional_shares / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0
        institutional_investors = institutional_summary.get("investorsHolding") or 0
        institutional_change = institutional_summary.get("investorsHoldingChange") or 0

        # Short interest
        shares_short = (finra or {}).get("currentShortPositionQuantity") or 0
        short_pct_float = round(shares_short / float_shares * 100, 2) if float_shares > 0 else 0
        short_pct_outstanding = round(shares_short / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0

        # Implied retail ownership (float - institutional - short, as proxy)
        # Note: institutional and short can overlap, so this is an approximation
        retail_implied_shares = max(0, float_shares - institutional_shares)
        retail_implied_pct = round(retail_implied_shares / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0

        result = {
            "symbol": symbol,
            "reporting_period": f"Q{quarter} {year}",
            "shares_breakdown": {
                "outstanding_shares": outstanding_shares,
                "float_shares": float_shares,
                "insider_shares": insider_shares,
                "institutional_shares": institutional_shares,
                "short_shares": shares_short,
                "retail_implied_shares": retail_implied_shares,
            },
            "ownership_percentages": {
                "insider_pct": insider_pct,
                "institutional_pct": institutional_pct,
                "short_pct_float": short_pct_float,
                "short_pct_outstanding": short_pct_outstanding,
                "retail_implied_pct": retail_implied_pct,
            },
            "institutional_details": {
                "investors_holding": institutional_investors,
                "investors_change_qoq": institutional_change,
            },
            "short_interest_details": {
                "settlement_date": (finra or {}).get("settlementDate"),
                "days_to_cover": (finra or {}).get("daysToCoverQuantity"),
                "change_pct": (finra or {}).get("changePercent"),
            },
        }

        if polygon_short:
            result["polygon_short_interest"] = polygon_short

        _warnings = []
        if not float_info:
            _warnings.append("float data unavailable")
        if not insider_stats:
            _warnings.append("insider statistics unavailable")
        if not institutional_summary:
            _warnings.append("institutional summary unavailable")
        if finra is None:
            _warnings.append("FINRA short interest unavailable")
        if polygon_client is not None and not polygon_short:
            _warnings.append("Polygon short interest unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Fund Disclosure",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def fund_disclosure(
        symbol: str = "",
        mode: str = "holdings",
        name: str | None = None,
        year: int | None = None,
        quarter: int | None = None,
        limit: int = 50,
    ) -> dict:
        """Get SEC-filed fund disclosure data (NPORT-P holdings, fund search, holder lookup).

        Multi-mode tool for closed-end funds, mutual funds, and ETFs:
        - "holdings": Get a fund's SEC-disclosed portfolio (e.g. DXYZ → SpaceX shares).
          Auto-resolves to latest available quarter if year/quarter omitted.
        - "search": Find funds by name (e.g. "Destiny Tech", "SpaceX", "private credit").
          Returns matching fund entities with CIK, symbol, series/class info.
        - "holders": Show which funds hold a specific stock (reverse lookup).
          Returns fund names, share counts, changes, weight %.

        Args:
            symbol: Fund or stock ticker (required for holdings/holders, optional for search)
            mode: "holdings", "search", or "holders"
            name: Fund name for search mode (e.g. "Destiny Tech")
            year: Disclosure year (auto-resolved if omitted in holdings mode)
            quarter: Disclosure quarter 1-4 (auto-resolved if omitted)
            limit: Max results to return (default 50)
        """
        limit = max(1, min(limit, 200))
        mode = mode.lower().strip()

        if mode == "search":
            search_term = name or symbol
            if not search_term:
                return {"error": "Provide 'name' or 'symbol' for search mode"}

            data = await _safe_call(
                client.investment.search_fund_disclosure_holders,
                name=search_term,
                ttl=TTL_HOURLY,
                default=[],
            )
            results = _as_list(data)

            if not results:
                return {"error": f"No funds found matching '{search_term}'"}

            funds = []
            for f in results[:limit]:
                funds.append({
                    "symbol": f.get("symbol"),
                    "cik": f.get("cik"),
                    "entity_name": f.get("entityName"),
                    "series_name": f.get("seriesName"),
                    "class_name": f.get("className"),
                    "org_type": f.get("entityOrgType"),
                    "series_id": f.get("seriesId"),
                    "class_id": f.get("classId"),
                    "state": f.get("state"),
                    "city": f.get("city"),
                })

            return {
                "mode": "search",
                "query": search_term,
                "count": len(funds),
                "funds": funds,
            }

        if mode == "holders":
            if not symbol:
                return {"error": "Provide 'symbol' for holders mode"}
            symbol = symbol.upper().strip()

            data = await _safe_call(
                client.investment.get_fund_disclosure_holders_latest,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=[],
            )
            holders_list = _as_list(data)

            if not holders_list:
                return {"error": f"No fund holders found for '{symbol}'"}

            holders = []
            for h in holders_list[:limit]:
                holders.append({
                    "holder": h.get("holder"),
                    "cik": h.get("cik"),
                    "shares": h.get("shares"),
                    "change": h.get("change"),
                    "weight_pct": h.get("weightPercent"),
                    "date_reported": _date_only(h.get("dateReported")),
                })

            return {
                "mode": "holders",
                "symbol": symbol,
                "count": len(holders),
                "holders": holders,
            }

        # Default: holdings mode
        if not symbol:
            return {"error": "Provide 'symbol' for holdings mode"}
        symbol = symbol.upper().strip()

        # Auto-resolve latest disclosure period if year/quarter omitted
        if year is None or quarter is None:
            dates_data = await _safe_call(
                client.investment.get_fund_disclosure_dates,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=[],
            )
            dates_list = _as_list(dates_data)

            if dates_list:
                # Dates come as PortfolioDate objects with date/year/quarter fields
                # Sort by date descending to get latest
                def _sort_key(d: dict) -> str:
                    dt = d.get("date") or d.get("portfolioDate") or ""
                    return str(dt)

                dates_list.sort(key=_sort_key, reverse=True)
                latest = dates_list[0]

                if year is None:
                    year = latest.get("year")
                    if year is None:
                        # Derive from date
                        dt = _to_date(latest.get("date") or latest.get("portfolioDate"))
                        if dt:
                            year = dt.year
                if quarter is None:
                    quarter = latest.get("quarter")
                    if quarter is None:
                        dt = _to_date(latest.get("date") or latest.get("portfolioDate"))
                        if dt:
                            quarter = (dt.month - 1) // 3 + 1

        # Final fallback to latest completed quarter
        if year is None or quarter is None:
            year, quarter = _latest_quarter()

        data = await _safe_call(
            client.investment.get_fund_disclosure,
            symbol=symbol,
            year=year,
            quarter=quarter,
            ttl=TTL_HOURLY,
            default=[],
        )
        holdings_list = _as_list(data)

        if not holdings_list:
            return {"error": f"No disclosure holdings found for '{symbol}' in Q{quarter} {year}"}

        # Sort by portfolio weight descending
        holdings_list.sort(key=lambda x: x.get("pctVal") or 0, reverse=True)

        total_value = sum(h.get("valUsd") or 0 for h in holdings_list)

        holdings = []
        for h in holdings_list[:limit]:
            holdings.append({
                "name": h.get("name") or h.get("title"),
                "symbol": h.get("symbol"),
                "cusip": h.get("cusip"),
                "isin": h.get("isin"),
                "shares": h.get("balance"),
                "value_usd": h.get("valUsd"),
                "portfolio_pct": h.get("pctVal"),
                "asset_category": h.get("assetCat"),
                "issuer_category": h.get("issuerCat"),
                "country": h.get("invCountry"),
                "fair_value_level": h.get("fairValLevel"),
            })

        return {
            "mode": "holdings",
            "symbol": symbol,
            "period": f"Q{quarter} {year}",
            "total_value_usd": total_value,
            "holdings_count": len(holdings_list),
            "showing": len(holdings),
            "holdings": holdings,
        }
