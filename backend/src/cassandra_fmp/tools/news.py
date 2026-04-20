"""Market news, press release, and M&A activity tools."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cassandra_fmp.tools._helpers import TTL_HOURLY, TTL_REALTIME, _as_list, _safe_call

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    @mcp.tool(
        annotations={
            "title": "Market News",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def market_news(
        category: str = "stock",
        symbol: str | None = None,
        page: int = 0,
        limit: int = 20,
    ) -> dict:
        """Get news articles across asset classes."""
        category = category.lower().strip()
        limit = max(1, min(limit, 50))
        page = max(0, page)

        data = []
        if category == "stock":
            if symbol:
                symbol = symbol.upper().strip()
                data = await _safe_call(
                    client.intelligence.get_stock_symbol_news,
                    symbol=symbol,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
            else:
                data = await _safe_call(
                    client.intelligence.get_stock_news,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
        elif category == "press_releases":
            if symbol:
                symbol = symbol.upper().strip()
                data = await _safe_call(
                    client.intelligence.get_press_releases_by_symbol,
                    symbol=symbol,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
            else:
                data = await _safe_call(
                    client.intelligence.get_press_releases,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
        elif category == "crypto":
            if symbol:
                symbol = symbol.upper().strip()
                data = await _safe_call(
                    client.intelligence.get_crypto_symbol_news,
                    symbol=symbol,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
            else:
                data = await _safe_call(
                    client.intelligence.get_crypto_news,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
        elif category == "forex":
            if symbol:
                symbol = symbol.upper().strip()
                data = await _safe_call(
                    client.intelligence.get_forex_symbol_news,
                    symbol=symbol,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
            else:
                data = await _safe_call(
                    client.intelligence.get_forex_news,
                    page=page,
                    limit=limit,
                    ttl=TTL_REALTIME,
                    default=[],
                )
        elif category == "general":
            symbol = None
            data = await _safe_call(
                client.intelligence.get_general_news,
                page=page,
                limit=limit,
                ttl=TTL_REALTIME,
                default=[],
            )
        else:
            return {"error": f"Invalid category '{category}'. Use: stock, press_releases, crypto, forex, general"}

        articles_list = _as_list(data)
        if not articles_list:
            msg = f"No {category} news found"
            if symbol:
                msg += f" for '{symbol}'"
            msg += f" (page {page})"
            return {"error": msg}

        articles = []
        for a in articles_list:
            articles.append(
                {
                    "title": a.get("title"),
                    "date": a.get("publishedDate"),
                    "symbol": a.get("symbol"),
                    "source": a.get("site") or a.get("publisher"),
                    "url": a.get("url"),
                    "snippet": (a.get("text") or "")[:300],
                }
            )

        result = {
            "category": category,
            "symbol": symbol,
            "page": page,
            "count": len(articles),
            "articles": articles,
        }

        if category == "stock" and not symbol:
            result["_warnings"] = [
                "Stock news is unfiltered. Did you mean to pass symbol='TICKER' for company-specific headlines?",
            ]

        return result

    @mcp.tool(
        annotations={
            "title": "M&A Activity",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def mna_activity(
        symbol: str | None = None,
        limit: int = 20,
    ) -> dict:
        """Get merger and acquisition activity."""
        limit = max(1, min(limit, 50))

        if symbol:
            symbol = symbol.upper().strip()
            data = await _safe_call(
                client.company.get_mergers_acquisitions_search,
                name=symbol,
                page=0,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            )
        else:
            data = await _safe_call(
                client.company.get_mergers_acquisitions_latest,
                page=0,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            )

        entries = _as_list(data)
        if not entries:
            msg = "No M&A activity found"
            if symbol:
                msg += f" for '{symbol}'"
            return {"error": msg}

        entries.sort(key=lambda x: x.get("transactionDate") or "", reverse=True)

        deals = []
        for e in entries[:limit]:
            deals.append(
                {
                    "symbol": e.get("symbol"),
                    "company": e.get("companyName"),
                    "targeted_company": e.get("targetedCompanyName"),
                    "targeted_symbol": e.get("targetedSymbol"),
                    "transaction_date": e.get("transactionDate"),
                    "accepted_date": e.get("acceptedDate"),
                    "filing_url": e.get("link"),
                }
            )

        return {
            "symbol": symbol,
            "count": len(deals),
            "deals": deals,
        }
