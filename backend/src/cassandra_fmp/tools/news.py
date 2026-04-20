"""Market news, press release, and M&A activity tools."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from cassandra_fmp.tools._helpers import TTL_HOURLY, TTL_REALTIME, _as_list, _safe_call

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient
    from cass_market_sdk.clients.tv_proxy import TvProxyClient


logger = logging.getLogger(__name__)


# TV news-mediator section for each FMP category. Keeps the merge roughly
# aligned so a `category="stock"` call pulls stocks-section headlines from
# both providers, not stocks from FMP and "everything" from TV.
_TV_SECTION: dict[str, str] = {
    "stock": "stocks",
    "press_releases": "stocks",  # TV doesn't split press releases out
    "crypto": "crypto",
    "forex": "forex",
    "general": "markets",
}


async def _fetch_tv_news(
    proxy: "TvProxyClient",
    *,
    section: str,
    symbol: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Pull headlines from TradingView's news-mediator, shaped to FMP schema.

    news-mediator aggregates Reuters/Benzinga/MT Newswire/etc. — different
    provider mix from FMP. On any failure we swallow and return [] so the
    tool degrades to FMP-only, not errors.
    """
    params: dict[str, Any] = {
        "client": "web",
        "category": "base",
        "section": section,
        "lang": "en",
    }
    if symbol:
        params["symbol"] = symbol
    try:
        try:
            data = await proxy.get_json("news-mediator", "/headlines/v2", params=params)
        except Exception:
            data = await proxy.get_json("news-mediator", "/headlines", params=params)
    except Exception as e:  # noqa: BLE001
        logger.debug("TV news fetch failed (degrading to FMP-only): %s", e)
        return []

    if isinstance(data, dict):
        items = data.get("items") or data.get("headlines") or data.get("news") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []

    shaped: list[dict[str, Any]] = []
    for it in items[:limit]:
        provider = it.get("provider")
        if isinstance(provider, dict):
            provider = provider.get("name")
        shaped.append({
            "title": it.get("title"),
            "date": it.get("published"),
            "symbol": (it.get("relatedSymbols") or [{}])[0].get("symbol")
                if it.get("relatedSymbols") else symbol,
            "source": provider or "tradingview",
            "url": it.get("link") or it.get("storyPath") or it.get("url"),
            "snippet": (it.get("shortDescription") or it.get("summary") or "")[:300],
        })
    return shaped


_TITLE_PUNCT = ".!?…\u2026\"'`,;:-—–()[]"


def _norm_title(title: str | None) -> str:
    """Normalize a headline for cross-provider dedup.

    FMP and TV both aggregate Reuters/Benzinga/AP headlines, but with
    different URL rewrites — the canonical `reuters.com/...` URL on one
    side becomes a provider-scoped path on the other. Titles match
    much more reliably across providers, so we make them the primary
    dedup key with light normalization: collapse whitespace, lowercase,
    strip trailing punctuation, drop symbol suffixes like " - $AAPL".
    """
    if not title:
        return ""
    t = " ".join(title.split()).lower()
    # Drop trailing " - $TICKER" or " (TICKER)" annotations that one
    # provider sometimes adds — they're decoration, not content.
    for sep in (" - $", " — $", " – $", " (", " - "):
        idx = t.rfind(sep)
        if idx > 40:  # only strip if the core headline is still substantial
            t = t[:idx]
    return t.strip(_TITLE_PUNCT + " \t")


def _url_host_path(url: str | None) -> str:
    """Extract `host/path` from a URL, lowercased. Falls back to the raw
    URL when urlparse can't make sense of it. Used only as a secondary
    dedup key when titles are missing or identical for unrelated
    stories (rare)."""
    if not url:
        return ""
    try:
        from urllib.parse import urlparse  # noqa: PLC0415
        p = urlparse(url)
        if p.netloc:
            return f"{p.netloc}{p.path}".lower().rstrip("/")
    except Exception:  # noqa: BLE001
        pass
    return url.lower().strip()


def _merge_articles(
    fmp: list[dict[str, Any]],
    tv: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    """Combine FMP + TV articles, dedupe by normalized title (fallback to
    host+path when a title is missing), sort by date desc, cap.

    FMP goes first in iteration order so when the same headline appears
    from both sides, we keep FMP's copy — snippets there are typically
    fuller, and FMP's `symbol` field is more reliable.
    """
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for art in (*fmp, *tv):
        title_key = _norm_title(art.get("title"))
        url_key = _url_host_path(art.get("url"))
        key = title_key or url_key
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(art)
    out.sort(key=lambda a: a.get("date") or "", reverse=True)
    return out[:limit]


def register(
    mcp: FastMCP,
    client: AsyncFMPDataClient,
    *,
    tv_proxy: TvProxyClient | None = None,
) -> None:
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

        # Kick off TV fetch in parallel with FMP when we can use it
        tv_section = _TV_SECTION.get(category)
        tv_task: asyncio.Task | None = None
        if tv_proxy is not None and page == 0 and tv_section:
            tv_task = asyncio.create_task(
                _fetch_tv_news(
                    tv_proxy,
                    section=tv_section,
                    symbol=symbol.upper().strip() if symbol else None,
                    limit=limit,
                )
            )

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

        fmp_articles: list[dict] = []
        for a in articles_list:
            fmp_articles.append(
                {
                    "title": a.get("title"),
                    "date": a.get("publishedDate"),
                    "symbol": a.get("symbol"),
                    "source": a.get("site") or a.get("publisher"),
                    "url": a.get("url"),
                    "snippet": (a.get("text") or "")[:300],
                }
            )

        # TV provides a genuinely different provider mix (Reuters/Benzinga/MT
        # Newswire vs FMP's aggregators). Joined in parallel above; only on
        # page 0 since TV doesn't paginate the same way.
        tv_articles: list[dict] = []
        if tv_task is not None:
            try:
                tv_articles = await tv_task
            except Exception as e:  # noqa: BLE001
                logger.debug("TV merge skipped: %s", e)

        articles = _merge_articles(fmp_articles, tv_articles, limit)
        if not articles:
            msg = f"No {category} news found"
            if symbol:
                msg += f" for '{symbol}'"
            msg += f" (page {page})"
            return {"error": msg}

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
