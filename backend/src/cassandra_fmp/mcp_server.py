"""FastMCP server for Cassandra FMP — financial data with auth."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastmcp import FastMCP
from cassandra_mcp_auth import DiscoveryTransform
from fmp_data import AsyncFMPDataClient
from fmp_data.config import ClientConfig, RateLimitConfig

from cassandra_fmp.auth import McpKeyAuthProvider, build_auth
from cassandra_mcp_auth import AclMiddleware
from cassandra_fmp.clients.polygon import PolygonClient
from cassandra_fmp.clients.treasury import TreasuryClient
from cassandra_fmp.config import Settings
from cassandra_fmp.tools import (
    assets,
    auctions,
    cot,
    economy,
    edgar,
    esg,
    financials,
    llm,
    macro,
    market,
    meta,
    news,
    options,
    overview,
    ownership,
    transcripts,
    valuation,
    workflows,
)

logger = logging.getLogger(__name__)

SERVICE_ID = "market-research"


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def create_mcp_server(settings: Settings) -> FastMCP:
    """Create and configure the FastMCP server with auth and all tools."""

    auth_provider = None
    mcp_key_provider = None
    if settings.auth_url and settings.auth_secret:
        if settings.workos_client_id and settings.workos_authkit_domain and settings.base_url:
            auth_provider, mcp_key_provider = build_auth(
                acl_url=settings.auth_url,
                acl_secret=settings.auth_secret,
                service_id=SERVICE_ID,
                base_url=settings.base_url,
                workos_client_id=settings.workos_client_id,
                workos_authkit_domain=settings.workos_authkit_domain,
            )
        else:
            mcp_key_provider = McpKeyAuthProvider(
                acl_url=settings.auth_url,
                acl_secret=settings.auth_secret,
                service_id=SERVICE_ID,
            )
            auth_provider = mcp_key_provider

    # Service-level API keys from env vars (k8s Secrets)
    fmp_api_key = os.environ.get("FMP_API_KEY", "")
    polygon_api_key = os.environ.get("POLYGON_API_KEY") or None
    fred_api_key = os.environ.get("FRED_API_KEY") or None

    if not fmp_api_key:
        raise RuntimeError("FMP_API_KEY env var is required")

    rate_limit_config = RateLimitConfig(
        daily_limit=_env_int("FMP_DAILY_LIMIT", 1_000_000),
        requests_per_second=_env_int("FMP_REQUESTS_PER_SECOND", 30),
        requests_per_minute=_env_int("FMP_REQUESTS_PER_MINUTE", 6_000),
    )

    client = AsyncFMPDataClient(
        config=ClientConfig(
            api_key=fmp_api_key,
            timeout=30,
            max_retries=3,
            rate_limit=rate_limit_config,
        )
    )

    treasury_client = TreasuryClient(
        fred_api_key=fred_api_key,
    )

    polygon_client: PolygonClient | None = None
    if polygon_api_key:
        polygon_client = PolygonClient(api_key=polygon_api_key)

    @asynccontextmanager
    async def lifespan(server):
        yield
        await client.aclose()
        await treasury_client.close()
        if polygon_client is not None:
            await polygon_client.close()
        if mcp_key_provider is not None:
            mcp_key_provider.close()

    mcp_kwargs: dict = {
        "name": "Cassandra FMP",
        "instructions": (
            "# Cassandra Market Research\n\n"
            "Financial market data — stocks, SEC filings, macro, options, crypto, forex. "
            "Wraps FMP (Financial Modeling Prep), Polygon.io, FRED, Treasury Fiscal Data, "
            "and EDGAR fund holdings into one unified interface.\n\n"
            "## When to use\n"
            "- Stock prices, quotes, company fundamentals, analyst estimates\n"
            "- SEC filings — 10-K risk factors, 8-Ks, NPORT-P fund holdings (e.g. 'who holds SpaceX?')\n"
            "- Macro — index performance, sector rotation, economic calendar, treasury rates\n"
            "- Earnings — transcripts, estimates, surprises, postmortems\n"
            "- Options chains, crypto/forex/commodity quotes, ETF data\n\n"
            "## Getting started\n"
            "Workflow tools collapse common questions into one call: `stock_brief`, `market_context`, "
            "`earnings_preview`, `fair_value_estimate`, `ownership_deep_dive`, `industry_analysis`. "
            "Reach for workflows before atomic tools.\n\n"
            "For SEC content, use `filing_sections` with a `queries` list — one fetch, multiple topics.\n\n"
            "## Discovery\n"
            "`tags()` → browse categories, `search(query, tags=[...])` → find tools, "
            "`get_schema(tools=[...])` → see params. Execution happens on a SEPARATE server (cassandra-gateway). Do NOT call `execute` here — this server only has discovery tools. Look up tool names/schemas here, then switch to the gateway server to call `execute(code)` with `call_tool(name, args)`."
        ),
        "lifespan": lifespan,
    }
    if settings.code_mode:
        mcp_kwargs["transforms"] = [DiscoveryTransform()]
    acl_mw = AclMiddleware(service_id=SERVICE_ID, acl_path=settings.auth_yaml_path)
    if acl_mw._enabled:  # noqa: SLF001
        mcp_kwargs["middleware"] = [acl_mw]

    if auth_provider:
        mcp_kwargs["auth"] = auth_provider

    mcp = FastMCP(**mcp_kwargs)

    # Health check
    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(request):  # noqa: ANN001, ARG001
        from starlette.responses import JSONResponse  # noqa: PLC0415

        return JSONResponse({"ok": True, "service": "cassandra-fmp"})

    # Register all tool modules
    overview.register(mcp, client)
    financials.register(mcp, client)
    valuation.register(mcp, client)
    market.register(mcp, client, polygon_client=polygon_client)
    ownership.register(mcp, client, polygon_client=polygon_client)
    news.register(mcp, client)
    macro.register(mcp, client)
    transcripts.register(mcp, client)
    assets.register(mcp, client)
    workflows.register(mcp, client)
    edgar.register(mcp, client)
    auctions.register(mcp, treasury_client)
    cot.register(mcp)
    esg.register(mcp)
    llm.register(mcp)
    meta.register(mcp, client)

    if polygon_client is not None:
        options.register(mcp, polygon_client=polygon_client)
        economy.register(mcp, polygon_client)

    # Tag tools for Code Mode category discovery (GetTags)
    _apply_tags(mcp)

    return mcp


# Tool → tag mapping. Tags are the categories shown by GetTags discovery.
_TOOL_TAGS: dict[str, set[str]] = {
    # overview
    "quote": {"price", "overview"},
    "company_overview": {"overview"},
    "stock_search": {"overview", "screening"},
    "company_executives": {"overview"},
    "employee_history": {"overview"},
    "delisted_companies": {"overview"},
    "sec_filings": {"overview", "sec"},
    "symbol_lookup": {"overview"},
    # financials
    "financial_statements": {"financials"},
    "financial_health": {"financials"},
    "ratio_history": {"financials"},
    "revenue_segments": {"financials"},
    # valuation
    "valuation_history": {"valuation"},
    "discounted_cash_flow": {"valuation"},
    "peer_comparison": {"valuation"},
    # market
    "intraday_prices": {"price", "market"},
    "price_history": {"price", "market"},
    "technical_indicators": {"market"},
    "market_hours": {"market"},
    # ownership
    "institutional_ownership": {"ownership"},
    "insider_activity": {"ownership"},
    "short_interest": {"ownership"},
    "ownership_structure": {"ownership"},
    "senate_trading": {"ownership"},
    # news
    "market_news": {"news"},
    # macro
    "index_performance": {"macro", "market"},
    "index_constituents": {"macro"},
    "sector_performance": {"macro", "market"},
    "industry_performance": {"macro"},
    "sector_valuation": {"macro", "valuation"},
    "market_overview": {"macro", "market"},
    "economic_calendar": {"macro"},
    "dividends_calendar": {"macro"},
    "dividends_info": {"macro"},
    "splits_calendar": {"macro"},
    "ipo_calendar": {"macro"},
    "mna_activity": {"macro"},
    "treasury_rates": {"macro", "fixed-income"},
    # transcripts
    "earnings_transcript": {"earnings"},
    "earnings_calendar": {"earnings"},
    # assets
    "commodity_quotes": {"price", "commodities"},
    "crypto_quotes": {"price", "crypto"},
    "forex_quotes": {"price", "forex"},
    # workflows (high-level orchestration)
    "stock_brief": {"workflow"},
    "market_context": {"workflow"},
    "earnings_setup": {"workflow", "earnings"},
    "earnings_preview": {"workflow", "earnings"},
    "earnings_postmortem": {"workflow", "earnings"},
    "fair_value_estimate": {"workflow", "valuation"},
    "ownership_deep_dive": {"workflow", "ownership"},
    "industry_analysis": {"workflow", "macro"},
    "estimate_revisions": {"workflow", "earnings"},
    # edgar
    "sec_filings_search": {"sec"},
    "filing_sections": {"sec"},
    "fund_holdings": {"sec", "ownership"},
    "fund_search": {"sec"},
    "fund_disclosure": {"sec", "ownership"},
    # auctions / fixed income
    "treasury_auctions": {"fixed-income"},
    "auction_analysis": {"fixed-income"},
    # cot
    "cot_report": {"commodities", "fixed-income"},
    # esg
    "esg_ratings": {"esg"},
    "esg_benchmark": {"esg"},
    # llm
    "analyze": {"analysis"},
    # meta
    "fmp_coverage_gaps": {"meta"},
    # options (polygon)
    "options_chain": {"options"},
    # economy (polygon)
    "economy_indicators": {"macro"},
    # fundraising
    "crowdfunding_offerings": {"sec"},
    "fundraising": {"sec"},
    # historical
    "historical_market_cap": {"market"},
    "analyst_consensus": {"earnings", "valuation"},
    # etf
    "etf_lookup": {"overview"},
}


def _apply_tags(mcp: FastMCP) -> None:
    """Apply category tags to all registered tools for Code Mode discovery."""
    import asyncio

    async def _tag_all() -> None:
        for tool_name, tags in _TOOL_TAGS.items():
            try:
                tool = await mcp.get_tool(tool_name)
                tool.tags.update(tags)
            except Exception:
                pass  # Tool may not exist (e.g. polygon not configured)

    # Run in existing loop if available, otherwise create one
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_tag_all())
    except RuntimeError:
        asyncio.run(_tag_all())
