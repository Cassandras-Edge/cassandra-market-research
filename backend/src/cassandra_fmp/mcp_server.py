"""FastMCP server for Cassandra FMP — financial data with auth."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastmcp import FastMCP
from fastmcp.experimental.transforms.code_mode import (
    CodeMode,
    GetSchemas,
    GetTags,
    MontySandboxProvider,
    Search,
)
from fmp_data import AsyncFMPDataClient
from fmp_data.config import ClientConfig, RateLimitConfig

from cassandra_fmp.auth import McpKeyAuthProvider, build_auth
from cassandra_mcp_auth import AclMiddleware
from cass_market_sdk.clients.polygon import PolygonClient
from cass_market_sdk.clients.thetadata import ThetaDataClient
from cass_market_sdk.clients.treasury import TreasuryClient
from cass_market_sdk.clients.tv_proxy import TvProxyClient
from cass_market_sdk.clients.tv_ws import TvWsClient
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
    scanner,
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

    theta_client: ThetaDataClient | None = None
    if settings.theta_terminal_url:
        theta_client = ThetaDataClient(base_url=settings.theta_terminal_url)

    tv_proxy_client: TvProxyClient | None = None
    if settings.tv_proxy_http_url and settings.tv_proxy_mcp_key:
        tv_proxy_client = TvProxyClient(
            base_url=settings.tv_proxy_http_url,
            mcp_key=settings.tv_proxy_mcp_key,
        )

    tv_ws_client: TvWsClient | None = None
    if settings.tv_proxy_ws_url and settings.tv_proxy_mcp_key:
        tv_ws_client = TvWsClient(
            ws_url=settings.tv_proxy_ws_url,
            mcp_key=settings.tv_proxy_mcp_key,
        )

    @asynccontextmanager
    async def lifespan(server):
        yield
        await client.aclose()
        await treasury_client.close()
        if polygon_client is not None:
            await polygon_client.close()
        if theta_client is not None:
            await theta_client.close()
        if tv_proxy_client is not None:
            await tv_proxy_client.close()
        if mcp_key_provider is not None:
            mcp_key_provider.close()

    mcp_kwargs: dict = {
        "name": "Cassandra Market Research",
        "instructions": (
            "# Cassandra Market Research\n\n"
            "Use this when the user wants financial data. This service covers the "
            "full spectrum — stock prices, company fundamentals, SEC filings, macro "
            "indicators, earnings, options, crypto, forex, commodities, ETFs.\n\n"
            "Think of this server when the user asks about:\n"
            "- A stock, company, or ticker — price, valuation, financials, ownership\n"
            "- SEC filings — 10-K risk factors, 8-Ks, fund holdings (e.g. 'who holds SpaceX?')\n"
            "- Market conditions — indices, sector rotation, treasury rates, economic calendar\n"
            "- Earnings — transcripts, estimates, surprises, pre/post analysis\n"
            "- Options chains, crypto/forex/commodity quotes, ETF holdings\n\n"
            "This service has 70+ tools. Start with the workflow tools — they collapse "
            "common questions into a single call: `stock_brief`, `market_context`, "
            "`earnings_preview`, `fair_value_estimate`, `ownership_deep_dive`, "
            "`industry_analysis`. Reach for atomic tools only when workflows don't cover it.\n\n"
            "For SEC content, use `filing_sections` with a `queries` list — one fetch, "
            "multiple topics.\n\n"
            "## How to use\n\n"
            "### Find tools\n"
            "Call `cass_market_search` to look up tools and get their full parameter schemas.\n\n"
            "```\n"
            "cass_market_search(\n"
            "  query: str,           # what you're looking for, e.g. 'earnings' or 'options'\n"
            "  tags: list[str]=None, # optional tag filter\n"
            "  detail: str='full',   # 'brief' for names only, 'detailed' for markdown, 'full' for JSON schemas\n"
            "  limit: int=None       # max results\n"
            ")\n"
            "```\n\n"
            "### Execute tools\n"
            "Use `cass_market_run` to execute tools via Python code with `call_tool()`:\n\n"
            "```\n"
            "cass_market_run(code=\"return await call_tool('stock_brief', {'symbol': 'AAPL'})\")\n"
            "```\n\n"
            "Chain multiple calls in one block for multi-step analysis:\n"
            "```\n"
            "cass_market_run(code=\"\"\"\n"
            "brief = await call_tool('stock_brief', {'symbol': 'AAPL'})\n"
            "earnings = await call_tool('earnings_preview', {'symbol': 'AAPL'})\n"
            "return {'brief': brief, 'earnings': earnings}\n"
            "\"\"\")\n"
            "```"
        ),
        "lifespan": lifespan,
        "transforms": [
            CodeMode(
                sandbox_provider=MontySandboxProvider(limits={"max_duration_secs": 60}),
                discovery_tools=[
                    Search(name="cass_market_search", default_detail="full"),
                    GetSchemas(name="cass_market_get_schema"),
                    GetTags(name="cass_market_tags"),
                ],
                execute_tool_name="cass_market_run",
            ),
        ],
    }
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

    # Register all tool modules. TV clients are threaded into the tools
    # where TradingView adds coverage (news mix, economic prints, non-US
    # symbols, futures continuations) — callers see a single unified tool
    # surface, not tv_* prefixed variants.
    overview.register(mcp, client, tv_proxy=tv_proxy_client)
    financials.register(mcp, client)
    valuation.register(mcp, client)
    market.register(
        mcp, client,
        polygon_client=polygon_client,
        theta_client=theta_client,
        tv_ws=tv_ws_client,
        tv_proxy=tv_proxy_client,
    )
    ownership.register(mcp, client, polygon_client=polygon_client)
    news.register(mcp, client, tv_proxy=tv_proxy_client)
    macro.register(mcp, client, tv_proxy=tv_proxy_client)
    transcripts.register(mcp, client)
    assets.register(mcp, client)
    workflows.register(mcp, client)
    edgar.register(mcp, client)
    auctions.register(mcp, treasury_client)
    cot.register(mcp)
    esg.register(mcp)
    llm.register(mcp)
    meta.register(mcp, client)

    if theta_client is not None:
        options.register(mcp, theta_client=theta_client)

    if polygon_client is not None:
        economy.register(mcp, polygon_client)

    # TV-scanner-backed analytical tools. No-op when tv_proxy_client is None.
    scanner.register(mcp, tv_proxy=tv_proxy_client)

    # Tag tools for Code Mode category discovery (GetTags)
    _apply_tags(mcp)

    return mcp


# Tool → tag mapping. Tags are the categories shown by GetTags discovery.
_TOOL_TAGS: dict[str, set[str]] = {
    # overview
    "quote": {"price", "overview"},
    "company_overview": {"overview"},
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
    "cot_symbols": {"commodities", "fixed-income"},
    # esg
    "esg_ratings": {"esg"},
    "esg_benchmark": {"esg"},
    # llm
    "analyze": {"analysis"},
    # meta
    "fmp_coverage_gaps": {"meta"},
    # options (polygon)
    "options_chain": {"options"},
    "historical_options": {"options"},
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
    # screener + scanner-backed analytics (TV-backed; register only when TV proxy configured)
    "screener": {"screening", "market"},
    "screener_columns": {"screening", "meta"},
    "technical_rating": {"market", "technicals"},
    "perf_matrix": {"market", "price"},
    "fundamentals_matrix": {"valuation", "financials"},
    "proximity_scan": {"screening", "market"},
    "pattern_scan": {"screening", "technicals"},
    "premarket_movers": {"screening", "market"},
    "relative_volume_leaders": {"screening", "market"},
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
