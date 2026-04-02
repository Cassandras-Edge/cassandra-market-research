"""FastMCP server for Cassandra FMP — financial data with auth."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastmcp import FastMCP
from fastmcp.experimental.transforms.code_mode import CodeMode
from fmp_data import AsyncFMPDataClient
from fmp_data.config import ClientConfig, RateLimitConfig

from cassandra_fmp.auth import McpKeyAuthProvider, build_auth
from cassandra_fmp.clients.polygon import PolygonClient
from cassandra_fmp.clients.treasury import TreasuryClient
from cassandra_fmp.config import Settings
from cassandra_fmp.tools import (
    assets,
    auctions,
    economy,
    edgar,
    financials,
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
        if settings.workos_client_id and settings.workos_client_secret and settings.workos_authkit_domain and settings.base_url:
            auth_provider, mcp_key_provider = build_auth(
                acl_url=settings.auth_url,
                acl_secret=settings.auth_secret,
                service_id=SERVICE_ID,
                base_url=settings.base_url,
                workos_client_id=settings.workos_client_id,
                workos_client_secret=settings.workos_client_secret,
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
            "Financial data API. Start with workflow tools (stock_brief, market_context, "
            "earnings_setup, earnings_preview, fair_value_estimate, earnings_postmortem, "
            "ownership_deep_dive, industry_analysis) for common questions. "
            "Use atomic tools for targeted queries. All tools are self-documenting. "
            "For 'which funds hold [private company]?' questions (SpaceX, Anthropic, xAI, etc.), "
            "use sec_filings_search with forms='NPORT-P' — this parses actual NPORT-P fund holdings "
            "with position sizes, values, and portfolio weights via edgartools. "
            "For reading SEC filing content (risk factors, MD&A, business description), "
            "use filing_sections with query-based LLM filtering to extract only relevant paragraphs. "
            "filing_sections accepts a `queries` list — pass ALL topics in one call "
            "(e.g. queries=['tariffs trade policy', 'supply chain', 'export restrictions']). "
            "The filing is fetched once and each query is routed independently."
        ),
        "lifespan": lifespan,
        "transforms": [CodeMode()],
    }
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
    meta.register(mcp, client)

    if polygon_client is not None:
        options.register(mcp, polygon_client=polygon_client)
        economy.register(mcp, polygon_client)

    return mcp
