"""Meta tools for coverage introspection and planning."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cassandra_fmp.tools._endpoint_registry import IMPLEMENTED_FAMILIES

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient

DOCS_URL = "https://site.financialmodelingprep.com/developer/docs"

# Family-level coverage map against FMP docs categories.
# These are endpoint family prefixes, not exhaustive endpoint lists.
DOCUMENTED_FAMILIES_BY_CATEGORY: dict[str, list[str]] = {
    "Profile & Company Data": [
        "profile",
        "search-name",
        "search-symbol",
        "search-cik",
        "search-cusip",
        "stock-list",
        "financial-statement-symbol-lists",
        "isin-search",
        "isin",
        "delisted-companies",
        "employee-count",
        "key-executives",
        "executive-compensation",
        "executive-compensation-benchmark",
        "sec-filings-search",
    ],
    "Quote & Price Data": [
        "quote",
        "batch-quote",
        "historical-price-eod",
        "historical-chart",
        "historical-market-capitalization",
    ],
    "Technical Indicators": [
        "technical-indicators",
    ],
    "Financial Statements & Metrics": [
        "income-statement",
        "balance-sheet-statement",
        "cash-flow-statement",
        "key-metrics",
        "key-metrics-ttm",
        "financial-ratios",
        "ratios-ttm",
        "owner-earnings",
        "financial-scores",
        "revenue-product-segmentation",
        "revenue-geographic-segmentation",
    ],
    "Estimates & Ratings": [
        "analyst-estimates",
        "price-target-consensus",
        "grades",
        "grades-consensus",
        "ratings-snapshot",
        "earnings",
        "earnings-calendar",
        "earning-call-transcript",
        "earning-call-transcript-dates",
    ],
    "DCF & Valuation": [
        "discounted-cash-flow",
    ],
    "Ownership & Regulatory Flows": [
        "stock-peers",
        "institutional-ownership",
        "insider-trading",
        "shares-float",
        "senate-trading",
    ],
    "ETF & Funds": [
        "etf",
        "mutual-fund",
        "funds-disclosure",
    ],
    "Macro & Market": [
        "treasury-rates",
        "market-risk-premium",
        "economic-calendar",
        "sector-performance-snapshot",
        "industry-performance-snapshot",
        "biggest-gainers",
        "biggest-losers",
        "most-actives",
        "sector-pe-snapshot",
        "industry-pe-snapshot",
        "sp500-constituent",
        "nasdaq-constituent",
        "dowjones-constituent",
        "exchange-market-hours",
        "holidays-by-exchange",
        "dividends-calendar",
        "stock-splits-calendar",
        "ipos-calendar",
        "ipos-prospectus",
        "ipos-disclosure",
        "mergers-acquisitions-latest",
        "mergers-acquisitions-search",
    ],
    "News": [
        "news",
    ],
    "Asset Quotes": [
        "batch-commodity-quotes",
        "batch-crypto-quotes",
        "batch-forex-quotes",
    ],
    "ESG": [
        "esg",
    ],
    "Commitment of Traders": [
        "commitment-of-traders",
    ],
    "Fundraisers": [
        "crowdfunding-offerings",
        "fundraising",
    ],
    "Bulk Datasets": [
        "bulk",
    ],
    "WebSocket Streams": [
        "websocket",
    ],
}


def _extract_implemented_families() -> set[str]:
    """Return static implemented endpoint families."""
    return set(IMPLEMENTED_FAMILIES)


def _build_coverage_snapshot(include_implemented_categories: bool) -> dict:
    documented_families = {
        family
        for families in DOCUMENTED_FAMILIES_BY_CATEGORY.values()
        for family in families
    }
    implemented_families = _extract_implemented_families()
    missing_families = sorted(documented_families - implemented_families)

    categories: list[dict] = []
    for category, families in DOCUMENTED_FAMILIES_BY_CATEGORY.items():
        fam_set = set(families)
        implemented = sorted(fam_set & implemented_families)
        missing = sorted(fam_set - implemented_families)

        if not missing:
            status = "covered"
        elif implemented:
            status = "partial"
        else:
            status = "missing"

        if include_implemented_categories or missing:
            categories.append(
                {
                    "category": category,
                    "status": status,
                    "documented_families": sorted(families),
                    "implemented_families": implemented,
                    "missing_families": missing,
                }
            )

    categories.sort(key=lambda c: (c["status"], c["category"]))

    return {
        "docs_url": DOCS_URL,
        "coverage_basis": "family-level endpoint prefixes",
        "documented_family_count": len(documented_families),
        "implemented_family_count": len(implemented_families),
        "unimplemented_family_count": len(missing_families),
        "unimplemented_families": missing_families,
        "categories": categories,
        "notes": [
            "Coverage is computed from a static endpoint-family registry.",
            "Families are category-level prefixes, not exhaustive endpoint-by-endpoint parity.",
            "WebSocket support is tracked as a docs capability but is not a /stable REST family.",
        ],
    }


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    # client is accepted for consistent module signature; unused by this tool.
    del client

    @mcp.tool(
        annotations={
            "title": "FMP Coverage Gaps",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        }
    )
    async def fmp_coverage_gaps(
        include_implemented_categories: bool = False,
    ) -> dict:
        """List FMP docs endpoint families not implemented in this MCP server.

        Useful for agents deciding whether a missing capability requires new
        tooling or can be answered with existing tools.

        Args:
            include_implemented_categories: Include fully-covered categories too.
        """
        return _build_coverage_snapshot(include_implemented_categories)
