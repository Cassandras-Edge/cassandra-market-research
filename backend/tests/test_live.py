"""Live integration tests against the real FMP stable API.

Run examples:
  uv run pytest tests/test_live.py -m live_smoke -q
  uv run pytest tests/test_live.py -m live_full -q
  uv run pytest tests/test_live.py -m live -v -s
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from typing import Any, Callable

from dotenv import load_dotenv
import pytest
import pytest_asyncio

from fastmcp import Client, FastMCP
from tests.conftest import build_test_client
from cassandra_fmp.tools import assets, edgar, financials, macro, market, meta, news, overview, ownership, transcripts, valuation, workflows


Validator = Callable[[dict[str, Any]], None]


def resolve_api_key() -> tuple[str, str | None, str | None]:
    """Resolve FMP API key with deterministic precedence.

    Resolution order:
    1) Process environment
    2) Repo-local .env
    3) Parent .env
    """
    env_var = "FMP_API_KEY"
    checked_paths: list[str] = [f"process env ({env_var})"]

    key = os.environ.get(env_var)
    if key:
        return key, "process env", None

    repo_root = Path(__file__).resolve().parent.parent
    dotenv_candidates = [repo_root / ".env", repo_root.parent / ".env"]

    for dotenv_path in dotenv_candidates:
        checked_paths.append(str(dotenv_path))
        if not dotenv_path.exists():
            continue

        load_dotenv(dotenv_path=dotenv_path, override=False)
        key = os.environ.get(env_var)
        if key:
            return key, str(dotenv_path), None

    reason = (
        f"{env_var} not found. Resolution order checked: "
        + " -> ".join(checked_paths)
    )
    return "", None, reason


API_KEY, API_KEY_SOURCE, API_KEY_SKIP_REASON = resolve_api_key()

pytestmark = [pytest.mark.live]
if not API_KEY:
    pytestmark.append(pytest.mark.skip(reason=API_KEY_SKIP_REASON or "FMP_API_KEY not set"))


@dataclass(frozen=True)
class ToolCase:
    tool_name: str
    args: dict[str, Any]
    required_keys: tuple[str, ...]
    marker_set: str  # "live_smoke" or "live_full"
    validator: Validator | None = None
    fallback_args: tuple[dict[str, Any], ...] = ()


SMOKE_TOOLS = {
    "company_overview",
    "financial_statements",
    "ratio_history",
    "valuation_history",
    "price_history",
    "etf_lookup",
    "market_hours",
    "market_news",
    "treasury_rates",
    "stock_brief",
    "market_context",
    "ownership_deep_dive",
}


def _default_marker(tool_name: str) -> str:
    return "live_smoke" if tool_name in SMOKE_TOOLS else "live_full"


def _case(
    tool_name: str,
    args: dict[str, Any],
    required_keys: tuple[str, ...],
    marker_set: str | None = None,
    validator: Validator | None = None,
    fallback_args: tuple[dict[str, Any], ...] = (),
) -> ToolCase:
    return ToolCase(
        tool_name=tool_name,
        args=args,
        required_keys=required_keys,
        marker_set=marker_set or _default_marker(tool_name),
        validator=validator,
        fallback_args=fallback_args,
    )


def _as_param(case: ToolCase) -> Any:
    marks: list[Any] = [pytest.mark.live_full]
    if case.marker_set == "live_smoke":
        marks.append(pytest.mark.live_smoke)
    return pytest.param(case, id=case.tool_name, marks=marks)


def _assert_list_key(data: dict[str, Any], key: str) -> None:
    assert isinstance(data.get(key), list), f"'{key}' must be a list"


def _assert_non_empty_list_key(data: dict[str, Any], key: str) -> None:
    _assert_list_key(data, key)
    assert len(data[key]) > 0, f"'{key}' must be non-empty"


def _validate_market_hours(data: dict[str, Any]) -> None:
    assert data["exchange"] == "NYSE"
    assert "upcoming_holidays" in data
    assert isinstance(data["upcoming_holidays"], list)
    if "_warnings" in data:
        assert isinstance(data["_warnings"], list)


def _validate_etf_profile(data: dict[str, Any]) -> None:
    assert data.get("mode") == "profile"
    assert isinstance(data.get("info"), dict)
    _assert_list_key(data, "top_holdings")


CANONICAL_CASES = [
    _case(
        "fmp_coverage_gaps",
        {},
        (
            "docs_url",
            "coverage_basis",
            "documented_family_count",
            "implemented_family_count",
            "unimplemented_family_count",
            "unimplemented_families",
            "categories",
        ),
    ),
    # overview
    _case("company_overview", {"symbol": "AAPL"}, ("symbol", "name", "price")),
    _case("company_executives", {"symbol": "AAPL"}, ("symbol", "count", "executives")),
    _case("employee_history", {"symbol": "AAPL"}, ("symbol", "count", "history")),
    _case("delisted_companies", {"limit": 10}, ("count", "companies")),
    _case("sec_filings", {"symbol": "AAPL", "limit": 10}, ("symbol", "count", "filings")),
    _case("symbol_lookup", {"query": "0000320193", "type": "cik"}, ("query", "lookup_type", "count", "results")),
    # financials
    _case("ratio_history", {"symbol": "AAPL", "period": "annual", "limit": 10}, ("symbol", "period_type", "time_series", "trends")),
    _case("financial_statements", {"symbol": "AAPL", "period": "annual", "limit": 5}, ("symbol", "period_type", "periods")),
    _case("revenue_segments", {"symbol": "AAPL"}, ("symbol",)),
    _case("financial_health", {"symbol": "AAPL"}, ("symbol",)),
    # valuation
    _case("valuation_history", {"symbol": "AAPL", "period": "annual", "limit": 10}, ("symbol", "period_type", "current_ttm", "historical", "percentiles")),
    _case("analyst_consensus", {"symbol": "AAPL"}, ("symbol", "price_targets", "analyst_grades", "fmp_rating")),
    _case("peer_comparison", {"symbol": "AAPL"}, ("symbol", "peers", "peer_count", "comparisons", "peer_details")),
    _case("estimate_revisions", {"symbol": "AAPL"}, ("symbol", "forward_estimates", "recent_analyst_actions", "earnings_track_record")),
    # market
    _case("price_history", {"symbol": "AAPL", "period": "1y"}, ("symbol", "current_price", "data_points")),
    _case("dividends_info", {"symbol": "AAPL"}, ("symbol", "recent_dividends", "stock_splits")),
    _case(
        "earnings_calendar",
        {"days_ahead": 7},
        ("from_date", "to_date", "count", "earnings"),
        fallback_args=({"days_ahead": 30},),
    ),
    _case(
        "etf_lookup",
        {"symbol": "QQQ", "mode": "profile", "limit": 10},
        ("symbol", "mode", "info", "top_holdings", "sector_weights", "country_allocation"),
        validator=_validate_etf_profile,
    ),
    _case(
        "intraday_prices",
        {"symbol": "AAPL"},
        ("symbol", "mode", "candle_count", "summary", "candles"),
    ),
    _case("historical_market_cap", {"symbol": "AAPL", "limit": 10}, ("symbol", "current_market_cap", "data_points", "history")),
    _case("technical_indicators", {"symbol": "AAPL", "indicator": "rsi", "period_length": 14}, ("symbol", "indicator", "current_value", "data_points", "values")),
    # ownership
    _case("insider_activity", {"symbol": "AAPL"}, ("symbol", "net_activity_30d", "statistics", "notable_trades", "float_context")),
    _case("institutional_ownership", {"symbol": "AAPL"}, ("symbol", "reporting_period", "top_holders", "position_changes", "ownership_summary")),
    _case("short_interest", {"symbol": "AAPL"}, ("symbol",)),
    _case("fund_holdings", {"cik": "0001166559"}, ("cik", "reporting_period", "portfolio_summary", "top_holdings", "performance", "industry_allocation")),
    _case("ownership_structure", {"symbol": "AAPL"}, ("symbol", "reporting_period", "shares_breakdown", "ownership_percentages", "institutional_details", "short_interest_details")),
    _case("fund_disclosure", {"symbol": "SPY"}, ("mode", "symbol", "period", "holdings_count", "holdings")),
    _case("fund_disclosure", {"symbol": "AAPL", "mode": "holders"}, ("mode", "symbol", "count", "holders"), marker_set="live_full"),
    _case("fund_disclosure", {"mode": "search", "name": "Vanguard"}, ("mode", "query", "count", "funds"), marker_set="live_full"),
    # news
    _case("market_news", {"category": "stock", "symbol": "AAPL", "limit": 10}, ("category", "count", "articles")),
    _case("mna_activity", {"limit": 10}, ("count", "deals")),
    # macro
    _case("treasury_rates", {}, ("date", "yields", "curve_slope_10y_2y", "curve_inverted", "dcf_inputs")),
    _case("economic_calendar", {"days_ahead": 14}, ("events", "count", "period")),
    _case("market_overview", {}, ("sectors", "top_gainers", "top_losers", "most_active")),
    _case("ipo_calendar", {"days_ahead": 14}, ("ipos", "count", "period")),
    _case("dividends_calendar", {"days_ahead": 14}, ("dividends", "count", "period")),
    _case("index_constituents", {"index": "sp500"}, ("index", "count", "constituents", "sector_breakdown")),
    _case("index_performance", {}, ("indices", "count")),
    _case("market_hours", {"exchange": "NYSE"}, ("exchange", "upcoming_holidays"), validator=_validate_market_hours),
    _case("industry_performance", {}, ("date", "industries", "count")),
    _case("splits_calendar", {"days_ahead": 30}, ("splits", "count", "period")),
    _case("sector_valuation", {}, ("date",)),
    _case("crowdfunding_offerings", {}, ("mode", "count", "offerings")),
    _case("fundraising", {"query": "SpaceX"}, ("mode", "query", "count", "entities")),
    _case("fundraising", {"cik": "0001181412"}, ("mode", "cik", "company_name", "filing_count", "filings")),
    _case("fundraising", {}, ("mode", "count", "filings")),
    # transcripts
    _case("earnings_transcript", {"symbol": "AAPL"}, ("symbol", "year", "quarter", "content", "length_chars", "total_chars", "offset", "truncated")),
    # assets
    _case("commodity_quotes", {"symbol": "GCUSD"}, ("symbol", "mode", "asset_type", "price")),
    _case("crypto_quotes", {"symbol": "BTCUSD"}, ("symbol", "mode", "asset_type", "price")),
    _case("forex_quotes", {"symbol": "EURUSD"}, ("symbol", "mode", "asset_type", "price")),
    # workflows
    _case("stock_brief", {"symbol": "AAPL"}, ("symbol", "company_name", "price", "momentum", "valuation", "analyst", "insider", "news", "quick_take")),
    _case("market_context", {}, ("date", "rates", "rotation", "breadth", "movers", "calendar", "environment")),
    _case("earnings_setup", {"symbol": "AAPL"}, ("symbol", "company_name", "consensus", "surprise_history", "analyst_momentum", "setup_summary")),
    _case("earnings_preview", {"symbol": "AAPL"}, ("symbol", "setup_signal", "composite_score", "price_context", "consensus", "beat_history", "signals")),
    _case("fair_value_estimate", {"symbol": "AAPL"}, ("symbol", "current_price", "fundamentals", "growth", "multiples", "fair_value", "quality", "summary")),
    _case("earnings_postmortem", {"symbol": "AAPL"}, ("symbol", "earnings_date", "results", "yoy", "qoq", "guidance", "analyst_reaction", "market_reaction", "summary")),
    _case("ownership_deep_dive", {"symbol": "AAPL"}, ("symbol", "ownership_structure", "insider_activity", "institutional_ownership", "short_interest", "ownership_analysis")),
    _case(
        "industry_analysis",
        {"industry": "Software", "limit": 10},
        ("industry", "overview", "top_stocks", "industry_medians", "valuation_spread", "rotation", "summary"),
        fallback_args=(
            {"industry": "Semiconductors", "limit": 10},
            {"industry": "Technology", "limit": 10},
        ),
    ),
    # edgar (SEC EDGAR full-text search)
    _case("sec_filings_search", {"query": "SpaceX", "forms": "NPORT-P"}, ("query", "total_hits", "filings", "showing")),
    _case("sec_filings_search", {"query": "Anthropic", "forms": "NPORT-P"}, ("query", "total_hits", "filings", "showing")),
    _case("sec_filings_search", {"query": "SpaceX", "limit": 10}, ("query", "total_hits", "filings", "form_breakdown")),
]

assert len(CANONICAL_CASES) == 63


@pytest_asyncio.fixture
async def live_server() -> FastMCP:
    """Create one FastMCP instance with all tool modules registered."""
    mcp = FastMCP("Live E2E")
    client = build_test_client(API_KEY)

    overview.register(mcp, client)
    financials.register(mcp, client)
    valuation.register(mcp, client)
    market.register(mcp, client)
    ownership.register(mcp, client)
    news.register(mcp, client)
    macro.register(mcp, client)
    transcripts.register(mcp, client)
    assets.register(mcp, client)
    workflows.register(mcp, client)
    edgar.register(mcp, client)
    meta.register(mcp, client)

    try:
        yield mcp
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [_as_param(c) for c in CANONICAL_CASES])
async def test_live_tool_contracts(live_server: FastMCP, case: ToolCase) -> None:
    data: dict[str, Any] | None = None
    all_args = (case.args, *case.fallback_args)

    for current_args in all_args:
        async with Client(live_server) as c:
            result = await c.call_tool(case.tool_name, current_args)
        data = result.data
        assert isinstance(data, dict), f"{case.tool_name} must return dict data"
        if "error" not in data:
            break

    assert data is not None
    assert "error" not in data, (
        f"{case.tool_name} returned error after args attempts {all_args}: {data.get('error')}"
    )

    for key in case.required_keys:
        assert key in data, f"{case.tool_name} missing required key '{key}'"

    if case.validator is not None:
        case.validator(data)


@pytest.mark.asyncio
@pytest.mark.live_full
@pytest.mark.parametrize(
    ("args", "expected_mode", "list_key"),
    [
        pytest.param({"symbol": "QQQ", "mode": "holdings", "limit": 10}, "holdings", "holdings", id="etf-holdings"),
        pytest.param({"symbol": "AAPL", "mode": "exposure", "limit": 10}, "exposure", "etf_holders", id="etf-exposure"),
        pytest.param({"symbol": "AAPL", "mode": "auto", "limit": 10}, "auto", "auto", id="etf-auto"),
    ],
)
async def test_live_etf_lookup_modes(live_server: FastMCP, args: dict[str, Any], expected_mode: str, list_key: str) -> None:
    async with Client(live_server) as c:
        result = await c.call_tool("etf_lookup", args)

    data = result.data
    assert isinstance(data, dict)
    assert "error" not in data, f"etf_lookup returned error: {data.get('error')}"

    if expected_mode == "auto":
        assert data.get("mode") in {"holdings", "exposure"}
        if data.get("mode") == "holdings":
            _assert_non_empty_list_key(data, "holdings")
        else:
            _assert_non_empty_list_key(data, "etf_holders")
        return

    assert data.get("mode") == expected_mode
    _assert_non_empty_list_key(data, list_key)


@pytest.mark.asyncio
@pytest.mark.live_full
@pytest.mark.parametrize(
    "args",
    [
        pytest.param({"category": "general", "limit": 10}, id="general-latest"),
        pytest.param({"category": "press_releases", "symbol": "AAPL", "limit": 10}, id="press-releases-aapl"),
    ],
)
async def test_live_market_news_categories(live_server: FastMCP, args: dict[str, Any]) -> None:
    async with Client(live_server) as c:
        result = await c.call_tool("market_news", args)

    data = result.data
    assert isinstance(data, dict)
    assert "error" not in data, f"market_news returned error: {data.get('error')}"
    for key in ("category", "count", "articles"):
        assert key in data
    _assert_list_key(data, "articles")


@pytest.mark.asyncio
@pytest.mark.live_full
async def test_live_market_hours_warning_tolerant(live_server: FastMCP) -> None:
    async with Client(live_server) as c:
        result = await c.call_tool("market_hours", {"exchange": "NYSE"})

    data = result.data
    assert isinstance(data, dict)
    assert "error" not in data, f"market_hours returned error: {data.get('error')}"
    assert data.get("exchange") == "NYSE"
    assert "upcoming_holidays" in data
    assert isinstance(data["upcoming_holidays"], list)

    if "_warnings" in data:
        assert isinstance(data["_warnings"], list)
