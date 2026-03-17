"""Tests for FMP MCP server tools using respx mocking."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
import respx
import httpx
from pydantic import BaseModel, Field

from fastmcp import FastMCP, Client
from fmp_data import AsyncFMPDataClient
from tests.conftest import (
    build_test_client,
    AAPL_PROFILE, AAPL_QUOTE, AAPL_RATIOS,
    AAPL_INCOME, AAPL_BALANCE, AAPL_CASHFLOW,
    AAPL_PRICE_TARGET, AAPL_GRADES, AAPL_RATING,
    AAPL_SEARCH, AAPL_SCREENER, AAPL_HISTORICAL,
    AAPL_ANALYST_ESTIMATES, AAPL_QUARTERLY_INCOME,
    AAPL_INSIDER_TRADES, AAPL_INSIDER_STATS, AAPL_SHARES_FLOAT,
    AAPL_INSTITUTIONAL_SUMMARY, AAPL_INSTITUTIONAL_HOLDERS,
    AAPL_NEWS, AAPL_PRESS_RELEASES,
    TREASURY_RATES, MARKET_RISK_PREMIUM, ECONOMIC_CALENDAR,
    SECTOR_PERFORMANCE_NYSE, SECTOR_PERFORMANCE_NASDAQ,
    BIGGEST_GAINERS, BIGGEST_LOSERS, MOST_ACTIVES, MOVERS_BATCH_QUOTE,
    AAPL_TRANSCRIPT_DATES, AAPL_TRANSCRIPT,
    AAPL_PRODUCT_SEGMENTS, AAPL_GEO_SEGMENTS,
    AAPL_PEERS, AAPL_KEY_METRICS,
    MSFT_RATIOS, MSFT_KEY_METRICS,
    GOOGL_RATIOS, GOOGL_KEY_METRICS,
    AMZN_RATIOS, AMZN_KEY_METRICS,
    AAPL_DIVIDENDS, AAPL_STOCK_SPLITS,
    AAPL_SHORT_INTEREST,
    EARNINGS_CALENDAR, EARNINGS_BATCH_QUOTE,
    QQQ_HOLDINGS, AAPL_ETF_EXPOSURE,
    AAPL_EARNINGS, AAPL_GRADES_DETAIL,
    # Existing new tool fixtures
    AAPL_EXECUTIVES, AAPL_PROFILE_WITH_CIK, AAPL_SEC_FILINGS,
    AAPL_RSI,
    AAPL_FINANCIAL_SCORES, AAPL_OWNER_EARNINGS,
    IPO_CALENDAR, DIVIDENDS_CALENDAR,
    SP500_CONSTITUENTS,
    SECTOR_PE_NYSE, SECTOR_PE_NASDAQ, INDUSTRY_PE_NYSE, INDUSTRY_PE_NASDAQ,
    MNA_LATEST, MNA_SEARCH_AAPL,
    GOLD_QUOTE, BATCH_COMMODITIES,
    BTCUSD_QUOTE, BATCH_CRYPTO,
    EURUSD_QUOTE, BATCH_FOREX,
    AAPL_KEY_METRICS_HISTORICAL, AAPL_FINANCIAL_RATIOS_HISTORICAL,
    # NEW fixtures for enhanced/new tools
    AAPL_EXECUTIVE_COMPENSATION, AAPL_EXECUTIVE_COMPENSATION_BENCHMARK,
    AAPL_EMPLOYEE_COUNT, DELISTED_COMPANIES, CIK_SEARCH_RESULTS,
    VANGUARD_HOLDINGS, VANGUARD_PERFORMANCE, VANGUARD_INDUSTRY_BREAKDOWN,
    AAPL_INTRADAY_5M, AAPL_INTRADAY_15M, AAPL_INTRADAY_1H,
    AAPL_HISTORICAL_MARKET_CAP,
    QQQ_INFO, QQQ_SECTOR_WEIGHTING, QQQ_COUNTRY_ALLOCATION,
    INDEX_QUOTES, INDEX_HISTORICAL,
    MARKET_HOURS_DATA, MARKET_HOLIDAYS,
    INDUSTRY_PERFORMANCE_NYSE, INDUSTRY_PERFORMANCE_NASDAQ,
    SPLITS_CALENDAR, IPO_PROSPECTUS, IPO_DISCLOSURES,
    AAPL_KEY_METRICS_TTM, MSFT_KEY_METRICS_TTM,
)
from cassandra_fmp.tools.ownership import FINRA_URL
from cassandra_fmp.tools.overview import register as register_overview
from cassandra_fmp.tools.financials import register as register_financials
from cassandra_fmp.tools.valuation import register as register_valuation
from cassandra_fmp.tools.market import register as register_market
from cassandra_fmp.tools.ownership import register as register_ownership
from cassandra_fmp.tools.news import register as register_news
from cassandra_fmp.tools.macro import register as register_macro
from cassandra_fmp.tools.transcripts import register as register_transcripts
from cassandra_fmp.tools.assets import register as register_assets
from cassandra_fmp.tools._helpers import _CACHE, _as_dict, _as_list, _dump, _safe_call, _safe_first

BASE = "https://financialmodelingprep.com"


# --- Helper Tests ---


class TestHelpers:
    @pytest.mark.asyncio
    async def test_safe_call_returns_default_on_exception(self):
        async def _boom() -> dict:
            raise RuntimeError("boom")

        result = await _safe_call(_boom, default={"ok": False})
        assert result == {"ok": False}

    @pytest.mark.asyncio
    async def test_safe_call_ttl_cache_hit(self):
        _CACHE.clear()
        call_count = 0

        async def _fn(symbol: str) -> dict:
            nonlocal call_count
            call_count += 1
            return {"symbol": symbol}

        first = await _safe_call(_fn, "AAPL", ttl=60, default={})
        second = await _safe_call(_fn, "AAPL", ttl=60, default={})
        assert first == {"symbol": "AAPL"}
        assert second == {"symbol": "AAPL"}
        assert call_count == 1

    def test_dump_uses_aliases(self):
        class AliasModel(BaseModel):
            company_name: str = Field(alias="companyName")

        dumped = _dump(AliasModel(companyName="Apple Inc."))
        assert dumped["companyName"] == "Apple Inc."
        assert "company_name" not in dumped

    def test_normalization_helpers(self):
        assert _as_dict([{"a": 1}]) == {"a": 1}
        assert _as_list({"items": [{"a": 1}]}, list_key="items") == [{"a": 1}]
        assert _safe_first([{"x": 2}]) == {"x": 2}


# --- Tool Integration Tests (via FastMCP Client) ---


def _make_server(register_fn) -> tuple[FastMCP, AsyncFMPDataClient]:
    """Create a FastMCP server with registered tools."""
    mcp = FastMCP("Test")
    client = build_test_client("test_key")
    register_fn(mcp, client)
    return mcp, client


class TestQuote:
    @pytest.mark.asyncio
    @respx.mock
    async def test_quote_regular_session(self):
        """Returns quote price during regular hours."""
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("quote", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["price"] == 273.68
        assert "price_source" not in data  # omitted when source is quote
        assert "regular_close" not in data
        assert data["volume"] == 34311675
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_quote_afterhours(self):
        """Picks afterhours price when fresher."""
        quote = [{**AAPL_QUOTE[0], "timestamp": 1000}]
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=quote))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        afterhours = [{"symbol": "AAPL", "price": 280.00, "tradeSize": 5, "timestamp": 2_000_000}]
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=afterhours))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("quote", {"symbol": "AAPL"})

        data = result.data
        assert data["price"] == 280.00
        assert data["price_source"] == "afterhours"
        assert data["regular_close"] == 273.68
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_quote_unknown_symbol(self):
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("quote", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestCompanyOverview:
    @pytest.mark.asyncio
    @respx.mock
    async def test_lean_overview(self):
        """Default mode: quote + extended hours for freshest price."""
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("company_overview", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["price"] == 273.68
        assert data["price_source"] == "quote"
        assert data["sma_50"] == 268.66
        assert "ratios" not in data
        assert "sector" not in data
        assert "description" not in data
        assert "regular_close" not in data  # not present when source is quote
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_lean_overview_afterhours_price(self):
        """Default mode picks afterhours price when it's fresher than quote."""
        # Quote with timestamp 1000 (epoch seconds)
        quote = [{**AAPL_QUOTE[0], "timestamp": 1000}]
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=quote))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        # Afterhours with timestamp 2_000_000 ms (= 2000s, fresher)
        afterhours = [{"symbol": "AAPL", "price": 280.00, "tradeSize": 5, "timestamp": 2_000_000}]
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=afterhours))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("company_overview", {"symbol": "AAPL"})

        data = result.data
        assert data["price"] == 280.00
        assert data["price_source"] == "afterhours"
        assert data["regular_close"] == 273.68
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_detail_overview(self):
        """detail=True: full profile + quote + ratios + extended hours."""
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("company_overview", {"symbol": "AAPL", "detail": True})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["name"] == "Apple Inc."
        assert data["price"] == 273.68
        assert data["price_source"] == "quote"
        assert data["ratios"]["pe_ttm"] == 34.27
        assert data["sma_50"] == 268.66
        assert data["sector"] == "Technology"
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_detail_partial_data(self):
        """detail=True with missing ratios shows warning."""
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("company_overview", {"symbol": "AAPL", "detail": True})

        data = result.data
        assert data["name"] == "Apple Inc."
        assert data["_warnings"] == ["ratio data unavailable"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("company_overview", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestStockSearch:
    @pytest.mark.asyncio
    @respx.mock
    async def test_name_search(self):
        respx.get(f"{BASE}/stable/search-name").mock(return_value=httpx.Response(200, json=AAPL_SEARCH))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("stock_search", {"query": "apple"})

        data = result.data
        assert data["count"] == 2
        assert data["results"][0]["symbol"] == "APC.F"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_screener_search(self):
        respx.get(f"{BASE}/stable/company-screener").mock(return_value=httpx.Response(200, json=AAPL_SCREENER))

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("stock_search", {
                "query": "",
                "sector": "Technology",
                "market_cap_min": 100000000000,
            })

        data = result.data
        assert data["count"] == 2
        assert data["results"][0]["symbol"] == "NVDA"
        await fmp.aclose()


class TestFinancialStatements:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_financials(self):
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=AAPL_INCOME))
        respx.get(f"{BASE}/stable/balance-sheet-statement").mock(return_value=httpx.Response(200, json=AAPL_BALANCE))
        respx.get(f"{BASE}/stable/cash-flow-statement").mock(return_value=httpx.Response(200, json=AAPL_CASHFLOW))

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("financial_statements", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert len(data["periods"]) == 4
        assert data["periods"][0]["revenue"] == 416161000000
        assert data["periods"][0]["free_cash_flow"] == 98767000000
        assert "growth_3y_cagr" in data
        assert data["growth_3y_cagr"]["revenue_cagr_3y"] is not None
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_margins_calculated(self):
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=AAPL_INCOME))
        respx.get(f"{BASE}/stable/balance-sheet-statement").mock(return_value=httpx.Response(200, json=AAPL_BALANCE))
        respx.get(f"{BASE}/stable/cash-flow-statement").mock(return_value=httpx.Response(200, json=AAPL_CASHFLOW))

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("financial_statements", {"symbol": "AAPL"})

        data = result.data
        p = data["periods"][0]
        assert p["gross_margin"] is not None
        assert p["gross_margin"] > 40
        await fmp.aclose()


class TestAnalystConsensus:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_consensus(self):
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=AAPL_GRADES))
        respx.get(f"{BASE}/stable/ratings-snapshot").mock(return_value=httpx.Response(200, json=AAPL_RATING))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("analyst_consensus", {"symbol": "AAPL"})

        data = result.data
        assert data["price_targets"]["consensus"] == 303.11
        assert data["price_targets"]["upside_pct"] is not None
        assert data["analyst_grades"]["buy"] == 68
        assert data["analyst_grades"]["strong_buy"] == 1
        assert data["fmp_rating"]["rating"] == "B"
        assert data["fmp_rating"]["overall_score"] == 3
        await fmp.aclose()


class TestPriceHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_price_history_lean(self):
        """Default mode: no recent_closes."""
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("price_history", {"symbol": "AAPL"})

        data = result.data
        assert data["current_price"] == 273.68
        assert data["year_high"] == 288.62
        assert data["sma_50"] == 268.66
        assert "recent_closes" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_price_history_detail(self):
        """detail=True: includes TOON-encoded recent_closes."""
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("price_history", {"symbol": "AAPL", "detail": True})

        data = result.data
        assert data["current_price"] == 273.68
        assert "recent_closes" in data
        # TOON-encoded string starts with array length marker
        assert isinstance(data["recent_closes"], str)
        assert data["recent_closes"].startswith("[")
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_period(self):
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("price_history", {"symbol": "AAPL", "period": "10y"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


# --- New Tool Tests (Tier 1: Unblock Broken Agents) ---


class TestInsiderActivity:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_insider_data(self):
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_STATS))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("insider_activity", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["statistics"]["total_bought"] == 23000
        assert data["statistics"]["total_sold"] == 70000
        assert data["float_context"]["float_shares"] == 14700000000
        assert len(data["notable_trades"]) > 0
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_data(self):
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(500, text="error"))

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("insider_activity", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert "insider statistics unavailable" in data["_warnings"]
        assert "float data unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("insider_activity", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestInstitutionalOwnership:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_institutional(self):
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(return_value=httpx.Response(200, json=AAPL_INSTITUTIONAL_SUMMARY))
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(return_value=httpx.Response(200, json=AAPL_INSTITUTIONAL_HOLDERS))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("institutional_ownership", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert len(data["top_holders"]) == 5
        assert data["top_holders"][0]["holder"] == "VANGUARD GROUP INC"
        assert data["position_changes"]["investors_holding"] == 3557
        assert data["ownership_summary"]["institutional_pct_of_float"] is not None
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("institutional_ownership", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_falls_back_to_previous_available_quarter(self):
        today = date.today()
        if today.month <= 3:
            latest_year, latest_quarter = today.year - 1, 4
        elif today.month <= 6:
            latest_year, latest_quarter = today.year, 1
        elif today.month <= 9:
            latest_year, latest_quarter = today.year, 2
        else:
            latest_year, latest_quarter = today.year, 3
        prev_year, prev_quarter = (latest_year - 1, 4) if latest_quarter == 1 else (latest_year, latest_quarter - 1)

        def summary_side_effect(request: httpx.Request) -> httpx.Response:
            year = int(request.url.params.get("year", 0))
            quarter = int(request.url.params.get("quarter", 0))
            if (year, quarter) == (latest_year, latest_quarter):
                return httpx.Response(200, json=[])
            if (year, quarter) == (prev_year, prev_quarter):
                return httpx.Response(200, json=AAPL_INSTITUTIONAL_SUMMARY)
            return httpx.Response(200, json=[])

        def holders_side_effect(request: httpx.Request) -> httpx.Response:
            year = int(request.url.params.get("year", 0))
            quarter = int(request.url.params.get("quarter", 0))
            if (year, quarter) == (latest_year, latest_quarter):
                return httpx.Response(200, json=[])
            if (year, quarter) == (prev_year, prev_quarter):
                return httpx.Response(200, json=AAPL_INSTITUTIONAL_HOLDERS)
            return httpx.Response(200, json=[])

        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(side_effect=summary_side_effect)
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(side_effect=holders_side_effect)
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("institutional_ownership", {"symbol": "AAPL"})

        data = result.data
        assert data["reporting_period"] == f"Q{prev_quarter} {prev_year}"
        assert len(data["top_holders"]) > 0
        await fmp.aclose()


class TestMarketNews:
    @pytest.mark.asyncio
    @respx.mock
    async def test_stock_news_with_symbol(self):
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=AAPL_NEWS))

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("market_news", {"category": "stock", "symbol": "AAPL"})

        data = result.data
        assert data["category"] == "stock"
        assert data["symbol"] == "AAPL"
        assert data["count"] == 3
        assert data["articles"][0]["title"] == "Apple Reports Record Q1 Earnings"
        assert data["articles"][0]["source"] == "Bloomberg"
        assert data["articles"][0]["date"] is not None
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_stock_latest_no_symbol(self):
        respx.get(f"{BASE}/stable/news/stock-latest").mock(return_value=httpx.Response(200, json=AAPL_NEWS))

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("market_news", {"category": "stock"})

        data = result.data
        assert data["category"] == "stock"
        assert data["symbol"] is None
        assert data["count"] == 3
        assert any("Did you mean to pass symbol" in w for w in data.get("_warnings", []))
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_press_releases_with_symbol(self):
        respx.get(f"{BASE}/stable/news/press-releases").mock(return_value=httpx.Response(200, json=AAPL_PRESS_RELEASES))

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("market_news", {"category": "press_releases", "symbol": "AAPL"})

        data = result.data
        assert data["category"] == "press_releases"
        assert data["symbol"] == "AAPL"
        assert data["count"] == 2
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_general_ignores_symbol(self):
        respx.get(f"{BASE}/stable/news/general-latest").mock(return_value=httpx.Response(200, json=AAPL_NEWS))

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("market_news", {"category": "general", "symbol": "AAPL"})

        data = result.data
        assert data["category"] == "general"
        # Symbol should be None since general doesn't support it
        assert data["symbol"] is None
        assert data["count"] == 3
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_category(self):
        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("market_news", {"category": "bad_category"})

        data = result.data
        assert "error" in data
        assert "Invalid category" in data["error"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_results(self):
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("market_news", {"category": "stock", "symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        assert "ZZZZ" in data["error"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_pagination(self):
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=AAPL_NEWS))

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("market_news", {"category": "stock", "symbol": "AAPL", "page": 1})

        data = result.data
        assert data["page"] == 1
        assert data["count"] == 3
        await fmp.aclose()


class TestTreasuryRates:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_treasury(self):
        respx.get(f"{BASE}/stable/treasury-rates").mock(return_value=httpx.Response(200, json=TREASURY_RATES))
        respx.get(f"{BASE}/stable/market-risk-premium").mock(return_value=httpx.Response(200, json=MARKET_RISK_PREMIUM))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_rates", {})

        data = result.data
        assert data["date"] == "2026-02-10"
        assert data["yields"]["10y"] == 4.05
        assert data["yields"]["2y"] == 3.82
        assert data["curve_slope_10y_2y"] == 0.23
        assert data["curve_inverted"] is False
        assert data["dcf_inputs"]["risk_free_rate"] == 4.05
        assert data["dcf_inputs"]["equity_risk_premium"] == 4.60
        assert data["dcf_inputs"]["implied_cost_of_equity"] == 8.65
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_erp(self):
        respx.get(f"{BASE}/stable/treasury-rates").mock(return_value=httpx.Response(200, json=TREASURY_RATES))
        respx.get(f"{BASE}/stable/market-risk-premium").mock(return_value=httpx.Response(500, text="error"))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_rates", {})

        data = result.data
        assert data["yields"]["10y"] == 4.05
        assert data["dcf_inputs"]["equity_risk_premium"] is None
        assert "equity risk premium unavailable" in data["_warnings"]
        await fmp.aclose()


class TestEconomicCalendar:
    @pytest.mark.asyncio
    @respx.mock
    async def test_filtered_events(self):
        respx.get(f"{BASE}/stable/economic-calendar").mock(return_value=httpx.Response(200, json=ECONOMIC_CALENDAR))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("economic_calendar", {"days_ahead": 7})

        data = result.data
        assert data["count"] > 0
        # ECB event should be filtered out (non-US)
        events = data["events"]
        countries = set()
        for e in events:
            # All events should be US (non-US filtered)
            assert "ECB" not in (e.get("event") or "")
        # CPI and Retail Sales should be present
        event_names = [e["event"] for e in events]
        assert any("CPI" in n for n in event_names)
        assert any("Retail Sales" in n for n in event_names)
        # Business Inventories should be filtered out (not high-impact keyword)
        assert not any("Business Inventories" in n for n in event_names)
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_calendar(self):
        respx.get(f"{BASE}/stable/economic-calendar").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("economic_calendar", {})

        data = result.data
        assert data["count"] == 0
        assert data["events"] == []
        await fmp.aclose()


class TestMarketOverview:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_market_overview(self):
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NYSE))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NASDAQ))
        respx.get(f"{BASE}/stable/biggest-gainers").mock(return_value=httpx.Response(200, json=BIGGEST_GAINERS))
        respx.get(f"{BASE}/stable/biggest-losers").mock(return_value=httpx.Response(200, json=BIGGEST_LOSERS))
        respx.get(f"{BASE}/stable/most-actives").mock(return_value=httpx.Response(200, json=MOST_ACTIVES))
        respx.get(f"{BASE}/stable/batch-quote").mock(return_value=httpx.Response(200, json=MOVERS_BATCH_QUOTE))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("market_overview", {})

        data = result.data
        assert len(data["sectors"]) == 4
        assert data["sectors"][0]["sector"] == "Technology"  # Highest change
        # TINY ($50M mcap) should be filtered out from gainers
        gainer_symbols = [g["symbol"] for g in data["top_gainers"]]
        assert "TINY" not in gainer_symbols
        assert "XYZ" in gainer_symbols
        assert len(data["top_losers"]) == 2
        assert len(data["most_active"]) == 2
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_market(self):
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NYSE))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NASDAQ))
        respx.get(f"{BASE}/stable/biggest-gainers").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/biggest-losers").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/most-actives").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/batch-quote").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("market_overview", {})

        data = result.data
        assert len(data["sectors"]) == 4
        assert "gainers data unavailable" in data["_warnings"]
        await fmp.aclose()


# --- New Tool Tests (Tier 2: Research Power-Ups) ---


class TestEarningsTranscript:
    @pytest.mark.asyncio
    @respx.mock
    async def test_latest_transcript(self):
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT_DATES))
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))

        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["year"] == 2026
        assert data["quarter"] == 1
        assert "Tim Cook" in data["content"]
        assert data["length_chars"] > 0
        assert data["total_chars"] > 0
        assert data["offset"] == 0
        # Mock transcript is small enough to fit in default max_chars
        assert data["truncated"] is False
        assert "next_offset" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_specific_quarter(self):
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))

        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "AAPL", "year": 2025, "quarter": 4})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["quarter"] == 4
        assert data["year"] == 2025
        assert data["total_chars"] > 0
        assert isinstance(data["truncated"], bool)
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_latest_for_quarter_when_year_omitted(self):
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT_DATES))
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))

        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "AAPL", "quarter": 4})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["year"] == 2025
        assert data["quarter"] == 4
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_latest_for_year_when_quarter_omitted(self):
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT_DATES))
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))

        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "AAPL", "year": 2025})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["year"] == 2025
        assert data["quarter"] == 4
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_quarter(self):
        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "AAPL", "quarter": 5})

        data = result.data
        assert "error" in data
        assert "Invalid quarter" in data["error"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_latest_expected_met(self):
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT_DATES))
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))

        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "AAPL", "latest_expected": True})

        data = result.data
        assert data["latest_expected"] is True
        assert data["latest_expected_met"] is True
        assert data["latest_completed_earnings_date"] == "2026-01-29"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_latest_expected_not_met_warns(self):
        lagged_dates = [{"quarter": 4, "fiscalYear": 2025, "date": "2025-10-30"}]
        lagged_transcript = [{**AAPL_TRANSCRIPT[0], "year": 2025, "quarter": 4, "date": "2025-10-30"}]

        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=lagged_dates))
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=lagged_transcript))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))

        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "AAPL", "latest_expected": True})

        data = result.data
        assert data["latest_expected"] is True
        assert data["latest_expected_met"] is False
        assert data["latest_completed_earnings_date"] == "2026-01-29"
        assert any("newer than available transcript" in w for w in data.get("_warnings", []))
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_transcripts(self):
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_transcripts)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_transcript_pagination(self):
        """Test paginating through a transcript with small max_chars."""
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))

        mcp, fmp = _make_server(register_transcripts)

        # First page
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_transcript", {
                "symbol": "AAPL", "year": 2026, "quarter": 1, "max_chars": 200,
            })

        data = result.data
        assert data["truncated"] is True
        assert "next_offset" in data
        assert data["length_chars"] <= 200
        assert data["length_chars"] > 0
        assert data["offset"] == 0
        # Content should end at a line boundary (newline)
        assert data["content"].endswith("\n")
        first_chunk = data["content"]
        next_offset = data["next_offset"]

        # Second page
        async with Client(mcp) as c:
            result2 = await c.call_tool("earnings_transcript", {
                "symbol": "AAPL", "year": 2026, "quarter": 1,
                "max_chars": 200, "offset": next_offset,
            })

        data2 = result2.data
        assert data2["offset"] == next_offset
        assert data2["length_chars"] > 0
        # No overlap between chunks
        assert data2["content"] != first_chunk
        # Total chars should be consistent
        assert data2["total_chars"] == data["total_chars"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_transcript_full_reassembly(self):
        """Paginating through entire transcript should yield all content."""
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))

        mcp, fmp = _make_server(register_transcripts)
        all_content = ""
        offset = 0
        max_iters = 20  # safety limit

        for _ in range(max_iters):
            async with Client(mcp) as c:
                result = await c.call_tool("earnings_transcript", {
                    "symbol": "AAPL", "year": 2026, "quarter": 1,
                    "max_chars": 200, "offset": offset,
                })
            data = result.data
            all_content += data["content"]
            if not data["truncated"]:
                break
            offset = data["next_offset"]

        # Reassembled content should match the original
        assert all_content == AAPL_TRANSCRIPT[0]["content"]
        await fmp.aclose()


class TestRevenueSegments:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_segments(self):
        respx.get(f"{BASE}/stable/revenue-product-segmentation").mock(return_value=httpx.Response(200, json=AAPL_PRODUCT_SEGMENTS))
        respx.get(f"{BASE}/stable/revenue-geographic-segmentation").mock(return_value=httpx.Response(200, json=AAPL_GEO_SEGMENTS))

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("revenue_segments", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert "product_segments" in data
        assert "geographic_segments" in data
        # Product segments should be sorted by revenue desc
        products = data["product_segments"]["segments"]
        assert products[0]["name"] == "iPhone"
        assert products[0]["pct_of_total"] is not None
        # iPhone should have concentration risk (>50%? Let's check)
        assert data["product_segments"]["concentration_risk"] is not None
        # Geographic segments
        geos = data["geographic_segments"]["segments"]
        assert geos[0]["name"] == "Americas"
        # YoY growth should be calculated
        assert products[0].get("yoy_growth_pct") is not None
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_product_only(self):
        respx.get(f"{BASE}/stable/revenue-product-segmentation").mock(return_value=httpx.Response(200, json=AAPL_PRODUCT_SEGMENTS))
        respx.get(f"{BASE}/stable/revenue-geographic-segmentation").mock(return_value=httpx.Response(500, text="error"))

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("revenue_segments", {"symbol": "AAPL"})

        data = result.data
        assert "product_segments" in data
        assert "geographic_segments" not in data
        assert "geographic segmentation unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/revenue-product-segmentation").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/revenue-geographic-segmentation").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("revenue_segments", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestPeerComparison:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_peer_comparison(self):
        respx.get(f"{BASE}/stable/stock-peers").mock(return_value=httpx.Response(200, json=AAPL_PEERS))
        # Target (AAPL) ratios, metrics, estimates, income
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "AAPL"}).mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "AAPL"}).mock(return_value=httpx.Response(200, json=AAPL_KEY_METRICS))
        respx.get(f"{BASE}/stable/analyst-estimates", params__contains={"symbol": "AAPL"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/income-statement", params__contains={"symbol": "AAPL"}).mock(return_value=httpx.Response(200, json=[]))
        # Peer ratios, metrics, estimates, income
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "MSFT"}).mock(return_value=httpx.Response(200, json=MSFT_RATIOS))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "MSFT"}).mock(return_value=httpx.Response(200, json=MSFT_KEY_METRICS))
        respx.get(f"{BASE}/stable/analyst-estimates", params__contains={"symbol": "MSFT"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/income-statement", params__contains={"symbol": "MSFT"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "GOOGL"}).mock(return_value=httpx.Response(200, json=GOOGL_RATIOS))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "GOOGL"}).mock(return_value=httpx.Response(200, json=GOOGL_KEY_METRICS))
        respx.get(f"{BASE}/stable/analyst-estimates", params__contains={"symbol": "GOOGL"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/income-statement", params__contains={"symbol": "GOOGL"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "AMZN"}).mock(return_value=httpx.Response(200, json=AMZN_RATIOS))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "AMZN"}).mock(return_value=httpx.Response(200, json=AMZN_KEY_METRICS))
        respx.get(f"{BASE}/stable/analyst-estimates", params__contains={"symbol": "AMZN"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/income-statement", params__contains={"symbol": "AMZN"}).mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("peer_comparison", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert set(data["peers"]) == {"MSFT", "GOOGL", "AMZN"}
        assert data["peer_count"] == 3
        # Check comparisons structure
        pe_comp = data["comparisons"]["pe_ttm"]
        assert pe_comp["target"] == 34.27
        assert pe_comp["peer_median"] is not None
        assert pe_comp["premium_discount_pct"] is not None
        assert pe_comp["rank"] is not None
        assert len(data["peer_details"]) == 3
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_peers(self):
        respx.get(f"{BASE}/stable/stock-peers").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("peer_comparison", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestDividendsInfo:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_dividends(self):
        respx.get(f"{BASE}/stable/dividends").mock(return_value=httpx.Response(200, json=AAPL_DIVIDENDS))
        respx.get(f"{BASE}/stable/splits").mock(return_value=httpx.Response(200, json=AAPL_STOCK_SPLITS))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_info", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["current_price"] == 273.68
        assert data["trailing_annual_dividend"] is not None
        # Trailing 4 quarters: 0.26 + 0.26 + 0.26 + 0.25 = 1.03
        assert data["trailing_annual_dividend"] == 1.03
        assert data["dividend_yield_pct"] is not None
        assert data["dividend_yield_pct"] > 0.3  # ~0.38% for AAPL
        assert len(data["recent_dividends"]) == 8
        assert len(data["stock_splits"]) == 2
        assert data["stock_splits"][0]["label"] == "4:1"
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_dividends(self):
        respx.get(f"{BASE}/stable/dividends").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/splits").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_info", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_dividends_partial(self):
        respx.get(f"{BASE}/stable/dividends").mock(return_value=httpx.Response(200, json=AAPL_DIVIDENDS))
        respx.get(f"{BASE}/stable/splits").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(500, text="error"))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_info", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert len(data["recent_dividends"]) > 0
        assert "stock split data unavailable" in data["_warnings"]
        assert "quote data unavailable" in data["_warnings"]
        await fmp.aclose()


class TestShortInterest:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_short_interest(self):
        # Mock FINRA: return 204 for all dates except one that matches
        respx.post(FINRA_URL).mock(side_effect=self._finra_side_effect)
        respx.get(f"{BASE}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT)
        )

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("short_interest", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["settlement_date"] == "2026-01-30"
        assert data["short_interest"]["shares_short"] == 116854414
        assert data["short_interest"]["days_to_cover"] == 2.0
        assert data["short_interest"]["change_pct"] == 2.89
        # Verify computed percentages
        assert data["float_context"]["float_shares"] == 14700000000
        assert data["float_context"]["short_pct_of_float"] is not None
        assert data["float_context"]["short_pct_of_float"] > 0
        assert data["float_context"]["short_pct_of_outstanding"] is not None
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_finra_data(self):
        # All FINRA dates return 204 (no data)
        respx.post(FINRA_URL).mock(return_value=httpx.Response(204))
        respx.get(f"{BASE}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT)
        )

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("short_interest", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert "short_interest" not in data
        assert data["float_context"]["float_shares"] == 14700000000
        assert data["float_context"]["short_pct_of_float"] is None
        assert "FINRA short interest unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_float_data(self):
        # FINRA works but FMP shares-float fails
        respx.post(FINRA_URL).mock(side_effect=self._finra_side_effect)
        respx.get(f"{BASE}/stable/shares-float").mock(
            return_value=httpx.Response(500, text="error")
        )

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("short_interest", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["short_interest"]["shares_short"] == 116854414
        assert "float_context" not in data
        assert "float data unavailable" in data["_warnings"]
        await fmp.aclose()

    @staticmethod
    def _finra_side_effect(request: httpx.Request) -> httpx.Response:
        """Return short interest data only for the 2026-01-30 date."""
        import json
        body = json.loads(request.content)
        date_filters = body.get("dateRangeFilters", [])
        for f in date_filters:
            if f.get("startDate") == "2026-01-30":
                return httpx.Response(200, json=AAPL_SHORT_INTEREST)
        return httpx.Response(204)


# --- New Tool Tests (Tier 3: Market Data) ---


class TestEarningsCalendar:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_calendar_filters_by_mcap(self):
        """Default browsing mode filters to $2B+ market cap stocks."""
        respx.get(f"{BASE}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )
        respx.get(f"{BASE}/stable/batch-quote").mock(
            return_value=httpx.Response(200, json=EARNINGS_BATCH_QUOTE)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {})

        data = result.data
        assert data["count"] == 4
        assert data["min_market_cap"] == 2_000_000_000
        # Sorted by market cap descending (AAPL 3.5T > MSFT 3.2T > GOOGL 2.1T > TSLA 1.1T)
        symbols = [e["symbol"] for e in data["earnings"]]
        assert symbols == ["AAPL", "MSFT", "GOOGL", "TSLA"]
        assert data["earnings"][0]["market_cap"] == 3500000000000
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_mcap_filter(self):
        """Setting min_market_cap=0 returns all entries without batch-quote."""
        respx.get(f"{BASE}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {"min_market_cap": 0})

        data = result.data
        assert data["count"] == 4
        # No market_cap field since we skipped batch-quote
        assert "market_cap" not in data["earnings"][0]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_symbols_post_filter(self):
        respx.get(f"{BASE}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {"symbols": ["AAPL", "TSLA"], "min_market_cap": 0})

        data = result.data
        assert data["count"] == 2
        assert [e["symbol"] for e in data["earnings"]] == ["AAPL", "TSLA"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_extended_days_ahead(self):
        respx.get(f"{BASE}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )
        respx.get(f"{BASE}/stable/batch-quote").mock(
            return_value=httpx.Response(200, json=EARNINGS_BATCH_QUOTE)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {"days_ahead": 90})

        data = result.data
        assert data["count"] > 0
        assert data["to_date"] > data["from_date"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_filtered_by_symbol(self):
        respx.get(f"{BASE}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {"symbol": "AAPL"})

        data = result.data
        assert data["count"] == 1
        assert data["symbol"] == "AAPL"
        assert data["earnings"][0]["symbol"] == "AAPL"
        assert data["earnings"][0]["eps_estimate"] == 2.35
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_earnings(self):
        respx.get(f"{BASE}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_symbol_not_found(self):
        respx.get(f"{BASE}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        assert "ZZZZ" in data["error"]
        await fmp.aclose()


class TestETFLookup:
    @pytest.mark.asyncio
    @respx.mock
    async def test_holdings_mode(self):
        respx.get(f"{BASE}/stable/etf/holdings").mock(
            return_value=httpx.Response(200, json=QQQ_HOLDINGS)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "QQQ", "mode": "holdings"})

        data = result.data
        assert data["symbol"] == "QQQ"
        assert data["mode"] == "holdings"
        assert data["count"] == 10
        # Should be sorted by weight descending
        assert data["holdings"][0]["symbol"] == "AAPL"
        assert data["holdings"][0]["weight_pct"] == 12.5
        assert data["top_10_concentration_pct"] is not None
        assert data["top_10_concentration_pct"] > 50
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_exposure_mode(self):
        respx.get(f"{BASE}/stable/etf/asset-exposure").mock(
            return_value=httpx.Response(200, json=AAPL_ETF_EXPOSURE)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "AAPL", "mode": "exposure"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["mode"] == "exposure"
        assert data["count"] == 5
        # Should be sorted by weight descending
        assert data["etf_holders"][0]["etf_symbol"] == "XLK"
        assert data["etf_holders"][0]["weight_pct"] == 22.3
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_auto_mode_finds_holdings(self):
        respx.get(f"{BASE}/stable/etf/holdings").mock(
            return_value=httpx.Response(200, json=QQQ_HOLDINGS)
        )
        respx.get(f"{BASE}/stable/etf/asset-exposure").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "QQQ"})

        data = result.data
        assert data["mode"] == "holdings"
        assert data["count"] == 10
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_auto_mode_falls_back_to_exposure(self):
        respx.get(f"{BASE}/stable/etf/holdings").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/etf/asset-exposure").mock(
            return_value=httpx.Response(200, json=AAPL_ETF_EXPOSURE)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "AAPL"})

        data = result.data
        assert data["mode"] == "exposure"
        assert data["count"] == 5
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/etf/holdings").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/etf/asset-exposure").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_mode(self):
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "QQQ", "mode": "bad"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_limit_applied(self):
        respx.get(f"{BASE}/stable/etf/holdings").mock(
            return_value=httpx.Response(200, json=QQQ_HOLDINGS)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "QQQ", "mode": "holdings", "limit": 3})

        data = result.data
        assert data["count"] == 3
        assert len(data["holdings"]) == 3
        await fmp.aclose()


class TestEstimateRevisions:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_revisions(self):
        respx.get(f"{BASE}/stable/analyst-estimates").mock(
            return_value=httpx.Response(200, json=AAPL_ANALYST_ESTIMATES)
        )
        respx.get(f"{BASE}/stable/grades").mock(
            return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL)
        )
        respx.get(f"{BASE}/stable/earnings").mock(
            return_value=httpx.Response(200, json=AAPL_EARNINGS)
        )

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("estimate_revisions", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"

        # Forward estimates
        assert len(data["forward_estimates"]) == 3
        assert data["forward_estimates"][0]["eps_avg"] is not None

        # Analyst actions
        actions = data["recent_analyst_actions"]
        assert actions["period"] == "90d"
        summary = actions["summary"]
        assert summary["upgrades"] >= 1
        assert summary["downgrades"] >= 0
        assert summary["net_sentiment"] in ("bullish", "bearish", "neutral")

        # Earnings track record
        track = data["earnings_track_record"]
        assert track["beat_rate_eps"] is not None
        assert track["avg_eps_surprise_pct"] is not None
        assert len(track["last_8_quarters"]) > 0
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_data(self):
        respx.get(f"{BASE}/stable/analyst-estimates").mock(
            return_value=httpx.Response(200, json=AAPL_ANALYST_ESTIMATES)
        )
        respx.get(f"{BASE}/stable/grades").mock(
            return_value=httpx.Response(500, text="error")
        )
        respx.get(f"{BASE}/stable/earnings").mock(
            return_value=httpx.Response(500, text="error")
        )

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("estimate_revisions", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert len(data["forward_estimates"]) == 3
        assert "analyst grades unavailable" in data["_warnings"]
        assert "earnings history unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/analyst-estimates").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/grades").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/earnings").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("estimate_revisions", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


# --- New Tool Tests (12 New Tools) ---


class TestCompanyExecutives:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_executives(self):
        respx.get(f"{BASE}/stable/key-executives").mock(
            return_value=httpx.Response(200, json=AAPL_EXECUTIVES)
        )
        respx.get(f"{BASE}/stable/governance-executive-compensation").mock(
            return_value=httpx.Response(200, json=AAPL_EXECUTIVE_COMPENSATION)
        )
        respx.get(f"{BASE}/stable/executive-compensation-benchmark").mock(
            return_value=httpx.Response(200, json=AAPL_EXECUTIVE_COMPENSATION_BENCHMARK)
        )

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("company_executives", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["count"] == 4
        # Sorted by pay descending - CEO should be first
        assert data["executives"][0]["name"] == "Timothy D. Cook"
        assert data["executives"][0]["pay"] == 16425933
        assert data["executives"][0]["title"] == "Chief Executive Officer"
        # Check compensation breakdown
        assert "compensation_breakdown" in data["executives"][0]
        assert data["executives"][0]["compensation_breakdown"]["total"] == 16425933
        # Check benchmarks
        assert "industry_benchmarks" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/key-executives").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/governance-executive-compensation").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/executive-compensation-benchmark").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("company_executives", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestSecFilings:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_filings(self):
        respx.get(f"{BASE}/stable/sec-filings-search/symbol").mock(
            return_value=httpx.Response(200, json=AAPL_SEC_FILINGS)
        )

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("sec_filings", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["count"] == 4
        # Sorted by date descending
        assert data["filings"][0]["form_type"] == "10-Q"
        assert data["filings"][0]["filing_date"] == "2026-01-30"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_type_filter(self):
        respx.get(f"{BASE}/stable/sec-filings-search/symbol").mock(
            return_value=httpx.Response(200, json=AAPL_SEC_FILINGS)
        )

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("sec_filings", {"symbol": "AAPL", "form_type": "8-K"})

        data = result.data
        assert data["count"] == 2
        assert all(f["form_type"] == "8-K" for f in data["filings"])
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_filings(self):
        respx.get(f"{BASE}/stable/sec-filings-search/symbol").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("sec_filings", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestIntradayPrices:
    @pytest.mark.asyncio
    @respx.mock
    async def test_adaptive_mode(self):
        """Default adaptive mode: tiered candles in TOON format."""
        respx.get(f"{BASE}/stable/historical-chart/5min").mock(return_value=httpx.Response(200, json=AAPL_INTRADAY_5M))
        respx.get(f"{BASE}/stable/historical-chart/15min").mock(return_value=httpx.Response(200, json=AAPL_INTRADAY_15M))
        respx.get(f"{BASE}/stable/historical-chart/1hour").mock(return_value=httpx.Response(200, json=AAPL_INTRADAY_1H))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["mode"] == "adaptive"
        assert data["candle_count"] > 0
        assert isinstance(data["candles"], str)
        assert "tier" in data["candles"]
        assert data["summary"]["total_volume"] > 0
        assert data["summary"]["vwap"] is not None
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_detail_mode(self):
        """detail=True: uniform candles in TOON format."""
        respx.get(f"{BASE}/stable/historical-chart/5min").mock(return_value=httpx.Response(200, json=AAPL_INTRADAY_5M))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "AAPL", "detail": True})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["mode"] == "detail"
        assert data["interval"] == "5m"
        assert data["candle_count"] == 3
        assert isinstance(data["candles"], str)
        # TOON format has column headers
        assert "{t,o,h,l,c,v}" in data["candles"]
        # No tier column in detail mode
        assert "tier" not in data["candles"]
        assert data["summary"]["total_volume"] == 1200000 + 1100000 + 1300000
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_detail_invalid_interval(self):
        """detail=True with invalid interval returns error."""
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "AAPL", "detail": True, "interval": "2m"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        """Returns error when no candle data available."""
        respx.get(f"{BASE}/stable/historical-chart/5min").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/historical-chart/15min").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/historical-chart/1hour").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_adaptive_est_timestamps(self):
        """Adaptive mode candles use EST 12h AM/PM timestamps."""
        respx.get(f"{BASE}/stable/historical-chart/5min").mock(return_value=httpx.Response(200, json=AAPL_INTRADAY_5M))
        respx.get(f"{BASE}/stable/historical-chart/15min").mock(return_value=httpx.Response(200, json=AAPL_INTRADAY_15M))
        respx.get(f"{BASE}/stable/historical-chart/1hour").mock(return_value=httpx.Response(200, json=AAPL_INTRADAY_1H))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "AAPL"})

        data = result.data
        # All fixture dates are from Feb 10-11 (in the past), so timestamps should include date
        # and use AM/PM format
        candles_str = data["candles"]
        assert "AM" in candles_str or "PM" in candles_str
        await fmp.aclose()


class TestTechnicalIndicators:
    @pytest.mark.asyncio
    @respx.mock
    async def test_rsi(self):
        respx.get(f"{BASE}/stable/technical-indicators/rsi").mock(
            return_value=httpx.Response(200, json=AAPL_RSI)
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("technical_indicators", {"symbol": "AAPL", "indicator": "rsi"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["indicator"] == "rsi"
        assert data["current_value"] == 58.32
        assert data["data_points"] == 3
        assert data["values"][0]["rsi"] == 58.32
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_indicator(self):
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("technical_indicators", {"symbol": "AAPL", "indicator": "bollinger"})

        data = result.data
        assert "error" in data
        assert "Invalid indicator" in data["error"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/technical-indicators/sma").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("technical_indicators", {"symbol": "ZZZZ", "indicator": "sma"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestFinancialHealth:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_health(self):
        respx.get(f"{BASE}/stable/financial-scores").mock(
            return_value=httpx.Response(200, json=AAPL_FINANCIAL_SCORES)
        )
        respx.get(f"{BASE}/stable/owner-earnings").mock(
            return_value=httpx.Response(200, json=AAPL_OWNER_EARNINGS)
        )

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("financial_health", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["scores"]["altman_z_score"] == 8.21
        assert data["scores"]["piotroski_score"] == 7
        assert data["owner_earnings"]["owner_earnings"] == 95432000000
        assert data["owner_earnings"]["maintenance_capex"] == -8500000000
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_data(self):
        respx.get(f"{BASE}/stable/financial-scores").mock(
            return_value=httpx.Response(200, json=AAPL_FINANCIAL_SCORES)
        )
        respx.get(f"{BASE}/stable/owner-earnings").mock(
            return_value=httpx.Response(500, text="error")
        )

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("financial_health", {"symbol": "AAPL"})

        data = result.data
        assert data["scores"]["altman_z_score"] == 8.21
        assert "owner_earnings" not in data
        assert "owner earnings unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/financial-scores").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/owner-earnings").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("financial_health", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestIPOCalendar:
    @pytest.mark.asyncio
    @respx.mock
    async def test_with_data(self):
        respx.get(f"{BASE}/stable/ipos-calendar").mock(
            return_value=httpx.Response(200, json=IPO_CALENDAR)
        )
        respx.get(f"{BASE}/stable/ipos-prospectus").mock(
            return_value=httpx.Response(200, json=IPO_PROSPECTUS)
        )
        respx.get(f"{BASE}/stable/ipos-disclosure").mock(
            return_value=httpx.Response(200, json=IPO_DISCLOSURES)
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("ipo_calendar", {})

        data = result.data
        assert data["count"] == 2
        # Sorted by date ascending
        assert data["ipos"][0]["symbol"] == "FRESH"
        assert data["ipos"][1]["company"] == "NewCo Technologies"
        assert data["ipos"][1]["price_range"] == "18.00 - 22.00"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty(self):
        respx.get(f"{BASE}/stable/ipos-calendar").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/ipos-prospectus").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/ipos-disclosure").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("ipo_calendar", {})

        data = result.data
        assert data["count"] == 0
        assert data["ipos"] == []
        await fmp.aclose()


class TestDividendsCalendar:
    @pytest.mark.asyncio
    @respx.mock
    async def test_browse_with_market_cap(self):
        """Browse mode filters by market cap, sorts by mcap descending, TOON-encodes."""
        respx.get(f"{BASE}/stable/dividends-calendar").mock(
            return_value=httpx.Response(200, json=DIVIDENDS_CALENDAR)
        )
        respx.get(f"{BASE}/stable/batch-quote").mock(
            return_value=httpx.Response(200, json=[
                {"symbol": "AAPL", "marketCap": 3_000_000_000_000},
                {"symbol": "MSFT", "marketCap": 2_800_000_000_000},
                {"symbol": "JNJ", "marketCap": 400_000_000_000},
            ])
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_calendar", {})

        data = result.data
        assert data["count"] == 3
        assert data["total_matching"] == 3
        assert data["country"] == "US"
        # dividends is TOON-encoded string
        assert isinstance(data["dividends"], str)
        # Sorted by market cap descending: AAPL first
        assert data["dividends"].index("AAPL") < data["dividends"].index("MSFT")
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_browse_no_market_cap_filter(self):
        """Browse mode with min_market_cap=0 skips batch-quote, TOON-encodes."""
        respx.get(f"{BASE}/stable/dividends-calendar").mock(
            return_value=httpx.Response(200, json=DIVIDENDS_CALENDAR)
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_calendar", {"min_market_cap": 0})

        data = result.data
        assert data["count"] == 3
        assert isinstance(data["dividends"], str)
        assert "AAPL" in data["dividends"]
        assert "0.26" in data["dividends"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_single_symbol(self):
        """Single-symbol mode returns full details without market cap filtering."""
        respx.get(f"{BASE}/stable/dividends-calendar").mock(
            return_value=httpx.Response(200, json=DIVIDENDS_CALENDAR)
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_calendar", {"symbol": "JNJ"})

        data = result.data
        assert data["count"] == 1
        assert data["symbol"] == "JNJ"
        assert data["dividends"][0]["dividend"] == 1.24
        assert data["dividends"][0]["record_date"] == "2026-02-19"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_min_yield_filter(self):
        """min_yield filter excludes low-yield dividends."""
        respx.get(f"{BASE}/stable/dividends-calendar").mock(
            return_value=httpx.Response(200, json=DIVIDENDS_CALENDAR)
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_calendar", {"min_yield": 3.0, "min_market_cap": 0})

        data = result.data
        assert data["count"] == 1
        assert isinstance(data["dividends"], str)
        assert "JNJ" in data["dividends"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_country_filter_global(self):
        """country=None returns all tickers including international."""
        intl_calendar = DIVIDENDS_CALENDAR + [
            {"symbol": "SAP.DE", "date": "2026-02-16", "dividend": 2.20, "adjDividend": 2.20,
             "recordDate": "2026-02-19", "paymentDate": "2026-02-28", "yield": 1.0, "frequency": "annual"},
        ]
        respx.get(f"{BASE}/stable/dividends-calendar").mock(
            return_value=httpx.Response(200, json=intl_calendar)
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_calendar", {"min_market_cap": 0, "country": ""})

        data = result.data
        assert data["count"] == 4
        assert "SAP.DE" in data["dividends"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_country_filter_us(self):
        """Default country=US excludes international tickers."""
        intl_calendar = DIVIDENDS_CALENDAR + [
            {"symbol": "SAP.DE", "date": "2026-02-16", "dividend": 2.20, "adjDividend": 2.20,
             "recordDate": "2026-02-19", "paymentDate": "2026-02-28", "yield": 1.0, "frequency": "annual"},
        ]
        respx.get(f"{BASE}/stable/dividends-calendar").mock(
            return_value=httpx.Response(200, json=intl_calendar)
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_calendar", {"min_market_cap": 0})

        data = result.data
        assert data["count"] == 3
        assert "SAP.DE" not in data["dividends"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty(self):
        respx.get(f"{BASE}/stable/dividends-calendar").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("dividends_calendar", {})

        data = result.data
        assert data["count"] == 0
        assert data["dividends"] == []
        await fmp.aclose()


class TestIndexConstituents:
    @pytest.mark.asyncio
    @respx.mock
    async def test_sp500(self):
        respx.get(f"{BASE}/stable/sp500-constituent").mock(
            return_value=httpx.Response(200, json=SP500_CONSTITUENTS)
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("index_constituents", {"index": "sp500"})

        data = result.data
        assert data["index"] == "sp500"
        assert data["count"] == 3
        # Sorted alphabetically
        assert data["constituents"][0]["symbol"] == "AAPL"
        assert data["constituents"][0]["sector"] == "Information Technology"
        assert "Information Technology" in data["sector_breakdown"]
        assert "Consumer Discretionary" in data["sector_breakdown"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_index(self):
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("index_constituents", {"index": "ftse100"})

        data = result.data
        assert "error" in data
        assert "Invalid index" in data["error"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty(self):
        respx.get(f"{BASE}/stable/nasdaq-constituent").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("index_constituents", {"index": "nasdaq"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestSectorValuation:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_valuation(self):
        respx.get(f"{BASE}/stable/sector-pe-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=SECTOR_PE_NYSE))
        respx.get(f"{BASE}/stable/sector-pe-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=SECTOR_PE_NASDAQ))
        respx.get(f"{BASE}/stable/industry-pe-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=INDUSTRY_PE_NYSE))
        respx.get(f"{BASE}/stable/industry-pe-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=INDUSTRY_PE_NASDAQ))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("sector_valuation", {})

        data = result.data
        assert len(data["sectors"]) == 3
        # Sectors sorted by PE ascending
        assert data["sectors"][0]["name"] == "Financial Services"
        assert data["sectors"][-1]["name"] == "Technology"
        # Industry lists
        assert len(data["top_10_cheapest"]) > 0
        assert len(data["top_10_most_expensive"]) > 0
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_data(self):
        respx.get(f"{BASE}/stable/sector-pe-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=SECTOR_PE_NYSE))
        respx.get(f"{BASE}/stable/sector-pe-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=SECTOR_PE_NASDAQ))
        respx.get(f"{BASE}/stable/industry-pe-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/industry-pe-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(500, text="error"))

        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("sector_valuation", {})

        data = result.data
        assert len(data["sectors"]) == 3
        assert "industry PE data unavailable" in data["_warnings"]
        await fmp.aclose()


class TestMNAActivity:
    @pytest.mark.asyncio
    @respx.mock
    async def test_latest(self):
        respx.get(f"{BASE}/stable/mergers-acquisitions-latest").mock(
            return_value=httpx.Response(200, json=MNA_LATEST)
        )

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("mna_activity", {})

        data = result.data
        assert data["symbol"] is None
        assert data["count"] == 2
        # Sorted by date descending
        assert data["deals"][0]["symbol"] == "TGT"
        assert data["deals"][0]["targeted_company"] == "SmallRetail Inc"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_with_symbol(self):
        respx.get(f"{BASE}/stable/mergers-acquisitions-search").mock(
            return_value=httpx.Response(200, json=MNA_SEARCH_AAPL)
        )

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("mna_activity", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["count"] == 1
        assert data["deals"][0]["targeted_company"] == "AI Labs Corp"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/mergers-acquisitions-latest").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_news)
        async with Client(mcp) as c:
            result = await c.call_tool("mna_activity", {})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestCommodityQuotes:
    @pytest.mark.asyncio
    @respx.mock
    async def test_single_quote(self):
        respx.get(f"{BASE}/stable/quote").mock(
            return_value=httpx.Response(200, json=GOLD_QUOTE)
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("commodity_quotes", {"symbol": "GCUSD"})

        data = result.data
        assert data["symbol"] == "GCUSD"
        assert data["mode"] == "single"
        assert data["price"] == 2045.30
        assert data["change"] == 12.50
        assert data["name"] == "Gold"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_single_quote_stale_warning(self):
        stale_ts = int((datetime.now(timezone.utc) - timedelta(minutes=20)).timestamp())
        stale_quote = [{**GOLD_QUOTE[0], "timestamp": stale_ts}]
        respx.get(f"{BASE}/stable/quote").mock(
            return_value=httpx.Response(200, json=stale_quote)
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("commodity_quotes", {"symbol": "GCUSD"})

        data = result.data
        assert data["quote_age_minutes"] >= 15
        assert any("stale" in w.lower() for w in data.get("_warnings", []))
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_batch(self):
        respx.get(f"{BASE}/stable/batch-commodity-quotes").mock(
            return_value=httpx.Response(200, json=BATCH_COMMODITIES)
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("commodity_quotes", {})

        data = result.data
        assert data["mode"] == "batch"
        assert data["asset_type"] == "commodity"
        assert data["count"] == 3
        # Sorted by absolute change descending
        assert data["quotes"][0]["symbol"] == "GCUSD"  # |12.50| > |1.23| > |0.45|
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/batch-commodity-quotes").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("commodity_quotes", {})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestCryptoQuotes:
    @pytest.mark.asyncio
    @respx.mock
    async def test_single_quote(self):
        respx.get(f"{BASE}/stable/quote").mock(
            return_value=httpx.Response(200, json=BTCUSD_QUOTE)
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("crypto_quotes", {"symbol": "BTCUSD"})

        data = result.data
        assert data["symbol"] == "BTCUSD"
        assert data["mode"] == "single"
        assert data["price"] == 97500.00
        assert data["name"] == "Bitcoin"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_batch(self):
        respx.get(f"{BASE}/stable/batch-crypto-quotes").mock(
            return_value=httpx.Response(200, json=BATCH_CRYPTO)
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("crypto_quotes", {})

        data = result.data
        assert data["mode"] == "batch"
        assert data["count"] == 3
        # Sorted by absolute change descending
        assert data["quotes"][0]["symbol"] == "BTCUSD"  # |1250| > |45| > |8.5|
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/batch-crypto-quotes").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("crypto_quotes", {})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestForexQuotes:
    @pytest.mark.asyncio
    @respx.mock
    async def test_single_quote(self):
        respx.get(f"{BASE}/stable/quote").mock(
            return_value=httpx.Response(200, json=EURUSD_QUOTE)
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("forex_quotes", {"symbol": "EURUSD"})

        data = result.data
        assert data["symbol"] == "EURUSD"
        assert data["mode"] == "single"
        assert data["price"] == 1.0842
        assert data["name"] == "EUR/USD"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_batch(self):
        respx.get(f"{BASE}/stable/batch-forex-quotes").mock(
            return_value=httpx.Response(200, json=BATCH_FOREX)
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("forex_quotes", {})

        data = result.data
        assert data["mode"] == "batch"
        assert data["count"] == 3
        # Sorted by absolute change descending
        assert data["quotes"][0]["symbol"] == "USDJPY"  # |0.35| > |0.0045| > |0.0023|
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/batch-forex-quotes").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_assets)
        async with Client(mcp) as c:
            result = await c.call_tool("forex_quotes", {})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestValuationHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_valuation_history(self):
        respx.get(f"{BASE}/stable/key-metrics").mock(
            return_value=httpx.Response(200, json=AAPL_KEY_METRICS_HISTORICAL)
        )
        respx.get(f"{BASE}/stable/ratios-ttm").mock(
            return_value=httpx.Response(200, json=AAPL_RATIOS)
        )

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("valuation_history", {"symbol": "AAPL", "period": "annual", "limit": 5})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["period_type"] == "annual"
        # Check current TTM values
        assert data["current_ttm"]["pe_ttm"] == 34.27
        assert data["current_ttm"]["ps_ttm"] == 9.23
        # Check historical series
        assert len(data["historical"]) == 5
        assert data["historical"][0]["date"] == "2025-09-27"
        assert data["historical"][0]["pe"] == 34.27
        # Check percentiles
        assert "pe" in data["percentiles"]
        assert "min" in data["percentiles"]["pe"]
        assert "max" in data["percentiles"]["pe"]
        assert "current_percentile" in data["percentiles"]["pe"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/key-metrics").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/ratios-ttm").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("valuation_history", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestRatioHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_ratio_history(self):
        respx.get(f"{BASE}/stable/ratios").mock(
            return_value=httpx.Response(200, json=AAPL_FINANCIAL_RATIOS_HISTORICAL)
        )
        respx.get(f"{BASE}/stable/key-metrics").mock(
            return_value=httpx.Response(200, json=AAPL_KEY_METRICS_HISTORICAL)
        )

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("ratio_history", {"symbol": "AAPL", "period": "annual", "limit": 5})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["period_type"] == "annual"
        # Check time series
        assert len(data["time_series"]) == 5
        assert data["time_series"][0]["date"] == "2025-09-27"
        assert data["time_series"][0]["roe"] == 1.56
        assert data["time_series"][0]["gross_margin"] == 0.469
        # Check trends
        assert "profitability" in data["trends"]
        assert "roe" in data["trends"]["profitability"]
        assert data["trends"]["profitability"]["roe"] in ["improving", "deteriorating", "stable", None]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_data(self):
        respx.get(f"{BASE}/stable/ratios").mock(
            return_value=httpx.Response(200, json=AAPL_FINANCIAL_RATIOS_HISTORICAL)
        )
        respx.get(f"{BASE}/stable/key-metrics").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("ratio_history", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert len(data["time_series"]) == 5
        assert "key metrics unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/ratios").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/key-metrics").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("ratio_history", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()
