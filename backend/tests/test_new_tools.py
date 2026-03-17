"""Tests for newly added tools (employee_history, delisted_companies, symbol_lookup, fund_holdings, ownership_structure, valuation_history, ratio_history, intraday_prices, historical_market_cap, etf_lookup profile mode, index_performance, market_hours, industry_performance, splits_calendar)."""

from __future__ import annotations

from datetime import date

import pytest
import respx
import httpx

from fastmcp import FastMCP, Client
from fmp_data import AsyncFMPDataClient
from tests.conftest import (
    build_test_client,
    AAPL_EMPLOYEE_COUNT, DELISTED_COMPANIES, CIK_SEARCH_RESULTS,
    VANGUARD_HOLDINGS, VANGUARD_PERFORMANCE, VANGUARD_INDUSTRY_BREAKDOWN,
    AAPL_SHARES_FLOAT, AAPL_INSIDER_STATS,
    AAPL_INSTITUTIONAL_SUMMARY, AAPL_SHORT_INTEREST,
    AAPL_KEY_METRICS_HISTORICAL, AAPL_RATIOS,
    AAPL_FINANCIAL_RATIOS_HISTORICAL, AAPL_KEY_METRICS,
    AAPL_INTRADAY_5M, AAPL_HISTORICAL_MARKET_CAP,
    QQQ_INFO, QQQ_HOLDINGS, QQQ_SECTOR_WEIGHTING, QQQ_COUNTRY_ALLOCATION,
    INDEX_QUOTES, INDEX_HISTORICAL,
    MARKET_HOURS_DATA, MARKET_HOLIDAYS,
    INDUSTRY_PERFORMANCE_NYSE, INDUSTRY_PERFORMANCE_NASDAQ,
    INDUSTRY_PE_NYSE, INDUSTRY_PE_NASDAQ,
    SPLITS_CALENDAR, IPO_CALENDAR, IPO_PROSPECTUS, IPO_DISCLOSURES,
)
from cassandra_fmp.tools.ownership import FINRA_URL, _short_interest_dates
from cassandra_fmp.tools.overview import register as register_overview
from cassandra_fmp.tools.ownership import register as register_ownership
from cassandra_fmp.tools.valuation import register as register_valuation
from cassandra_fmp.tools.financials import register as register_financials
from cassandra_fmp.tools.market import register as register_market
from cassandra_fmp.tools.macro import register as register_macro

BASE = "https://financialmodelingprep.com"


def _make_server(register_fn) -> tuple[FastMCP, AsyncFMPDataClient]:
    """Create a FastMCP server with registered tools."""
    mcp = FastMCP("Test")
    client = build_test_client("test_key")
    register_fn(mcp, client)
    return mcp, client


# --- OVERVIEW TOOLS ---

class TestEmployeeHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_data(self):
        respx.get(f"{BASE}/stable/employee-count").mock(
            return_value=httpx.Response(200, json=AAPL_EMPLOYEE_COUNT)
        )
        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("employee_history", {"symbol": "AAPL"})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["current_employee_count"] == 164000
        assert len(data["history"]) == 4
        # Check growth metrics are present
        assert "growth_metrics" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/employee-count").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("employee_history", {"symbol": "ZZZZ"})
        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestDelistedCompanies:
    @pytest.mark.asyncio
    @respx.mock
    async def test_search_delisted(self):
        respx.get(f"{BASE}/stable/delisted-companies").mock(
            return_value=httpx.Response(200, json=DELISTED_COMPANIES)
        )
        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("delisted_companies", {"query": "oldco"})
        data = result.data
        assert data["count"] == 2
        assert data["companies"][0]["symbol"] == "OLDCO"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_results(self):
        respx.get(f"{BASE}/stable/delisted-companies").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("delisted_companies", {})
        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestSymbolLookup:
    @pytest.mark.asyncio
    @respx.mock
    async def test_cik_search(self):
        respx.get(f"{BASE}/stable/search-cik").mock(
            return_value=httpx.Response(200, json=CIK_SEARCH_RESULTS)
        )
        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("symbol_lookup", {"query": "0000320193", "type": "cik"})
        data = result.data
        assert data["count"] == 1
        assert data["results"][0]["symbol"] == "AAPL"
        assert data["results"][0]["cik"] == "0000320193"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_results(self):
        respx.get(f"{BASE}/stable/search-name").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/search-symbol").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_overview)
        async with Client(mcp) as c:
            result = await c.call_tool("symbol_lookup", {"query": "NONEXISTENT", "type": "name"})
        data = result.data
        assert "error" in data
        await fmp.aclose()


# --- OWNERSHIP TOOLS ---

class TestFundHoldings:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_portfolio(self):
        respx.get(f"{BASE}/stable/institutional-ownership/extract").mock(
            return_value=httpx.Response(200, json=VANGUARD_HOLDINGS)
        )
        respx.get(f"{BASE}/stable/institutional-ownership/holder-performance-summary").mock(
            return_value=httpx.Response(200, json=VANGUARD_PERFORMANCE)
        )
        respx.get(f"{BASE}/stable/institutional-ownership/holder-industry-breakdown").mock(
            return_value=httpx.Response(200, json=VANGUARD_INDUSTRY_BREAKDOWN)
        )
        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("fund_holdings", {"cik": "0001166559"})
        data = result.data
        assert data["cik"] == "0001166559"
        assert len(data["top_holdings"]) == 3
        assert data["top_holdings"][0]["symbol"] == "AAPL"
        assert "performance" in data
        assert "industry_allocation" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_cik(self):
        respx.get(f"{BASE}/stable/institutional-ownership/extract").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/institutional-ownership/holder-performance-summary").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/institutional-ownership/holder-industry-breakdown").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("fund_holdings", {"cik": "0000000000"})
        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_defaults_to_latest_available_quarter(self):
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

        def extract_side_effect(request: httpx.Request) -> httpx.Response:
            year = int(request.url.params.get("year", 0))
            quarter = int(request.url.params.get("quarter", 0))
            if (year, quarter) == (latest_year, latest_quarter):
                return httpx.Response(200, json=[])
            if (year, quarter) == (prev_year, prev_quarter):
                return httpx.Response(200, json=VANGUARD_HOLDINGS)
            return httpx.Response(200, json=[])

        def industry_side_effect(request: httpx.Request) -> httpx.Response:
            year = int(request.url.params.get("year", 0))
            quarter = int(request.url.params.get("quarter", 0))
            if (year, quarter) == (latest_year, latest_quarter):
                return httpx.Response(200, json=[])
            if (year, quarter) == (prev_year, prev_quarter):
                return httpx.Response(200, json=VANGUARD_INDUSTRY_BREAKDOWN)
            return httpx.Response(200, json=[])

        respx.get(f"{BASE}/stable/institutional-ownership/extract").mock(side_effect=extract_side_effect)
        respx.get(f"{BASE}/stable/institutional-ownership/holder-performance-summary").mock(
            return_value=httpx.Response(200, json=VANGUARD_PERFORMANCE)
        )
        respx.get(f"{BASE}/stable/institutional-ownership/holder-industry-breakdown").mock(side_effect=industry_side_effect)

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("fund_holdings", {"cik": "0001166559"})
        data = result.data
        assert data["reporting_period"] == f"Q{prev_quarter} {prev_year}"
        assert len(data["top_holdings"]) > 0
        await fmp.aclose()


class TestOwnershipStructure:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_ownership(self):
        respx.get(f"{BASE}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT)
        )
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(
            return_value=httpx.Response(200, json=AAPL_INSIDER_STATS)
        )
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(
            return_value=httpx.Response(200, json=AAPL_INSTITUTIONAL_SUMMARY)
        )
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(
            return_value=httpx.Response(200, json=[])
        )
        # Mock FINRA short interest (external API) - match any POST to FINRA_URL
        respx.post(FINRA_URL).mock(
            return_value=httpx.Response(200, json=AAPL_SHORT_INTEREST)
        )

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("ownership_structure", {"symbol": "AAPL"})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert "shares_breakdown" in data
        assert "ownership_percentages" in data
        assert data["shares_breakdown"]["outstanding_shares"] == 15200000000
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_float_data(self):
        respx.get(f"{BASE}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.post(FINRA_URL).mock(
            return_value=httpx.Response(204, content=b"")
        )

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("ownership_structure", {"symbol": "ZZZZ"})
        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_uses_latest_available_institutional_period(self):
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

        respx.get(f"{BASE}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT)
        )
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(
            return_value=httpx.Response(200, json=AAPL_INSIDER_STATS)
        )
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(side_effect=summary_side_effect)
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.post(FINRA_URL).mock(
            return_value=httpx.Response(200, json=AAPL_SHORT_INTEREST)
        )

        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("ownership_structure", {"symbol": "AAPL"})
        data = result.data
        assert data["reporting_period"] == f"Q{prev_quarter} {prev_year}"
        assert data["ownership_percentages"]["institutional_pct"] > 0
        await fmp.aclose()


class TestSenateTrading:
    @pytest.mark.asyncio
    @respx.mock
    async def test_latest_mode(self):
        senate_rows = [
            {
                "symbol": "LMT",
                "disclosureDate": "2026-02-10",
                "transactionDate": "2026-02-05",
                "representative": "Sen. Jane Doe",
                "office": "Senate",
                "assetDescription": "Lockheed Martin",
                "assetType": "Stock",
                "type": "Purchase",
                "amount": "$15,001 - $50,000",
                "owner": "Self",
            }
        ]
        respx.get(f"{BASE}/stable/senate-latest").mock(
            return_value=httpx.Response(200, json=senate_rows)
        )
        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("senate_trading", {"limit": 10})
        data = result.data
        assert data["mode"] == "latest"
        assert data["count"] == 1
        assert data["trades"][0]["symbol"] == "LMT"
        await fmp.aclose()


class TestFundSearch:
    @pytest.mark.asyncio
    @respx.mock
    async def test_fund_search_maps_name_to_cik(self):
        disclosure_rows = [
            {
                "symbol": "DXYZ",
                "cik": "0001166559",
                "entityName": "Destiny XYZ Fund",
                "seriesName": "Destiny Series",
                "className": "Class A",
                "seriesId": "S0001",
                "classId": "C0001",
            }
        ]
        mutual_rows = [
            {
                "symbol": "DXYZ",
                "holder": "Destiny XYZ Fund",
                "cik": "0001166559",
                "seriesName": "Destiny Series",
                "className": "Class A",
            }
        ]
        respx.get(f"{BASE}/stable/funds/disclosure-holders-search").mock(
            return_value=httpx.Response(200, json=disclosure_rows)
        )
        respx.get(f"{BASE}/stable/mutual-fund-holdings/name").mock(
            return_value=httpx.Response(200, json=mutual_rows)
        )
        mcp, fmp = _make_server(register_ownership)
        async with Client(mcp) as c:
            result = await c.call_tool("fund_search", {"query": "Destiny"})
        data = result.data
        assert data["query"] == "Destiny"
        assert data["count"] >= 1
        assert data["results"][0]["cik"] == "0001166559"
        await fmp.aclose()


# --- VALUATION TOOLS ---

class TestValuationHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_history(self):
        respx.get(f"{BASE}/stable/key-metrics").mock(
            return_value=httpx.Response(200, json=AAPL_KEY_METRICS_HISTORICAL)
        )
        respx.get(f"{BASE}/stable/ratios-ttm").mock(
            return_value=httpx.Response(200, json=AAPL_RATIOS)
        )
        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("valuation_history", {"symbol": "AAPL"})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert "current_ttm" in data
        assert "historical" in data
        assert "percentiles" in data
        assert "pe" in data["percentiles"]
        await fmp.aclose()


class TestDiscountedCashFlow:
    @pytest.mark.asyncio
    @respx.mock
    async def test_dcf_both_modes(self):
        dcf_rows = [{"date": "2026-02-10", "dcf": 210.0, "stockPrice": 190.0}]
        levered_rows = [{"date": "2026-02-10", "leveredDCF": 205.0, "stockPrice": 190.0}]
        respx.get(f"{BASE}/stable/discounted-cash-flow").mock(
            return_value=httpx.Response(200, json=dcf_rows)
        )
        respx.get(f"{BASE}/stable/levered-discounted-cash-flow").mock(
            return_value=httpx.Response(200, json=levered_rows)
        )
        mcp, fmp = _make_server(register_valuation)
        async with Client(mcp) as c:
            result = await c.call_tool("discounted_cash_flow", {"symbol": "AAPL", "mode": "both"})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["mode"] == "both"
        assert data["unlevered"][0]["dcf_value"] == 210.0
        assert data["levered"][0]["dcf_value"] == 205.0
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


# --- FINANCIALS TOOLS ---

class TestRatioHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_ratios(self):
        respx.get(f"{BASE}/stable/ratios").mock(
            return_value=httpx.Response(200, json=AAPL_FINANCIAL_RATIOS_HISTORICAL)
        )
        respx.get(f"{BASE}/stable/key-metrics").mock(
            return_value=httpx.Response(200, json=AAPL_KEY_METRICS_HISTORICAL)
        )
        mcp, fmp = _make_server(register_financials)
        async with Client(mcp) as c:
            result = await c.call_tool("ratio_history", {"symbol": "AAPL"})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert "time_series" in data
        assert "trends" in data
        assert len(data["time_series"]) == 5
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


# --- MARKET TOOLS ---

class TestIntradayPrices:
    @pytest.mark.asyncio
    @respx.mock
    async def test_detail_mode_with_extended_hours(self):
        """detail=True returns uniform candles with extended hours."""
        respx.get(f"{BASE}/stable/historical-chart/5min").mock(
            return_value=httpx.Response(200, json=AAPL_INTRADAY_5M)
        )
        respx.get(f"{BASE}/stable/pre-post-market").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(
            return_value=httpx.Response(200, json=[{"symbol": "AAPL", "price": 176.50, "tradeSize": 10, "timestamp": 1700000000000}])
        )
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "AAPL", "detail": True, "interval": "5m"})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["mode"] == "detail"
        assert data["interval"] == "5m"
        assert data["candle_count"] == 3
        assert isinstance(data["candles"], str)
        assert "summary" in data
        assert "vwap" in data["summary"]
        # After-hours data should be present
        assert "extended_hours" in data
        assert data["extended_hours"]["afterhours"]["price"] == 176.50
        assert "change_pct" in data["extended_hours"]["afterhours"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_adaptive_no_extended_hours(self):
        """Default adaptive mode: when extended-hours endpoints return empty, key omitted."""
        respx.get(f"{BASE}/stable/historical-chart/5min").mock(
            return_value=httpx.Response(200, json=AAPL_INTRADAY_5M)
        )
        respx.get(f"{BASE}/stable/historical-chart/15min").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/historical-chart/1hour").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/pre-post-market").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "AAPL"})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["mode"] == "adaptive"
        assert "extended_hours" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        """Adaptive mode with no data returns error."""
        respx.get(f"{BASE}/stable/historical-chart/5min").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/historical-chart/15min").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/historical-chart/1hour").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/pre-post-market").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("intraday_prices", {"symbol": "ZZZZ"})
        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestHistoricalMarketCap:
    @pytest.mark.asyncio
    @respx.mock
    async def test_market_cap_history(self):
        respx.get(f"{BASE}/stable/historical-market-capitalization").mock(
            return_value=httpx.Response(200, json=AAPL_HISTORICAL_MARKET_CAP)
        )
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("historical_market_cap", {"symbol": "AAPL", "limit": 5})
        data = result.data
        assert data["symbol"] == "AAPL"
        assert len(data["history"]) == 5
        assert data["current_market_cap"] == 4022528102504
        assert "change_pct" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/historical-market-capitalization").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("historical_market_cap", {"symbol": "ZZZZ"})
        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestETFLookupProfile:
    @pytest.mark.asyncio
    @respx.mock
    async def test_profile_mode(self):
        respx.get(f"{BASE}/stable/etf/info").mock(
            return_value=httpx.Response(200, json=QQQ_INFO)
        )
        respx.get(f"{BASE}/stable/etf/holdings").mock(
            return_value=httpx.Response(200, json=QQQ_HOLDINGS)
        )
        respx.get(f"{BASE}/stable/etf/sector-weightings").mock(
            return_value=httpx.Response(200, json=QQQ_SECTOR_WEIGHTING)
        )
        respx.get(f"{BASE}/stable/etf/country-weightings").mock(
            return_value=httpx.Response(200, json=QQQ_COUNTRY_ALLOCATION)
        )
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "QQQ", "mode": "profile"})
        data = result.data
        assert data["symbol"] == "QQQ"
        assert data["mode"] == "profile"
        assert "info" in data
        assert data["info"]["name"] == "Invesco QQQ Trust"
        assert "top_holdings" in data
        assert "sector_weights" in data
        assert "country_allocation" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_etf_data(self):
        respx.get(f"{BASE}/stable/etf/info").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/etf/holdings").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/etf/sector-weightings").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/etf/country-weightings").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_market)
        async with Client(mcp) as c:
            result = await c.call_tool("etf_lookup", {"symbol": "NOTANETF", "mode": "profile"})
        data = result.data
        assert "error" in data
        await fmp.aclose()


# --- MACRO TOOLS ---

class TestIndexPerformance:
    @pytest.mark.asyncio
    @respx.mock
    async def test_index_quotes(self):
        respx.get(f"{BASE}/stable/batch-quote").mock(
            return_value=httpx.Response(200, json=INDEX_QUOTES)
        )
        # Mock historical data for each index
        for idx in ["^GSPC", "^DJI", "^IXIC", "^RUT"]:
            respx.get(f"{BASE}/stable/historical-price-eod/full").mock(
                return_value=httpx.Response(200, json=INDEX_HISTORICAL)
            )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("index_performance", {})
        data = result.data
        assert "indices" in data
        assert data["count"] == 4
        assert data["indices"][0]["symbol"] == "^GSPC"
        assert "performance" in data["indices"][0]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_quotes(self):
        respx.get(f"{BASE}/stable/batch-quote").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("index_performance", {})
        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestMarketHours:
    @pytest.mark.asyncio
    @respx.mock
    async def test_market_hours_data(self):
        respx.get(f"{BASE}/stable/exchange-market-hours").mock(
            return_value=httpx.Response(200, json=MARKET_HOURS_DATA)
        )
        respx.get(f"{BASE}/stable/holidays-by-exchange").mock(
            return_value=httpx.Response(200, json=MARKET_HOLIDAYS)
        )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("market_hours", {"exchange": "NYSE"})
        data = result.data
        assert data["exchange"] == "NYSE"
        assert "is_open" in data
        assert "regular_hours" in data
        assert "upcoming_holidays" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/exchange-market-hours").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/holidays-by-exchange").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("market_hours", {"exchange": "NYSE"})
        data = result.data
        assert "_warnings" in data
        await fmp.aclose()


class TestIndustryPerformance:
    @pytest.mark.asyncio
    @respx.mock
    async def test_industry_data(self):
        respx.get(f"{BASE}/stable/industry-performance-snapshot").mock(
            side_effect=[
                httpx.Response(200, json=INDUSTRY_PERFORMANCE_NYSE),
                httpx.Response(200, json=INDUSTRY_PERFORMANCE_NASDAQ),
            ]
        )
        respx.get(f"{BASE}/stable/industry-pe-snapshot").mock(
            side_effect=[
                httpx.Response(200, json=INDUSTRY_PE_NYSE),
                httpx.Response(200, json=INDUSTRY_PE_NASDAQ),
            ]
        )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("industry_performance", {})
        data = result.data
        assert "industries" in data
        assert len(data["industries"]) >= 2
        assert "change_pct" in data["industries"][0]
        assert "median_pe" in data["industries"][0]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        respx.get(f"{BASE}/stable/industry-performance-snapshot").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/industry-pe-snapshot").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("industry_performance", {})
        data = result.data
        assert "error" in data
        await fmp.aclose()


class TestSplitsCalendar:
    @pytest.mark.asyncio
    @respx.mock
    async def test_upcoming_splits(self):
        respx.get(f"{BASE}/stable/splits-calendar").mock(
            return_value=httpx.Response(200, json=SPLITS_CALENDAR)
        )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("splits_calendar", {"days_ahead": 30})
        data = result.data
        assert data["count"] == 2
        assert data["splits"][0]["symbol"] == "NVDA"
        assert data["splits"][0]["label"] == "10:1"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_splits(self):
        respx.get(f"{BASE}/stable/splits-calendar").mock(
            return_value=httpx.Response(200, json=[])
        )
        mcp, fmp = _make_server(register_macro)
        async with Client(mcp) as c:
            result = await c.call_tool("splits_calendar", {})
        data = result.data
        assert data["count"] == 0
        await fmp.aclose()


class TestEnhancedIPOCalendar:
    @pytest.mark.asyncio
    @respx.mock
    async def test_ipos_with_docs(self):
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
            result = await c.call_tool("ipo_calendar", {"days_ahead": 14})
        data = result.data
        assert data["count"] == 2
        # Find the IPO with docs (NEWCO)
        ipo_with_docs = [ipo for ipo in data["ipos"] if ipo["symbol"] == "NEWCO"][0]
        assert "prospectus" in ipo_with_docs
        assert "disclosures" in ipo_with_docs
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_ipos(self):
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
        await fmp.aclose()
