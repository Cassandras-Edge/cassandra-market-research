"""Tests for workflow tools using respx mocking."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
import respx
import httpx

from fastmcp import FastMCP, Client
from fmp_data import AsyncFMPDataClient
from tests.conftest import build_test_client
from tests.conftest import (
    AAPL_PROFILE, AAPL_QUOTE, AAPL_RATIOS,
    AAPL_INCOME, AAPL_BALANCE, AAPL_CASHFLOW,
    AAPL_PRICE_TARGET, AAPL_GRADES, AAPL_HISTORICAL,
    AAPL_ANALYST_ESTIMATES, AAPL_QUARTERLY_INCOME,
    AAPL_INSIDER_TRADES, AAPL_INSIDER_STATS, AAPL_SHARES_FLOAT,
    AAPL_INSTITUTIONAL_SUMMARY, AAPL_INSTITUTIONAL_HOLDERS,
    AAPL_SHORT_INTEREST,
    AAPL_NEWS, AAPL_KEY_METRICS,
    AAPL_PEERS, AAPL_EARNINGS, AAPL_GRADES_DETAIL,
    AAPL_TRANSCRIPT_DATES, AAPL_TRANSCRIPT,
    TREASURY_RATES, MARKET_RISK_PREMIUM, ECONOMIC_CALENDAR,
    SECTOR_PERFORMANCE_NYSE, SECTOR_PERFORMANCE_NASDAQ,
    BIGGEST_GAINERS, BIGGEST_LOSERS, MOST_ACTIVES, MOVERS_BATCH_QUOTE,
    MSFT_RATIOS, MSFT_KEY_METRICS,
    GOOGL_RATIOS, GOOGL_KEY_METRICS,
    AMZN_RATIOS, AMZN_KEY_METRICS,
    INDUSTRY_PE_NYSE, INDUSTRY_PE_NASDAQ,
)
from cassandra_fmp.tools.workflows import register as register_workflows

BASE = "https://financialmodelingprep.com"


def _make_server() -> tuple[FastMCP, AsyncFMPDataClient]:
    mcp = FastMCP("Test")
    client = build_test_client("test_key")
    register_workflows(mcp, client)
    return mcp, client


# ================================================================
# stock_brief
# ================================================================


class TestStockBrief:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_brief(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=AAPL_GRADES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=AAPL_NEWS))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[{"symbol": "AAPL", "price": 275.00, "tradeSize": 5, "timestamp": 1700000000000}]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("stock_brief", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["company_name"] == "Apple Inc."
        assert data["price"]["current"] == 273.68
        assert data["valuation"]["pe"] == 34.27
        assert data["analyst"]["consensus"] == "Buy"
        assert data["analyst"]["target"] == 303.11
        assert data["analyst"]["upside_pct"] is not None
        assert isinstance(data["news"], list)
        assert len(data["news"]) <= 5
        assert data["quick_take"]["signal"] in ("bullish", "neutral", "bearish")
        # Extended hours data should be present in price section
        assert "extended_hours" in data["price"]
        assert data["price"]["extended_hours"]["afterhours"]["price"] == 275.00
        assert "change_pct" in data["price"]["extended_hours"]["afterhours"]
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_brief(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("stock_brief", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["company_name"] == "Apple Inc."
        assert "ratios unavailable" in data["_warnings"]
        assert "historical prices unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("stock_brief", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_filters_cross_symbol_news(self):
        mixed_news = [
            *AAPL_NEWS,
            {
                "symbol": "MSFT",
                "title": "Microsoft unrelated headline",
                "publishedDate": "2026-02-09T12:00:00.000Z",
                "site": "Example",
                "url": "https://example.com/msft",
                "text": "MSFT article",
            },
        ]
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=AAPL_GRADES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=mixed_news))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("stock_brief", {"symbol": "AAPL"})

        data = result.data
        assert all((item.get("title") or "").find("Microsoft") == -1 for item in data["news"])
        assert any("cross-symbol news item" in w for w in data.get("_warnings", []))
        await fmp.aclose()


# ================================================================
# market_context
# ================================================================


class TestMarketContext:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_context(self):
        respx.get(f"{BASE}/stable/treasury-rates").mock(return_value=httpx.Response(200, json=TREASURY_RATES))
        respx.get(f"{BASE}/stable/market-risk-premium").mock(return_value=httpx.Response(200, json=MARKET_RISK_PREMIUM))
        respx.get(f"{BASE}/stable/economic-calendar").mock(return_value=httpx.Response(200, json=ECONOMIC_CALENDAR))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NYSE))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NASDAQ))
        respx.get(f"{BASE}/stable/biggest-gainers").mock(return_value=httpx.Response(200, json=BIGGEST_GAINERS))
        respx.get(f"{BASE}/stable/biggest-losers").mock(return_value=httpx.Response(200, json=BIGGEST_LOSERS))
        respx.get(f"{BASE}/stable/most-actives").mock(return_value=httpx.Response(200, json=MOST_ACTIVES))
        respx.get(f"{BASE}/stable/batch-quote").mock(return_value=httpx.Response(200, json=MOVERS_BATCH_QUOTE))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("market_context", {})

        data = result.data
        assert data["rates"]["10y"] == 4.05
        assert data["rates"]["2y"] == 3.82
        assert data["rates"]["spread_bps"] == 23
        assert data["rates"]["inverted"] is False
        assert data["rates"]["erp"] == 4.60
        assert data["rates"]["cost_of_equity"] == 8.65
        assert len(data["rotation"]["leaders"]) > 0
        assert data["rotation"]["signal"] in ("risk_on", "risk_off", "mixed")
        assert data["breadth"]["signal"] in ("bullish", "bearish", "neutral")
        assert data["environment"]["regime"] in ("risk_on", "risk_off", "neutral")
        # TINY should be filtered out from movers
        gainer_symbols = [g["symbol"] for g in data["movers"]["gainers"]]
        assert "TINY" not in gainer_symbols
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_context(self):
        respx.get(f"{BASE}/stable/treasury-rates").mock(return_value=httpx.Response(200, json=TREASURY_RATES))
        respx.get(f"{BASE}/stable/market-risk-premium").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/economic-calendar").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NYSE))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NASDAQ))
        respx.get(f"{BASE}/stable/biggest-gainers").mock(return_value=httpx.Response(200, json=BIGGEST_GAINERS))
        respx.get(f"{BASE}/stable/biggest-losers").mock(return_value=httpx.Response(200, json=BIGGEST_LOSERS))
        respx.get(f"{BASE}/stable/most-actives").mock(return_value=httpx.Response(200, json=MOST_ACTIVES))
        respx.get(f"{BASE}/stable/batch-quote").mock(return_value=httpx.Response(200, json=MOVERS_BATCH_QUOTE))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("market_context", {})

        data = result.data
        assert data["rates"]["10y"] == 4.05
        assert data["rates"]["erp"] is None
        assert "risk premium unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_context(self):
        respx.get(f"{BASE}/stable/treasury-rates").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/market-risk-premium").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/economic-calendar").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NYSE"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NASDAQ"}).mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/biggest-gainers").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/biggest-losers").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/most-actives").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/batch-quote").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("market_context", {})

        data = result.data
        assert "error" in data
        await fmp.aclose()


# ================================================================
# earnings_setup
# ================================================================


class TestEarningsSetup:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_setup(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_STATS))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_setup", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["company_name"] == "Apple Inc."
        assert data["current_price"] == 273.68
        # Next earnings date should exist
        assert data["earnings_date"] is not None
        assert data["days_until_earnings"] is not None
        # Consensus
        assert data["consensus"]["eps"] is not None
        # Surprise history
        assert data["surprise_history"]["beat_rate"] is not None
        assert data["surprise_history"]["beat_rate"] > 0
        assert len(data["surprise_history"]["last_quarters"]) > 0
        # Analyst momentum
        assert data["analyst_momentum"]["signal"] in ("positive", "negative", "neutral")
        # Setup summary
        assert data["setup_summary"]["signal"] in ("bullish", "neutral", "bearish")
        assert isinstance(data["setup_summary"]["key_factors"], list)
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_setup(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(500, text="error"))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_setup", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["surprise_history"]["beat_rate"] is not None  # earnings data still works
        assert "analyst grades unavailable" in data["_warnings"]
        assert "historical prices unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_setup", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


# ================================================================
# earnings_preview
# ================================================================


class TestEarningsPreview:
    @pytest.mark.asyncio
    @respx.mock
    async def test_earnings_preview_full(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_STATS))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=AAPL_GRADES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=AAPL_NEWS))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_preview", {"symbol": "AAPL", "days_ahead": 400})

        data = result.data
        assert data["ticker"] == "AAPL"
        assert data["company_name"] == "Apple Inc."
        assert data["setup_signal"] in ("BULLISH", "NEUTRAL", "BEARISH")
        assert isinstance(data["composite_score"], float)
        assert set(data["signals"]) == {"beat_history", "price_setup", "analyst", "insider"}
        assert isinstance(data["in_window"], bool)
        assert isinstance(data["key_questions"], list)
        assert isinstance(data["bull_triggers"], list)
        assert isinstance(data["bear_triggers"], list)
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_earnings_preview_accepts_symbol_param(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_STATS))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=AAPL_GRADES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=AAPL_NEWS))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_preview", {"symbol": "AAPL", "days_ahead": 400})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["ticker"] == "AAPL"  # backward compatibility
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_earnings_preview_not_in_window(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_STATS))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=AAPL_GRADES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=AAPL_NEWS))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_preview", {"symbol": "AAPL", "days_ahead": 1})

        data = result.data
        assert data["ticker"] == "AAPL"
        assert data["in_window"] is False
        assert "outside requested horizon" in " ".join(data.get("_warnings", []))
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_earnings_preview_unknown_symbol(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_preview", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_earnings_preview_partial_data_degrades_gracefully(self):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_preview", {"symbol": "AAPL"})

        data = result.data
        assert "error" not in data
        assert data["signals"]["price_setup"] == 0.0
        assert data["signals"]["analyst"] == 0.0
        assert data["signals"]["insider"] == 0.0
        assert isinstance(data.get("_warnings"), list)
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_earnings_preview_threshold_classification(self, monkeypatch):
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_STATS))
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/grades-consensus").mock(return_value=httpx.Response(200, json=AAPL_GRADES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/news/stock").mock(return_value=httpx.Response(200, json=AAPL_NEWS))
        respx.get(f"{BASE}/stable/pre-post-market").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/aftermarket-trade").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            monkeypatch.setattr("tools.workflows._score_beat_rate", lambda *_: 1.0)
            monkeypatch.setattr("tools.workflows._score_price_setup", lambda *_: 0.0)
            monkeypatch.setattr("tools.workflows._score_analyst", lambda *_: 0.0)
            monkeypatch.setattr("tools.workflows._score_insider", lambda *_: 0.0)
            bull = await c.call_tool("earnings_preview", {"symbol": "AAPL"})

            monkeypatch.setattr("tools.workflows._score_beat_rate", lambda *_: -1.0)
            bear = await c.call_tool("earnings_preview", {"symbol": "AAPL"})

            monkeypatch.setattr("tools.workflows._score_beat_rate", lambda *_: 0.2)
            neutral = await c.call_tool("earnings_preview", {"symbol": "AAPL"})

        assert bull.data["setup_signal"] == "BULLISH"
        assert bear.data["setup_signal"] == "BEARISH"
        assert neutral.data["setup_signal"] == "NEUTRAL"
        await fmp.aclose()


# ================================================================
# fair_value_estimate
# ================================================================


class TestFairValueEstimate:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_valuation(self):
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=AAPL_INCOME))
        respx.get(f"{BASE}/stable/balance-sheet-statement").mock(return_value=httpx.Response(200, json=AAPL_BALANCE))
        respx.get(f"{BASE}/stable/cash-flow-statement").mock(return_value=httpx.Response(200, json=AAPL_CASHFLOW))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "AAPL"}).mock(return_value=httpx.Response(200, json=AAPL_KEY_METRICS))
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "AAPL"}).mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/analyst-estimates").mock(return_value=httpx.Response(200, json=AAPL_ANALYST_ESTIMATES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/stock-peers").mock(return_value=httpx.Response(200, json=AAPL_PEERS))
        # Peer data
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "MSFT"}).mock(return_value=httpx.Response(200, json=MSFT_RATIOS))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "MSFT"}).mock(return_value=httpx.Response(200, json=MSFT_KEY_METRICS))
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "GOOGL"}).mock(return_value=httpx.Response(200, json=GOOGL_RATIOS))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "GOOGL"}).mock(return_value=httpx.Response(200, json=GOOGL_KEY_METRICS))
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "AMZN"}).mock(return_value=httpx.Response(200, json=AMZN_RATIOS))
        respx.get(f"{BASE}/stable/key-metrics-ttm", params__contains={"symbol": "AMZN"}).mock(return_value=httpx.Response(200, json=AMZN_KEY_METRICS))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("fair_value_estimate", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["current_price"] == 273.68
        # Fundamentals
        assert data["fundamentals"]["market_cap"] is not None
        assert data["fundamentals"]["ttm_revenue"] is not None
        assert data["fundamentals"]["ttm_fcf"] is not None
        # Multiples
        assert data["multiples"]["current"]["pe"] == 34.27
        assert data["multiples"]["peer_median"]["pe"] is not None
        assert data["multiples"]["premium_pct"]["pe"] is not None
        # Fair value methods
        fv = data["fair_value"]
        assert fv["analyst_target"] == 303.11
        assert fv["blended"] is not None
        assert fv["upside_pct"] is not None
        # Summary
        assert data["summary"]["rating"] in ("undervalued", "overvalued", "fairly_valued", "insufficient_data")
        assert data["summary"]["confidence"] in ("high", "medium", "low")
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_peers(self):
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=AAPL_INCOME))
        respx.get(f"{BASE}/stable/balance-sheet-statement").mock(return_value=httpx.Response(200, json=AAPL_BALANCE))
        respx.get(f"{BASE}/stable/cash-flow-statement").mock(return_value=httpx.Response(200, json=AAPL_CASHFLOW))
        respx.get(f"{BASE}/stable/key-metrics-ttm").mock(return_value=httpx.Response(200, json=AAPL_KEY_METRICS))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=AAPL_RATIOS))
        respx.get(f"{BASE}/stable/analyst-estimates").mock(return_value=httpx.Response(200, json=AAPL_ANALYST_ESTIMATES))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/stock-peers").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("fair_value_estimate", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        # Should still have analyst target even without peers
        assert data["fair_value"]["analyst_target"] == 303.11
        assert "peer data unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol(self):
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/balance-sheet-statement").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/cash-flow-statement").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/key-metrics-ttm").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/ratios-ttm").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/analyst-estimates").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/stock-peers").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("fair_value_estimate", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()


# ================================================================
# earnings_postmortem
# ================================================================


class TestEarningsPostmortem:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_postmortem(self):
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=AAPL_QUARTERLY_INCOME))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT_DATES))
        # The transcript fetch may or may not happen depending on date matching
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=AAPL_TRANSCRIPT))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_postmortem", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["earnings_date"] is not None
        # Results
        assert data["results"]["actual_eps"] is not None
        assert data["results"]["est_eps"] is not None
        assert data["results"]["surprise_pct"] is not None
        assert data["results"]["beat"] is True  # AAPL mock data beats estimates
        # Summary
        assert "beat" in data["summary"]["headline"].lower() or "miss" in data["summary"]["headline"].lower() or "results" in data["summary"]["headline"].lower()
        assert isinstance(data["summary"]["key_positives"], list)
        assert isinstance(data["summary"]["key_concerns"], list)
        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_postmortem(self):
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=AAPL_EARNINGS))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_postmortem", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["results"]["beat"] is True  # earnings data still works
        assert "income statement unavailable" in data["_warnings"]
        assert "analyst grades unavailable" in data["_warnings"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_earnings(self):
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_postmortem", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_specific_quarter_uses_transcript_period_mapping(self):
        earnings = [
            {
                "date": "2025-05-08",
                "symbol": "AAPL",
                "epsActual": 1.61,
                "epsEstimated": 1.55,
                "revenueActual": 91000000000,
                "revenueEstimated": 90000000000,
                "fiscalDateEnding": "2025-03-31",
            },
            {
                "date": "2025-02-06",
                "symbol": "AAPL",
                "epsActual": 2.44,
                "epsEstimated": 2.36,
                "revenueActual": 124200000000,
                "revenueEstimated": 120000000000,
                "fiscalDateEnding": "2024-12-31",
            },
        ]
        transcript_dates = [
            {"quarter": 2, "fiscalYear": 2025, "date": "2025-05-08"},
            {"quarter": 1, "fiscalYear": 2025, "date": "2025-02-06"},
        ]
        transcript = [{**AAPL_TRANSCRIPT[0], "date": "2025-02-06", "year": 2025, "quarter": 1}]

        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=earnings))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=AAPL_QUARTERLY_INCOME))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=transcript_dates))
        respx.get(f"{BASE}/stable/earning-call-transcript").mock(return_value=httpx.Response(200, json=transcript))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_postmortem", {"symbol": "AAPL", "year": 2025, "quarter": 1})

        data = result.data
        assert data["earnings_date"] == "2025-02-06"
        assert data["guidance"]["has_transcript"] is True
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_recent_report_triggers_lag_guard(self):
        recent_date = date.today().isoformat()
        recent_earnings = [
            {
                "date": recent_date,
                "symbol": "AAPL",
                "epsActual": 2.0,
                "epsEstimated": 1.8,
                "revenueActual": 100000000000,
                "revenueEstimated": 98000000000,
                "fiscalDateEnding": (date.today() - timedelta(days=30)).isoformat(),
            }
        ]
        respx.get(f"{BASE}/stable/earnings").mock(return_value=httpx.Response(200, json=recent_earnings))
        respx.get(f"{BASE}/stable/income-statement").mock(return_value=httpx.Response(200, json=AAPL_QUARTERLY_INCOME))
        respx.get(f"{BASE}/stable/grades").mock(return_value=httpx.Response(200, json=AAPL_GRADES_DETAIL))
        respx.get(f"{BASE}/stable/price-target-consensus").mock(return_value=httpx.Response(200, json=AAPL_PRICE_TARGET))
        respx.get(f"{BASE}/stable/historical-price-eod/full").mock(return_value=httpx.Response(200, json=AAPL_HISTORICAL))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/earning-call-transcript-dates").mock(return_value=httpx.Response(200, json=[]))

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_postmortem", {"symbol": "AAPL"})

        data = result.data
        assert "error" in data
        assert "lag" in data["error"].lower() or "reported" in data["error"].lower()
        await fmp.aclose()


# ================================================================
# ownership_deep_dive
# ================================================================


class TestOwnershipDeepDive:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_ownership(self):
        # Mock FMP endpoints
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_TRADES))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=AAPL_INSIDER_STATS))
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(return_value=httpx.Response(200, json=AAPL_INSTITUTIONAL_SUMMARY))
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(return_value=httpx.Response(200, json=AAPL_INSTITUTIONAL_HOLDERS))

        # Mock FINRA short interest endpoints (external)
        # The workflow tries multiple settlement dates via POST
        respx.post("https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest").mock(
            return_value=httpx.Response(200, json=AAPL_SHORT_INTEREST)
        )

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("ownership_deep_dive", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["company_name"] == "Apple Inc."

        # Ownership structure
        assert data["ownership_structure"]["outstanding_shares"] == 15200000000
        assert data["ownership_structure"]["float_shares"] == 14700000000
        assert data["ownership_structure"]["insider_pct"] > 0
        assert data["ownership_structure"]["institutional_pct"] > 0

        # Insider activity
        assert data["insider_activity"]["signal"] in ("net_buying", "net_selling", "neutral")
        assert isinstance(data["insider_activity"]["notable_trades"], list)

        # Institutional ownership
        assert data["institutional_ownership"]["total_shares"] > 0
        assert len(data["institutional_ownership"]["top_holders"]) > 0

        # Short interest
        assert data["short_interest"]["shares_short"] > 0
        assert data["short_interest"]["pct_of_float"] is not None

        # Ownership analysis
        assert data["ownership_analysis"]["signal"] in ("bullish", "neutral", "bearish")
        assert isinstance(data["ownership_analysis"]["key_insights"], list)
        assert "_warnings" not in data

        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_ownership(self):
        # Mock with some failures
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT))
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=AAPL_PROFILE))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=AAPL_QUOTE))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(return_value=httpx.Response(500, text="error"))
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(return_value=httpx.Response(200, json=[]))
        respx.post("https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest").mock(
            return_value=httpx.Response(500, text="error")
        )

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("ownership_deep_dive", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["ownership_structure"]["outstanding_shares"] == 15200000000  # from float data
        assert "insider trades unavailable" in data["_warnings"]
        assert "institutional summary unavailable" in data["_warnings"]
        assert "FINRA short interest unavailable" in data["_warnings"]

        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_symbol_ownership(self):
        respx.get(f"{BASE}/stable/shares-float").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/profile").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/quote").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/search").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/insider-trading/statistics").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/institutional-ownership/symbol-positions-summary").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{BASE}/stable/institutional-ownership/extract-analytics/holder").mock(return_value=httpx.Response(200, json=[]))
        respx.post("https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("ownership_deep_dive", {"symbol": "ZZZZ"})

        data = result.data
        assert "error" in data

        await fmp.aclose()


# ================================================================
# industry_analysis
# ================================================================

# Test fixtures for industry analysis
SOFTWARE_SCREENER = [
    {"symbol": "MSFT", "companyName": "Microsoft Corporation", "marketCap": 3100000000000, "sector": "Technology", "industry": "Software", "price": 420.50},
    {"symbol": "ORCL", "companyName": "Oracle Corporation", "marketCap": 350000000000, "sector": "Technology", "industry": "Software", "price": 130.25},
    {"symbol": "SAP", "companyName": "SAP SE", "marketCap": 180000000000, "sector": "Technology", "industry": "Software", "price": 155.00},
]

INDUSTRY_PERFORMANCE_NYSE = [
    {"date": "2026-02-11", "industry": "Software", "sector": "Technology", "exchange": "NYSE", "averageChange": 1.25},
    {"date": "2026-02-11", "industry": "Banks", "sector": "Financial Services", "exchange": "NYSE", "averageChange": -0.35},
]

INDUSTRY_PERFORMANCE_NASDAQ = [
    {"date": "2026-02-11", "industry": "Software", "sector": "Technology", "exchange": "NASDAQ", "averageChange": 1.45},
    {"date": "2026-02-11", "industry": "Biotechnology", "sector": "Healthcare", "exchange": "NASDAQ", "averageChange": 0.80},
]

MSFT_INCOME = [
    {"date": "2025-12-31", "revenue": 68500000000, "netIncome": 24800000000},
    {"date": "2025-09-30", "revenue": 65300000000, "netIncome": 22100000000},
    {"date": "2025-06-30", "revenue": 64700000000, "netIncome": 21900000000},
    {"date": "2024-12-31", "revenue": 62000000000, "netIncome": 20500000000},
]

ORCL_INCOME = [
    {"date": "2025-11-30", "revenue": 13500000000, "netIncome": 3200000000},
    {"date": "2025-08-31", "revenue": 13200000000, "netIncome": 3100000000},
    {"date": "2025-05-31", "revenue": 14100000000, "netIncome": 3500000000},
    {"date": "2024-11-30", "revenue": 12800000000, "netIncome": 2900000000},
]

SAP_INCOME = [
    {"date": "2025-12-31", "revenue": 8900000000, "netIncome": 1800000000},
    {"date": "2025-09-30", "revenue": 8500000000, "netIncome": 1600000000},
    {"date": "2025-06-30", "revenue": 8200000000, "netIncome": 1500000000},
    {"date": "2024-12-31", "revenue": 8100000000, "netIncome": 1400000000},
]


class TestIndustryAnalysis:
    @pytest.mark.asyncio
    @respx.mock
    async def test_full_industry(self):
        # Mock industry performance
        respx.get(f"{BASE}/stable/industry-performance-snapshot", params__contains={"exchange": "NYSE"}).mock(
            return_value=httpx.Response(200, json=INDUSTRY_PERFORMANCE_NYSE)
        )
        respx.get(f"{BASE}/stable/industry-performance-snapshot", params__contains={"exchange": "NASDAQ"}).mock(
            return_value=httpx.Response(200, json=INDUSTRY_PERFORMANCE_NASDAQ)
        )

        # Mock industry PE
        respx.get(f"{BASE}/stable/industry-pe-snapshot", params__contains={"exchange": "NYSE"}).mock(
            return_value=httpx.Response(200, json=INDUSTRY_PE_NYSE)
        )
        respx.get(f"{BASE}/stable/industry-pe-snapshot", params__contains={"exchange": "NASDAQ"}).mock(
            return_value=httpx.Response(200, json=INDUSTRY_PE_NASDAQ)
        )

        # Mock screener
        respx.get(f"{BASE}/stable/company-screener").mock(
            return_value=httpx.Response(200, json=SOFTWARE_SCREENER)
        )

        # Mock stock data for top stocks
        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "MSFT"}).mock(
            return_value=httpx.Response(200, json=MSFT_RATIOS)
        )
        respx.get(f"{BASE}/stable/income-statement", params__contains={"symbol": "MSFT"}).mock(
            return_value=httpx.Response(200, json=MSFT_INCOME)
        )
        respx.get(f"{BASE}/stable/quote", params__contains={"symbol": "MSFT"}).mock(
            return_value=httpx.Response(200, json=[{"symbol": "MSFT", "price": 420.50, "changePercentage": 1.2}])
        )

        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "ORCL"}).mock(
            return_value=httpx.Response(200, json=[{"symbol": "ORCL", "priceToEarningsRatioTTM": 28.5, "priceToSalesRatioTTM": 6.8, "returnOnEquityTTM": 0.35}])
        )
        respx.get(f"{BASE}/stable/income-statement", params__contains={"symbol": "ORCL"}).mock(
            return_value=httpx.Response(200, json=ORCL_INCOME)
        )
        respx.get(f"{BASE}/stable/quote", params__contains={"symbol": "ORCL"}).mock(
            return_value=httpx.Response(200, json=[{"symbol": "ORCL", "price": 130.25, "changePercentage": 0.5}])
        )

        respx.get(f"{BASE}/stable/ratios-ttm", params__contains={"symbol": "SAP"}).mock(
            return_value=httpx.Response(200, json=[{"symbol": "SAP", "priceToEarningsRatioTTM": 22.1, "priceToSalesRatioTTM": 5.2, "returnOnEquityTTM": 0.28}])
        )
        respx.get(f"{BASE}/stable/income-statement", params__contains={"symbol": "SAP"}).mock(
            return_value=httpx.Response(200, json=SAP_INCOME)
        )
        respx.get(f"{BASE}/stable/quote", params__contains={"symbol": "SAP"}).mock(
            return_value=httpx.Response(200, json=[{"symbol": "SAP", "price": 155.00, "changePercentage": -0.3}])
        )

        # Mock sector performance for rotation signal
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NYSE"}).mock(
            return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NYSE)
        )
        respx.get(f"{BASE}/stable/sector-performance-snapshot", params__contains={"exchange": "NASDAQ"}).mock(
            return_value=httpx.Response(200, json=SECTOR_PERFORMANCE_NASDAQ)
        )

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("industry_analysis", {"industry": "Software", "limit": 10})

        data = result.data
        assert data["industry"] == "Software"
        assert data["sector"] == "Technology"

        # Overview
        assert data["overview"]["performance_pct"] is not None
        assert data["overview"]["median_pe"] is not None

        # Top stocks
        assert len(data["top_stocks"]) > 0
        assert data["top_stocks"][0]["symbol"] == "MSFT"  # highest market cap

        # Medians
        assert data["industry_medians"]["pe"] is not None
        assert data["industry_medians"]["ps"] is not None

        # Valuation spread
        assert data["valuation_spread"]["cheapest"] is not None
        assert data["valuation_spread"]["most_expensive"] is not None

        # Rotation
        assert data["rotation"]["signal"] in ("money_flowing_in", "money_flowing_out", "neutral")

        assert "_warnings" not in data
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_industry(self):
        # Mock with some failures
        respx.get(f"{BASE}/stable/industry-performance-snapshot").mock(
            return_value=httpx.Response(500, text="error")
        )
        respx.get(f"{BASE}/stable/industry-pe-snapshot").mock(
            return_value=httpx.Response(500, text="error")
        )
        respx.get(f"{BASE}/stable/company-screener").mock(
            return_value=httpx.Response(200, json=SOFTWARE_SCREENER)
        )

        # Mock stock data
        respx.get(f"{BASE}/stable/ratios-ttm").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/income-statement").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/quote").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/sector-performance-snapshot").mock(
            return_value=httpx.Response(500, text="error")
        )

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("industry_analysis", {"industry": "Software"})

        data = result.data
        assert data["industry"] == "Software"
        assert "industry performance unavailable" in data["_warnings"]
        assert "sector performance unavailable for rotation signal" in data["_warnings"]

        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_industry(self):
        respx.get(f"{BASE}/stable/industry-performance-snapshot").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/industry-pe-snapshot").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/company-screener").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE}/stable/sector-performance-snapshot").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, fmp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("industry_analysis", {"industry": "NonExistent"})

        data = result.data
        assert "error" in data

        await fmp.aclose()
