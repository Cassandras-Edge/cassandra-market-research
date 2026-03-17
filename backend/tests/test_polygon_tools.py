"""Tests for Polygon.io-backed tools: options, MACD, economy, short volume."""

from __future__ import annotations

import httpx
import pytest
import respx

from fastmcp import FastMCP, Client
from fmp_data import AsyncFMPDataClient
from cassandra_fmp.clients.polygon import PolygonClient
from tests.conftest import build_test_client
from cassandra_fmp.tools.options import register as register_options
from cassandra_fmp.tools.economy import register as register_economy
from cassandra_fmp.tools.market import register as register_market
from cassandra_fmp.tools.ownership import register as register_ownership
from tests.conftest import (
    AAPL_RSI,
    AAPL_SHARES_FLOAT,
    AAPL_SHORT_INTEREST,
    AAPL_INSTITUTIONAL_SUMMARY,
    EARNINGS_CALENDAR, EARNINGS_BATCH_QUOTE,
    POLYGON_INFLATION,
    POLYGON_INFLATION_EXPECTATIONS,
    POLYGON_LABOR_MARKET,
    POLYGON_MACD,
    POLYGON_OPTIONS_SNAPSHOT,
    POLYGON_SHORT_INTEREST,
    POLYGON_TREASURY_YIELDS,
)

BASE_FMP = "https://financialmodelingprep.com"
BASE_POLYGON = "https://api.polygon.io"


def _make_options_server() -> tuple[FastMCP, PolygonClient]:
    mcp = FastMCP("Test")
    pc = PolygonClient(api_key="test_polygon_key")
    register_options(mcp, polygon_client=pc)
    return mcp, pc


def _make_economy_server() -> tuple[FastMCP, PolygonClient]:
    mcp = FastMCP("Test")
    pc = PolygonClient(api_key="test_polygon_key")
    register_economy(mcp, pc)
    return mcp, pc


def _make_market_server(*, with_polygon: bool = True) -> tuple[FastMCP, AsyncFMPDataClient, PolygonClient | None]:
    mcp = FastMCP("Test")
    fmp = build_test_client("test_key")
    pc = PolygonClient(api_key="test_polygon_key") if with_polygon else None
    register_market(mcp, fmp, polygon_client=pc)
    return mcp, fmp, pc


def _make_ownership_server(*, with_polygon: bool = True) -> tuple[FastMCP, AsyncFMPDataClient, PolygonClient | None]:
    mcp = FastMCP("Test")
    fmp = build_test_client("test_key")
    pc = PolygonClient(api_key="test_polygon_key") if with_polygon else None
    register_ownership(mcp, fmp, polygon_client=pc)
    return mcp, fmp, pc


# ---------------------------------------------------------------------------
# Options chain
# ---------------------------------------------------------------------------


class TestOptionsChain:
    @pytest.mark.asyncio
    @respx.mock
    async def test_basic_options_chain(self):
        """Options chain returns grouped contracts with Greeks and put/call ratio."""
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/AAPL").mock(
            return_value=httpx.Response(200, json=POLYGON_OPTIONS_SNAPSHOT)
        )

        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool("options_chain", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["source"] == "polygon.io"
        assert data["total_contracts"] == 4

        # Summary should count calls and puts
        summary = data["summary"]
        assert summary["total_calls"] == 3  # 2 calls on 2/20, 1 call on 2/27
        assert summary["total_puts"] == 1
        assert summary["overall_put_call_ratio"] is not None

        # Two expirations
        expirations = data["expirations"]
        assert len(expirations) == 2
        assert expirations[0]["expiration"] == "2026-02-20"
        assert expirations[1]["expiration"] == "2026-02-27"

        # Contracts sorted by strike
        contracts_0220 = expirations[0]["contracts"]
        assert contracts_0220[0]["strike"] == 270.0
        assert contracts_0220[0]["greeks"]["delta"] == 0.55
        assert contracts_0220[0]["iv"] == 0.32
        assert contracts_0220[0]["open_interest"] == 15000
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_with_filters(self):
        """Options chain accepts contract_type and limit filters."""
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/AAPL").mock(
            return_value=httpx.Response(200, json=POLYGON_OPTIONS_SNAPSHOT)
        )

        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool(
                "options_chain",
                {"symbol": "AAPL", "contract_type": "call", "limit": 10},
            )

        data = result.data
        assert data["symbol"] == "AAPL"
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_data(self):
        """Options chain returns error when no data found."""
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/ZZZZZ").mock(
            return_value=httpx.Response(200, json={"status": "OK", "results": []})
        )

        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool("options_chain", {"symbol": "ZZZZZ"})

        data = result.data
        assert "error" in data
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_contract_type(self):
        """Options chain rejects invalid contract_type."""
        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool(
                "options_chain", {"symbol": "AAPL", "contract_type": "straddle"}
            )

        data = result.data
        assert "error" in data
        assert "straddle" in data["error"]
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_put_call_ratio_calculation(self):
        """Put/call ratio is correctly computed from open interest."""
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/AAPL").mock(
            return_value=httpx.Response(200, json=POLYGON_OPTIONS_SNAPSHOT)
        )

        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool("options_chain", {"symbol": "AAPL"})

        data = result.data
        # 2/20 expiration: calls OI = 15000 + 8000 = 23000, puts OI = 12000
        exp_0220 = data["expirations"][0]
        assert exp_0220["call_open_interest"] == 23000
        assert exp_0220["put_open_interest"] == 12000
        assert exp_0220["put_call_oi_ratio"] == round(12000 / 23000, 2)
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_expiry_range_filter(self):
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/AAPL").mock(
            return_value=httpx.Response(200, json=POLYGON_OPTIONS_SNAPSHOT)
        )

        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool(
                "options_chain",
                {"symbol": "AAPL", "expiry_from": "2026-02-21", "expiry_to": "2026-02-27"},
            )

        data = result.data
        assert data["total_contracts"] == 1
        assert len(data["expirations"]) == 1
        assert data["expirations"][0]["expiration"] == "2026-02-27"
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_put_selling_metrics_present(self):
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/AAPL").mock(
            return_value=httpx.Response(200, json=POLYGON_OPTIONS_SNAPSHOT)
        )

        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool("options_chain", {"symbol": "AAPL"})

        data = result.data
        puts = [
            c
            for exp in data["expirations"]
            for c in exp["contracts"]
            if c.get("type") == "put"
        ]
        assert puts
        assert "put_selling" in puts[0]
        assert puts[0]["put_selling"]["breakeven"] is not None
        assert data["summary"]["atm_implied_move_pct"] is not None
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_stale_data_warning(self):
        stale_snapshot = {
            "status": "OK",
            "results": [
                {
                    "details": {
                        "ticker": f"O:AAPL260220C00{i:05d}",
                        "strike_price": 250 + i,
                        "contract_type": "call" if i % 2 == 0 else "put",
                        "expiration_date": "2026-02-20",
                    },
                    "greeks": {},
                    "implied_volatility": 0.3,
                    "open_interest": 1000 + i,
                    "day": {"volume": 0, "close": 1.0},
                    "last_quote": {"bid": 0, "ask": 0, "bid_size": 0, "ask_size": 0},
                }
                for i in range(10)
            ],
        }
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/AAPL").mock(
            return_value=httpx.Response(200, json=stale_snapshot)
        )

        mcp, pc = _make_options_server()
        async with Client(mcp) as c:
            result = await c.call_tool("options_chain", {"symbol": "AAPL"})

        data = result.data
        assert any("delayed" in w.lower() or "eod" in w.lower() for w in data.get("_warnings", []))
        await pc.close()


# ---------------------------------------------------------------------------
# MACD via Polygon
# ---------------------------------------------------------------------------


class TestMACDIndicator:
    @pytest.mark.asyncio
    @respx.mock
    async def test_macd_returns_value_signal_histogram(self):
        """MACD indicator returns value, signal, and histogram from Polygon."""
        respx.get(f"{BASE_POLYGON}/v1/indicators/macd/AAPL").mock(
            return_value=httpx.Response(200, json=POLYGON_MACD)
        )

        mcp, fmp, pc = _make_market_server()
        async with Client(mcp) as c:
            result = await c.call_tool(
                "technical_indicators", {"symbol": "AAPL", "indicator": "macd"}
            )

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["indicator"] == "macd"
        assert data["source"] == "polygon.io"
        assert data["current_value"] == 2.35
        assert data["current_signal"] == 1.80
        assert data["current_histogram"] == 0.55
        assert len(data["values"]) == 3

        # Each value has macd, signal, histogram, date
        v = data["values"][0]
        assert "macd" in v
        assert "signal" in v
        assert "histogram" in v
        assert "date" in v
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_macd_without_polygon_returns_error(self):
        """MACD returns descriptive error when polygon_client is None."""
        mcp, fmp, _ = _make_market_server(with_polygon=False)
        async with Client(mcp) as c:
            result = await c.call_tool(
                "technical_indicators", {"symbol": "AAPL", "indicator": "macd"}
            )

        data = result.data
        assert "error" in data
        assert "POLYGON_API_KEY" in data["error"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_rsi_still_uses_fmp(self):
        """RSI indicator still uses FMP (non-Polygon path)."""
        respx.get(f"{BASE_FMP}/stable/technical-indicators/rsi").mock(
            return_value=httpx.Response(200, json=AAPL_RSI)
        )

        mcp, fmp, pc = _make_market_server()
        async with Client(mcp) as c:
            result = await c.call_tool(
                "technical_indicators", {"symbol": "AAPL", "indicator": "rsi"}
            )

        data = result.data
        assert data["symbol"] == "AAPL"
        assert data["indicator"] == "rsi"
        assert "source" not in data  # FMP path doesn't set source
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_macd_polygon_error_returns_no_data(self):
        """MACD returns error when Polygon API fails."""
        respx.get(f"{BASE_POLYGON}/v1/indicators/macd/AAPL").mock(
            return_value=httpx.Response(500, json={"status": "ERROR", "error": "server error"})
        )

        mcp, fmp, pc = _make_market_server()
        async with Client(mcp) as c:
            result = await c.call_tool(
                "technical_indicators", {"symbol": "AAPL", "indicator": "macd"}
            )

        data = result.data
        assert "error" in data
        await fmp.aclose()


# ---------------------------------------------------------------------------
# Earnings calendar OI enrichment (Polygon)
# ---------------------------------------------------------------------------


class TestEarningsCalendarOI:
    @pytest.mark.asyncio
    @respx.mock
    async def test_browse_includes_oi(self):
        """Browsing mode enriches entries with options OI from Polygon."""
        respx.get(f"{BASE_FMP}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )
        respx.get(f"{BASE_FMP}/stable/batch-quote").mock(
            return_value=httpx.Response(200, json=EARNINGS_BATCH_QUOTE)
        )
        # Mock Polygon options snapshot for each symbol
        for sym in ("AAPL", "MSFT", "GOOGL", "TSLA"):
            respx.get(f"{BASE_POLYGON}/v3/snapshot/options/{sym}").mock(
                return_value=httpx.Response(200, json=POLYGON_OPTIONS_SNAPSHOT)
            )

        mcp, fmp, pc = _make_market_server(with_polygon=True)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {})

        data = result.data
        assert data["count"] == 4
        # Every entry should have options OI
        for e in data["earnings"]:
            assert "options" in e, f"{e['symbol']} missing options OI"
            assert e["options"]["total_oi"] > 0
            assert "call_oi" in e["options"]
            assert "put_oi" in e["options"]
            assert "put_call_ratio" in e["options"]
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_single_symbol_includes_oi(self):
        """Single-symbol lookup enriches with OI."""
        respx.get(f"{BASE_FMP}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )
        respx.get(f"{BASE_POLYGON}/v3/snapshot/options/AAPL").mock(
            return_value=httpx.Response(200, json=POLYGON_OPTIONS_SNAPSHOT)
        )

        mcp, fmp, pc = _make_market_server(with_polygon=True)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {"symbol": "AAPL"})

        data = result.data
        assert data["count"] == 1
        assert "options" in data["earnings"][0]
        # POLYGON_OPTIONS_SNAPSHOT has: calls OI = 15000+8000+6000=29000, puts OI = 12000
        assert data["earnings"][0]["options"]["total_oi"] == 41000
        assert data["earnings"][0]["options"]["call_oi"] == 29000
        assert data["earnings"][0]["options"]["put_oi"] == 12000
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_polygon_skips_oi(self):
        """Without polygon_client, earnings still work but without OI."""
        respx.get(f"{BASE_FMP}/stable/earnings-calendar").mock(
            return_value=httpx.Response(200, json=EARNINGS_CALENDAR)
        )
        respx.get(f"{BASE_FMP}/stable/batch-quote").mock(
            return_value=httpx.Response(200, json=EARNINGS_BATCH_QUOTE)
        )

        mcp, fmp, _ = _make_market_server(with_polygon=False)
        async with Client(mcp) as c:
            result = await c.call_tool("earnings_calendar", {})

        data = result.data
        assert data["count"] == 4
        for e in data["earnings"]:
            assert "options" not in e
        await fmp.aclose()


# ---------------------------------------------------------------------------
# Economy indicators
# ---------------------------------------------------------------------------


class TestEconomyIndicators:
    @pytest.mark.asyncio
    @respx.mock
    async def test_all_category(self):
        """Economy indicators 'all' category returns inflation, labor, and rates."""
        respx.get(f"{BASE_POLYGON}/fed/v1/inflation").mock(
            return_value=httpx.Response(200, json=POLYGON_INFLATION)
        )
        respx.get(f"{BASE_POLYGON}/fed/v1/inflation-expectations").mock(
            return_value=httpx.Response(200, json=POLYGON_INFLATION_EXPECTATIONS)
        )
        respx.get(f"{BASE_POLYGON}/fed/v1/labor-market").mock(
            return_value=httpx.Response(200, json=POLYGON_LABOR_MARKET)
        )
        respx.get(f"{BASE_POLYGON}/fed/v1/treasury-yields").mock(
            return_value=httpx.Response(200, json=POLYGON_TREASURY_YIELDS)
        )

        mcp, pc = _make_economy_server()
        async with Client(mcp) as c:
            result = await c.call_tool("economy_indicators", {"category": "all"})

        data = result.data
        assert data["category"] == "all"
        assert data["source"] == "polygon.io"

        # Inflation
        assert "inflation" in data
        assert data["inflation"]["latest"]["cpi_yoy_pct"] == 2.8
        assert len(data["inflation"]["cpi_yoy_trend"]) == 2

        # Inflation expectations
        assert "inflation_expectations" in data
        assert data["inflation_expectations"]["latest"]["market_5y"] == 2.35

        # Labor
        assert "labor" in data
        assert data["labor"]["latest"]["unemployment_rate"] == 4.1

        # Treasury yields
        assert "treasury_yields" in data
        assert data["treasury_yields"]["latest"]["yield_10y"] == 4.05
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_inflation_only(self):
        """Economy indicators 'inflation' category returns only inflation data."""
        respx.get(f"{BASE_POLYGON}/fed/v1/inflation").mock(
            return_value=httpx.Response(200, json=POLYGON_INFLATION)
        )
        respx.get(f"{BASE_POLYGON}/fed/v1/inflation-expectations").mock(
            return_value=httpx.Response(200, json=POLYGON_INFLATION_EXPECTATIONS)
        )

        mcp, pc = _make_economy_server()
        async with Client(mcp) as c:
            result = await c.call_tool("economy_indicators", {"category": "inflation"})

        data = result.data
        assert data["category"] == "inflation"
        assert "inflation" in data
        assert "inflation_expectations" in data
        assert "labor" not in data
        assert "treasury_yields" not in data
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_rates_only(self):
        """Economy indicators 'rates' category returns only treasury yields."""
        respx.get(f"{BASE_POLYGON}/fed/v1/treasury-yields").mock(
            return_value=httpx.Response(200, json=POLYGON_TREASURY_YIELDS)
        )

        mcp, pc = _make_economy_server()
        async with Client(mcp) as c:
            result = await c.call_tool("economy_indicators", {"category": "rates"})

        data = result.data
        assert data["category"] == "rates"
        assert "treasury_yields" in data
        assert "inflation" not in data
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_labor_only(self):
        """Economy indicators 'labor' category returns only labor data."""
        respx.get(f"{BASE_POLYGON}/fed/v1/labor-market").mock(
            return_value=httpx.Response(200, json=POLYGON_LABOR_MARKET)
        )

        mcp, pc = _make_economy_server()
        async with Client(mcp) as c:
            result = await c.call_tool("economy_indicators", {"category": "labor"})

        data = result.data
        assert data["category"] == "labor"
        assert "labor" in data
        assert "inflation" not in data
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_category(self):
        """Economy indicators rejects invalid category."""
        mcp, pc = _make_economy_server()
        async with Client(mcp) as c:
            result = await c.call_tool("economy_indicators", {"category": "bonds"})

        data = result.data
        assert "error" in data
        await pc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_partial_failure_warns(self):
        """Economy indicators adds warnings when some endpoints fail."""
        respx.get(f"{BASE_POLYGON}/fed/v1/inflation").mock(
            return_value=httpx.Response(500, text="error")
        )
        respx.get(f"{BASE_POLYGON}/fed/v1/inflation-expectations").mock(
            return_value=httpx.Response(200, json=POLYGON_INFLATION_EXPECTATIONS)
        )
        respx.get(f"{BASE_POLYGON}/fed/v1/labor-market").mock(
            return_value=httpx.Response(200, json=POLYGON_LABOR_MARKET)
        )
        respx.get(f"{BASE_POLYGON}/fed/v1/treasury-yields").mock(
            return_value=httpx.Response(200, json=POLYGON_TREASURY_YIELDS)
        )

        mcp, pc = _make_economy_server()
        async with Client(mcp) as c:
            result = await c.call_tool("economy_indicators", {"category": "all"})

        data = result.data
        assert "_warnings" in data
        assert "inflation data unavailable" in data["_warnings"]
        # Other sections should still work
        assert "inflation_expectations" in data
        assert "labor" in data
        assert "treasury_yields" in data
        await pc.close()


# ---------------------------------------------------------------------------
# Short interest with Polygon enrichment
# ---------------------------------------------------------------------------


class TestShortInterestPolygon:
    @pytest.mark.asyncio
    @respx.mock
    async def test_short_interest_with_polygon(self):
        """Short interest includes Polygon short interest when available."""
        # FINRA mock
        respx.post("https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest").mock(
            return_value=httpx.Response(200, json=AAPL_SHORT_INTEREST)
        )
        # FMP float
        respx.get(f"{BASE_FMP}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT)
        )
        # Polygon short interest
        respx.get(f"{BASE_POLYGON}/stocks/v1/short-interest").mock(
            return_value=httpx.Response(200, json=POLYGON_SHORT_INTEREST)
        )

        mcp, fmp, pc = _make_ownership_server()
        async with Client(mcp) as c:
            result = await c.call_tool("short_interest", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"

        # FINRA data should be present
        assert "short_interest" in data

        # Polygon enrichment
        assert "polygon_short_interest" in data
        polygon = data["polygon_short_interest"]
        assert polygon["source"] == "polygon.io"
        assert polygon["short_interest"] == 118000000
        assert polygon["settlement_date"] == "2026-02-10"
        await fmp.aclose()

    @pytest.mark.asyncio
    @respx.mock
    async def test_short_interest_without_polygon(self):
        """Short interest works normally without Polygon client."""
        # FINRA mock
        respx.post("https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest").mock(
            return_value=httpx.Response(200, json=AAPL_SHORT_INTEREST)
        )
        # FMP float
        respx.get(f"{BASE_FMP}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT)
        )

        mcp, fmp, _ = _make_ownership_server(with_polygon=False)
        async with Client(mcp) as c:
            result = await c.call_tool("short_interest", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert "short_interest" in data
        assert "polygon_short_interest" not in data
        await fmp.aclose()


# ---------------------------------------------------------------------------
# Ownership structure with Polygon enrichment
# ---------------------------------------------------------------------------


class TestOwnershipStructurePolygon:
    @pytest.mark.asyncio
    @respx.mock
    async def test_ownership_structure_with_polygon(self):
        """Ownership structure includes Polygon short interest when available."""
        # FMP mocks
        respx.get(f"{BASE_FMP}/stable/institutional-ownership/symbol-positions-summary").mock(
            return_value=httpx.Response(200, json=AAPL_INSTITUTIONAL_SUMMARY)
        )
        respx.get(f"{BASE_FMP}/stable/institutional-ownership/extract-analytics/holder").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{BASE_FMP}/stable/shares-float").mock(
            return_value=httpx.Response(200, json=AAPL_SHARES_FLOAT)
        )
        respx.get(f"{BASE_FMP}/stable/insider-trading/statistics").mock(
            return_value=httpx.Response(200, json=[{"totalAcquired": 23000}])
        )
        # FINRA mock
        respx.post("https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest").mock(
            return_value=httpx.Response(200, json=AAPL_SHORT_INTEREST)
        )
        # Polygon short interest
        respx.get(f"{BASE_POLYGON}/stocks/v1/short-interest").mock(
            return_value=httpx.Response(200, json=POLYGON_SHORT_INTEREST)
        )

        mcp, fmp, pc = _make_ownership_server()
        async with Client(mcp) as c:
            result = await c.call_tool("ownership_structure", {"symbol": "AAPL"})

        data = result.data
        assert data["symbol"] == "AAPL"
        assert "polygon_short_interest" in data
        assert data["polygon_short_interest"]["source"] == "polygon.io"
        await fmp.aclose()
