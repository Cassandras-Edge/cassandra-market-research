"""Tests for Treasury auction tools (treasury_auctions, auction_analysis)."""

from __future__ import annotations

import pytest
import respx
import httpx

from fastmcp import FastMCP, Client
from cassandra_fmp.clients.treasury import TreasuryClient
from cassandra_fmp.tools.auctions import register as register_auctions

TREASURY_BASE = "https://api.fiscaldata.treasury.gov"
FRED_BASE = "https://api.stlouisfed.org"


def _make_auction_server(*, fred_api_key: str | None = None) -> tuple[FastMCP, TreasuryClient]:
    """Create a FastMCP server with auction tools and a fresh TreasuryClient."""
    mcp = FastMCP("Test")
    tc = TreasuryClient(fred_api_key=fred_api_key)
    register_auctions(mcp, tc)
    return mcp, tc


# ---------------------------------------------------------------------------
# Fixtures: realistic auction records (API returns all values as strings)
# ---------------------------------------------------------------------------

NOTE_10Y = {
    "cusip": "91282CKV6",
    "security_type": "Note",
    "security_term": "10-Year",
    "auction_date": "2026-02-11",
    "issue_date": "2026-02-15",
    "high_yield": "4.320",
    "avg_med_yield": "4.310",
    "bid_to_cover_ratio": "2.58",
    "offering_amt": "42000000000",
    "total_tendered": "108360000000",
    "total_accepted": "42000000000",
    "comp_accepted": "38500000000",
    "noncomp_accepted": "3500000000",
    "direct_bidder_accepted": "7700000000",
    "indirect_bidder_accepted": "27720000000",
    "primary_dealer_accepted": "3080000000",
    "soma_accepted": "5200000000",
    "cash_management_bill_cmb": "",
    "high_discnt_rate": None,
    "high_investment_rate": None,
}

NOTE_10Y_OLDER = {
    **NOTE_10Y,
    "cusip": "91282CKV5",
    "auction_date": "2026-01-14",
    "issue_date": "2026-01-18",
    "high_yield": "4.400",
    "avg_med_yield": "4.385",
    "bid_to_cover_ratio": "2.45",
    "direct_bidder_accepted": "7000000000",
    "indirect_bidder_accepted": "25000000000",
    "primary_dealer_accepted": "6500000000",
}

BILL_4W = {
    "cusip": "912797KF8",
    "security_type": "Bill",
    "security_term": "4-Week",
    "auction_date": "2026-02-18",
    "issue_date": "2026-02-20",
    "high_yield": None,
    "avg_med_yield": None,
    "high_discnt_rate": "4.250",
    "high_investment_rate": "4.340",
    "bid_to_cover_ratio": "3.10",
    "offering_amt": "75000000000",
    "total_tendered": "232500000000",
    "total_accepted": "75000000000",
    "comp_accepted": "72000000000",
    "noncomp_accepted": "3000000000",
    "direct_bidder_accepted": "14400000000",
    "indirect_bidder_accepted": "36000000000",
    "primary_dealer_accepted": "21600000000",
    "soma_accepted": "0",
    "cash_management_bill_cmb": "No",
}

# Unsettled future auction (no results yet)
UPCOMING_NOTE = {
    "cusip": "91282CKW4",
    "security_type": "Note",
    "security_term": "7-Year",
    "auction_date": "2026-03-05",
    "issue_date": "2026-03-10",
    "high_yield": None,
    "avg_med_yield": None,
    "bid_to_cover_ratio": None,
    "offering_amt": "44000000000",
    "total_tendered": None,
    "total_accepted": None,
    "comp_accepted": None,
    "noncomp_accepted": None,
    "direct_bidder_accepted": None,
    "indirect_bidder_accepted": None,
    "primary_dealer_accepted": None,
    "soma_accepted": None,
    "cash_management_bill_cmb": "",
    "high_discnt_rate": None,
    "high_investment_rate": None,
}


def _treasury_response(records: list[dict], total_pages: int = 1) -> dict:
    """Wrap records in Treasury API response envelope."""
    return {
        "data": records,
        "meta": {
            "total-pages": total_pages,
            "total-count": len(records),
        },
    }


def _fred_response(value: str, obs_date: str = "2026-02-11") -> dict:
    return {
        "observations": [
            {"date": obs_date, "value": value},
        ],
    }


# ---------------------------------------------------------------------------
# Tests: treasury_auctions
# ---------------------------------------------------------------------------


class TestTreasuryAuctions:
    @pytest.mark.asyncio
    @respx.mock
    async def test_note_graded_and_bill_ungraded(self):
        """Notes get graded, bills don't, unsettled auctions are filtered."""
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(
                200, json=_treasury_response([NOTE_10Y, BILL_4W, UPCOMING_NOTE])
            )
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_auctions", {"days_back": 30})
        data = result.data

        assert data["count"] == 2  # unsettled filtered out
        note = data["auctions"][0]
        bill = data["auctions"][1]

        # Note should have grade
        assert note["security_type"] == "Note"
        assert "grade" in note
        assert note["grade"]["composite_grade"] in {"A", "B", "C", "D", "F"}
        assert note["bid_to_cover"] == 2.58
        assert note["tail_bps"] is not None

        # Bill should NOT have grade
        assert bill["security_type"] == "Bill"
        assert "grade" not in bill
        assert bill["high_discnt_rate"] == 4.25

        # Summary
        assert data["graded_summary"]["count"] == 1
        assert data["bill_count"] == 1

        # No FRED key → warning
        assert "_warnings" in data
        assert any("FRED_API_KEY" in w for w in data["_warnings"])
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_with_fred_cmt_yields(self):
        """When FRED_API_KEY is set, tail uses CMT yield instead of avg_med."""
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([NOTE_10Y]))
        )
        respx.get(f"{FRED_BASE}/fred/series/observations").mock(
            return_value=httpx.Response(200, json=_fred_response("4.300"))
        )
        mcp, tc = _make_auction_server(fred_api_key="test_fred_key")
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_auctions", {"days_back": 14})
        data = result.data

        note = data["auctions"][0]
        assert note["wi_source"] == "fred_cmt"
        # Tail = (4.320 - 4.300) * 100 = 2.0 bps
        assert note["tail_bps"] == 2.0
        assert data["wi_source"] == "fred_cmt"
        # No FRED warning when key is set
        assert "_warnings" not in data
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_bills_only_warns(self):
        """When only bills are returned, warn about no graded auctions."""
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([BILL_4W]))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_auctions", {"security_type": "Bill"})
        data = result.data

        assert data["count"] == 1
        assert "graded_summary" not in data
        assert "_warnings" in data
        assert any("only bills" in w.lower() for w in data["_warnings"])
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_settled_auctions(self):
        """Unsettled-only results return count=0."""
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([UPCOMING_NOTE]))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_auctions", {"days_back": 7})
        data = result.data

        assert data["count"] == 0
        assert data["auctions"] == []
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_response(self):
        """Treasury API returns no data."""
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([]))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_auctions", {})
        data = result.data
        assert data["count"] == 0
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_limit_respected(self):
        """Limit caps the number of returned auctions."""
        records = [
            {**NOTE_10Y, "cusip": f"CUSIP{i}", "auction_date": f"2026-02-{10+i:02d}"}
            for i in range(10)
        ]
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response(records))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("treasury_auctions", {"limit": 3})
        data = result.data
        assert data["count"] == 3
        await tc.close()


# ---------------------------------------------------------------------------
# Tests: auction_analysis
# ---------------------------------------------------------------------------


class TestAuctionAnalysis:
    def _make_note_series(self, count: int = 6) -> list[dict]:
        """Generate a series of 10Y note auctions for trend analysis."""
        records = []
        for i in range(count):
            records.append({
                **NOTE_10Y,
                "cusip": f"CUSIP10Y{i}",
                "auction_date": f"2026-{max(1, 2 - i // 3):02d}-{28 - i * 3:02d}",
                "high_yield": str(4.30 + i * 0.02),
                "avg_med_yield": str(4.29 + i * 0.02),
                "bid_to_cover_ratio": str(2.6 - i * 0.03),
            })
        return records

    @pytest.mark.asyncio
    @respx.mock
    async def test_demand_signal_and_trends(self):
        """Enough auctions produce trends and a demand signal."""
        records = self._make_note_series(6)
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response(records))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("auction_analysis", {"days_back": 90})
        data = result.data

        assert data["demand_signal"] in {"strong", "healthy", "soft", "weak", "neutral"}
        assert data["total_auctions"] == 6
        assert "notes_bonds" in data
        assert data["notes_bonds"]["count"] == 6
        assert "by_maturity" in data["notes_bonds"]

        # Should have trend data for 10-Year
        maturity = data["notes_bonds"]["by_maturity"][0]
        assert maturity["term"] == "10-Year"
        assert "trends" in maturity
        assert maturity["trends"]["overall"] in {"improving", "deteriorating", "stable"}

        # FRED warning present (no key)
        assert "_warnings" in data
        assert any("FRED_API_KEY" in w for w in data["_warnings"])
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_insufficient_data_for_trends(self):
        """Too few auctions → insufficient_data for trends."""
        records = [NOTE_10Y]  # only 1 auction
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response(records))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("auction_analysis", {"days_back": 30})
        data = result.data

        maturity = data["notes_bonds"]["by_maturity"][0]
        assert maturity["trends"]["overall"] == "insufficient_data"
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_bills_only_neutral_signal(self):
        """All bills → neutral demand signal + warning."""
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([BILL_4W]))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("auction_analysis", {"days_back": 30})
        data = result.data

        assert data["demand_signal"] == "neutral"
        assert "notes_bonds" not in data
        assert "bills" in data
        assert data["bills"]["count"] == 1
        assert "_warnings" in data
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_data(self):
        """No auction data returns neutral signal."""
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([]))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("auction_analysis", {"days_back": 30})
        data = result.data

        assert data["total_auctions"] == 0
        assert data["demand_signal"] == "neutral"
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_mixed_notes_and_bills(self):
        """Mixed results include both graded and bill summaries."""
        records = self._make_note_series(4) + [BILL_4W]
        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response(records))
        )
        mcp, tc = _make_auction_server()
        async with Client(mcp) as c:
            result = await c.call_tool("auction_analysis", {"days_back": 90})
        data = result.data

        assert data["total_auctions"] == 5
        assert "notes_bonds" in data
        assert "bills" in data
        assert data["notes_bonds"]["count"] == 4
        assert data["bills"]["count"] == 1
        await tc.close()


# ---------------------------------------------------------------------------
# Tests: grading helpers (unit tests)
# ---------------------------------------------------------------------------


class TestGradingHelpers:
    def test_grade_lower_is_better(self):
        from cassandra_fmp.tools.auctions import _grade_lower_is_better, TAIL_THRESHOLDS
        assert _grade_lower_is_better(-2.0, TAIL_THRESHOLDS) == "A"
        assert _grade_lower_is_better(0.0, TAIL_THRESHOLDS) == "B"
        assert _grade_lower_is_better(1.0, TAIL_THRESHOLDS) == "C"
        assert _grade_lower_is_better(3.0, TAIL_THRESHOLDS) == "D"
        assert _grade_lower_is_better(5.0, TAIL_THRESHOLDS) == "F"
        assert _grade_lower_is_better(None, TAIL_THRESHOLDS) == "N/A"

    def test_grade_higher_is_better(self):
        from cassandra_fmp.tools.auctions import _grade_higher_is_better, BTC_THRESHOLDS
        assert _grade_higher_is_better(3.0, BTC_THRESHOLDS) == "A"
        assert _grade_higher_is_better(2.6, BTC_THRESHOLDS) == "B"
        assert _grade_higher_is_better(2.3, BTC_THRESHOLDS) == "C"
        assert _grade_higher_is_better(2.1, BTC_THRESHOLDS) == "D"
        assert _grade_higher_is_better(1.5, BTC_THRESHOLDS) == "F"
        assert _grade_higher_is_better(None, BTC_THRESHOLDS) == "N/A"

    def test_compute_metrics_with_avg_med_fallback(self):
        from cassandra_fmp.tools.auctions import _compute_metrics
        metrics = _compute_metrics(NOTE_10Y, wi_yield=None)
        assert metrics["wi_source"] == "avg_med_yield"
        assert metrics["tail_bps"] is not None
        assert metrics["bid_to_cover"] == 2.58
        assert metrics["dealer_pct"] is not None
        assert metrics["indirect_pct"] is not None

    def test_compute_metrics_with_fred_cmt(self):
        from cassandra_fmp.tools.auctions import _compute_metrics
        metrics = _compute_metrics(NOTE_10Y, wi_yield=4.300)
        assert metrics["wi_source"] == "fred_cmt"
        # Tail = (4.320 - 4.300) * 100 = 2.0
        assert metrics["tail_bps"] == 2.0

    def test_tips_guard_discards_divergent_cmt(self):
        """FRED CMT yield that diverges >150bps from high_yield is discarded."""
        from cassandra_fmp.tools.auctions import _compute_metrics
        # Simulate TIPS: real yield ~1.8%, nominal CMT ~4.3%
        tips_record = {**NOTE_10Y, "high_yield": "1.800", "security_type": "TIPS"}
        metrics = _compute_metrics(tips_record, wi_yield=4.300)
        # Should fall back to avg_med since gap > 1.5
        assert metrics["wi_source"] == "avg_med_yield"

    def test_safe_float(self):
        from cassandra_fmp.tools.auctions import _safe_float
        assert _safe_float("4.320") == 4.32
        assert _safe_float("null") is None
        assert _safe_float(None) is None
        assert _safe_float("") is None
        assert _safe_float("not_a_number") is None

    def test_grade_auction_composite(self):
        from cassandra_fmp.tools.auctions import _grade_auction
        # Strong auction: negative tail, high BTC, low dealer, high indirect
        metrics = {
            "tail_bps": -1.5,
            "bid_to_cover": 3.0,
            "dealer_pct": 7.0,
            "indirect_pct": 76.0,
        }
        grade = _grade_auction(metrics)
        assert grade["composite_grade"] == "A"
        assert grade["gpa"] >= 3.5

    def test_trend_direction(self):
        from cassandra_fmp.tools.auctions import _trend_direction
        # Improving: recent lower values for lower-is-better metric
        assert _trend_direction([1.0, 1.5], [3.0, 3.5], lower_is_better=True) == "improving"
        # Deteriorating: recent higher for lower-is-better
        assert _trend_direction([3.0, 3.5], [1.0, 1.5], lower_is_better=True) == "deteriorating"
        # Stable
        assert _trend_direction([2.0, 2.01], [2.0, 1.99], lower_is_better=True) == "stable"
        # Insufficient
        assert _trend_direction([], [1.0], lower_is_better=True) == "insufficient_data"


# ---------------------------------------------------------------------------
# Tests: TreasuryClient
# ---------------------------------------------------------------------------


class TestTreasuryClient:
    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_auctions_filters(self):
        """Verify query params are passed correctly."""
        route = respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([NOTE_10Y]))
        )
        tc = TreasuryClient()
        result = await tc.fetch_auctions(days_back=14, security_type="Note", security_term="10-Year")
        assert len(result) == 1
        assert result[0]["cusip"] == NOTE_10Y["cusip"]

        # Verify filter params
        request = route.calls[0].request
        filter_param = str(request.url.params.get("filter", ""))
        assert "security_type:eq:Note" in filter_param
        assert "security_term:eq:10-Year" in filter_param
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_cmt_yield(self):
        """FRED CMT fetch returns parsed yield."""
        respx.get(f"{FRED_BASE}/fred/series/observations").mock(
            return_value=httpx.Response(200, json=_fred_response("4.300"))
        )
        tc = TreasuryClient(fred_api_key="test_key")
        result = await tc.fetch_cmt_yield("10-Year", "2026-02-11")
        assert result == 4.3
        await tc.close()

    @pytest.mark.asyncio
    async def test_fetch_cmt_yield_no_key(self):
        """Without FRED key, returns None without making any request."""
        tc = TreasuryClient(fred_api_key=None)
        result = await tc.fetch_cmt_yield("10-Year", "2026-02-11")
        assert result is None
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_cmt_yield_unknown_term(self):
        """Unknown security term returns None."""
        tc = TreasuryClient(fred_api_key="test_key")
        result = await tc.fetch_cmt_yield("42-Day", "2026-02-11")
        assert result is None
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_caching(self):
        """Second call for same params returns cached result."""
        route = respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            return_value=httpx.Response(200, json=_treasury_response([NOTE_10Y]))
        )
        tc = TreasuryClient()
        await tc.fetch_auctions(days_back=14)
        await tc.fetch_auctions(days_back=14)
        # Should only hit API once due to cache
        assert route.call_count == 1
        await tc.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_pagination_cap(self):
        """Pagination stops at max_pages safety cap."""
        call_count = 0

        def _paginated_response(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, json=_treasury_response([NOTE_10Y], total_pages=100))

        respx.get(f"{TREASURY_BASE}/services/api/fiscal_service/v1/accounting/od/auctions_query").mock(
            side_effect=_paginated_response
        )
        tc = TreasuryClient()
        tc._cache.clear()  # Ensure no caching interferes
        result = await tc.fetch_auctions(days_back=365)
        # Should stop at max_pages (10), not 100
        assert call_count <= 10
        assert len(result) == call_count  # 1 record per page
        await tc.close()
