"""Tests for meta/introspection tools."""

from __future__ import annotations

import pytest

from fastmcp import Client, FastMCP
from tests.conftest import build_test_client
from cassandra_fmp.tools.meta import register as register_meta


def _make_server() -> FastMCP:
    mcp = FastMCP("Test")
    client = build_test_client("test_key")
    register_meta(mcp, client)
    return mcp


class TestFMPCoverageGaps:
    @pytest.mark.asyncio
    async def test_default_gap_report(self):
        mcp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool("fmp_coverage_gaps", {})

        data = result.data
        assert isinstance(data, dict)
        for key in (
            "docs_url",
            "coverage_basis",
            "documented_family_count",
            "implemented_family_count",
            "unimplemented_family_count",
            "unimplemented_families",
            "categories",
        ):
            assert key in data

        assert isinstance(data["unimplemented_families"], list)
        assert isinstance(data["categories"], list)
        # Explicitly track key docs families we do not currently support.
        # Note: 'esg' was once here but is now implemented via tools/esg.py.
        assert "bulk" in data["unimplemented_families"]
        assert "websocket" in data["unimplemented_families"]
        assert "senate-trading" not in data["unimplemented_families"]
        assert "discounted-cash-flow" not in data["unimplemented_families"]

    @pytest.mark.asyncio
    async def test_include_implemented_categories(self):
        mcp = _make_server()
        async with Client(mcp) as c:
            result = await c.call_tool(
                "fmp_coverage_gaps",
                {"include_implemented_categories": True},
            )

        data = result.data
        assert isinstance(data, dict)
        assert isinstance(data.get("categories"), list)
        assert len(data["categories"]) > 0
        statuses = {c.get("status") for c in data["categories"]}
        assert statuses.issubset({"covered", "partial", "missing"})
