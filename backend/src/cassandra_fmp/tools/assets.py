"""Commodity, crypto, and forex quote tools."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from cassandra_fmp.tools._helpers import TTL_REALTIME, _as_dict, _as_list, _safe_call

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    @mcp.tool(
        annotations={
            "title": "Commodity Quotes",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def commodity_quotes(
        symbol: str | None = None,
        limit: int = 10,
    ) -> dict:
        return await _fetch_asset_quotes(client, "commodity", symbol, limit)

    @mcp.tool(
        annotations={
            "title": "Crypto Quotes",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def crypto_quotes(
        symbol: str | None = None,
        limit: int = 10,
    ) -> dict:
        return await _fetch_asset_quotes(client, "crypto", symbol, limit)

    @mcp.tool(
        annotations={
            "title": "Forex Quotes",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def forex_quotes(
        symbol: str | None = None,
        limit: int = 10,
    ) -> dict:
        return await _fetch_asset_quotes(client, "forex", symbol, limit)


async def _fetch_asset_quotes(
    client: AsyncFMPDataClient,
    asset_type: str,
    symbol: str | None,
    limit: int,
) -> dict:
    def _timestamp_to_epoch(value) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            ts = float(value)
            return ts / 1000 if ts > 1e12 else ts
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return None
            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(normalized).timestamp()
            except ValueError:
                return None
        return None

    def _quote_age_minutes(quote: dict) -> float | None:
        timestamp = (
            quote.get("timestamp")
            or quote.get("lastUpdated")
            or quote.get("lastUpdatedAt")
            or quote.get("updatedAt")
            or quote.get("date")
        )
        ts_epoch = _timestamp_to_epoch(timestamp)
        if ts_epoch is None:
            return None
        age_seconds = datetime.now(timezone.utc).timestamp() - ts_epoch
        return round(max(age_seconds, 0) / 60, 2)

    limit = max(1, min(limit, 50))

    if symbol:
        symbol = symbol.upper().strip()
        data = await _safe_call(
            client.company.get_quote,
            symbol=symbol,
            ttl=TTL_REALTIME,
            default=None,
        )
        quote = _as_dict(data)
        if not quote:
            return {"error": f"No {asset_type} quote found for '{symbol}'"}

        result = {
            "symbol": symbol,
            "mode": "single",
            "asset_type": asset_type,
            "price": quote.get("price"),
            "change": quote.get("change"),
            "change_pct": quote.get("changePercentage") or quote.get("changesPercentage"),
            "day_low": quote.get("dayLow"),
            "day_high": quote.get("dayHigh"),
            "year_low": quote.get("yearLow"),
            "year_high": quote.get("yearHigh"),
            "volume": quote.get("volume"),
            "name": quote.get("name"),
        }
        age_minutes = _quote_age_minutes(quote)
        if age_minutes is not None:
            result["quote_age_minutes"] = age_minutes
            if asset_type == "commodity" and age_minutes > 15:
                result["_warnings"] = [
                    f"Quote timestamp appears stale ({age_minutes:.1f} minutes old).",
                ]
        return result

    if asset_type == "commodity":
        data = await _safe_call(client.batch.get_commodity_quotes, ttl=TTL_REALTIME, default=[])
    elif asset_type == "crypto":
        data = await _safe_call(client.batch.get_crypto_quotes, ttl=TTL_REALTIME, default=[])
    else:
        data = await _safe_call(client.batch.get_forex_quotes, ttl=TTL_REALTIME, default=[])

    batch_list = _as_list(data)
    if not batch_list:
        return {"error": f"No {asset_type} batch quotes available"}

    batch_list.sort(key=lambda x: abs(x.get("change") or 0), reverse=True)
    quotes = []
    stale_commodity_count = 0
    for q in batch_list[:limit]:
        change = q.get("change")
        price = q.get("price")
        change_pct = q.get("changePercentage") or q.get("changesPercentage")
        if change_pct is None and change is not None and price is not None:
            prev = price - change
            if prev != 0:
                change_pct = round(change / prev * 100, 2)
        entry = {
            "symbol": q.get("symbol"),
            "price": price,
            "change": change,
            "change_pct": change_pct,
            "volume": q.get("volume"),
        }
        age_minutes = _quote_age_minutes(q)
        if age_minutes is not None:
            entry["quote_age_minutes"] = age_minutes
            if asset_type == "commodity" and age_minutes > 15:
                stale_commodity_count += 1
                entry["stale"] = True
        quotes.append(entry)

    result = {
        "mode": "batch",
        "asset_type": asset_type,
        "count": len(quotes),
        "quotes": quotes,
    }
    if asset_type == "commodity" and stale_commodity_count > 0:
        result["_warnings"] = [
            f"{stale_commodity_count} commodity quote(s) appear stale (>15 minutes old).",
        ]
    return result
