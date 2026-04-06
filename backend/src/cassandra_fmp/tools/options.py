"""Options chain tool via Schwab (primary) and Polygon.io (fallback)."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from cassandra_fmp.clients.polygon import PolygonClient
    from cassandra_fmp.clients.schwab import SchwabClient


def _parse_iso_date(raw: str | None, field_name: str) -> tuple[date | None, str | None]:
    if raw is None:
        return None, None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date(), None
    except ValueError:
        return None, f"Invalid {field_name} '{raw}'. Expected YYYY-MM-DD."


def _option_mark(bid: float | None, ask: float | None, last_price: float | None) -> float | None:
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return round((bid + ask) / 2, 4)
    if bid is not None and bid > 0:
        return bid
    if ask is not None and ask > 0:
        return ask
    if last_price is not None and last_price > 0:
        return last_price
    return None


def _build_put_selling_metrics(
    *,
    underlying_price: float | None,
    strike: float | None,
    premium: float | None,
    expiration: str,
    today_dt: date,
) -> dict | None:
    if underlying_price is None or underlying_price <= 0:
        return None
    if strike is None or strike <= 0:
        return None
    if premium is None or premium <= 0:
        return None

    exp_dt, _err = _parse_iso_date(expiration, "expiration")
    if exp_dt is None:
        return None

    days_to_expiry = (exp_dt - today_dt).days
    if days_to_expiry <= 0:
        return None

    breakeven = round(strike - premium, 4)
    cushion_pct = round((underlying_price - breakeven) / underlying_price * 100, 2)
    premium_dollars = round(premium * 100, 2)
    dollars_per_day = round(premium_dollars / days_to_expiry, 2)
    annualized_return_pct = round((premium / strike) * (365 / days_to_expiry) * 100, 2)

    return {
        "premium_dollars": premium_dollars,
        "dollars_per_day": dollars_per_day,
        "days_to_expiry": days_to_expiry,
        "breakeven": breakeven,
        "cushion_pct": cushion_pct,
        "annualized_return_pct": annualized_return_pct,
    }


def _normalize_schwab_contracts(
    data: dict,
    *,
    contract_type_filter: str | None = None,
    strike_gte: float | None = None,
    strike_lte: float | None = None,
    expiry_from_dt: date | None = None,
    expiry_to_dt: date | None = None,
    limit: int = 250,
) -> tuple[list[dict], float | None]:
    """Flatten Schwab's nested expiration maps into a normalized contract list.

    Returns (contracts, underlying_price).
    """
    underlying_price = data.get("underlyingPrice")
    contracts: list[dict] = []

    maps_to_process: list[str] = []
    if contract_type_filter is None or contract_type_filter == "call":
        maps_to_process.append("callExpDateMap")
    if contract_type_filter is None or contract_type_filter == "put":
        maps_to_process.append("putExpDateMap")

    for map_key in maps_to_process:
        exp_map = data.get(map_key, {})
        if not isinstance(exp_map, dict):
            continue

        for exp_key, strikes in exp_map.items():
            # exp_key format: "YYYY-MM-DD:DTE"
            exp_date_str = exp_key.split(":")[0] if ":" in exp_key else exp_key

            # Apply expiration date filters
            exp_dt, _ = _parse_iso_date(exp_date_str, "expiration")
            if exp_dt is not None:
                if expiry_from_dt and exp_dt < expiry_from_dt:
                    continue
                if expiry_to_dt and exp_dt > expiry_to_dt:
                    continue

            if not isinstance(strikes, dict):
                continue

            for _strike_str, contract_list in strikes.items():
                if not isinstance(contract_list, list):
                    continue

                for c in contract_list:
                    strike = c.get("strikePrice")

                    # Apply strike filters
                    if strike_gte is not None and (strike is None or strike < strike_gte):
                        continue
                    if strike_lte is not None and (strike is None or strike > strike_lte):
                        continue

                    put_call = (c.get("putCall") or "").lower()
                    mark = c.get("mark")
                    bid = c.get("bid")
                    ask = c.get("ask")

                    entry = {
                        "ticker": c.get("symbol"),
                        "strike": strike,
                        "type": put_call,
                        "expiration": exp_date_str,
                        "greeks": {
                            "delta": c.get("delta"),
                            "gamma": c.get("gamma"),
                            "theta": c.get("theta"),
                            "vega": c.get("vega"),
                        },
                        "iv": c.get("volatility"),
                        "open_interest": c.get("openInterest"),
                        "volume": c.get("totalVolume"),
                        "last_price": c.get("last"),
                        "bid": bid,
                        "ask": ask,
                        "bid_size": c.get("bidSize"),
                        "ask_size": c.get("askSize"),
                    }

                    # Schwab provides mark directly
                    if mark is not None:
                        entry["mark"] = mark
                    else:
                        computed_mark = _option_mark(bid, ask, c.get("last"))
                        if computed_mark is not None:
                            entry["mark"] = computed_mark

                    contracts.append(entry)

                    if len(contracts) >= limit:
                        return contracts, underlying_price

    return contracts, underlying_price


def _epoch_ms_to_date(ts: int) -> str:
    """Convert epoch millis to YYYY-MM-DD."""
    return datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")


def register(
    mcp: FastMCP,
    *,
    polygon_client: PolygonClient | None = None,
    schwab_client: SchwabClient | None = None,
) -> None:
    @mcp.tool(
        annotations={
            "title": "Options Chain",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def options_chain(
        symbol: str,
        expiration_date: str | None = None,
        expiry_from: str | None = None,
        expiry_to: str | None = None,
        contract_type: str | None = None,
        strike_gte: float | None = None,
        strike_lte: float | None = None,
        limit: int = 100,
    ) -> dict:
        """Get options chain with Greeks, IV, open interest, and bid/ask.

        Returns contracts grouped by expiration with delta, gamma, theta, vega,
        implied volatility, open interest, and volume. Includes put/call ratio
        summary per expiration.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            expiration_date: Filter to specific expiration (YYYY-MM-DD)
            expiry_from: Start expiration date for multi-expiry range mode (YYYY-MM-DD)
            expiry_to: End expiration date for multi-expiry range mode (YYYY-MM-DD)
            contract_type: Filter by "call" or "put"
            strike_gte: Minimum strike price
            strike_lte: Maximum strike price
            limit: Max contracts to return (default 100, max 250)
        """
        symbol = symbol.upper().strip()
        limit = max(1, min(limit, 250))

        if expiration_date and (expiry_from or expiry_to):
            return {
                "error": "Use either expiration_date (single expiry) or expiry_from/expiry_to (range mode), not both."
            }

        expiration_dt, expiration_err = _parse_iso_date(expiration_date, "expiration_date")
        if expiration_err:
            return {"error": expiration_err}
        expiry_from_dt, expiry_from_err = _parse_iso_date(expiry_from, "expiry_from")
        if expiry_from_err:
            return {"error": expiry_from_err}
        expiry_to_dt, expiry_to_err = _parse_iso_date(expiry_to, "expiry_to")
        if expiry_to_err:
            return {"error": expiry_to_err}
        if expiry_from_dt and expiry_to_dt and expiry_from_dt > expiry_to_dt:
            return {"error": "expiry_from must be <= expiry_to."}

        ct = None
        if contract_type:
            ct = contract_type.lower().strip()
            if ct not in ("call", "put"):
                return {"error": f"Invalid contract_type '{contract_type}'. Use 'call' or 'put'."}

        # ------------------------------------------------------------------
        # Try Schwab first
        # ------------------------------------------------------------------
        schwab_contracts = None
        underlying_price = None

        if schwab_client is not None:
            schwab_kwargs: dict = {}
            if ct:
                schwab_kwargs["contract_type"] = ct.upper()
            if expiration_dt:
                schwab_kwargs["from_date"] = expiration_date
                schwab_kwargs["to_date"] = expiration_date
            else:
                if expiry_from_dt:
                    schwab_kwargs["from_date"] = expiry_from
                if expiry_to_dt:
                    schwab_kwargs["to_date"] = expiry_to
            # Request enough strikes to cover the limit
            schwab_kwargs["strike_count"] = max(limit // 2, 20)

            raw = await schwab_client.get_option_chain(symbol, **schwab_kwargs)

            if raw and isinstance(raw, dict):
                # The response is wrapped: raw may be {"status_code": 200, "data": {...}}
                chain_data = raw.get("data", raw)
                if isinstance(chain_data, dict) and chain_data.get("status") == "SUCCESS":
                    schwab_contracts, underlying_price = _normalize_schwab_contracts(
                        chain_data,
                        contract_type_filter=ct,
                        strike_gte=strike_gte,
                        strike_lte=strike_lte,
                        expiry_from_dt=expiry_from_dt if not expiration_dt else None,
                        expiry_to_dt=expiry_to_dt if not expiration_dt else None,
                        limit=limit,
                    )

        if schwab_contracts:
            return _build_chain_result(
                symbol, schwab_contracts, underlying_price, source="schwab"
            )

        # ------------------------------------------------------------------
        # Fallback to Polygon
        # ------------------------------------------------------------------
        if polygon_client is None:
            return {"error": f"No options data available for '{symbol}' (no data sources configured)"}

        params: dict = {"limit": limit}
        if expiration_dt:
            params["expiration_date"] = expiration_date
        if ct:
            params["contract_type"] = ct
        if strike_gte is not None:
            params["strike_price.gte"] = strike_gte
        if strike_lte is not None:
            params["strike_price.lte"] = strike_lte

        data = await polygon_client.get_safe(
            f"/v3/snapshot/options/{symbol}",
            params=params,
            cache_ttl=polygon_client.TTL_REALTIME,
        )

        if not data or not isinstance(data, dict):
            return {"error": f"No options data found for '{symbol}'"}

        results = data.get("results", [])
        if expiry_from_dt or expiry_to_dt:
            filtered_results = []
            for contract in results:
                exp_raw = (contract.get("details") or {}).get("expiration_date")
                exp_dt, _ = _parse_iso_date(exp_raw, "expiration")
                if exp_dt is None:
                    continue
                if expiry_from_dt and exp_dt < expiry_from_dt:
                    continue
                if expiry_to_dt and exp_dt > expiry_to_dt:
                    continue
                filtered_results.append(contract)
            results = filtered_results

        if not results:
            return {"error": f"No options contracts found for '{symbol}' with given filters"}

        # Normalize Polygon contracts
        polygon_contracts, poly_underlying = _normalize_polygon_contracts(results, data)
        return _build_chain_result(
            symbol, polygon_contracts, poly_underlying, source="polygon.io"
        )

    if polygon_client is not None:
        @mcp.tool(
            annotations={
                "title": "Historical Options",
                "readOnlyHint": True,
                "destructiveHint": False,
                "idempotentHint": True,
                "openWorldHint": True,
            }
        )
        async def historical_options(
            option_ticker: str,
            date_from: str,
            date_to: str,
            timespan: str = "day",
            multiplier: int = 1,
            symbol: str | None = None,
            limit: int = 250,
        ) -> dict:
            """Get historical OHLCV price bars for an options contract.

            Returns open, high, low, close, volume, VWAP, and trade count for each
            bar in the date range. Use this for backtesting, charting option price
            history, or analyzing how a specific contract traded over time.

            Args:
                option_ticker: Polygon option ticker (e.g. "O:AAPL250516C00220000").
                    Format: O:{UNDERLYING}{YYMMDD}{C|P}{STRIKE*1000 zero-padded 8 digits}
                date_from: Start date (YYYY-MM-DD)
                date_to: End date (YYYY-MM-DD)
                timespan: Bar size — "minute", "hour", "day", "week", "month" (default "day")
                multiplier: Multiplier for timespan (e.g. 5 with "minute" = 5-min bars, default 1)
                symbol: Optional underlying ticker for context in the response
                limit: Max bars to return (default 250, max 5000)
            """
            option_ticker = option_ticker.strip()
            if not option_ticker.startswith("O:"):
                option_ticker = f"O:{option_ticker}"

            _, from_err = _parse_iso_date(date_from, "date_from")
            if from_err:
                return {"error": from_err}
            _, to_err = _parse_iso_date(date_to, "date_to")
            if to_err:
                return {"error": to_err}

            valid_timespans = {"minute", "hour", "day", "week", "month"}
            if timespan not in valid_timespans:
                return {"error": f"Invalid timespan '{timespan}'. Use one of: {', '.join(sorted(valid_timespans))}"}

            limit = max(1, min(limit, 5000))

            data = await polygon_client.get_safe(
                f"/v2/aggs/ticker/{option_ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}",
                params={"adjusted": "true", "sort": "asc", "limit": limit},
                cache_ttl=polygon_client.TTL_DAILY,
            )

            if not data or not isinstance(data, dict):
                return {"error": f"No historical data found for '{option_ticker}'"}

            results = data.get("results") or []
            if not results:
                return {
                    "error": f"No bars found for '{option_ticker}' from {date_from} to {date_to}",
                    "hint": "Check that the option ticker format is correct: O:AAPL250516C00220000",
                }

            bars = []
            for bar in results:
                bars.append({
                    "date": _epoch_ms_to_date(bar["t"]),
                    "open": bar.get("o"),
                    "high": bar.get("h"),
                    "low": bar.get("l"),
                    "close": bar.get("c"),
                    "volume": bar.get("v"),
                    "vwap": bar.get("vw"),
                    "trades": bar.get("n"),
                })

            return {
                "option_ticker": option_ticker,
                "symbol": symbol,
                "timespan": f"{multiplier}{timespan}" if multiplier > 1 else timespan,
                "date_from": date_from,
                "date_to": date_to,
                "bar_count": len(bars),
                "bars": bars,
                "source": "polygon.io",
            }


def _normalize_polygon_contracts(
    results: list[dict], data: dict
) -> tuple[list[dict], float | None]:
    """Normalize Polygon option snapshot results into our standard contract format."""
    underlying_price = None
    top_underlying = data.get("underlying_asset")
    if isinstance(top_underlying, dict):
        underlying_price = top_underlying.get("price")
    if underlying_price is None:
        for contract in results:
            per_contract_underlying = contract.get("underlying_asset")
            if isinstance(per_contract_underlying, dict) and per_contract_underlying.get("price") is not None:
                underlying_price = per_contract_underlying.get("price")
                break
    if underlying_price is None:
        strike_with_oi = [
            (
                (contract.get("open_interest") or 0),
                (contract.get("details") or {}).get("strike_price"),
            )
            for contract in results
            if isinstance((contract.get("details") or {}).get("strike_price"), (int, float))
        ]
        if strike_with_oi:
            underlying_price = max(strike_with_oi, key=lambda row: row[0])[1]

    contracts: list[dict] = []
    for contract in results:
        details = contract.get("details", {})
        greeks = contract.get("greeks", {})
        day = contract.get("day", {})
        last_quote = contract.get("last_quote", {})

        entry = {
            "ticker": details.get("ticker"),
            "strike": details.get("strike_price"),
            "type": details.get("contract_type"),
            "expiration": details.get("expiration_date", "unknown"),
            "greeks": {
                "delta": greeks.get("delta"),
                "gamma": greeks.get("gamma"),
                "theta": greeks.get("theta"),
                "vega": greeks.get("vega"),
            },
            "iv": contract.get("implied_volatility"),
            "open_interest": contract.get("open_interest"),
            "volume": day.get("volume"),
            "last_price": day.get("close"),
            "bid": last_quote.get("bid"),
            "ask": last_quote.get("ask"),
            "bid_size": last_quote.get("bid_size"),
            "ask_size": last_quote.get("ask_size"),
        }
        mark = _option_mark(entry.get("bid"), entry.get("ask"), entry.get("last_price"))
        if mark is not None:
            entry["mark"] = mark

        contracts.append(entry)

    return contracts, underlying_price


def _build_chain_result(
    symbol: str,
    contracts: list[dict],
    underlying_price: float | None,
    source: str,
) -> dict:
    """Build the final grouped-by-expiration result from normalized contracts."""
    warnings: list[str] = []

    # Check for stale quotes
    zero_bid_ask_count = sum(
        1 for c in contracts
        if (c.get("bid") or 0) == 0 and (c.get("ask") or 0) == 0
    )
    if contracts and zero_bid_ask_count / len(contracts) >= 0.8:
        warnings.append(
            "Data appears delayed/EOD-only: over 80% of contracts show $0 bid/ask."
        )

    if underlying_price is None:
        warnings.append("Unable to resolve underlying price; some computed fields are unavailable.")

    # Add put selling metrics
    today_dt = date.today()
    for entry in contracts:
        if entry.get("type") == "put":
            put_metrics = _build_put_selling_metrics(
                underlying_price=underlying_price,
                strike=entry.get("strike"),
                premium=entry.get("mark"),
                expiration=entry.get("expiration", ""),
                today_dt=today_dt,
            )
            if put_metrics is not None:
                entry["put_selling"] = put_metrics

    # Group by expiration
    by_expiration: dict[str, list[dict]] = {}
    for entry in contracts:
        exp = entry.get("expiration", "unknown")
        by_expiration.setdefault(exp, []).append(entry)

    # Sort contracts within each expiration by strike
    for exp in by_expiration:
        by_expiration[exp].sort(key=lambda c: c.get("strike") or 0)

    # Build per-expiration summaries
    expirations = []
    total_calls = 0
    total_puts = 0
    total_call_oi = 0
    total_put_oi = 0
    first_expiry_atm_implied_move_pct = None

    for exp in sorted(by_expiration.keys()):
        exp_contracts = by_expiration[exp]
        calls = [c for c in exp_contracts if c.get("type") == "call"]
        puts = [c for c in exp_contracts if c.get("type") == "put"]

        call_oi = sum(c.get("open_interest") or 0 for c in calls)
        put_oi = sum(c.get("open_interest") or 0 for c in puts)
        pc_ratio = round(put_oi / call_oi, 2) if call_oi > 0 else None

        total_calls += len(calls)
        total_puts += len(puts)
        total_call_oi += call_oi
        total_put_oi += put_oi

        atm_straddle = {
            "strike": None,
            "premium": None,
            "implied_move_pct": None,
        }
        if underlying_price is not None and calls and puts:
            calls_by_strike = {
                c.get("strike"): c
                for c in calls
                if isinstance(c.get("strike"), (int, float))
            }
            puts_by_strike = {
                p.get("strike"): p
                for p in puts
                if isinstance(p.get("strike"), (int, float))
            }
            common_strikes = sorted(set(calls_by_strike) & set(puts_by_strike))
            if common_strikes:
                atm_strike = min(common_strikes, key=lambda strike: abs(strike - underlying_price))
                call_contract = calls_by_strike[atm_strike]
                put_contract = puts_by_strike[atm_strike]
                call_mark = call_contract.get("mark")
                put_mark = put_contract.get("mark")
                if call_mark is not None and put_mark is not None:
                    premium = round(call_mark + put_mark, 4)
                    implied_move_pct = round(premium / underlying_price * 100, 2) if underlying_price > 0 else None
                    atm_straddle = {
                        "strike": atm_strike,
                        "premium": premium,
                        "implied_move_pct": implied_move_pct,
                    }
                    if first_expiry_atm_implied_move_pct is None:
                        first_expiry_atm_implied_move_pct = implied_move_pct

        best_put_sale = None
        put_candidates = [p for p in puts if isinstance(p.get("put_selling"), dict)]
        if put_candidates:
            best_put = max(
                put_candidates,
                key=lambda p: (p.get("put_selling") or {}).get("annualized_return_pct") or 0,
            )
            best_put_sale = {
                "strike": best_put.get("strike"),
                "mark": best_put.get("mark"),
                **(best_put.get("put_selling") or {}),
            }

        expirations.append({
            "expiration": exp,
            "contract_count": len(exp_contracts),
            "call_count": len(calls),
            "put_count": len(puts),
            "call_open_interest": call_oi,
            "put_open_interest": put_oi,
            "put_call_oi_ratio": pc_ratio,
            "atm_straddle": atm_straddle,
            "best_put_sale": best_put_sale,
            "contracts": exp_contracts,
        })

    overall_pc_ratio = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else None

    result = {
        "symbol": symbol,
        "total_contracts": sum(len(e["contracts"]) for e in expirations),
        "summary": {
            "total_calls": total_calls,
            "total_puts": total_puts,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "overall_put_call_ratio": overall_pc_ratio,
            "underlying_price": underlying_price,
            "atm_implied_move_pct": first_expiry_atm_implied_move_pct,
        },
        "expirations": expirations,
        "source": source,
    }
    if warnings:
        result["_warnings"] = warnings
    return result
