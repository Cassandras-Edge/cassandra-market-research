"""Options chain, historical bars, greeks, IV, OI — all via ThetaData v3.

v3 responses carry contract metadata on every row (symbol, expiration,
strike, right) so we don't need to reconstruct it from ticker strings or
array positions. All tool code works off flat ``list[dict]`` returned by
``ThetaDataClient._flatten()``.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from cassandra_fmp.clients.thetadata import ThetaDataClient


# ---------------------------------------------------------------------------
# Parsing / normalization helpers
# ---------------------------------------------------------------------------


def _parse_iso_date(raw: str | None, field_name: str) -> tuple[date | None, str | None]:
    if raw is None:
        return None, None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date(), None
    except ValueError:
        return None, f"Invalid {field_name} '{raw}'. Expected YYYY-MM-DD."


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _iso_expiration(raw: str | None) -> str | None:
    """Normalize a v3-reported expiration ('2024-11-08') to ISO YYYY-MM-DD.

    v3 returns ISO dates already, so this is mostly a pass-through validator.
    """
    if not raw or not isinstance(raw, str):
        return None
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        return raw
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def _to_yyyymmdd(iso: str) -> str:
    """Convert ISO 'YYYY-MM-DD' to 'YYYYMMDD' (ThetaData's preferred format)."""
    return iso.replace("-", "")


def _strike_param(strike: float) -> str:
    """Format a strike price as v3 expects: three decimal places ('220.000')."""
    return f"{float(strike):.3f}"


def _right_param(contract_type: str | None) -> str | None:
    """Map 'call'/'put' to v3 right param. Returns None for unspecified."""
    if not contract_type:
        return None
    ct = contract_type.lower().strip()
    if ct in ("call", "c"):
        return "call"
    if ct in ("put", "p"):
        return "put"
    return None


def _normalize_contract_type(right: str | None) -> str | None:
    """Map v3's 'CALL'/'PUT' → 'call'/'put' for our output schema."""
    if not right or not isinstance(right, str):
        return None
    r = right.upper().strip()
    if r == "CALL":
        return "call"
    if r == "PUT":
        return "put"
    return None


def _synthesize_occ_ticker(symbol: str, expiration_iso: str, right: str, strike: float) -> str | None:
    """Build an OCC-style ticker for display: O:AAPL250516C00220000."""
    if not (symbol and expiration_iso and right and strike is not None):
        return None
    try:
        yymmdd = expiration_iso.replace("-", "")[2:]
        if len(yymmdd) != 6:
            return None
        r = "C" if right.upper().startswith("C") else "P"
        strike_int = int(round(float(strike) * 1000))
        return f"O:{symbol}{yymmdd}{r}{strike_int:08d}"
    except (ValueError, TypeError):
        return None


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


def _first_positive(*values: float | None) -> float | None:
    for v in values:
        if v is not None and isinstance(v, (int, float)) and v > 0:
            return float(v)
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


# ---------------------------------------------------------------------------
# Chain builder — merges v3 snapshot calls into a normalized contract list
# ---------------------------------------------------------------------------


def _contract_key(row: dict) -> tuple | None:
    """Stable merge key: (expiration_iso, strike_float, right_upper)."""
    exp = _iso_expiration(row.get("expiration"))
    strike = row.get("strike")
    right = row.get("right")
    if exp is None or strike is None or not right:
        return None
    try:
        return (exp, round(float(strike), 4), str(right).upper())
    except (TypeError, ValueError):
        return None


def _merge_snapshot_rows(
    *,
    greeks_rows: list[dict],
    oi_rows: list[dict],
    ohlc_rows: list[dict],
    quote_rows: list[dict],
) -> tuple[list[dict], float | None]:
    """Merge the four v3 snapshot payloads into normalized contracts.

    Returns (contracts, underlying_price). The greeks snapshot is the spine;
    other payloads decorate it via contract key lookup.
    """
    oi_by_key: dict[tuple, int | None] = {}
    for r in oi_rows:
        key = _contract_key(r)
        if key is not None:
            oi_by_key[key] = r.get("open_interest")

    ohlc_by_key: dict[tuple, dict] = {}
    for r in ohlc_rows:
        key = _contract_key(r)
        if key is not None:
            ohlc_by_key[key] = r

    quote_by_key: dict[tuple, dict] = {}
    for r in quote_rows:
        key = _contract_key(r)
        if key is not None:
            quote_by_key[key] = r

    contracts: list[dict] = []
    underlying_price: float | None = None

    for row in greeks_rows:
        key = _contract_key(row)
        if key is None:
            continue

        expiration_iso = _iso_expiration(row.get("expiration")) or "unknown"
        right = row.get("right") or ""
        strike = row.get("strike")
        try:
            strike_f = float(strike) if strike is not None else None
        except (TypeError, ValueError):
            strike_f = None
        ct = _normalize_contract_type(right)

        # Underlying price comes from the greeks snapshot
        u_price = row.get("underlying_price")
        if underlying_price is None and isinstance(u_price, (int, float)) and u_price > 0:
            underlying_price = float(u_price)

        # Prefer dedicated quote snapshot bid/ask (has sizes); fall back to
        # the greeks row which also carries last NBBO bid/ask.
        q = quote_by_key.get(key, {})
        o = ohlc_by_key.get(key, {})

        bid = q.get("bid") if q.get("bid") is not None else row.get("bid")
        ask = q.get("ask") if q.get("ask") is not None else row.get("ask")
        bid_size = q.get("bid_size")
        ask_size = q.get("ask_size")

        last_price = o.get("close")
        volume = o.get("volume")

        ticker = _synthesize_occ_ticker(
            row.get("symbol") or "",
            expiration_iso,
            right,
            strike_f if strike_f is not None else 0.0,
        )

        entry: dict[str, Any] = {
            "ticker": ticker,
            "strike": strike_f,
            "type": ct,
            "expiration": expiration_iso,
            "greeks": {
                "delta": row.get("delta"),
                "gamma": row.get("gamma"),
                "theta": row.get("theta"),
                "vega": row.get("vega"),
            },
            "iv": row.get("implied_vol"),
            "open_interest": oi_by_key.get(key),
            "volume": volume,
            "last_price": last_price,
            "bid": bid,
            "ask": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
        }
        mark = _option_mark(bid, ask, last_price)
        if mark is not None:
            entry["mark"] = mark

        contracts.append(entry)

    return contracts, underlying_price


# ---------------------------------------------------------------------------
# Per-expiration grouping + summary helpers (unchanged contract output schema)
# ---------------------------------------------------------------------------


def _build_chain_result(
    symbol: str,
    contracts: list[dict],
    underlying_price: float | None,
    source: str,
) -> dict:
    """Build the final grouped-by-expiration result from normalized contracts."""
    warnings: list[str] = []

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

    by_expiration: dict[str, list[dict]] = {}
    for entry in contracts:
        exp = entry.get("expiration", "unknown")
        by_expiration.setdefault(exp, []).append(entry)

    for exp in by_expiration:
        by_expiration[exp].sort(key=lambda c: c.get("strike") or 0)

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

        atm_straddle = {"strike": None, "premium": None, "implied_move_pct": None}
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
                call_mark = calls_by_strike[atm_strike].get("mark")
                put_mark = puts_by_strike[atm_strike].get("mark")
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


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def register(
    mcp: FastMCP,
    *,
    theta_client: ThetaDataClient | None = None,
) -> None:
    import asyncio  # noqa: PLC0415

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

        ct = _right_param(contract_type) if contract_type else None
        if contract_type and ct is None:
            return {"error": f"Invalid contract_type '{contract_type}'. Use 'call' or 'put'."}

        if theta_client is None:
            return {"error": f"No options data available for '{symbol}' (ThetaData not configured)"}

        # ------------------------------------------------------------------
        # Determine which expirations to query
        # ------------------------------------------------------------------
        # For a single specific expiration, pass it directly. Otherwise use
        # "*" to fetch every expiration, then client-side filter by range.
        # When no range is given, default to the next ~6 expiries (same
        # budget guard as the v2 implementation).
        target_expiration_param: str
        if expiration_dt:
            target_expiration_param = _yyyymmdd(expiration_dt)
        else:
            target_expiration_param = "*"

        # ------------------------------------------------------------------
        # Fan out the four snapshot calls in parallel
        # ------------------------------------------------------------------
        greeks_task = theta_client.snapshot_greeks_first_order(
            symbol=symbol, expiration=target_expiration_param, right=ct,
        )
        oi_task = theta_client.snapshot_open_interest(
            symbol=symbol, expiration=target_expiration_param, right=ct,
        )
        ohlc_task = theta_client.snapshot_ohlc(
            symbol=symbol, expiration=target_expiration_param, right=ct,
        )
        quote_task = theta_client.snapshot_quote(
            symbol=symbol, expiration=target_expiration_param, right=ct,
        )

        greeks_rows, oi_rows, ohlc_rows, quote_rows = await asyncio.gather(
            greeks_task, oi_task, ohlc_task, quote_task,
        )

        if not greeks_rows:
            return {"error": f"No options data found for '{symbol}'"}

        merged_contracts, underlying_price = _merge_snapshot_rows(
            greeks_rows=greeks_rows,
            oi_rows=oi_rows,
            ohlc_rows=ohlc_rows,
            quote_rows=quote_rows,
        )

        # ------------------------------------------------------------------
        # Apply range / strike / type filters, cap at limit
        # ------------------------------------------------------------------
        # If we pulled a full chain (expiration="*"), narrow to the requested
        # expiration window. Otherwise all rows already match the exact expiry.
        if target_expiration_param == "*" and (expiry_from_dt or expiry_to_dt or
                                                (not expiry_from_dt and not expiry_to_dt)):
            kept: list[dict] = []
            # Default window: next ~6 future expiries (bounded request cost)
            fallback_budget = 6
            today_dt = date.today()

            # Gather unique expirations present
            present_exps = sorted({c.get("expiration") for c in merged_contracts if c.get("expiration")})
            window_exps: set[str] = set()
            if expiry_from_dt or expiry_to_dt:
                for exp in present_exps:
                    exp_dt, _ = _parse_iso_date(exp, "expiration")
                    if exp_dt is None:
                        continue
                    if expiry_from_dt and exp_dt < expiry_from_dt:
                        continue
                    if expiry_to_dt and exp_dt > expiry_to_dt:
                        continue
                    window_exps.add(exp)
            else:
                future_exps: list[str] = []
                for exp in present_exps:
                    exp_dt, _ = _parse_iso_date(exp, "expiration")
                    if exp_dt is None or exp_dt < today_dt:
                        continue
                    future_exps.append(exp)
                window_exps = set(future_exps[:fallback_budget])

            if window_exps:
                merged_contracts = [c for c in merged_contracts if c.get("expiration") in window_exps]

        filtered: list[dict] = []
        ct_name = "call" if ct == "call" else ("put" if ct == "put" else None)
        for c in merged_contracts:
            if ct_name and c.get("type") != ct_name:
                continue
            strike = c.get("strike")
            if strike_gte is not None and (strike is None or strike < strike_gte):
                continue
            if strike_lte is not None and (strike is None or strike > strike_lte):
                continue
            filtered.append(c)
            if len(filtered) >= limit:
                break

        if not filtered:
            return {"error": f"No options contracts found for '{symbol}' with given filters"}

        return _build_chain_result(symbol, filtered, underlying_price, source="thetadata")

    # ------------------------------------------------------------------
    # Historical tools (only registered when ThetaData is wired in)
    # ------------------------------------------------------------------
    if theta_client is None:
        return

    async def _resolve_contract_params(
        *,
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
    ) -> tuple[dict | None, dict | None]:
        """Validate + format the common (symbol, expiration, strike, right) fields.

        Returns (params, error). ``params`` is a dict ready for keyword-splat
        into ThetaDataClient methods; ``error`` is None on success, or a
        user-facing error dict on failure.
        """
        sym = (symbol or "").upper().strip()
        if not sym:
            return None, {"error": "symbol is required"}

        exp_dt, exp_err = _parse_iso_date(expiration, "expiration")
        if exp_err:
            return None, {"error": exp_err}
        if exp_dt is None:
            return None, {"error": "expiration is required (YYYY-MM-DD)"}

        try:
            strike_f = float(strike)
        except (TypeError, ValueError):
            return None, {"error": f"Invalid strike '{strike}'"}
        if strike_f <= 0:
            return None, {"error": "strike must be positive"}

        right_p = _right_param(right)
        if right_p is None:
            return None, {"error": f"Invalid right '{right}'. Use 'call' or 'put'."}

        return (
            {
                "symbol": sym,
                "expiration": _yyyymmdd(exp_dt),
                "strike": _strike_param(strike_f),
                "right": right_p,
            },
            None,
        )

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
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        date_from: str,
        date_to: str,
        timespan: str = "day",
        interval: str | None = None,
    ) -> dict:
        """Get historical OHLCV price bars for an options contract.

        For ``timespan="day"``, ``"week"``, or ``"month"``, hits ThetaData's
        EOD endpoint which provides full OHLCV plus the closing NBBO for each
        day (week/month are rolled up client-side from daily bars).

        For intraday (``timespan="minute"`` or ``"hour"``), hits the intraday
        OHLC endpoint. You can specify an explicit ``interval`` string like
        ``"1m"``, ``"5m"``, ``"15m"``, ``"30m"``, or ``"1h"``. If omitted,
        ``"1m"`` is used for minute and ``"1h"`` is used for hour. Multi-day
        intraday requests are capped at 1 month of data by the upstream API.

        Args:
            symbol: Underlying ticker (e.g. "AAPL")
            expiration: Contract expiration (YYYY-MM-DD)
            strike: Strike price in dollars (e.g. 220.0)
            right: "call" or "put"
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            timespan: Bar size group — "minute", "hour", "day", "week", "month"
            interval: Explicit v3 interval string for intraday mode (e.g. "5m")
        """
        params, err = await _resolve_contract_params(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
        )
        if err:
            return err
        assert params is not None

        from_dt, from_err = _parse_iso_date(date_from, "date_from")
        if from_err:
            return {"error": from_err}
        to_dt, to_err = _parse_iso_date(date_to, "date_to")
        if to_err:
            return {"error": to_err}
        if from_dt is None or to_dt is None:
            return {"error": "date_from and date_to are required"}
        if from_dt > to_dt:
            return {"error": "date_from must be <= date_to"}

        valid_timespans = {"minute", "hour", "day", "week", "month"}
        if timespan not in valid_timespans:
            return {"error": f"Invalid timespan '{timespan}'. Use one of: {', '.join(sorted(valid_timespans))}"}

        start_date = _yyyymmdd(from_dt)
        end_date = _yyyymmdd(to_dt)

        if timespan in ("day", "week", "month"):
            rows = await theta_client.history_eod(
                **params, start_date=start_date, end_date=end_date,
            )
            bars = [
                {
                    "date": _iso_expiration(r.get("last_trade", ""))[:10]
                            if isinstance(r.get("last_trade"), str) and r.get("last_trade")
                            else (_iso_expiration(r.get("created", ""))[:10]
                                  if isinstance(r.get("created"), str) and r.get("created") else None),
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r.get("close"),
                    "volume": r.get("volume"),
                    "trades": r.get("count"),
                    "closing_bid": r.get("bid"),
                    "closing_ask": r.get("ask"),
                    "closing_mid": _option_mark(r.get("bid"), r.get("ask"), r.get("close")),
                }
                for r in rows
            ]
            if timespan in ("week", "month"):
                bars = _rollup_daily_bars(bars, timespan)
        else:
            # Intraday
            resolved_interval = interval or ("1m" if timespan == "minute" else "1h")
            if resolved_interval not in theta_client.VALID_INTERVALS:
                return {
                    "error": f"Invalid interval '{resolved_interval}'. Valid: {sorted(theta_client.VALID_INTERVALS)}"
                }
            rows = await theta_client.history_ohlc(
                **params,
                start_date=start_date,
                end_date=end_date,
                interval=resolved_interval,
            )
            bars = [
                {
                    "timestamp": r.get("timestamp"),
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r.get("close"),
                    "volume": r.get("volume"),
                    "trades": r.get("count"),
                    "vwap": r.get("vwap"),
                }
                for r in rows
            ]

        if not bars:
            return {
                "error": f"No historical data found for {params['symbol']} {params['expiration']} "
                         f"{params['strike']} {params['right']} from {date_from} to {date_to}",
                "hint": "Check the contract exists for the requested date range. ThetaData may "
                        "lag recent dates for some data types.",
            }

        return {
            "symbol": params["symbol"],
            "expiration": expiration,
            "strike": float(params["strike"]),
            "right": params["right"],
            "timespan": timespan,
            "interval": interval,
            "date_from": date_from,
            "date_to": date_to,
            "bar_count": len(bars),
            "bars": bars,
            "source": "thetadata",
        }

    @mcp.tool(
        annotations={
            "title": "Historical Option Implied Volatility",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def historical_option_iv(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        date_from: str,
        date_to: str,
        interval: str = "5m",
    ) -> dict:
        """Historical implied volatility series for an options contract.

        Returns bid IV, mid IV, ask IV, and the underlying price at each
        interval. Useful for IV rank, term-structure analysis, vol-of-vol,
        and comparing realized vs implied vol. Multi-day requests are capped
        at 1 month of data by the upstream API.

        Args:
            symbol: Underlying ticker (e.g. "AAPL")
            expiration: Contract expiration (YYYY-MM-DD)
            strike: Strike price in dollars
            right: "call" or "put"
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            interval: Sampling interval — "1m", "5m", "15m", "30m", "1h" (default "5m")
        """
        params, err = await _resolve_contract_params(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
        )
        if err:
            return err
        assert params is not None

        from_dt, from_err = _parse_iso_date(date_from, "date_from")
        if from_err:
            return {"error": from_err}
        to_dt, to_err = _parse_iso_date(date_to, "date_to")
        if to_err:
            return {"error": to_err}
        if from_dt is None or to_dt is None:
            return {"error": "date_from and date_to are required"}

        if interval not in theta_client.VALID_INTERVALS:
            return {
                "error": f"Invalid interval '{interval}'. Valid: {sorted(theta_client.VALID_INTERVALS)}"
            }

        rows = await theta_client.history_greeks_implied_volatility(
            **params,
            start_date=_yyyymmdd(from_dt),
            end_date=_yyyymmdd(to_dt),
            interval=interval,
        )
        if not rows:
            return {"error": f"No IV data found for the specified contract and date range"}

        points = [
            {
                "timestamp": r.get("timestamp"),
                "bid": r.get("bid"),
                "bid_iv": r.get("bid_implied_vol"),
                "mid": r.get("midpoint"),
                "mid_iv": r.get("implied_vol"),
                "ask": r.get("ask"),
                "ask_iv": r.get("ask_implied_vol"),
                "iv_error": r.get("iv_error"),
                "underlying_price": r.get("underlying_price"),
            }
            for r in rows
        ]

        mid_ivs = [p["mid_iv"] for p in points if isinstance(p["mid_iv"], (int, float)) and p["mid_iv"] > 0]
        summary: dict[str, Any] = {"point_count": len(points)}
        if mid_ivs:
            summary["mid_iv_min"] = round(min(mid_ivs), 4)
            summary["mid_iv_max"] = round(max(mid_ivs), 4)
            summary["mid_iv_avg"] = round(sum(mid_ivs) / len(mid_ivs), 4)
            summary["mid_iv_first"] = round(mid_ivs[0], 4)
            summary["mid_iv_last"] = round(mid_ivs[-1], 4)

        return {
            "symbol": params["symbol"],
            "expiration": expiration,
            "strike": float(params["strike"]),
            "right": params["right"],
            "interval": interval,
            "date_from": date_from,
            "date_to": date_to,
            "summary": summary,
            "points": points,
            "source": "thetadata",
        }

    @mcp.tool(
        annotations={
            "title": "Historical Option Greeks",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def historical_option_greeks(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        date_from: str,
        date_to: str,
        interval: str = "5m",
    ) -> dict:
        """Historical first-order greeks time series for an options contract.

        Returns delta, gamma, theta, vega, rho, plus implied vol and
        underlying price at each interval. Multi-day requests are capped at
        1 month of data.

        Args:
            symbol: Underlying ticker (e.g. "AAPL")
            expiration: Contract expiration (YYYY-MM-DD)
            strike: Strike price in dollars
            right: "call" or "put"
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            interval: Sampling interval — "1m", "5m", "15m", "30m", "1h" (default "5m")
        """
        params, err = await _resolve_contract_params(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
        )
        if err:
            return err
        assert params is not None

        from_dt, from_err = _parse_iso_date(date_from, "date_from")
        if from_err:
            return {"error": from_err}
        to_dt, to_err = _parse_iso_date(date_to, "date_to")
        if to_err:
            return {"error": to_err}
        if from_dt is None or to_dt is None:
            return {"error": "date_from and date_to are required"}

        if interval not in theta_client.VALID_INTERVALS:
            return {
                "error": f"Invalid interval '{interval}'. Valid: {sorted(theta_client.VALID_INTERVALS)}"
            }

        rows = await theta_client.history_greeks_first_order(
            **params,
            start_date=_yyyymmdd(from_dt),
            end_date=_yyyymmdd(to_dt),
            interval=interval,
        )
        if not rows:
            return {"error": f"No greeks data found for the specified contract and date range"}

        points = [
            {
                "timestamp": r.get("timestamp"),
                "bid": r.get("bid"),
                "ask": r.get("ask"),
                "delta": r.get("delta"),
                "gamma": None,  # first_order does not return gamma — use historical_option_iv for IV or snapshot chain for gamma
                "theta": r.get("theta"),
                "vega": r.get("vega"),
                "rho": r.get("rho"),
                "implied_vol": r.get("implied_vol"),
                "underlying_price": r.get("underlying_price"),
            }
            for r in rows
        ]

        return {
            "symbol": params["symbol"],
            "expiration": expiration,
            "strike": float(params["strike"]),
            "right": params["right"],
            "interval": interval,
            "date_from": date_from,
            "date_to": date_to,
            "point_count": len(points),
            "points": points,
            "source": "thetadata",
        }

    @mcp.tool(
        annotations={
            "title": "Historical Option Open Interest",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def historical_option_oi(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        date_from: str,
        date_to: str,
    ) -> dict:
        """Historical daily open interest series for an options contract.

        OPRA reports OI once per day around 06:30 ET, representing the OI
        at the end of the previous trading day. Useful for tracking
        positioning changes and contract stickiness.

        Args:
            symbol: Underlying ticker (e.g. "AAPL")
            expiration: Contract expiration (YYYY-MM-DD)
            strike: Strike price in dollars
            right: "call" or "put"
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
        """
        params, err = await _resolve_contract_params(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
        )
        if err:
            return err
        assert params is not None

        from_dt, from_err = _parse_iso_date(date_from, "date_from")
        if from_err:
            return {"error": from_err}
        to_dt, to_err = _parse_iso_date(date_to, "date_to")
        if to_err:
            return {"error": to_err}
        if from_dt is None or to_dt is None:
            return {"error": "date_from and date_to are required"}

        rows = await theta_client.history_open_interest(
            **params,
            start_date=_yyyymmdd(from_dt),
            end_date=_yyyymmdd(to_dt),
        )
        if not rows:
            return {"error": f"No OI data found for the specified contract and date range"}

        points = [
            {"timestamp": r.get("timestamp"), "open_interest": r.get("open_interest")}
            for r in rows
        ]
        oi_vals = [p["open_interest"] for p in points if isinstance(p["open_interest"], (int, float))]
        summary: dict[str, Any] = {"point_count": len(points)}
        if oi_vals:
            summary["oi_min"] = min(oi_vals)
            summary["oi_max"] = max(oi_vals)
            summary["oi_first"] = oi_vals[0]
            summary["oi_last"] = oi_vals[-1]
            summary["oi_change"] = oi_vals[-1] - oi_vals[0]

        return {
            "symbol": params["symbol"],
            "expiration": expiration,
            "strike": float(params["strike"]),
            "right": params["right"],
            "date_from": date_from,
            "date_to": date_to,
            "summary": summary,
            "points": points,
            "source": "thetadata",
        }

    @mcp.tool(
        annotations={
            "title": "Historical Option Daily",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def historical_option_daily(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        date_from: str,
        date_to: str,
    ) -> dict:
        """Daily EOD bars + greeks + IV + underlying price for an options contract.

        Wraps ThetaData's ``/option/history/greeks/eod`` endpoint, which
        returns one row per day containing: OHLCV, trade count, closing NBBO
        bid/ask, first-order greeks (delta/gamma/theta/vega/rho), implied
        vol, and underlying price. Unlike the intraday IV/greeks tools,
        this endpoint has **no 1-month cap** — multi-year backtests and
        long-range IV rank calculations just work in a single call.

        Args:
            symbol: Underlying ticker (e.g. "AAPL")
            expiration: Contract expiration (YYYY-MM-DD)
            strike: Strike price in dollars
            right: "call" or "put"
            date_from: Start date (YYYY-MM-DD) — no range cap
            date_to: End date (YYYY-MM-DD)
        """
        params, err = await _resolve_contract_params(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
        )
        if err:
            return err
        assert params is not None

        from_dt, from_err = _parse_iso_date(date_from, "date_from")
        if from_err:
            return {"error": from_err}
        to_dt, to_err = _parse_iso_date(date_to, "date_to")
        if to_err:
            return {"error": to_err}
        if from_dt is None or to_dt is None:
            return {"error": "date_from and date_to are required"}

        rows = await theta_client.history_greeks_eod(
            symbol=params["symbol"],
            expiration=params["expiration"],
            strike=params["strike"],
            right=params["right"],
            start_date=_yyyymmdd(from_dt),
            end_date=_yyyymmdd(to_dt),
        )
        if not rows:
            return {"error": f"No daily data found for the specified contract and date range"}

        points = [
            {
                "date": (r.get("timestamp") or "")[:10] if isinstance(r.get("timestamp"), str) else None,
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "volume": r.get("volume"),
                "trades": r.get("count"),
                "closing_bid": r.get("bid"),
                "closing_ask": r.get("ask"),
                "closing_mid": _option_mark(r.get("bid"), r.get("ask"), r.get("close")),
                "delta": r.get("delta"),
                "gamma": r.get("gamma"),
                "theta": r.get("theta"),
                "vega": r.get("vega"),
                "rho": r.get("rho"),
                "implied_vol": r.get("implied_vol"),
                "iv_error": r.get("iv_error"),
                "underlying_price": r.get("underlying_price"),
            }
            for r in rows
        ]

        # Summary: IV rank/percentile from the returned series
        mid_ivs = [p["implied_vol"] for p in points
                   if isinstance(p["implied_vol"], (int, float)) and p["implied_vol"] > 0]
        summary: dict[str, Any] = {"point_count": len(points)}
        if mid_ivs:
            iv_min = min(mid_ivs)
            iv_max = max(mid_ivs)
            iv_cur = mid_ivs[-1]
            summary["iv_min"] = round(iv_min, 4)
            summary["iv_max"] = round(iv_max, 4)
            summary["iv_avg"] = round(sum(mid_ivs) / len(mid_ivs), 4)
            summary["iv_first"] = round(mid_ivs[0], 4)
            summary["iv_last"] = round(iv_cur, 4)
            if iv_max > iv_min:
                summary["iv_rank"] = round((iv_cur - iv_min) / (iv_max - iv_min) * 100, 1)
            # IV percentile = % of days mid_iv was below current
            below = sum(1 for v in mid_ivs if v < iv_cur)
            summary["iv_percentile"] = round(below / len(mid_ivs) * 100, 1)

        return {
            "symbol": params["symbol"],
            "expiration": expiration,
            "strike": float(params["strike"]),
            "right": params["right"],
            "date_from": date_from,
            "date_to": date_to,
            "summary": summary,
            "points": points,
            "source": "thetadata",
        }

    @mcp.tool(
        annotations={
            "title": "Option Quote At Time",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def option_quote_at_time(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        date_from: str,
        date_to: str,
        time_of_day: str,
    ) -> dict:
        """Last NBBO quote at a specific wall-clock time on each date in a range.

        Perfect for event studies — "what was AAPL 220C quoted at 2:00pm ET
        on each FOMC day?" The ``time_of_day`` parameter accepts 24-hour
        format like "14:00:00" or "14:00:00.000".

        Args:
            symbol: Underlying ticker (e.g. "AAPL")
            expiration: Contract expiration (YYYY-MM-DD)
            strike: Strike price in dollars
            right: "call" or "put"
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            time_of_day: Wall-clock time in 24h format (e.g. "14:00:00")
        """
        params, err = await _resolve_contract_params(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
        )
        if err:
            return err
        assert params is not None

        from_dt, from_err = _parse_iso_date(date_from, "date_from")
        if from_err:
            return {"error": from_err}
        to_dt, to_err = _parse_iso_date(date_to, "date_to")
        if to_err:
            return {"error": to_err}
        if from_dt is None or to_dt is None:
            return {"error": "date_from and date_to are required"}

        if not time_of_day or not isinstance(time_of_day, str):
            return {"error": "time_of_day is required (e.g. '14:00:00')"}

        rows = await theta_client.at_time_quote(
            **params,
            start_date=_yyyymmdd(from_dt),
            end_date=_yyyymmdd(to_dt),
            time_of_day=time_of_day,
        )
        if not rows:
            return {"error": f"No quote data found for the specified contract/time/date range"}

        snapshots = [
            {
                "timestamp": r.get("timestamp"),
                "bid": r.get("bid"),
                "ask": r.get("ask"),
                "bid_size": r.get("bid_size"),
                "ask_size": r.get("ask_size"),
                "mid": _option_mark(r.get("bid"), r.get("ask"), None),
            }
            for r in rows
        ]

        return {
            "symbol": params["symbol"],
            "expiration": expiration,
            "strike": float(params["strike"]),
            "right": params["right"],
            "time_of_day": time_of_day,
            "date_from": date_from,
            "date_to": date_to,
            "snapshot_count": len(snapshots),
            "snapshots": snapshots,
            "source": "thetadata",
        }

    # ------------------------------------------------------------------
    # Trade flow — aggressor classification from tick-level trade_quote
    # ------------------------------------------------------------------

    @mcp.tool(
        annotations={
            "title": "Option Trade Flow",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def option_trade_flow(
        symbol: str,
        expiration: str,
        date: str,
        strike: float | None = None,
        strike_from: float | None = None,
        strike_to: float | None = None,
        right: str = "both",
        bucket_minutes: int = 30,
    ) -> dict:
        """Tick-level trade flow with NBBO-based aggressor classification.

        Fetches every OPRA trade for the given contract(s) on a single day,
        paired with the NBBO at time of trade. Classifies each trade as
        buyer-initiated (price >= ask), seller-initiated (price <= bid),
        or indeterminate (between). Aggregates into time buckets and
        per-strike summaries.

        Use strike=None (default) to get all strikes on the expiration.
        Use strike for a single strike, or strike_from/strike_to for a range.

        Args:
            symbol: Underlying ticker (e.g. "SPY")
            expiration: Contract expiration (YYYY-MM-DD)
            date: Trading date to analyze (YYYY-MM-DD)
            strike: Single strike price (overrides strike_from/strike_to)
            strike_from: Minimum strike price (inclusive)
            strike_to: Maximum strike price (inclusive)
            right: "call", "put", or "both" (default "both")
            bucket_minutes: Time bucket size in minutes (default 30)
        """
        if theta_client is None:
            return {"error": "ThetaData not configured"}

        sym = (symbol or "").upper().strip()
        if not sym:
            return {"error": "symbol is required"}

        exp_dt, exp_err = _parse_iso_date(expiration, "expiration")
        if exp_err:
            return {"error": exp_err}
        if exp_dt is None:
            return {"error": "expiration is required (YYYY-MM-DD)"}

        date_dt, date_err = _parse_iso_date(date, "date")
        if date_err:
            return {"error": date_err}
        if date_dt is None:
            return {"error": "date is required (YYYY-MM-DD)"}

        right_lower = (right or "both").lower().strip()
        if right_lower not in ("call", "put", "both"):
            return {"error": f"Invalid right '{right}'. Use 'call', 'put', or 'both'."}

        # Determine strike param
        if strike is not None:
            strike_param = _strike_param(strike)
        else:
            strike_param = "*"

        bucket_minutes = max(1, min(bucket_minutes, 390))
        date_str = _yyyymmdd(date_dt)
        exp_str = _yyyymmdd(exp_dt)

        rows = await theta_client.history_trade_quote(
            symbol=sym,
            expiration=exp_str,
            strike=strike_param,
            right=right_lower if right_lower != "both" else "both",
            start_date=date_str,
            end_date=date_str,
        )

        if not rows:
            return {
                "error": f"No trade data found for {sym} {expiration} on {date}",
                "hint": "ThetaData may not have tick data for this date yet, "
                        "or the terminal may be unreachable.",
            }

        # Filter by strike range if specified
        if strike is None and (strike_from is not None or strike_to is not None):
            filtered = []
            for r in rows:
                s = r.get("strike")
                if s is None:
                    continue
                try:
                    sf = float(s)
                except (TypeError, ValueError):
                    continue
                if strike_from is not None and sf < strike_from:
                    continue
                if strike_to is not None and sf > strike_to:
                    continue
                filtered.append(r)
            rows = filtered

        # Classify each trade
        trades: list[dict] = []
        for r in rows:
            price = r.get("price")
            size = r.get("size")
            bid = r.get("bid")
            ask = r.get("ask")
            if price is None or size is None:
                continue
            try:
                price_f = float(price)
                size_i = int(size)
                bid_f = float(bid) if bid is not None else 0.0
                ask_f = float(ask) if ask is not None else 0.0
            except (TypeError, ValueError):
                continue
            if size_i <= 0 or price_f <= 0:
                continue

            # Aggressor classification
            if bid_f <= 0 or ask_f <= 0:
                side = "unknown"
            elif price_f >= ask_f:
                side = "buy"
            elif price_f <= bid_f:
                side = "sell"
            else:
                side = "mid"

            s_raw = r.get("strike")
            try:
                strike_f = float(s_raw) if s_raw is not None else None
            except (TypeError, ValueError):
                strike_f = None

            ct = _normalize_contract_type(r.get("right"))
            ts = r.get("trade_timestamp") or r.get("timestamp") or ""

            trades.append({
                "price": price_f,
                "size": size_i,
                "side": side,
                "strike": strike_f,
                "right": ct,
                "dollars": round(price_f * size_i * 100, 2),
                "timestamp": ts,
                "bid": bid_f,
                "ask": ask_f,
                "exchange": r.get("exchange"),
                "condition": r.get("condition"),
            })

        if not trades:
            return {"error": f"No valid trades found for {sym} {expiration} on {date}"}

        # --- Aggregate by strike ---
        by_strike: dict[tuple, dict] = {}
        for t in trades:
            key = (t["strike"], t["right"])
            if key not in by_strike:
                by_strike[key] = {
                    "strike": t["strike"], "right": t["right"],
                    "buy_vol": 0, "sell_vol": 0, "mid_vol": 0, "unknown_vol": 0,
                    "buy_dollars": 0.0, "sell_dollars": 0.0,
                    "trade_count": 0,
                }
            b = by_strike[key]
            b["trade_count"] += 1
            b[f"{t['side']}_vol"] += t["size"]
            if t["side"] in ("buy", "sell"):
                b[f"{t['side']}_dollars"] += t["dollars"]

        strike_summary = sorted(by_strike.values(), key=lambda x: (x["strike"] or 0))
        for s in strike_summary:
            s["net_vol"] = s["buy_vol"] - s["sell_vol"]
            s["net_dollars"] = round(s["buy_dollars"] - s["sell_dollars"], 2)
            total = s["buy_vol"] + s["sell_vol"]
            if total > 0:
                buy_pct = s["buy_vol"] / total
                if buy_pct >= 0.6:
                    s["signal"] = "buy_dominant"
                elif buy_pct <= 0.4:
                    s["signal"] = "sell_dominant"
                else:
                    s["signal"] = "neutral"
            else:
                s["signal"] = "unknown"
            s["buy_dollars"] = round(s["buy_dollars"], 2)
            s["sell_dollars"] = round(s["sell_dollars"], 2)

        # --- Aggregate by time bucket ---
        def _bucket_key(ts: str) -> str:
            """Parse timestamp and bucket into N-minute windows."""
            if not ts:
                return "unknown"
            try:
                # v3 timestamps: "2026-04-09T10:30:00" or similar ISO
                if "T" in ts:
                    time_part = ts.split("T")[1][:8]
                else:
                    time_part = ts[:8]
                parts = time_part.split(":")
                h, m = int(parts[0]), int(parts[1])
                total_min = h * 60 + m
                bucket_start = (total_min // bucket_minutes) * bucket_minutes
                bh, bm = divmod(bucket_start, 60)
                bucket_end = bucket_start + bucket_minutes
                eh, em = divmod(bucket_end, 60)
                return f"{bh:02d}:{bm:02d}-{eh:02d}:{em:02d}"
            except (ValueError, IndexError):
                return "unknown"

        by_time: dict[str, dict] = {}
        for t in trades:
            bk = _bucket_key(t["timestamp"])
            if bk not in by_time:
                by_time[bk] = {
                    "bucket": bk,
                    "buy_vol": 0, "sell_vol": 0, "mid_vol": 0, "unknown_vol": 0,
                    "buy_dollars": 0.0, "sell_dollars": 0.0,
                    "trade_count": 0,
                }
            b = by_time[bk]
            b["trade_count"] += 1
            b[f"{t['side']}_vol"] += t["size"]
            if t["side"] in ("buy", "sell"):
                b[f"{t['side']}_dollars"] += t["dollars"]

        time_summary = sorted(by_time.values(), key=lambda x: x["bucket"])
        for b in time_summary:
            b["net_vol"] = b["buy_vol"] - b["sell_vol"]
            b["net_dollars"] = round(b["buy_dollars"] - b["sell_dollars"], 2)
            b["buy_dollars"] = round(b["buy_dollars"], 2)
            b["sell_dollars"] = round(b["sell_dollars"], 2)

        # --- Totals ---
        total_buy = sum(t["size"] for t in trades if t["side"] == "buy")
        total_sell = sum(t["size"] for t in trades if t["side"] == "sell")
        total_mid = sum(t["size"] for t in trades if t["side"] == "mid")
        total_unknown = sum(t["size"] for t in trades if t["side"] == "unknown")
        total_buy_dollars = round(sum(t["dollars"] for t in trades if t["side"] == "buy"), 2)
        total_sell_dollars = round(sum(t["dollars"] for t in trades if t["side"] == "sell"), 2)

        return {
            "symbol": sym,
            "expiration": expiration,
            "date": date,
            "trade_count": len(trades),
            "total_volume": total_buy + total_sell + total_mid + total_unknown,
            "net_flow": {
                "buy": total_buy,
                "sell": total_sell,
                "mid": total_mid,
                "unknown": total_unknown,
            },
            "net_premium": {
                "buy_dollars": total_buy_dollars,
                "sell_dollars": total_sell_dollars,
                "net_dollars": round(total_buy_dollars - total_sell_dollars, 2),
            },
            "by_strike": strike_summary,
            "by_time": time_summary,
            "source": "thetadata",
        }


# ---------------------------------------------------------------------------
# Week / month rollup from daily bars
# ---------------------------------------------------------------------------


def _rollup_daily_bars(bars: list[dict], target: str) -> list[dict]:
    """Aggregate daily bars into ISO weeks ('week') or calendar months ('month')."""
    if target not in ("week", "month") or not bars:
        return bars

    grouped: dict[str, list[dict]] = {}
    for bar in bars:
        date_str = bar.get("date")
        if not date_str:
            continue
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if target == "week":
            iso_year, iso_week, _ = d.isocalendar()
            key = f"{iso_year}-W{iso_week:02d}"
        else:
            key = d.strftime("%Y-%m")
        grouped.setdefault(key, []).append(bar)

    out: list[dict] = []
    for key in sorted(grouped.keys()):
        members = grouped[key]
        if not members:
            continue
        opens = [m.get("open") for m in members if m.get("open") is not None]
        highs = [m.get("high") for m in members if m.get("high") is not None]
        lows = [m.get("low") for m in members if m.get("low") is not None]
        closes = [m.get("close") for m in members if m.get("close") is not None]
        volumes = [m.get("volume") or 0 for m in members]
        trades = [m.get("trades") or 0 for m in members]
        out.append({
            "date": members[0].get("date"),
            "period": key,
            "open": opens[0] if opens else None,
            "high": max(highs) if highs else None,
            "low": min(lows) if lows else None,
            "close": closes[-1] if closes else None,
            "volume": sum(volumes),
            "trades": sum(trades),
        })
    return out
