"""Options chain + historical option bars via ThetaData."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from cassandra_fmp.clients.thetadata import ThetaDataClient


# ---------------------------------------------------------------------------
# Date / numeric helpers
# ---------------------------------------------------------------------------


def _parse_iso_date(raw: str | None, field_name: str) -> tuple[date | None, str | None]:
    if raw is None:
        return None, None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date(), None
    except ValueError:
        return None, f"Invalid {field_name} '{raw}'. Expected YYYY-MM-DD."


def _yyyymmdd_to_iso(yyyymmdd: int | str | None) -> str | None:
    if yyyymmdd is None:
        return None
    try:
        s = str(int(yyyymmdd))
    except (TypeError, ValueError):
        return None
    if len(s) != 8:
        return None
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


def _iso_to_yyyymmdd(iso: str | None) -> int | None:
    if not iso:
        return None
    try:
        return int(datetime.strptime(iso, "%Y-%m-%d").strftime("%Y%m%d"))
    except ValueError:
        return None


def _theta_strike_to_dollars(strike: int | float | None) -> float | None:
    """ThetaData strike is in 1/10 of a cent — divide by 1000 for dollars."""
    if strike is None:
        return None
    try:
        return round(float(strike) / 1000.0, 4)
    except (TypeError, ValueError):
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
# ThetaData response parsing
# ---------------------------------------------------------------------------


def _format_index(header: dict | None, name: str) -> int | None:
    """Look up the position of a named field in a ThetaData header.format list."""
    if not isinstance(header, dict):
        return None
    fmt = header.get("format")
    if not isinstance(fmt, list):
        return None
    try:
        return fmt.index(name)
    except ValueError:
        return None


def _first_tick_value(ticks: Any, idx: int | None) -> Any:
    """Pull a value out of the first tick row by column index."""
    if idx is None or not isinstance(ticks, list) or not ticks:
        return None
    row = ticks[0]
    if not isinstance(row, list) or idx >= len(row):
        return None
    return row[idx]


def _contract_key(contract: dict | None) -> tuple | None:
    """Stable merge key for a ThetaData contract: (exp_int, strike_int, right_str)."""
    if not isinstance(contract, dict):
        return None
    exp = contract.get("expiration")
    strike = contract.get("strike")
    right = contract.get("right")
    if exp is None or strike is None or not right:
        return None
    return (int(exp), int(strike), str(right).upper())


def _build_thetadata_contracts(
    *,
    greeks_payload: dict | None,
    quote_payload: dict | None,
    oi_payload: dict | None,
    ohlc_payload: dict | None,
) -> tuple[list[dict], float | None]:
    """Merge the four bulk_snapshot payloads into our normalized contract list."""

    # Greeks payload is the spine — it's the only one with greeks/IV/underlying.
    if not isinstance(greeks_payload, dict):
        return [], None

    g_header = greeks_payload.get("header") or {}
    g_idx = {
        "bid": _format_index(g_header, "bid"),
        "ask": _format_index(g_header, "ask"),
        "delta": _format_index(g_header, "delta"),
        "gamma": _format_index(g_header, "gamma"),
        "theta": _format_index(g_header, "theta"),
        "vega": _format_index(g_header, "vega"),
        "iv": _format_index(g_header, "implied_vol"),
        "underlying_price": _format_index(g_header, "underlying_price"),
    }

    # Side payloads — index by contract key for cheap merges.
    quote_by_key: dict[tuple, dict] = {}
    if isinstance(quote_payload, dict):
        q_header = quote_payload.get("header") or {}
        q_idx = {
            "bid_size": _format_index(q_header, "bid_size"),
            "ask_size": _format_index(q_header, "ask_size"),
            "bid": _format_index(q_header, "bid"),
            "ask": _format_index(q_header, "ask"),
        }
        for entry in quote_payload.get("response") or []:
            key = _contract_key(entry.get("contract"))
            if key is None:
                continue
            ticks = entry.get("ticks")
            quote_by_key[key] = {
                "bid_size": _first_tick_value(ticks, q_idx["bid_size"]),
                "ask_size": _first_tick_value(ticks, q_idx["ask_size"]),
                "bid": _first_tick_value(ticks, q_idx["bid"]),
                "ask": _first_tick_value(ticks, q_idx["ask"]),
            }

    oi_by_key: dict[tuple, int | None] = {}
    if isinstance(oi_payload, dict):
        o_header = oi_payload.get("header") or {}
        oi_idx = _format_index(o_header, "open_interest")
        for entry in oi_payload.get("response") or []:
            key = _contract_key(entry.get("contract"))
            if key is None:
                continue
            oi_by_key[key] = _first_tick_value(entry.get("ticks"), oi_idx)

    ohlc_by_key: dict[tuple, dict] = {}
    if isinstance(ohlc_payload, dict):
        h_header = ohlc_payload.get("header") or {}
        h_idx = {
            "volume": _format_index(h_header, "volume"),
            "close": _format_index(h_header, "close"),
        }
        for entry in ohlc_payload.get("response") or []:
            key = _contract_key(entry.get("contract"))
            if key is None:
                continue
            ticks = entry.get("ticks")
            ohlc_by_key[key] = {
                "volume": _first_tick_value(ticks, h_idx["volume"]),
                "close": _first_tick_value(ticks, h_idx["close"]),
            }

    # Build contracts off the greeks payload.
    contracts: list[dict] = []
    underlying_price: float | None = None
    for entry in greeks_payload.get("response") or []:
        contract_meta = entry.get("contract") or {}
        key = _contract_key(contract_meta)
        if key is None:
            continue

        ticks = entry.get("ticks") or []
        bid = _first_tick_value(ticks, g_idx["bid"])
        ask = _first_tick_value(ticks, g_idx["ask"])
        underlying = _first_tick_value(ticks, g_idx["underlying_price"])
        if underlying_price is None and isinstance(underlying, (int, float)) and underlying > 0:
            underlying_price = float(underlying)

        strike_dollars = _theta_strike_to_dollars(contract_meta.get("strike"))
        right = (contract_meta.get("right") or "").upper()
        contract_type = "call" if right == "C" else "put" if right == "P" else None
        exp_iso = _yyyymmdd_to_iso(contract_meta.get("expiration"))
        root = contract_meta.get("root") or ""

        # Synthesize an OCC-style ticker so downstream renderers (workflows,
        # historical_options) have something stable to call back into.
        ticker = None
        if root and exp_iso and strike_dollars is not None and right in ("C", "P"):
            yymmdd = exp_iso.replace("-", "")[2:]
            strike_int = int(round(strike_dollars * 1000))
            ticker = f"O:{root}{yymmdd}{right}{strike_int:08d}"

        merged_quote = quote_by_key.get(key, {})
        # Prefer last NBBO from the quote endpoint when available; fall back
        # to the bid/ask carried inside the all_greeks payload.
        eff_bid = merged_quote.get("bid") if merged_quote.get("bid") is not None else bid
        eff_ask = merged_quote.get("ask") if merged_quote.get("ask") is not None else ask
        ohlc_data = ohlc_by_key.get(key, {})
        last_price = ohlc_data.get("close")

        out: dict[str, Any] = {
            "ticker": ticker,
            "strike": strike_dollars,
            "type": contract_type,
            "expiration": exp_iso or "unknown",
            "greeks": {
                "delta": _first_tick_value(ticks, g_idx["delta"]),
                "gamma": _first_tick_value(ticks, g_idx["gamma"]),
                "theta": _first_tick_value(ticks, g_idx["theta"]),
                "vega": _first_tick_value(ticks, g_idx["vega"]),
            },
            "iv": _first_tick_value(ticks, g_idx["iv"]),
            "open_interest": oi_by_key.get(key),
            "volume": ohlc_data.get("volume"),
            "last_price": last_price,
            "bid": eff_bid,
            "ask": eff_ask,
            "bid_size": merged_quote.get("bid_size"),
            "ask_size": merged_quote.get("ask_size"),
        }
        mark = _option_mark(eff_bid, eff_ask, last_price)
        if mark is not None:
            out["mark"] = mark

        contracts.append(out)

    return contracts, underlying_price


# ---------------------------------------------------------------------------
# Polygon-style option ticker parsing (for historical_options compatibility)
# ---------------------------------------------------------------------------


def _parse_occ_ticker(raw: str) -> tuple[str, int, str, int] | None:
    """Parse 'O:AAPL250516C00220000' → (root, exp_int, right, strike_int).

    The strike at the end is the dollar strike * 1000 (so $220.00 → 00220000).
    ThetaData wants the same strike representation (1/10 cent), so we pass
    that integer through unchanged.
    """
    s = raw.strip().lstrip("O:").strip()
    if len(s) < 16:
        return None

    # Walk back from the end: 8-digit strike, 1-char right, 6-digit yymmdd
    try:
        strike_int = int(s[-8:])
        right = s[-9].upper()
        yymmdd = s[-15:-9]
        root = s[:-15]
    except (ValueError, IndexError):
        return None

    if right not in ("C", "P") or not root:
        return None
    try:
        # YYMMDD → YYYYMMDD (assume 20xx; ThetaData rejects pre-2000 anyway)
        yy = int(yymmdd[0:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        exp_int = (2000 + yy) * 10000 + mm * 100 + dd
    except ValueError:
        return None

    return root, exp_int, right, strike_int


def _bar_date_label(date_int: int | None) -> str | None:
    if date_int is None:
        return None
    try:
        s = str(int(date_int))
    except (TypeError, ValueError):
        return None
    if len(s) != 8:
        return None
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


def _ms_to_clock(ms: int | None) -> str | None:
    if ms is None:
        return None
    try:
        ms_int = int(ms)
    except (TypeError, ValueError):
        return None
    seconds = ms_int // 1000
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def register(
    mcp: FastMCP,
    *,
    theta_client: ThetaDataClient | None = None,
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

        if theta_client is None:
            return {"error": f"No options data available for '{symbol}' (ThetaData not configured)"}

        # ------------------------------------------------------------------
        # Determine which expirations to fetch
        # ------------------------------------------------------------------
        all_expirations = await theta_client.list_expirations(symbol)
        if not all_expirations:
            return {"error": f"No options expirations found for '{symbol}'"}

        target_expirations: list[str] = []
        if expiration_dt:
            wanted = expiration_dt.strftime("%Y%m%d")
            if wanted in all_expirations:
                target_expirations.append(wanted)
            else:
                return {
                    "error": f"Expiration {expiration_date} not available for '{symbol}'",
                    "hint": "Use expiry_from/expiry_to to browse a date range.",
                }
        else:
            for exp_str in all_expirations:
                exp_dt = _parse_iso_date(_yyyymmdd_to_iso(exp_str), "expiration")[0]
                if exp_dt is None:
                    continue
                if expiry_from_dt and exp_dt < expiry_from_dt:
                    continue
                if expiry_to_dt and exp_dt > expiry_to_dt:
                    continue
                target_expirations.append(exp_str)

            # When no range filter is supplied, default to the next ~6 expiries
            # so we don't pull the whole chain on a busy underlying like SPY.
            if not expiry_from_dt and not expiry_to_dt:
                today_str = date.today().strftime("%Y%m%d")
                future = [e for e in target_expirations if e >= today_str]
                target_expirations = future[:6] if future else target_expirations[:6]

        if not target_expirations:
            return {"error": f"No options contracts found for '{symbol}' with given filters"}

        # ------------------------------------------------------------------
        # Fan out the four bulk_snapshot calls per expiration
        # ------------------------------------------------------------------
        import asyncio  # noqa: PLC0415

        async def _fetch_one(exp_str: str) -> tuple[dict | None, dict | None, dict | None, dict | None]:
            try:
                exp_int = int(exp_str)
            except ValueError:
                return None, None, None, None
            return await asyncio.gather(
                theta_client.bulk_snapshot_all_greeks(symbol, exp_int),
                theta_client.bulk_snapshot_quote(symbol, exp_int),
                theta_client.bulk_snapshot_open_interest(symbol, exp_int),
                theta_client.bulk_snapshot_ohlc(symbol, exp_int),
            )

        all_results = await asyncio.gather(*[_fetch_one(e) for e in target_expirations])

        # Merge per-expiration results into a single contract list
        merged_contracts: list[dict] = []
        underlying_price: float | None = None
        for greeks_p, quote_p, oi_p, ohlc_p in all_results:
            partial, partial_under = _build_thetadata_contracts(
                greeks_payload=greeks_p,
                quote_payload=quote_p,
                oi_payload=oi_p,
                ohlc_payload=ohlc_p,
            )
            merged_contracts.extend(partial)
            if underlying_price is None and partial_under is not None:
                underlying_price = partial_under

        # ------------------------------------------------------------------
        # Client-side strike + contract_type filters, then truncate to limit
        # ------------------------------------------------------------------
        filtered: list[dict] = []
        for c in merged_contracts:
            if ct and c.get("type") != ct:
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

    if theta_client is not None:
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

            Returns open, high, low, close, volume, and trade count for each
            bar in the date range. Use this for backtesting, charting option
            price history, or analyzing how a specific contract traded.

            Args:
                option_ticker: OCC-style option ticker (e.g. "O:AAPL250516C00220000").
                    Format: O:{UNDERLYING}{YYMMDD}{C|P}{STRIKE*1000 zero-padded 8 digits}
                date_from: Start date (YYYY-MM-DD)
                date_to: End date (YYYY-MM-DD)
                timespan: Bar size — "minute", "hour", "day", "week", "month" (default "day")
                multiplier: Multiplier for timespan (e.g. 5 with "minute" = 5-min bars, default 1)
                symbol: Optional underlying ticker for context in the response
                limit: Max bars to return (default 250, max 5000)
            """
            parsed = _parse_occ_ticker(option_ticker)
            if parsed is None:
                return {
                    "error": f"Invalid option ticker '{option_ticker}'.",
                    "hint": "Expected OCC format: O:AAPL250516C00220000 (root + YYMMDD + C/P + strike*1000)",
                }
            root, exp_int, right, strike_int = parsed

            from_dt, from_err = _parse_iso_date(date_from, "date_from")
            if from_err:
                return {"error": from_err}
            to_dt, to_err = _parse_iso_date(date_to, "date_to")
            if to_err:
                return {"error": to_err}
            if from_dt is None or to_dt is None:
                return {"error": "date_from and date_to are required"}

            valid_timespans = {"minute", "hour", "day", "week", "month"}
            if timespan not in valid_timespans:
                return {"error": f"Invalid timespan '{timespan}'. Use one of: {', '.join(sorted(valid_timespans))}"}

            limit = max(1, min(limit, 5000))
            multiplier = max(1, multiplier)

            # ThetaData has no native week/month bars; roll up from daily.
            ms_per_unit = {
                "minute": 60_000,
                "hour": 3_600_000,
                "day": 86_400_000,
            }
            rollup_target: str | None = None
            if timespan in ms_per_unit:
                ivl_ms = ms_per_unit[timespan] * multiplier
            else:
                ivl_ms = 86_400_000  # fetch dailies, roll up client-side
                rollup_target = timespan

            data = await theta_client.hist_option_ohlc(
                root=root,
                exp=exp_int,
                strike=strike_int,
                right=right,
                start_date=int(from_dt.strftime("%Y%m%d")),
                end_date=int(to_dt.strftime("%Y%m%d")),
                ivl_ms=ivl_ms,
            )

            if not data or not isinstance(data, dict):
                return {"error": f"No historical data found for '{option_ticker}'"}

            header = data.get("header") or {}
            response = data.get("response") or []
            if not response:
                return {
                    "error": f"No bars found for '{option_ticker}' from {date_from} to {date_to}",
                    "hint": "Check that the option ticker is valid for this date range.",
                }

            idx = {
                "ms_of_day": _format_index(header, "ms_of_day"),
                "open": _format_index(header, "open"),
                "high": _format_index(header, "high"),
                "low": _format_index(header, "low"),
                "close": _format_index(header, "close"),
                "volume": _format_index(header, "volume"),
                "count": _format_index(header, "count"),
                "date": _format_index(header, "date"),
            }

            def _row_value(row: list, name: str) -> Any:
                pos = idx[name]
                if pos is None or pos >= len(row):
                    return None
                return row[pos]

            bars: list[dict] = []
            for row in response:
                if not isinstance(row, list):
                    continue
                date_label = _bar_date_label(_row_value(row, "date"))
                bar_entry: dict[str, Any] = {
                    "date": date_label,
                    "open": _row_value(row, "open"),
                    "high": _row_value(row, "high"),
                    "low": _row_value(row, "low"),
                    "close": _row_value(row, "close"),
                    "volume": _row_value(row, "volume"),
                    "trades": _row_value(row, "count"),
                }
                if ivl_ms < 86_400_000:
                    bar_entry["time"] = _ms_to_clock(_row_value(row, "ms_of_day"))
                bars.append(bar_entry)

            if rollup_target:
                bars = _rollup_daily_bars(bars, rollup_target)

            if len(bars) > limit:
                bars = bars[-limit:]

            return {
                "option_ticker": option_ticker,
                "symbol": symbol,
                "timespan": f"{multiplier}{timespan}" if multiplier > 1 else timespan,
                "date_from": date_from,
                "date_to": date_to,
                "bar_count": len(bars),
                "bars": bars,
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


# ---------------------------------------------------------------------------
# Per-expiration grouping + summary helpers
# ---------------------------------------------------------------------------


def _build_chain_result(
    symbol: str,
    contracts: list[dict],
    underlying_price: float | None,
    source: str,
) -> dict:
    """Build the final grouped-by-expiration result from normalized contracts."""
    warnings: list[str] = []

    # Stale-quote heuristic mirrors the original Polygon implementation.
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
