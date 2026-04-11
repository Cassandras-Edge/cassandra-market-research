"""Options chain, historical bars, greeks, IV, OI — all via ThetaData v3.

Thin MCP tool wrappers around cass_market_sdk options analytics.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

from cass_market_sdk.options.helpers import (
    parse_iso_date as _parse_iso_date,
    yyyymmdd as _yyyymmdd,
    iso_expiration as _iso_expiration,
    strike_param as _strike_param,
    right_param as _right_param,
    normalize_contract_type as _normalize_contract_type,
    option_mark as _option_mark,
    rollup_daily_bars as _rollup_daily_bars,
)
from cass_market_sdk.options.chain import (
    merge_snapshot_rows as _merge_snapshot_rows,
    build_chain_result as _build_chain_result,
)
from cass_market_sdk.options.flow import (
    classify_trades,
    aggregate_by_strike,
    aggregate_by_time,
    aggregate_totals,
)

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from cass_market_sdk.clients.thetadata import ThetaDataClient


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
            "title": "Historical Option Trades",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def historical_option_trades(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        date_from: str,
        date_to: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict:
        """Get tick-level trades for an options contract, paired with NBBO at time of trade.

        Returns every OPRA trade with price, size, exchange, condition codes,
        and the contemporaneous NBBO bid/ask. Each trade is also classified as
        buyer-initiated (price >= ask), seller-initiated (price <= bid), or
        indeterminate. Multi-day requests are capped at 1 month.

        Args:
            symbol: Underlying ticker (e.g. "AAPL")
            expiration: Contract expiration (YYYY-MM-DD)
            strike: Strike price in dollars
            right: "call" or "put"
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            start_time: Start time filter (HH:MM:SS, e.g. "09:30:00")
            end_time: End time filter (HH:MM:SS, e.g. "16:00:00")
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

        if theta_client is None:
            return {"error": "ThetaData not configured"}

        rows = await theta_client.history_trade_quote(
            symbol=params["symbol"],
            expiration=params["expiration"],
            strike=params["strike"],
            right=params["right"],
            start_date=_yyyymmdd(from_dt),
            end_date=_yyyymmdd(to_dt),
            start_time=start_time,
            end_time=end_time,
        )

        if not rows:
            return {
                "error": f"No trade data found for {params['symbol']} {params['expiration']} "
                         f"{params['strike']} {params['right']} from {date_from} to {date_to}",
            }

        trades = []
        for r in rows:
            price = r.get("price")
            size = r.get("size")
            bid = r.get("bid")
            ask = r.get("ask")
            if price is None or size is None:
                continue
            try:
                pf = float(price)
                bf = float(bid) if bid is not None else 0.0
                af = float(ask) if ask is not None else 0.0
            except (TypeError, ValueError):
                continue

            if bf <= 0 or af <= 0:
                side = "unknown"
            elif pf >= af:
                side = "buy"
            elif pf <= bf:
                side = "sell"
            else:
                side = "mid"

            trades.append({
                "timestamp": r.get("trade_timestamp") or r.get("timestamp"),
                "price": price,
                "size": size,
                "side": side,
                "bid": bid,
                "ask": ask,
                "exchange": r.get("exchange"),
                "condition": r.get("condition"),
                "bid_size": r.get("bid_size"),
                "ask_size": r.get("ask_size"),
            })

        return {
            "symbol": params["symbol"],
            "expiration": expiration,
            "strike": float(params["strike"]),
            "right": params["right"],
            "date_from": date_from,
            "date_to": date_to,
            "trade_count": len(trades),
            "trades": trades,
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

        # Classify + aggregate via SDK
        trades = classify_trades(rows)

        if not trades:
            return {"error": f"No valid trades found for {sym} {expiration} on {date}"}

        totals = aggregate_totals(trades)

        return {
            "symbol": sym,
            "expiration": expiration,
            "date": date,
            **totals,
            "by_strike": aggregate_by_strike(trades),
            "by_time": aggregate_by_time(trades, bucket_minutes),
            "source": "thetadata",
        }
