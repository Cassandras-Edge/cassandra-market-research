"""Price history, earnings calendar, ETF, and technical indicator tools."""

from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import toon
from cass_market_sdk.clients.polygon import PolygonClient as _PolygonClientRuntime
from cassandra_fmp.tools._helpers import (
    TTL_12H,
    TTL_6H,
    TTL_DAILY,
    TTL_HOURLY,
    TTL_REALTIME,
    _as_dict,
    _as_list,
    _date_only,
    _ms_to_str,
    _safe_call,
    _safe_first,
    _to_date,
)

_EST = ZoneInfo("America/New_York")

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient
    from cass_market_sdk.clients.polygon import PolygonClient
    from cass_market_sdk.clients.thetadata import ThetaDataClient
    from cass_market_sdk.clients.tv_ws import TvWsClient
    from cass_market_sdk.clients.tv_proxy import TvProxyClient


import logging as _logging
_logger = _logging.getLogger(__name__)


def _looks_like_tv_only(symbol: str) -> bool:
    """True for symbols FMP definitely can't resolve — futures continuations
    (`CL1!`), exchange-qualified composites (`NASDAQ:AAPL`,
    `BINANCE:BTCUSDT`, `COMEX:GC1!`), and anything containing `!` or `:`.

    Used as a fast-path to skip FMP when TV is the only plausible backend.
    """
    return ":" in symbol or "!" in symbol


def _period_bar_count(period: str) -> int:
    """Rough daily-bar count for a given period string — sized to give
    enough history for SMA-200 when possible."""
    days = PERIOD_DAYS.get(period)
    if days is None:  # ytd or unknown
        return 260
    # Convert calendar days to trading days, floor at 260 so SMAs still work
    return max(260, int(days * 252 / 365) + 20)


def _analysis_from_bars(
    symbol: str,
    bars: list[dict],
    *,
    detail: bool,
) -> dict:
    """Build the same result shape as price_history from TV candle bars.

    Bars are TV's oldest-first `{t, o, h, l, c, v}`; we reverse so reuse of
    the existing helpers (_calc_performance, _calc_volatility, _calc_sma)
    stays identical — they assume newest-first.
    """
    if not bars:
        return {"error": f"No price data found for '{symbol}'"}

    rows_newest_first = [
        {
            "date": (
                datetime.fromtimestamp(b["t"], tz=timezone.utc)
                .date().isoformat()
            ),
            "open": b.get("o"),
            "high": b.get("h"),
            "low": b.get("l"),
            "close": b.get("c"),
            "volume": b.get("v"),
        }
        for b in reversed(bars)
    ]
    closes = [d["close"] for d in rows_newest_first if d.get("close") is not None]
    current_price = closes[0] if closes else None
    year_window = rows_newest_first[:252] if rows_newest_first else []
    year_high = max((d["high"] for d in year_window if d.get("high") is not None), default=None)
    year_low = min((d["low"] for d in year_window if d.get("low") is not None), default=None)

    performance = {}
    for perf_period, days in [("1w", 5), ("1m", 21), ("3m", 63), ("6m", 126), ("1y", 252)]:
        if current_price:
            perf = _calc_performance(current_price, rows_newest_first, days)
            if perf is not None:
                performance[perf_period] = perf

    result: dict = {
        "symbol": symbol,
        "current_price": current_price,
        "year_high": year_high,
        "year_low": year_low,
        "sma_50": _calc_sma(closes, 50),
        "sma_200": _calc_sma(closes, 200),
        "performance_pct": performance,
        "daily_volatility_annualized_pct": _calc_volatility(rows_newest_first),
        "data_points": len(rows_newest_first),
        "_source": "tradingview",
    }
    if detail:
        closes_rows = [
            {"d": d["date"], "c": d["close"], "v": d.get("volume")}
            for d in rows_newest_first[:30]
        ]
        result["recent_closes"] = toon.encode(closes_rows)
    return result


async def _tv_price_history(
    tv_ws: TvWsClient,
    symbol: str,
    *,
    period: str,
    detail: bool,
) -> dict:
    """Fetch daily bars from TV and synthesize the same shape price_history
    returns from FMP data. Used as a fallback when FMP has no coverage
    (futures continuations, global indices, non-US equities)."""
    count = _period_bar_count(period)
    try:
        candles = await tv_ws.get_candles(
            symbol,
            timeframe="D",
            count=count,
            adjustment="splits",
            timeout=12.0,
        )
    except Exception as e:  # noqa: BLE001
        _logger.debug("TV candle fallback failed for %s: %s", symbol, e)
        return {"error": f"No price data found for '{symbol}'"}

    if candles.get("error") and not candles.get("bars"):
        return {"error": f"No price data found for '{symbol}'"}
    return _analysis_from_bars(symbol, candles.get("bars") or [], detail=detail)


PERIOD_DAYS = {
    "1w": 7,
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "ytd": None,  # calculated
    "1y": 365,
    "2y": 730,
    "5y": 1825,
}

polygon_TTL_HOURLY = _PolygonClientRuntime.TTL_HOURLY
polygon_TTL_REALTIME = _PolygonClientRuntime.TTL_REALTIME


def _calc_sma(prices: list[float], window: int) -> float | None:
    """Calculate simple moving average from a list of prices."""
    if len(prices) < window:
        return None
    return round(sum(prices[:window]) / window, 2)


def _format_split_value(value) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _calc_performance(current: float, history: list[dict], days: int) -> float | None:
    """Calculate % performance over N days from daily history (newest first)."""
    if not current or len(history) < days:
        return None
    old_price = history[min(days - 1, len(history) - 1)].get("close")
    if not old_price or old_price == 0:
        return None
    return round((current / old_price - 1) * 100, 2)


def _calc_volatility(history: list[dict], window: int = 30) -> float | None:
    """Calculate annualized daily volatility from close prices."""
    closes = [d.get("close") for d in history[:window] if d.get("close")]
    if len(closes) < 2:
        return None
    returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] and closes[i - 1] != 0:
            returns.append(closes[i] / closes[i - 1] - 1)
    if not returns:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    daily_vol = math.sqrt(variance)
    return round(daily_vol * math.sqrt(252) * 100, 2)


def _format_holdings(symbol: str, holdings: list, limit: int) -> dict:
    """Format ETF holdings response."""
    # Sort by weight descending (use 0 as fallback)
    holdings.sort(key=lambda h: h.get("weightPercentage") or 0, reverse=True)
    trimmed = holdings[:limit]

    items = []
    for h in trimmed:
        items.append({
            "symbol": h.get("asset"),
            "name": h.get("name"),
            "weight_pct": h.get("weightPercentage"),
            "shares": h.get("sharesNumber"),
        })

    # Top 10 concentration
    top_10_weights = [h.get("weightPercentage") or 0 for h in holdings[:10]]
    top_10_concentration = round(sum(top_10_weights), 2) if top_10_weights else None

    return {
        "symbol": symbol,
        "mode": "holdings",
        "count": len(items),
        "holdings": items,
        "top_10_concentration_pct": top_10_concentration,
    }


def _format_exposure(symbol: str, exposure: list, limit: int) -> dict:
    """Format ETF exposure response."""
    # Sort by weight descending
    exposure.sort(key=lambda e: e.get("weightPercentage") or 0, reverse=True)
    trimmed = exposure[:limit]

    items = []
    for e in trimmed:
        items.append({
            "etf_symbol": e.get("etfSymbol"),
            "etf_name": e.get("etfName"),  # may not exist — will verify live
            "weight_pct": e.get("weightPercentage"),
        })

    return {
        "symbol": symbol,
        "mode": "exposure",
        "count": len(items),
        "etf_holders": items,
    }


_TV_SURPRISE_COLUMNS = [
    "earnings_per_share_fq",
    "earnings_per_share_forecast_fq",
    "eps_surprise_fq",
    "eps_surprise_percent_fq",
    "revenue_surprise_percent_fq",
]


async def _fetch_tv_earnings_surprise(
    tv_proxy: "TvProxyClient",
    symbols: list[str],
) -> dict[str, dict]:
    """Pull prior-quarter earnings surprise data from TV's scanner.

    FMP's earnings_calendar doesn't carry actual-vs-estimate history —
    you'd need a separate `earnings_transcript` call per symbol to
    reconstruct it. TV's scanner has it inline as
    `eps_surprise_*_fq` / `revenue_surprise_percent_fq` columns, so we
    get it in one request.

    Symbol format note: FMP returns bare tickers; TV needs
    `EXCHANGE:TICKER`. We pass the bare ticker via `symbols.tickers`
    and TV's scanner handles the lookup within the `america` market.
    Non-US symbols won't resolve and just come back missing.
    """
    if not symbols:
        return {}
    # TV scanner accepts both bare tickers and EXCHANGE:TICKER in
    # symbols.tickers; bare works for US equities in the america market.
    body = {
        "filter": [],
        "options": {"lang": "en"},
        "markets": ["america"],
        "symbols": {"query": {"types": []}, "tickers": symbols},
        "columns": _TV_SURPRISE_COLUMNS,
        "range": [0, len(symbols)],
    }
    data = await tv_proxy.post_json("scanner", "/america/scan", json=body)
    out: dict[str, dict] = {}
    for raw in data.get("data") or []:
        sym_full = raw.get("s") or ""
        # scanner returns "EXCHANGE:TICKER" in `s`; normalize to bare ticker
        # so the join-key matches FMP's symbol (which is just the ticker).
        sym = sym_full.split(":", 1)[-1].upper()
        values = raw.get("d") or []
        row = dict(zip(_TV_SURPRISE_COLUMNS, values, strict=False))
        out[sym] = {
            "actual_eps": row.get("earnings_per_share_fq"),
            "estimate_eps": row.get("earnings_per_share_forecast_fq"),
            "eps_surprise": row.get("eps_surprise_fq"),
            "eps_surprise_pct": row.get("eps_surprise_percent_fq"),
            "revenue_surprise_pct": row.get("revenue_surprise_percent_fq"),
        }
    return out


def register(
    mcp: FastMCP,
    client: AsyncFMPDataClient,
    polygon_client: PolygonClient | None = None,
    *,
    theta_client: ThetaDataClient | None = None,
    tv_ws: TvWsClient | None = None,
    tv_proxy: "TvProxyClient | None" = None,
) -> None:
    @mcp.tool(
        annotations={
            "title": "Price History",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def price_history(
        symbol: str,
        period: str = "1y",
        detail: bool = False,
    ) -> dict:
        """Get price performance, key levels, and momentum indicators.

        Returns current price, 52-week range, SMA-50/200, performance across
        timeframes, and volatility. Use detail=True to include recent daily closes.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            period: Time period - "1w", "1m", "3m", "6m", "ytd", "1y", "2y", "5y" (default "1y")
            detail: If True, include recent daily closes in TOON format (default False)
        """
        symbol = symbol.upper().strip()

        if period not in PERIOD_DAYS:
            return {"error": f"Invalid period '{period}'. Use: {', '.join(PERIOD_DAYS.keys())}"}

        # TV-only symbol shapes (composites, futures) bypass FMP entirely —
        # saves a wasted roundtrip that we know will return empty.
        if _looks_like_tv_only(symbol) and tv_ws is not None:
            return await _tv_price_history(
                tv_ws, symbol, period=period, detail=detail,
            )

        # Calculate date range
        today = date.today()
        if period == "ytd":
            from_date = date(today.year, 1, 1)
        else:
            from_date = today - timedelta(days=PERIOD_DAYS[period])

        # /stable/historical-price-eod/full returns a flat list of {symbol, date, open, high, low, close, volume, ...}
        history_data, quote_data = await asyncio.gather(
            _safe_call(
                client.company.get_historical_prices,
                symbol=symbol,
                from_date=from_date,
                to_date=today,
                ttl=TTL_12H,
                default=None,
            ),
            _safe_call(
                client.company.get_quote,
                symbol=symbol,
                ttl=TTL_REALTIME,
                default=None,
            ),
        )

        quote = _as_dict(quote_data)
        historical = _as_list(history_data, list_key="historical")

        # FMP miss → try TV for non-US/futures/crypto coverage before erroring.
        if not quote and not historical:
            if tv_ws is not None:
                return await _tv_price_history(
                    tv_ws, symbol, period=period, detail=detail,
                )
            return {"error": f"No price data found for '{symbol}'"}

        current_price = quote.get("price")

        # Extract close prices (newest first - API returns newest first)
        closes = [d.get("close") for d in historical if d.get("close")]

        # Build performance across timeframes
        performance = {}
        for perf_period, days in [("1w", 5), ("1m", 21), ("3m", 63), ("6m", 126), ("1y", 252)]:
            if current_price:
                perf = _calc_performance(current_price, historical, days)
                if perf is not None:
                    performance[perf_period] = perf

        result = {
            "symbol": symbol,
            "current_price": current_price,
            "year_high": quote.get("yearHigh"),
            "year_low": quote.get("yearLow"),
            "sma_50": quote.get("priceAvg50") or _calc_sma(closes, 50),
            "sma_200": quote.get("priceAvg200") or _calc_sma(closes, 200),
            "performance_pct": performance,
            "daily_volatility_annualized_pct": _calc_volatility(historical),
            "data_points": len(historical),
        }

        if detail:
            closes_rows = [
                {"d": d.get("date"), "c": d.get("close"), "v": d.get("volume")}
                for d in historical[:30]
            ]
            result["recent_closes"] = toon.encode(closes_rows)

        errors = []
        if not quote:
            errors.append("quote data unavailable")
        if not historical:
            errors.append("historical data unavailable")
        if errors:
            result["_warnings"] = errors

        return result

    @mcp.tool(
        annotations={
            "title": "Dividends Info",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def dividends_info(symbol: str) -> dict:
        """Get dividend history, yield, growth rates, and stock split history.

        Returns current dividend yield, 3Y/5Y dividend CAGR, upcoming ex-date,
        payout context, and stock split history.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()
        dividends_data, splits_data, quote_data = await asyncio.gather(
            _safe_call(client.company.get_dividends, symbol=symbol, ttl=TTL_6H, default=[]),
            _safe_call(client.company.get_stock_splits, symbol=symbol, ttl=TTL_DAILY, default=[]),
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
        )

        div_list = _as_list(dividends_data)
        split_list = _as_list(splits_data)
        quote = _as_dict(quote_data)

        if not div_list and not split_list:
            return {"error": f"No dividend or split data found for '{symbol}'"}

        # Sort dividends newest first
        div_list.sort(key=lambda d: _date_only(d.get("date")) or "", reverse=True)

        current_price = quote.get("price")

        # Trailing 12-month dividend (sum of most recent 4 payments)
        trailing_annual = None
        if len(div_list) >= 4:
            trailing_annual = round(sum(
                (d.get("dividend") or 0) for d in div_list[:4]
            ), 4)
        elif div_list:
            # If fewer than 4 payments, annualize based on frequency
            freq = div_list[0].get("frequency", "").lower()
            per_payment = div_list[0].get("dividend") or 0
            if "quarter" in freq:
                trailing_annual = round(per_payment * 4, 4)
            elif "semi" in freq or "half" in freq:
                trailing_annual = round(per_payment * 2, 4)
            elif "month" in freq:
                trailing_annual = round(per_payment * 12, 4)
            else:
                trailing_annual = round(per_payment * 4, 4)  # assume quarterly

        # Current yield from trailing annual
        current_yield = None
        if trailing_annual and current_price and current_price > 0:
            current_yield = round(trailing_annual / current_price * 100, 2)

        # Filter dividends to post-split only for CAGR (FMP doesn't adjust for splits)
        split_list.sort(key=lambda s: _date_only(s.get("date")) or "", reverse=True)
        latest_split_date = _date_only(split_list[0].get("date")) if split_list else None
        cagr_divs = [d for d in div_list if (_date_only(d.get("date")) or "") > latest_split_date] if latest_split_date else div_list

        # Build full-year totals using rolling 4-quarter windows for CAGR
        yearly_totals = []
        for i in range(0, len(cagr_divs) - 3, 4):
            chunk = cagr_divs[i:i + 4]
            if len(chunk) == 4:
                total = round(sum((d.get("dividend") or 0) for d in chunk), 4)
                yearly_totals.append(total)

        # Dividend CAGR from rolling annual totals
        def _div_cagr(n_years: int) -> float | None:
            if len(yearly_totals) < n_years + 1:
                return None
            end_val = yearly_totals[0]
            start_val = yearly_totals[n_years]
            if start_val <= 0 or end_val <= 0:
                return None
            try:
                return round(((end_val / start_val) ** (1 / n_years) - 1) * 100, 2)
            except (ZeroDivisionError, ValueError, OverflowError):
                return None

        # Upcoming ex-date (future or very recent)
        today_date = date.today()
        upcoming_ex_date = None
        for d in div_list:
            pay_date = _to_date(d.get("paymentDate"))
            ex_date = _to_date(d.get("date"))
            # Show if payment hasn't happened yet or ex-date is within a week
            if (pay_date and pay_date >= today_date) or (ex_date and ex_date >= today_date):
                upcoming_ex_date = {
                    "ex_date": _date_only(d.get("date")),
                    "dividend": d.get("dividend"),
                    "payment_date": _date_only(d.get("paymentDate")),
                    "record_date": _date_only(d.get("recordDate")),
                }
                break

        # Recent dividend history (last 8 payments)
        recent_dividends = []
        for d in div_list[:8]:
            recent_dividends.append({
                "date": _date_only(d.get("date")),
                "dividend": d.get("dividend"),
                "payment_date": _date_only(d.get("paymentDate")),
            })

        # Stock splits
        splits = []
        for s in split_list:
            num = s.get("numerator")
            den = s.get("denominator")
            label = f"{_format_split_value(num)}:{_format_split_value(den)}" if num is not None and den is not None else None
            splits.append({
                "date": _date_only(s.get("date")),
                "numerator": num,
                "denominator": den,
                "label": label,
            })

        result = {
            "symbol": symbol,
            "current_price": current_price,
            "trailing_annual_dividend": trailing_annual,
            "dividend_yield_pct": current_yield,
            "dividend_cagr_3y": _div_cagr(3),
            "dividend_cagr_5y": _div_cagr(5),
            "upcoming_ex_date": upcoming_ex_date,
            "recent_dividends": recent_dividends,
            "stock_splits": splits,
        }

        _warnings = []
        if not div_list:
            _warnings.append("dividend data unavailable")
        if not split_list:
            _warnings.append("stock split data unavailable")
        if not quote:
            _warnings.append("quote data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    async def _fetch_options_oi(symbols: list[str]) -> dict[str, dict]:
        """Fetch aggregate options OI for a list of symbols via ThetaData v3.

        Returns {symbol: {total_oi, call_oi, put_oi, put_call_ratio}} for
        each symbol where data is available. v3's snapshot_open_interest
        supports ``expiration="*"`` so one call covers every contract on
        every expiration for a symbol.
        """
        if not theta_client:
            return {}

        sem = asyncio.Semaphore(5)

        async def _get_oi(sym: str) -> tuple[str, dict | None]:
            async with sem:
                rows = await theta_client.snapshot_open_interest(
                    symbol=sym, expiration="*",
                )

            if not rows:
                return sym, None

            call_oi = 0
            put_oi = 0
            for entry in rows:
                right = (entry.get("right") or "").upper()
                oi_value = entry.get("open_interest") or 0
                if right == "CALL":
                    call_oi += oi_value
                elif right == "PUT":
                    put_oi += oi_value
            total = call_oi + put_oi
            if total == 0:
                return sym, None
            return sym, {
                "total_oi": total,
                "call_oi": call_oi,
                "put_oi": put_oi,
                "put_call_ratio": round(put_oi / call_oi, 2) if call_oi > 0 else None,
            }

        results = await asyncio.gather(*[_get_oi(s) for s in symbols])
        return {sym: oi for sym, oi in results if oi is not None}

    @mcp.tool(
        annotations={
            "title": "Earnings Calendar",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def earnings_calendar(
        symbol: str | None = None,
        symbols: list[str] | None = None,
        days_ahead: int = 7,
        min_market_cap: float = 2_000_000_000,
        limit: int = 50,
        country: str | None = "US",
        exchange: str | None = None,
    ) -> dict:
        """Get upcoming earnings report dates and estimates.

        Returns companies reporting earnings within the specified window.
        When no symbol is given, filters to liquid large-cap stocks (default
        $2B+ market cap) to keep results manageable and focused on names
        where options are liquid. Includes options open interest data from
        Polygon when available.

        Args:
            symbol: Optional stock ticker to filter results (e.g. "AAPL")
            symbols: Optional list of stock tickers for post-filtering in browsing mode
            days_ahead: Number of days to look ahead (default 7, max 365)
            min_market_cap: Minimum market cap filter in USD when browsing
                all earnings (default 2B). Ignored when symbol is specified.
                Set to 0 to include all companies.
            limit: Max results in browsing mode (default 50, max 100).
                Ignored when symbol is specified.
            country: Optional country filter in browsing mode (default "US")
            exchange: Optional exchange filter in browsing mode (e.g. "NASDAQ")
        """
        days_ahead = max(1, min(days_ahead, 365))
        limit = max(1, min(limit, 100))
        today = date.today()
        to_date = today + timedelta(days=days_ahead)
        warnings: list[str] = []

        symbols_filter = sorted({
            s.upper().strip()
            for s in (symbols or [])
            if isinstance(s, str) and s.strip()
        })
        country_filter = country.upper().strip() if isinstance(country, str) and country.strip() else None
        exchange_filter = exchange.upper().strip() if isinstance(exchange, str) and exchange.strip() else None

        data = await _safe_call(
            client.intelligence.get_earnings_calendar,
            start_date=today,
            end_date=to_date,
            ttl=TTL_REALTIME,
            default=[],
        )

        entries = _as_list(data)

        # Single-symbol lookup: skip market cap filtering, enrich with OI
        if symbol:
            symbol = symbol.upper().strip()
            entries = [e for e in entries if e.get("symbol", "").upper() == symbol]
            if not entries:
                return {"error": f"No earnings scheduled in the next {days_ahead} days for '{symbol}'"}
            e = entries[0]
            entry: dict = {
                "symbol": e.get("symbol"),
                "date": e.get("date"),
                "time": e.get("time"),
                "fiscal_date_ending": e.get("fiscalDateEnding"),
                "eps_estimate": e.get("epsEstimated"),
                "revenue_estimate": e.get("revenueEstimated"),
                "eps_actual": e.get("epsActual"),
                "revenue_actual": e.get("revenueActual"),
            }
            oi_map = await _fetch_options_oi([symbol])
            if symbol in oi_map:
                oi_payload = oi_map[symbol]
                entry["options_oi"] = oi_payload["total_oi"]
                entry["options"] = {
                    "total_oi": oi_payload["total_oi"],
                    "open_interest": oi_payload["total_oi"],
                    "call_oi": oi_payload["call_oi"],
                    "put_oi": oi_payload["put_oi"],
                    "call_open_interest": oi_payload["call_oi"],
                    "put_open_interest": oi_payload["put_oi"],
                    "put_call_ratio": oi_payload["put_call_ratio"],
                }
            result = {
                "from_date": today.isoformat(),
                "to_date": to_date.isoformat(),
                "symbol": symbol,
                "count": 1,
                "earnings": [entry],
            }
            if symbols_filter:
                result["_warnings"] = ["'symbols' is ignored when single 'symbol' is provided."]
            return result

        # Browsing mode: filter to liquid large-caps
        if not entries:
            return {"error": f"No earnings scheduled in the next {days_ahead} days"}

        if symbols_filter:
            symbol_set = set(symbols_filter)
            entries = [e for e in entries if (e.get("symbol") or "").upper() in symbol_set]
            if not entries:
                return {
                    "error": f"No earnings found for requested symbols in the next {days_ahead} days",
                    "symbols": symbols_filter,
                }

        if country_filter:
            country_fields = (
                "country",
                "countryCode",
            )
            has_country_data = any(any(e.get(field) for field in country_fields) for e in entries)
            if has_country_data:
                entries = [
                    e
                    for e in entries
                    if ((e.get("country") or e.get("countryCode") or "").upper() == country_filter)
                ]
            else:
                warnings.append("country filter could not be applied because source data omitted country fields")

        if exchange_filter:
            exchange_fields = (
                "exchange",
                "exchangeCode",
                "exchangeShortName",
            )
            has_exchange_data = any(any(e.get(field) for field in exchange_fields) for e in entries)
            if has_exchange_data:
                entries = [
                    e
                    for e in entries
                    if (
                        (e.get("exchange") or e.get("exchangeCode") or e.get("exchangeShortName") or "").upper()
                        == exchange_filter
                    )
                ]
            else:
                warnings.append("exchange filter could not be applied because source data omitted exchange fields")

        if not entries:
            filter_parts = []
            if country_filter:
                filter_parts.append(f"country={country_filter}")
            if exchange_filter:
                filter_parts.append(f"exchange={exchange_filter}")
            filter_text = ", ".join(filter_parts) if filter_parts else "the requested filters"
            return {"error": f"No earnings matched {filter_text} in the next {days_ahead} days"}

        # Pre-filter: skip entries with tiny/missing revenue estimates
        # to reduce the batch-quote call size ($50M rev ≈ mid-cap floor)
        MIN_REV_ESTIMATE = 50_000_000
        if min_market_cap > 0:
            candidates = [
                e for e in entries
                if (e.get("revenueEstimated") or 0) >= MIN_REV_ESTIMATE
            ]
        else:
            candidates = entries

        # Deduplicate symbols (same symbol can appear for multiple quarters)
        seen_symbols: set[str] = set()
        unique_candidates = []
        for e in candidates:
            sym = (e.get("symbol") or "").upper()
            if sym and sym not in seen_symbols:
                seen_symbols.add(sym)
                unique_candidates.append(e)

        # Batch-quote for market caps (chunk in groups of 200)
        mcap_map: dict[str, float] = {}
        if min_market_cap > 0 and unique_candidates:
            symbols = sorted({(e.get("symbol") or "").upper() for e in unique_candidates})
            chunk_size = 200
            chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
            batch_results = await asyncio.gather(*(
                _safe_call(
                    client.batch.get_quotes,
                    symbols=chunk,
                    ttl=TTL_REALTIME,
                    default=[],
                )
                for chunk in chunks
            ))
            for batch in batch_results:
                for q in _as_list(batch):
                    sym = q.get("symbol")
                    mc = q.get("marketCap")
                    if sym and mc:
                        mcap_map[sym] = mc

        # Build output, applying market cap filter
        earnings = []
        for e in unique_candidates:
            sym = (e.get("symbol") or "").upper()
            mc = mcap_map.get(sym)
            if min_market_cap > 0 and (mc is None or mc < min_market_cap):
                continue
            entry = {
                "symbol": sym,
                "date": e.get("date"),
                "time": e.get("time"),  # "amc" / "bmo" / "--"
                "fiscal_date_ending": e.get("fiscalDateEnding"),
                "eps_estimate": e.get("epsEstimated"),
                "revenue_estimate": e.get("revenueEstimated"),
            }
            if e.get("country"):
                entry["country"] = e.get("country")
            if e.get("exchange") or e.get("exchangeShortName"):
                entry["exchange"] = e.get("exchange") or e.get("exchangeShortName")
            if mc is not None:
                entry["market_cap"] = mc
            earnings.append(entry)

        if not earnings:
            return {"error": f"No earnings ≥${min_market_cap/1e9:.0f}B market cap in the next {days_ahead} days"}

        # Sort by market cap descending (biggest names first), then date
        earnings.sort(key=lambda x: (-(x.get("market_cap") or 0), x.get("date") or ""))

        total_matching = len(earnings)
        earnings = earnings[:limit]

        # Enrich with options OI from Polygon (only for the limited set) and,
        # in parallel, with prior-quarter earnings surprise from TV's scanner
        # when TV is configured. TV carries eps/revenue surprise percents as
        # scanner columns — lets an analyst see "did they beat last quarter
        # by how much" without a separate fundamentals tool call.
        filtered_symbols = [e["symbol"] for e in earnings]
        oi_task = asyncio.create_task(_fetch_options_oi(filtered_symbols))
        tv_surprise_task: asyncio.Task | None = None
        if tv_proxy is not None and filtered_symbols:
            tv_surprise_task = asyncio.create_task(
                _fetch_tv_earnings_surprise(tv_proxy, filtered_symbols)
            )

        oi_map = await oi_task
        tv_surprise_map: dict[str, dict] = {}
        if tv_surprise_task is not None:
            try:
                tv_surprise_map = await tv_surprise_task
            except Exception as e:  # noqa: BLE001
                _logger.debug("TV earnings surprise enrichment skipped: %s", e)

        for entry in earnings:
            sym = entry["symbol"]
            if sym in oi_map:
                oi_payload = oi_map[sym]
                entry["options_oi"] = oi_payload["total_oi"]
                entry["options"] = {
                    "total_oi": oi_payload["total_oi"],
                    "open_interest": oi_payload["total_oi"],
                    "call_oi": oi_payload["call_oi"],
                    "put_oi": oi_payload["put_oi"],
                    "call_open_interest": oi_payload["call_oi"],
                    "put_open_interest": oi_payload["put_oi"],
                    "put_call_ratio": oi_payload["put_call_ratio"],
                }
            if sym in tv_surprise_map:
                entry["prior_quarter"] = tv_surprise_map[sym]

        result = {
            "from_date": today.isoformat(),
            "to_date": to_date.isoformat(),
            "min_market_cap": min_market_cap,
            "country": country_filter,
            "exchange": exchange_filter,
            "symbols": symbols_filter or None,
            "count": len(earnings),
            "total_matching": total_matching,
            "earnings": earnings,
        }
        if warnings:
            result["_warnings"] = warnings
        return result

    @mcp.tool(
        annotations={
            "title": "ETF Lookup",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def etf_lookup(
        symbol: str,
        mode: str = "auto",
        limit: int = 25,
    ) -> dict:
        """Look up ETF holdings or find which ETFs hold a stock.

        Multi-mode tool: "holdings" shows ETF contents, "exposure" shows which ETFs
        hold a stock, "profile" gives comprehensive ETF analysis, "auto" detects automatically.

        Args:
            symbol: ETF ticker (e.g. "QQQ") or stock ticker (e.g. "AAPL")
            mode: "holdings", "exposure", "profile", or "auto" (default "auto")
            limit: Max results to return (default 25)
        """
        symbol = symbol.upper().strip()
        limit = max(1, min(limit, 100))

        if mode not in ("holdings", "exposure", "profile", "auto"):
            return {"error": f"Invalid mode '{mode}'. Use: holdings, exposure, profile, auto"}

        async def _try_holdings() -> list | None:
            data = await _safe_call(client.investment.get_etf_holdings, symbol=symbol, ttl=TTL_DAILY, default=[])
            result = _as_list(data)
            return result if result else None

        async def _try_exposure() -> list | None:
            data = await _safe_call(client.investment.get_etf_exposure, symbol=symbol, ttl=TTL_DAILY, default=[])
            result = _as_list(data)
            return result if result else None

        # Profile mode: comprehensive ETF analysis
        if mode == "profile":
            info_data, holdings_data, sector_data, country_data = await asyncio.gather(
                _safe_call(client.investment.get_etf_info, symbol=symbol, ttl=TTL_DAILY, default=None),
                _safe_call(client.investment.get_etf_holdings, symbol=symbol, ttl=TTL_DAILY, default=[]),
                _safe_call(client.investment.get_etf_sector_weightings, symbol=symbol, ttl=TTL_DAILY, default=[]),
                _safe_call(client.investment.get_etf_country_weightings, symbol=symbol, ttl=TTL_DAILY, default=[]),
            )

            info = _as_dict(info_data)
            holdings_list = _as_list(holdings_data)
            sectors = _as_list(sector_data)
            countries = _as_list(country_data)

            if not info and not holdings_list:
                return {"error": f"No ETF profile data found for '{symbol}'. Is it an ETF ticker?"}

            # ETF info
            etf_info = {
                "name": info.get("name"),
                "inception_date": info.get("inceptionDate"),
                "expense_ratio": info.get("expenseRatio"),
                "aum": info.get("aum"),
                "nav": info.get("nav"),
                "avg_volume": info.get("avgVolume"),
                "holdings_count": info.get("holdingsCount"),
                "description": info.get("description"),
            }

            # Top holdings
            holdings_list.sort(key=lambda h: h.get("weightPercentage") or 0, reverse=True)
            top_holdings = []
            for h in holdings_list[:limit]:
                top_holdings.append({
                    "symbol": h.get("asset"),
                    "name": h.get("name"),
                    "weight_pct": h.get("weightPercentage"),
                    "shares": h.get("sharesNumber"),
                })

            # Sector weights
            sectors.sort(key=lambda s: s.get("weightPercentage") or 0, reverse=True)
            sector_weights = []
            for s in sectors:
                sector_weights.append({
                    "sector": s.get("sector"),
                    "weight_pct": s.get("weightPercentage"),
                })

            # Country allocation
            countries.sort(key=lambda c: c.get("weightPercentage") or 0, reverse=True)
            country_allocation = []
            for c in countries:
                country_allocation.append({
                    "country": c.get("country"),
                    "weight_pct": c.get("weightPercentage"),
                })

            result = {
                "symbol": symbol,
                "mode": "profile",
                "info": etf_info,
                "top_holdings": top_holdings,
                "sector_weights": sector_weights,
                "country_allocation": country_allocation,
            }

            _warnings = []
            if not info:
                _warnings.append("ETF info unavailable")
            if not holdings_list:
                _warnings.append("holdings data unavailable")
            if not sectors:
                _warnings.append("sector weighting unavailable")
            if not countries:
                _warnings.append("country allocation unavailable")
            if _warnings:
                result["_warnings"] = _warnings

            return result

        if mode == "holdings":
            holdings = await _try_holdings()
            if not holdings:
                return {"error": f"No ETF holdings found for '{symbol}'. Is it an ETF ticker?"}
            return _format_holdings(symbol, holdings, limit)

        if mode == "exposure":
            exposure = await _try_exposure()
            if not exposure:
                return {"error": f"No ETF exposure found for '{symbol}'. Is it a stock ticker?"}
            return _format_exposure(symbol, exposure, limit)

        # Auto mode: try both in parallel
        holdings, exposure = await asyncio.gather(
            _try_holdings(),
            _try_exposure(),
        )

        if holdings:
            return _format_holdings(symbol, holdings, limit)
        if exposure:
            return _format_exposure(symbol, exposure, limit)

        return {"error": f"No ETF data found for '{symbol}'. Try specifying mode='holdings', 'exposure', or 'profile'."}

    @mcp.tool(
        annotations={
            "title": "Intraday Prices",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def intraday_prices(
        symbol: str,
        detail: bool = False,
        interval: str = "5m",
        days_back: int = 1,
    ) -> dict:
        """Get intraday price candles with summary statistics and extended-hours trades.

        Default mode returns adaptive-resolution candles: 5m for last ~2 hours,
        15m for rest of today, 1h for older data. Candles in compact TOON format
        with EST timestamps (12h AM/PM).

        Use detail=True for uniform single-interval candles (up to 500), useful
        for charting/backtesting. interval and days_back are only used in detail mode.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            detail: If True, return uniform candles at specified interval (default False)
            interval: Candle interval for detail mode - "1m", "5m", "15m", "30m", "1h", "4h" (default "5m")
            days_back: Days to look back for detail mode (default 1, max 7)
        """
        symbol = symbol.upper().strip()
        interval_map = {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "1hour",
            "4h": "4hour",
        }

        if detail and interval not in interval_map:
            return {"error": f"Invalid interval '{interval}'. Use: {', '.join(interval_map.keys())}"}

        days_back = max(1, min(days_back, 7))
        today = date.today()

        def _parse_candle_dt(date_val) -> datetime | None:
            """Parse FMP candle date value to EST-aware datetime."""
            if not date_val:
                return None
            if isinstance(date_val, datetime):
                if date_val.tzinfo is None:
                    return date_val.replace(tzinfo=_EST)
                return date_val.astimezone(_EST)
            try:
                return datetime.strptime(str(date_val), "%Y-%m-%d %H:%M:%S").replace(tzinfo=_EST)
            except ValueError:
                return None

        def _format_est(dt: datetime, include_date: bool) -> str:
            """Format datetime as EST 12h AM/PM string."""
            if include_date:
                return dt.strftime("%b %-d %-I:%M %p")
            return dt.strftime("%-I:%M %p")

        def _build_summary(candles_list: list[dict]) -> dict:
            """Compute summary stats from a list of raw candle dicts."""
            total_volume = sum(c.get("volume") or 0 for c in candles_list)
            highs = [c.get("high") for c in candles_list if c.get("high")]
            lows = [c.get("low") for c in candles_list if c.get("low")]
            period_high = max(highs) if highs else None
            period_low = min(lows) if lows else None
            range_pct = None
            if period_high and period_low and period_low > 0:
                range_pct = round((period_high / period_low - 1) * 100, 2)
            vwap = None
            if total_volume > 0:
                vwap_sum = sum((c.get("close") or 0) * (c.get("volume") or 0) for c in candles_list)
                vwap = round(vwap_sum / total_volume, 2)
            return {
                "total_volume": total_volume,
                "vwap": vwap,
                "period_high": period_high,
                "period_low": period_low,
                "range_pct": range_pct,
            }

        def _build_extended_hours_section(
            premarket_data, afterhours_data, last_close: float | None
        ) -> dict:
            """Build extended-hours dict from pre/post-market data."""
            extended_hours: dict = {}
            pre_candidates = [
                item for item in _as_list(premarket_data)
                if (item.get("symbol") or "").upper() == symbol and (item.get("session") or "").lower() == "pre"
            ]
            pre = _as_dict(pre_candidates)
            if pre and pre.get("price"):
                ts = pre.get("timestamp")
                extended_hours["premarket"] = {
                    "price": pre["price"],
                    "size": pre.get("tradeSize"),
                    "timestamp": _ms_to_str(ts),
                }
            post = _as_dict(afterhours_data)
            if post and post.get("price"):
                ts = post.get("timestamp")
                extended_hours["afterhours"] = {
                    "price": post["price"],
                    "size": post.get("tradeSize"),
                    "timestamp": _ms_to_str(ts),
                }
            if last_close and last_close > 0:
                for key in ("premarket", "afterhours"):
                    eh = extended_hours.get(key)
                    if eh and eh.get("price"):
                        eh["change_pct"] = round((eh["price"] / last_close - 1) * 100, 2)
            return extended_hours

        if detail:
            # --- Detail mode: single-interval, up to 500 candles ---
            from_date = today - timedelta(days=days_back)
            data, premarket_data, afterhours_data = await asyncio.gather(
                _safe_call(
                    client.company.get_intraday_prices,
                    symbol=symbol,
                    interval=interval_map[interval],
                    from_date=from_date,
                    to_date=today,
                    ttl=TTL_REALTIME,
                    default=[],
                ),
                _safe_call(client.market.get_pre_post_market, ttl=TTL_REALTIME, default=[]),
                _safe_call(client.company.get_aftermarket_trade, symbol=symbol, ttl=TTL_REALTIME, default=None),
            )
            candles = _as_list(data)
            if not candles:
                return {"error": f"No intraday data found for '{symbol}' with interval {interval}"}

            candles.sort(key=lambda c: c.get("date") or "", reverse=True)
            trimmed = candles[:500]

            today_date = today.isoformat()
            toon_rows = []
            for c in trimmed:
                dt = _parse_candle_dt(c.get("date") or "")
                if dt:
                    include_date = dt.strftime("%Y-%m-%d") != today_date
                    t_str = _format_est(dt, include_date)
                else:
                    t_str = c.get("date") or ""
                toon_rows.append({
                    "t": t_str,
                    "o": c.get("open"),
                    "h": c.get("high"),
                    "l": c.get("low"),
                    "c": c.get("close"),
                    "v": c.get("volume"),
                })

            summary = _build_summary(trimmed)
            last_close = trimmed[0].get("close") if trimmed else None
            extended_hours = _build_extended_hours_section(premarket_data, afterhours_data, last_close)

            result = {
                "symbol": symbol,
                "mode": "detail",
                "interval": interval,
                "days_back": days_back,
                "candle_count": len(toon_rows),
                "summary": summary,
                "candles": toon.encode(toon_rows),
            }
            if extended_hours:
                result["extended_hours"] = extended_hours
            return result

        # --- Adaptive mode (default): tiered resolution ---
        from_date = today - timedelta(days=2)  # fetch 2 days for yesterday + today

        data_5m, data_15m, data_1h, premarket_data, afterhours_data = await asyncio.gather(
            _safe_call(
                client.company.get_intraday_prices,
                symbol=symbol, interval="5min", from_date=from_date, to_date=today,
                ttl=TTL_REALTIME, default=[],
            ),
            _safe_call(
                client.company.get_intraday_prices,
                symbol=symbol, interval="15min", from_date=from_date, to_date=today,
                ttl=TTL_REALTIME, default=[],
            ),
            _safe_call(
                client.company.get_intraday_prices,
                symbol=symbol, interval="1hour", from_date=from_date, to_date=today,
                ttl=TTL_REALTIME, default=[],
            ),
            _safe_call(client.market.get_pre_post_market, ttl=TTL_REALTIME, default=[]),
            _safe_call(client.company.get_aftermarket_trade, symbol=symbol, ttl=TTL_REALTIME, default=None),
        )

        candles_5m = _as_list(data_5m)
        candles_15m = _as_list(data_15m)
        candles_1h = _as_list(data_1h)

        if not candles_5m and not candles_15m and not candles_1h:
            return {"error": f"No intraday data found for '{symbol}'"}

        # Determine time boundaries in EST
        now_est = datetime.now(_EST)
        cutoff_2h = now_est - timedelta(hours=2)
        cutoff_today = now_est.replace(hour=0, minute=0, second=0, microsecond=0)
        today_date = today.isoformat()

        # Tier 1: 5m candles for last ~2 hours
        rows: list[tuple[datetime, dict]] = []
        for c in candles_5m:
            dt = _parse_candle_dt(c.get("date") or "")
            if dt and dt >= cutoff_2h:
                include_date = dt.strftime("%Y-%m-%d") != today_date
                rows.append((dt, {
                    "t": _format_est(dt, include_date),
                    "o": c.get("open"), "h": c.get("high"),
                    "l": c.get("low"), "c": c.get("close"),
                    "v": c.get("volume"), "tier": "5m",
                }))

        # Tier 2: 15m candles for rest of today (before 2h cutoff)
        for c in candles_15m:
            dt = _parse_candle_dt(c.get("date") or "")
            if dt and cutoff_today <= dt < cutoff_2h:
                include_date = dt.strftime("%Y-%m-%d") != today_date
                rows.append((dt, {
                    "t": _format_est(dt, include_date),
                    "o": c.get("open"), "h": c.get("high"),
                    "l": c.get("low"), "c": c.get("close"),
                    "v": c.get("volume"), "tier": "15m",
                }))

        # Tier 3: 1h candles for yesterday and older
        for c in candles_1h:
            dt = _parse_candle_dt(c.get("date") or "")
            if dt and dt < cutoff_today:
                rows.append((dt, {
                    "t": _format_est(dt, True),
                    "o": c.get("open"), "h": c.get("high"),
                    "l": c.get("low"), "c": c.get("close"),
                    "v": c.get("volume"), "tier": "1h",
                }))

        # Sort newest first
        rows.sort(key=lambda r: r[0], reverse=True)
        toon_rows = [r[1] for r in rows]

        # Summary stats from 5m candles for accuracy (fallback to all if no 5m)
        summary_source = candles_5m if candles_5m else (candles_15m or candles_1h)
        summary = _build_summary(summary_source)

        last_close_val = candles_5m[0].get("close") if candles_5m else None
        if not last_close_val:
            # Fallback to most recent candle from any tier
            if rows:
                last_close_val = rows[0][1].get("c")
        extended_hours = _build_extended_hours_section(premarket_data, afterhours_data, last_close_val)

        result = {
            "symbol": symbol,
            "mode": "adaptive",
            "candle_count": len(toon_rows),
            "summary": summary,
            "candles": toon.encode(toon_rows),
        }
        if extended_hours:
            result["extended_hours"] = extended_hours

        return result

    @mcp.tool(
        annotations={
            "title": "Historical Market Cap",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def historical_market_cap(
        symbol: str,
        limit: int = 10,
    ) -> dict:
        """Get historical market capitalization time series for a stock.

        Returns market cap values over time, useful for tracking company
        growth and size changes.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            limit: Number of data points to return (default 10, max 100)
        """
        symbol = symbol.upper().strip()
        limit = max(1, min(limit, 100))

        data = await _safe_call(
            client.company.get_historical_market_cap,
            symbol=symbol,
            ttl=TTL_12H,
            default=[],
        )

        history = _as_list(data)

        if not history:
            return {"error": f"No historical market cap data found for '{symbol}'"}

        # Sort newest first and apply limit
        history.sort(key=lambda h: h.get("date") or "", reverse=True)
        trimmed = history[:limit]

        # Format data points
        data_points = []
        for entry in trimmed:
            data_points.append({
                "date": entry.get("date"),
                "market_cap": entry.get("marketCap"),
            })

        # Calculate change from oldest to newest in the trimmed set
        change_pct = None
        if len(data_points) >= 2:
            oldest_mc = data_points[-1].get("market_cap")
            newest_mc = data_points[0].get("market_cap")
            if oldest_mc and newest_mc and oldest_mc > 0:
                change_pct = round((newest_mc / oldest_mc - 1) * 100, 2)

        return {
            "symbol": symbol,
            "current_market_cap": data_points[0].get("market_cap") if data_points else None,
            "data_points": len(data_points),
            "change_pct": change_pct,
            "history": data_points,
        }

    VALID_INDICATORS = {"sma", "ema", "rsi", "adx", "wma", "dema", "tema", "williams", "standarddeviation", "macd"}

    @mcp.tool(
        annotations={
            "title": "Technical Indicators",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def technical_indicators(
        symbol: str,
        indicator: str = "rsi",
        period_length: int = 14,
        timeframe: str = "1day",
    ) -> dict:
        """Get technical indicator values for a stock.

        Supports SMA, EMA, RSI, ADX, WMA, DEMA, TEMA, Williams %R,
        Standard Deviation, and MACD (via Polygon.io). Returns last 30 data points.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            indicator: Indicator type - sma, ema, rsi, adx, wma, dema, tema, williams, standarddeviation, macd
            period_length: Lookback period (default 14)
            timeframe: Data timeframe - "1day", "1hour", "4hour", "1week" (default "1day")
        """
        symbol = symbol.upper().strip()
        indicator = indicator.lower().strip()

        if indicator not in VALID_INDICATORS:
            return {"error": f"Invalid indicator '{indicator}'. Use: {', '.join(sorted(VALID_INDICATORS))}"}

        # MACD is only available via Polygon
        if indicator == "macd":
            if polygon_client is None:
                return {"error": "MACD requires POLYGON_API_KEY to be configured"}

            # Map timeframe to Polygon timespan
            timespan_map = {"1day": "day", "1hour": "hour", "4hour": "hour", "1week": "week"}
            timespan = timespan_map.get(timeframe, "day")

            data = await polygon_client.get_safe(
                f"/v1/indicators/macd/{symbol}",
                params={
                    "timespan": timespan,
                    "series_type": "close",
                    "limit": 30,
                    "order": "desc",
                },
                cache_ttl=polygon_TTL_HOURLY,
            )

            if not data or not isinstance(data, dict):
                return {"error": f"No MACD data found for '{symbol}'"}

            results = data.get("results", {})
            raw_values = results.get("values", [])

            if not raw_values:
                return {"error": f"No MACD data found for '{symbol}'"}

            values = []
            for d in raw_values[:30]:
                ts = d.get("timestamp")
                date_str = _ms_to_str(ts, "%Y-%m-%d")
                values.append({
                    "date": date_str,
                    "macd": d.get("value"),
                    "signal": d.get("signal"),
                    "histogram": d.get("histogram"),
                })

            current = values[0] if values else {}
            return {
                "symbol": symbol,
                "indicator": "macd",
                "timeframe": timeframe,
                "current_value": current.get("macd"),
                "current_signal": current.get("signal"),
                "current_histogram": current.get("histogram"),
                "data_points": len(values),
                "values": values,
                "source": "polygon.io",
            }

        # All other indicators via FMP
        indicator_map = {
            "sma": client.technical.get_sma,
            "ema": client.technical.get_ema,
            "rsi": client.technical.get_rsi,
            "adx": client.technical.get_adx,
            "wma": client.technical.get_wma,
            "dema": client.technical.get_dema,
            "tema": client.technical.get_tema,
            "williams": client.technical.get_williams,
            "standarddeviation": client.technical.get_standard_deviation,
        }
        data = await _safe_call(
            indicator_map[indicator],
            symbol=symbol,
            period_length=period_length,
            timeframe=timeframe,
            ttl=TTL_HOURLY,
            default=[],
        )

        data_list = _as_list(data)

        if not data_list:
            return {"error": f"No {indicator.upper()} data found for '{symbol}'"}

        # Return last 30 data points, newest first
        trimmed = data_list[:30]

        values = []
        for d in trimmed:
            entry = {
                "date": d.get("date"),
                "close": d.get("close"),
                indicator: d.get(indicator),
            }
            values.append(entry)

        current_value = values[0].get(indicator) if values else None

        return {
            "symbol": symbol,
            "indicator": indicator,
            "period_length": period_length,
            "timeframe": timeframe,
            "current_value": current_value,
            "data_points": len(values),
            "values": values,
        }
