"""TradingView scanner-backed analytical tools.

Consolidates the scanner-column tools that previously lived in
cassandra-tradingview-mcp. Each is a thin wrapper on TV's
`scanner.tradingview.com/<market>/scan` (or `/metainfo`) endpoint,
called via `TvProxyClient`. All eight tools register only when the
TV proxy is configured.

Tool inventory:

- `technical_rating(symbol)`      — TV's composite Recommend.All + MA/Other
                                    sub-scores + 26 raw indicators.
- `perf_matrix(symbols, tfs)`     — N × M rolling-perf comp in one call.
- `fundamentals_matrix(symbols)`  — N × M valuation/profitability/growth
                                    comp in one call.
- `proximity_scan(side, thresh)`  — names within N% of a 52w/ATH/monthly
                                    extreme.
- `pattern_scan(patterns, bias)`  — candlestick pattern screener with
                                    multi-pattern cluster ranking.
- `premarket_movers(session)`     — pre/post-market gap leaders.
- `relative_volume_leaders(rvol)` — unusual-volume catalyst detector.
- `screener_columns(query)`       — searchable catalog of TV's 3500+
                                    scanner columns.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from cass_market_sdk.clients.tv_proxy import TvProxyClient


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# technical_rating
# ---------------------------------------------------------------------------

_RATING_BANDS = [
    (-1.01, -0.5, "Strong Sell"),
    (-0.5, -0.1, "Sell"),
    (-0.1, 0.1, "Neutral"),
    (0.1, 0.5, "Buy"),
    (0.5, 1.01, "Strong Buy"),
]

_RATING_COLUMNS = ["Recommend.All", "Recommend.MA", "Recommend.Other"]

_INDICATOR_COLUMNS = [
    # Momentum / oscillators (roll into Recommend.Other)
    "RSI", "RSI7", "Mom",
    "MACD.macd", "MACD.signal", "MACD.hist",
    "Stoch.K", "Stoch.D", "Stoch.RSI.K", "Stoch.RSI.D",
    "CCI20", "ADX", "AO", "W.R", "BBPower", "UO",
    # Trend (roll into Recommend.MA)
    "SMA10", "SMA20", "SMA30", "SMA50", "SMA100", "SMA200",
    "EMA10", "EMA20", "EMA30", "EMA50", "EMA100", "EMA200",
    "HullMA9", "VWMA", "Ichimoku.BLine",
]


def _rating_label(score: float | None) -> str | None:
    if score is None:
        return None
    for lo, hi, name in _RATING_BANDS:
        if lo <= score < hi:
            return name
    return None


def _rating_block(score: float | None) -> dict[str, Any]:
    return {"score": score, "label": _rating_label(score)}


# ---------------------------------------------------------------------------
# perf_matrix
# ---------------------------------------------------------------------------

_TIMEFRAME_COLUMNS: dict[str, str] = {
    "1D": "change",
    "1W": "Perf.W",
    "1M": "Perf.1M",
    "3M": "Perf.3M",
    "6M": "Perf.6M",
    "YTD": "Perf.YTD",
    "1Y": "Perf.Y",
    "5Y": "Perf.5Y",
    "ALL": "Perf.All",
}

_DEFAULT_PERF_TIMEFRAMES = ["1D", "1W", "1M", "3M", "YTD", "1Y"]


# ---------------------------------------------------------------------------
# fundamentals_matrix
# ---------------------------------------------------------------------------

# Every column here was verified against /america/metainfo. Use
# `screener_columns` to discover more.
_FUND_PRESETS: dict[str, list[str]] = {
    "valuation": [
        "price_earnings_ttm",
        "price_earnings_forward_fy",
        "price_book_fq",
        "price_sales_current",
        "price_free_cash_flow_ttm",
        "enterprise_value_ebitda_ttm",
        "enterprise_value_to_revenue_ttm",
        "price_earnings_growth_ttm",
    ],
    "profitability": [
        "gross_margin_ttm",
        "operating_margin_ttm",
        "net_margin_ttm",
        "ebitda_margin_ttm",
        "free_cash_flow_margin_ttm",
        "return_on_assets_fq",
        "return_on_equity_fq",
        "return_on_invested_capital_fq",
    ],
    "leverage": [
        "debt_to_equity_fq",
        "long_term_debt_to_equity_fq",
        "current_ratio_fq",
        "quick_ratio_fq",
        "total_debt_fq",
        "cash_n_short_term_invest_fq",
    ],
    "growth": [
        "total_revenue_yoy_growth_ttm",
        "earnings_per_share_diluted_yoy_growth_ttm",
        "earnings_per_share_diluted_yoy_growth_fq",
        "ebitda_yoy_growth_ttm",
        "free_cash_flow_yoy_growth_ttm",
    ],
    "dividends": [
        "dividend_yield_recent",
        "dividend_payout_ratio_ttm",
        "dividends_per_share_fq",
        "continuous_dividend_payout",
        "continuous_dividend_growth",
    ],
    "size": [
        "market_cap_basic",
        "enterprise_value_fq",
        "total_revenue_ttm",
        "ebitda_ttm",
        "net_income_ttm",
        "free_cash_flow_ttm",
        "number_of_employees",
    ],
    "default": [
        "market_cap_basic",
        "price_earnings_ttm",
        "price_book_fq",
        "price_sales_current",
        "enterprise_value_ebitda_ttm",
        "gross_margin_ttm",
        "operating_margin_ttm",
        "return_on_equity_fq",
        "debt_to_equity_fq",
        "total_revenue_yoy_growth_ttm",
        "earnings_per_share_diluted_yoy_growth_ttm",
        "dividend_yield_recent",
    ],
}

_FUND_CONTEXT_COLUMNS = ["name", "description", "close", "sector", "industry"]


def _resolve_fund_metrics(
    metrics: list[str] | None,
    preset: str,
) -> list[str]:
    if not metrics:
        return list(_FUND_PRESETS.get(preset, _FUND_PRESETS["default"]))
    resolved: list[str] = []
    for m in metrics:
        if m in _FUND_PRESETS:
            resolved.extend(_FUND_PRESETS[m])
        else:
            resolved.append(m)
    seen: set[str] = set()
    out: list[str] = []
    for m in resolved:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out


# ---------------------------------------------------------------------------
# proximity_scan
# ---------------------------------------------------------------------------

_PROX_SIDES: dict[str, tuple[str, bool]] = {
    "ath": ("High.All", True),
    "atl": ("Low.All", True),
    "52w_high": ("price_52_week_high", True),
    "52w_low": ("price_52_week_low", False),
    "1m_high": ("High.1M", True),
    "1m_low": ("Low.1M", False),
    "3m_high": ("High.3M", True),
    "3m_low": ("Low.3M", False),
    "6m_high": ("High.6M", True),
    "6m_low": ("Low.6M", False),
}

_PROX_CONTEXT = [
    "name", "description",
    "close", "change", "change_abs", "volume",
    "relative_volume_10d_calc",
    "market_cap_basic", "sector", "industry",
    "RSI",
]


# ---------------------------------------------------------------------------
# pattern_scan
# ---------------------------------------------------------------------------

_PATTERNS_BULLISH = [
    "Hammer", "InvertedHammer", "MorningStar", "3WhiteSoldiers",
    "Engulfing.Bullish", "AbandonedBaby.Bullish", "Harami.Bullish",
    "Kicking.Bullish", "TriStar.Bullish", "Marubozu.White", "Doji.Dragonfly",
]

_PATTERNS_BEARISH = [
    "HangingMan", "ShootingStar", "EveningStar", "3BlackCrows",
    "Engulfing.Bearish", "AbandonedBaby.Bearish", "Harami.Bearish",
    "Kicking.Bearish", "TriStar.Bearish", "Marubozu.Black", "Doji.Gravestone",
]

_PATTERNS_NEUTRAL = [
    "Doji", "SpinningTop.Black", "SpinningTop.White",
    "LongShadow.Lower", "LongShadow.Upper",
]

_ALL_PATTERNS = set(_PATTERNS_BULLISH) | set(_PATTERNS_BEARISH) | set(_PATTERNS_NEUTRAL)

_VALID_PATTERN_TIMEFRAMES = {
    "D", "1W", "1M", "1", "5", "15", "30", "60", "120", "240",
}

_PATTERN_CONTEXT = [
    "name", "description",
    "close", "change", "change_abs", "volume",
    "relative_volume_10d_calc",
    "market_cap_basic", "sector", "industry",
    "RSI",
]


def _pattern_col(pattern: str, timeframe: str) -> str:
    base = f"Candle.{pattern}"
    if timeframe == "D":
        return base
    return f"{base}|{timeframe}"


# ---------------------------------------------------------------------------
# premarket_movers
# ---------------------------------------------------------------------------

_PRE_COLUMNS = [
    "name", "description", "close",
    "premarket_change", "premarket_change_abs",
    "premarket_close", "premarket_volume", "premarket_gap",
    "volume", "average_volume_10d_calc",
    "market_cap_basic", "sector", "industry",
    "earnings_release_next_date", "earnings_release_trading_date_fq",
]

_POST_COLUMNS = [
    "name", "description", "close",
    "postmarket_change", "postmarket_change_abs",
    "postmarket_close", "postmarket_volume",
    "volume", "average_volume_10d_calc",
    "market_cap_basic", "sector", "industry",
    "earnings_release_date",
]


def _shape_pre(raw: dict) -> dict:
    values = raw.get("d") or []
    row = dict(zip(_PRE_COLUMNS, values, strict=False))
    return {
        "symbol": raw.get("s"),
        "name": row.get("name"),
        "description": row.get("description"),
        "prev_close": row.get("close"),
        "premarket_price": row.get("premarket_close"),
        "premarket_change_pct": row.get("premarket_change"),
        "premarket_change_abs": row.get("premarket_change_abs"),
        "premarket_gap_pct": row.get("premarket_gap"),
        "premarket_volume": row.get("premarket_volume"),
        "prior_day_volume": row.get("volume"),
        "avg_volume_10d": row.get("average_volume_10d_calc"),
        "market_cap": row.get("market_cap_basic"),
        "sector": row.get("sector"),
        "industry": row.get("industry"),
        "earnings_next": row.get("earnings_release_next_date"),
        "earnings_fq": row.get("earnings_release_trading_date_fq"),
    }


def _shape_post(raw: dict) -> dict:
    values = raw.get("d") or []
    row = dict(zip(_POST_COLUMNS, values, strict=False))
    return {
        "symbol": raw.get("s"),
        "name": row.get("name"),
        "description": row.get("description"),
        "regular_close": row.get("close"),
        "postmarket_price": row.get("postmarket_close"),
        "postmarket_change_pct": row.get("postmarket_change"),
        "postmarket_change_abs": row.get("postmarket_change_abs"),
        "postmarket_volume": row.get("postmarket_volume"),
        "regular_volume": row.get("volume"),
        "avg_volume_10d": row.get("average_volume_10d_calc"),
        "market_cap": row.get("market_cap_basic"),
        "sector": row.get("sector"),
        "industry": row.get("industry"),
        "earnings_date": row.get("earnings_release_date"),
    }


# ---------------------------------------------------------------------------
# relative_volume_leaders
# ---------------------------------------------------------------------------

_RVOL_CONTEXT = [
    "name", "description",
    "close", "change", "change_abs", "volume",
    "relative_volume_10d_calc", "average_volume_10d_calc",
    "market_cap_basic", "sector", "industry",
    "RSI", "ATR",
]


# ---------------------------------------------------------------------------
# screener_columns — metainfo catalog with per-process TTL cache
# ---------------------------------------------------------------------------

# {market: (fetched_at, fields)}. The metainfo payload is ~250KB and rarely
# changes; a 24h cache avoids reloading on every search.
_COLUMN_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_COLUMN_CACHE_TTL = 24 * 3600


async def _load_metainfo(
    tv_proxy: TvProxyClient,
    market: str,
) -> list[dict[str, Any]]:
    now = time.time()
    cached = _COLUMN_CACHE.get(market)
    if cached and (now - cached[0]) < _COLUMN_CACHE_TTL:
        return cached[1]
    data = await tv_proxy.get_json("scanner", f"/{market}/metainfo")
    fields = data.get("fields") or []
    _COLUMN_CACHE[market] = (now, fields)
    return fields


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def register(
    mcp: FastMCP,
    *,
    tv_proxy: TvProxyClient | None = None,
) -> None:
    """Register TV-scanner-backed tools. No-op if tv_proxy is unset."""

    if tv_proxy is None:
        return

    _register_technical_rating(mcp, tv_proxy)
    _register_perf_matrix(mcp, tv_proxy)
    _register_fundamentals_matrix(mcp, tv_proxy)
    _register_proximity_scan(mcp, tv_proxy)
    _register_pattern_scan(mcp, tv_proxy)
    _register_premarket_movers(mcp, tv_proxy)
    _register_relative_volume_leaders(mcp, tv_proxy)
    _register_screener_columns(mcp, tv_proxy)


def _register_technical_rating(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Technical Rating",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def technical_rating(
        symbol: str,
        *,
        market: str = "america",
        include_indicators: bool = True,
    ) -> dict:
        """TV's composite technical rating for a single symbol.

        Returns `Recommend.All` (overall buy/sell bar on TV charts)
        plus `Recommend.MA` (moving averages only) and
        `Recommend.Other` (oscillators only). Disagreement between
        the MA and oscillator sub-scores is often a reversal tell.

        Args:
            symbol: `EXCHANGE:TICKER` composite (e.g. `"NASDAQ:AAPL"`).
            market: Scanner market segment (default `"america"`).
            include_indicators: Include the raw 26 indicator values
                powering the composite (RSI, MACD, Stoch, CCI, ADX,
                AO, SMA/EMA 10..200, Hull MA, Ichimoku). Default True.

        Returns:
            `{symbol, market, overall: {score, label},
            moving_averages: {score, label},
            oscillators: {score, label},
            indicators: {RSI, MACD.macd, SMA50, ...} | null}`.
            Scores are in `[-1, 1]`; labels are Strong Sell / Sell /
            Neutral / Buy / Strong Buy.
        """
        cols = list(_RATING_COLUMNS)
        if include_indicators:
            cols += _INDICATOR_COLUMNS
        body = {
            "filter": [],
            "options": {"lang": "en"},
            "markets": [market],
            "symbols": {"query": {"types": []}, "tickers": [symbol]},
            "columns": cols,
            "range": [0, 1],
        }
        data = await tv_proxy.post_json(
            "scanner", f"/{market}/scan", json=body,
        )
        rows = data.get("data") or []
        if not rows:
            return {
                "symbol": symbol, "market": market,
                "error": "no data — symbol may not belong to this market segment",
            }
        values = rows[0].get("d") or []
        row = dict(zip(cols, values, strict=False))
        result: dict[str, Any] = {
            "symbol": symbol, "market": market,
            "overall": _rating_block(row.get("Recommend.All")),
            "moving_averages": _rating_block(row.get("Recommend.MA")),
            "oscillators": _rating_block(row.get("Recommend.Other")),
        }
        if include_indicators:
            result["indicators"] = {k: row.get(k) for k in _INDICATOR_COLUMNS}
        return result


def _register_perf_matrix(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Performance Matrix",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def perf_matrix(
        symbols: list[str],
        *,
        timeframes: list[str] | None = None,
        market: str = "america",
    ) -> dict:
        """N symbols × M rolling-performance windows in one call.

        Use when you need to cross-compare a basket across multiple
        windows — "leaders last 5 days that are also green YTD", "is
        SMH outperforming SOXX at every horizon". One scanner request
        returns every cell.

        Args:
            symbols: `EXCHANGE:TICKER` composites.
            timeframes: Subset of `["1D", "1W", "1M", "3M", "6M",
                "YTD", "1Y", "5Y", "ALL"]`. Default:
                `["1D", "1W", "1M", "3M", "YTD", "1Y"]`.
            market: Scanner market segment (default `"america"`).

        Returns:
            `{timeframes, rows: [{symbol, name, description, close,
            volume, market_cap, sector, perf: {tf: pct, ...}}, ...]}`.
            Missing cells are null (e.g. 5Y perf for a recent IPO).
        """
        if not symbols:
            return {"timeframes": [], "rows": []}
        tfs = [t.upper() for t in (timeframes or _DEFAULT_PERF_TIMEFRAMES)]
        unknown = [t for t in tfs if t not in _TIMEFRAME_COLUMNS]
        if unknown:
            return {
                "error": f"unknown timeframes: {unknown}. valid: "
                f"{sorted(_TIMEFRAME_COLUMNS.keys())}",
            }
        perf_cols = [_TIMEFRAME_COLUMNS[t] for t in tfs]
        cols = ["name", "description", "close", "volume",
                "market_cap_basic", "sector"] + perf_cols
        body = {
            "filter": [],
            "options": {"lang": "en"},
            "markets": [market],
            "symbols": {"query": {"types": []}, "tickers": list(symbols)},
            "columns": cols,
            "range": [0, len(symbols)],
        }
        data = await tv_proxy.post_json(
            "scanner", f"/{market}/scan", json=body,
        )
        by_sym: dict[str, dict] = {}
        for raw in data.get("data") or []:
            sym = raw.get("s")
            values = raw.get("d") or []
            row = dict(zip(cols, values, strict=False))
            by_sym[sym] = {
                "symbol": sym,
                "name": row.get("name"),
                "description": row.get("description"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "market_cap": row.get("market_cap_basic"),
                "sector": row.get("sector"),
                "perf": {
                    tf: row.get(_TIMEFRAME_COLUMNS[tf]) for tf in tfs
                },
            }
        ordered = [by_sym[s] for s in symbols if s in by_sym]
        return {"timeframes": tfs, "rows": ordered}


def _register_fundamentals_matrix(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Fundamentals Matrix",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def fundamentals_matrix(
        symbols: list[str],
        *,
        metrics: list[str] | None = None,
        preset: str = "default",
        market: str = "america",
    ) -> dict:
        """Fundamentals comp table for N symbols × M metrics, one call.

        Collapses looping `financial_statements` N times into a single
        scanner request. `financial_statements` still wins for full
        line items (segment revenue, detailed cash flow); use this
        for rapid cross-sectional comparison.

        Args:
            symbols: `EXCHANGE:TICKER` composites.
            metrics: Mix of raw scanner column names and preset group
                names. Presets: `"valuation"`, `"profitability"`,
                `"leverage"`, `"growth"`, `"dividends"`, `"size"`,
                `"default"`. Call `screener_columns` to discover
                additional columns.
            preset: Default preset when `metrics` is None. Default
                `"default"` — 12 first-pass fields.
            market: Scanner market segment (default `"america"`).

        Returns:
            `{metrics, rows: [{symbol, name, description, sector,
            industry, close, fundamentals: {metric: value, ...}},
            ...], missing: [symbols not returned]}`.
        """
        if not symbols:
            return {"metrics": [], "rows": []}
        resolved = _resolve_fund_metrics(metrics, preset)
        cols = _FUND_CONTEXT_COLUMNS + resolved
        body = {
            "filter": [],
            "options": {"lang": "en"},
            "markets": [market],
            "symbols": {"query": {"types": []}, "tickers": list(symbols)},
            "columns": cols,
            "range": [0, len(symbols)],
        }
        data = await tv_proxy.post_json(
            "scanner", f"/{market}/scan", json=body,
        )
        by_sym: dict[str, dict] = {}
        for raw in data.get("data") or []:
            sym = raw.get("s")
            if not sym:
                continue
            values = raw.get("d") or []
            row = dict(zip(cols, values, strict=False))
            by_sym[sym] = {
                "symbol": sym,
                "name": row.get("name"),
                "description": row.get("description"),
                "close": row.get("close"),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "fundamentals": {m: row.get(m) for m in resolved},
            }
        ordered = [by_sym[s] for s in symbols if s in by_sym]
        missing = [s for s in symbols if s not in by_sym]
        result: dict[str, Any] = {"metrics": resolved, "rows": ordered}
        if missing:
            result["missing"] = missing
        return result


def _register_proximity_scan(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Proximity Scan",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def proximity_scan(
        *,
        side: str = "52w_high",
        threshold_pct: float = 2.0,
        market: str = "america",
        min_market_cap: float = 1_000_000_000,
        min_price: float = 5.0,
        min_volume: int = 500_000,
        universe_size: int = 500,
        limit: int = 50,
    ) -> dict:
        """Names within N% of a 52-week / multi-month / all-time extreme.

        Breakout hunting (`side="52w_high"`, `threshold_pct=1.0`),
        breakdown hunting (`side="52w_low"`), and mean-reversion
        candidates (`side="6m_low"`). TV's scanner filter grammar
        doesn't support cross-column comparisons, so the universe is
        pre-filtered by liquidity and the distance math runs
        client-side.

        Args:
            side: One of `ath`, `atl`, `52w_high`, `52w_low`,
                `1m_high`, `1m_low`, `3m_high`, `3m_low`,
                `6m_high`, `6m_low`.
            threshold_pct: Proximity window in percent (default 2.0).
            market: Scanner market segment (default `"america"`).
            min_market_cap: Liquidity floor (default $1B).
            min_price: Minimum last price (default $5).
            min_volume: Minimum volume (default 500k).
            universe_size: Pre-filter universe size before client-side
                proximity check (default 500, cap 1000).
            limit: Max rows after proximity filter (default 50, cap 200).

        Returns:
            `{side, threshold_pct, reference_column, count,
            rows: [{symbol, close, reference, distance_pct, ...}, ...]}`,
            sorted by `|distance_pct|` ascending.
        """
        universe_size = max(1, min(universe_size, 1000))
        limit = max(1, min(limit, 200))
        threshold = threshold_pct / 100.0
        if side not in _PROX_SIDES:
            return {"error": f"side must be one of {sorted(_PROX_SIDES)}, got {side!r}"}
        ref_col, _ = _PROX_SIDES[side]
        is_high = not side.endswith("_low") and side != "atl"
        cols = _PROX_CONTEXT + [ref_col]
        filters: list[dict[str, Any]] = [
            {"left": "market_cap_basic", "operation": "egreater",
             "right": min_market_cap},
            {"left": "close", "operation": "egreater", "right": min_price},
            {"left": "volume", "operation": "egreater", "right": min_volume},
            {"left": "is_primary", "operation": "equal", "right": True},
            {"left": "type", "operation": "in_range",
             "right": ["stock", "dr", "fund"]},
        ]
        body = {
            "filter": filters,
            "options": {"lang": "en"},
            "markets": [market],
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": cols,
            "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
            "range": [0, universe_size],
        }
        data = await tv_proxy.post_json(
            "scanner", f"/{market}/scan", json=body,
        )
        matches: list[dict[str, Any]] = []
        for raw in data.get("data") or []:
            values = raw.get("d") or []
            row = dict(zip(cols, values, strict=False))
            close = row.get("close")
            ref = row.get(ref_col)
            if close is None or ref in (None, 0):
                continue
            distance = (close - ref) / ref
            if is_high:
                if distance < -threshold:
                    continue
            else:
                if distance > threshold:
                    continue
            matches.append({
                "symbol": raw.get("s"),
                "name": row.get("name"),
                "description": row.get("description"),
                "close": close,
                "reference": ref,
                "distance_pct": round(distance * 100, 3),
                "change": row.get("change"),
                "change_abs": row.get("change_abs"),
                "volume": row.get("volume"),
                "rvol_10d": row.get("relative_volume_10d_calc"),
                "market_cap": row.get("market_cap_basic"),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "rsi": row.get("RSI"),
            })
        matches.sort(key=lambda r: abs(r["distance_pct"]))
        matches = matches[:limit]
        return {
            "side": side,
            "threshold_pct": threshold_pct,
            "reference_column": ref_col,
            "universe_size": universe_size,
            "count": len(matches),
            "rows": matches,
        }


def _register_pattern_scan(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Pattern Scan",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def pattern_scan(
        patterns: list[str] | None = None,
        *,
        bias: str = "bullish",
        timeframe: str = "D",
        market: str = "america",
        min_market_cap: float = 300_000_000,
        min_price: float = 2.0,
        min_volume: int = 200_000,
        limit: int = 50,
    ) -> dict:
        """Candlestick pattern screener — 27 patterns × 10 timeframes.

        TV computes each pattern server-side; its column is `> 0` on
        the bar it fires. This tool fans out one scan per requested
        pattern in parallel, merges by symbol, and ranks multi-pattern
        clusters first (a Hammer + Engulfing.Bullish cluster is a
        stronger signal than either alone).

        Args:
            patterns: Subset to scan. Names omit the `Candle.` prefix
                — `"Hammer"`, `"Engulfing.Bullish"`, etc. When None,
                uses the default group selected by `bias`.
            bias: Default group when `patterns` is None — `"bullish"`,
                `"bearish"`, `"neutral"`, or `"all"`.
            timeframe: Bar resolution. `"D"` (default), `"1W"`,
                `"1M"`, or minutes `"1"`/`"5"`/`"15"`/`"30"`/`"60"`/
                `"120"`/`"240"`.
            market: Scanner market segment (default `"america"`).
            min_market_cap: Minimum market cap (default $300M).
            min_price: Minimum last price (default $2).
            min_volume: Minimum volume (default 200k).
            limit: Max rows (default 50, cap 200).

        Returns:
            `{patterns, timeframe, count, rows: [{symbol, close,
            patterns_fired: [...], ...}, ...]}`. Sorted by cluster
            size (most patterns fired first), then RVOL.
        """
        limit = max(1, min(limit, 200))
        if timeframe not in _VALID_PATTERN_TIMEFRAMES:
            return {"error": f"timeframe must be one of {sorted(_VALID_PATTERN_TIMEFRAMES)}"}
        if patterns is None:
            pool = (_PATTERNS_BULLISH if bias == "bullish"
                    else _PATTERNS_BEARISH if bias == "bearish"
                    else _PATTERNS_NEUTRAL if bias == "neutral"
                    else sorted(_ALL_PATTERNS) if bias == "all"
                    else None)
            if pool is None:
                return {"error": f"bias must be bullish|bearish|neutral|all, got {bias!r}"}
            patterns = pool
        unknown = [p for p in patterns if p not in _ALL_PATTERNS]
        if unknown:
            return {"error": f"unknown patterns: {unknown}. valid: {sorted(_ALL_PATTERNS)}"}
        pattern_cols = [_pattern_col(p, timeframe) for p in patterns]
        cols = _PATTERN_CONTEXT + pattern_cols
        base_filters: list[dict[str, Any]] = [
            {"left": "market_cap_basic", "operation": "egreater",
             "right": min_market_cap},
            {"left": "close", "operation": "egreater", "right": min_price},
            {"left": "volume", "operation": "egreater", "right": min_volume},
            {"left": "is_primary", "operation": "equal", "right": True},
            {"left": "type", "operation": "in_range",
             "right": ["stock", "dr", "fund"]},
        ]

        async def _scan_one(pat_col: str) -> list[dict[str, Any]]:
            body = {
                "filter": base_filters + [
                    {"left": pat_col, "operation": "greater", "right": 0},
                ],
                "options": {"lang": "en"},
                "markets": [market],
                "symbols": {"query": {"types": []}, "tickers": []},
                "columns": cols,
                "sort": {"sortBy": "relative_volume_10d_calc",
                         "sortOrder": "desc"},
                "range": [0, limit],
            }
            data = await tv_proxy.post_json(
                "scanner", f"/{market}/scan", json=body,
            )
            return data.get("data") or []

        per_pattern = await asyncio.gather(
            *(_scan_one(pc) for pc in pattern_cols)
        )
        by_symbol: dict[str, dict[str, Any]] = {}
        for pname, raws in zip(patterns, per_pattern, strict=False):
            for raw in raws:
                sym = raw.get("s")
                if not sym:
                    continue
                values = raw.get("d") or []
                row = dict(zip(cols, values, strict=False))
                entry = by_symbol.setdefault(sym, {
                    "symbol": sym,
                    "name": row.get("name"),
                    "description": row.get("description"),
                    "close": row.get("close"),
                    "change": row.get("change"),
                    "change_abs": row.get("change_abs"),
                    "volume": row.get("volume"),
                    "rvol_10d": row.get("relative_volume_10d_calc"),
                    "market_cap": row.get("market_cap_basic"),
                    "sector": row.get("sector"),
                    "industry": row.get("industry"),
                    "rsi": row.get("RSI"),
                    "patterns_fired": [],
                })
                entry["patterns_fired"].append(pname)
        rows = list(by_symbol.values())
        rows.sort(key=lambda r: (
            -len(r["patterns_fired"]),
            -(r.get("rvol_10d") or 0),
        ))
        rows = rows[:limit]
        return {
            "patterns": patterns,
            "timeframe": timeframe,
            "count": len(rows),
            "rows": rows,
        }


def _register_premarket_movers(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Pre/Post-Market Movers",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def premarket_movers(
        *,
        session: str = "premarket",
        min_change_pct: float = 2.0,
        min_volume: int = 50_000,
        min_market_cap: float = 300_000_000,
        direction: str = "any",
        limit: int = 30,
    ) -> dict:
        """Names gapping the most before / after the regular session.

        Uses TV scanner's session-specific change/volume columns so
        the filter runs server-side. No FMP equivalent — its tools
        cover regular hours only.

        Args:
            session: `"premarket"` (04:00–09:30 ET) or
                `"postmarket"` (16:00–20:00 ET).
            min_change_pct: Absolute session % change floor (default
                2.0). Drop to 1.0 for a wider net; 5.0 for outliers.
            min_volume: Minimum session volume (default 50k). Below
                this is illiquid print noise.
            min_market_cap: Minimum market cap (default $300M).
            direction: `"up"`, `"down"`, or `"any"` (default).
            limit: Max rows (default 30, cap 100).

        Returns:
            `{session, criteria, count, rows: [{symbol, prev_close,
            premarket_price, premarket_change_pct, premarket_volume,
            ...}, ...]}`, sorted by `|change_pct|` desc.
        """
        limit = max(1, min(limit, 100))
        session = session.lower()
        direction = direction.lower()
        if session == "premarket":
            change_col = "premarket_change"
            vol_col = "premarket_volume"
            cols = _PRE_COLUMNS
            shape = _shape_pre
        elif session == "postmarket":
            change_col = "postmarket_change"
            vol_col = "postmarket_volume"
            cols = _POST_COLUMNS
            shape = _shape_post
        else:
            return {"error": f"session must be 'premarket' or 'postmarket', got {session!r}"}
        filters: list[dict[str, Any]] = [
            {"left": vol_col, "operation": "egreater", "right": min_volume},
            {"left": "market_cap_basic", "operation": "egreater",
             "right": min_market_cap},
            {"left": "is_primary", "operation": "equal", "right": True},
            {"left": "type", "operation": "in_range",
             "right": ["stock", "dr", "fund"]},
        ]
        if direction == "up":
            filters.append({"left": change_col, "operation": "egreater",
                            "right": min_change_pct})
        elif direction == "down":
            filters.append({"left": change_col, "operation": "eless",
                            "right": -min_change_pct})
        else:
            filters.append({"left": change_col, "operation": "nempty",
                            "right": None})
        body = {
            "filter": filters,
            "options": {"lang": "en"},
            "markets": ["america"],
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": cols,
            "sort": {"sortBy": change_col, "sortOrder": "desc"},
            "range": [0, limit * 2 if direction == "any" else limit],
        }
        data = await tv_proxy.post_json("scanner", "/america/scan", json=body)
        rows: list[dict[str, Any]] = []
        for raw in data.get("data") or []:
            r = shape(raw)
            if direction == "any":
                ch = (r.get("premarket_change_pct")
                      if session == "premarket"
                      else r.get("postmarket_change_pct"))
                if ch is None or abs(ch) < min_change_pct:
                    continue
            rows.append(r)

        def _abs_ch(r):
            ch = (r.get("premarket_change_pct")
                  if session == "premarket"
                  else r.get("postmarket_change_pct"))
            return abs(ch or 0)
        rows.sort(key=_abs_ch, reverse=True)
        rows = rows[:limit]
        return {
            "session": session,
            "criteria": {
                "min_change_pct": min_change_pct,
                "min_volume": min_volume,
                "min_market_cap": min_market_cap,
                "direction": direction,
            },
            "count": len(rows),
            "rows": rows,
        }


def _register_relative_volume_leaders(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Relative Volume Leaders",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def relative_volume_leaders(
        *,
        market: str = "america",
        min_rvol: float = 3.0,
        min_market_cap: float = 300_000_000,
        min_price: float = 2.0,
        direction: str = "any",
        limit: int = 30,
    ) -> dict:
        """Names trading on unusually high volume today.

        Filters the scanner on `today_volume / 10d_avg >= min_rvol`
        (default 3x the 10-day average). FMP's `market_overview` most-
        active list is raw-volume-ranked, so it's always dominated by
        mega-caps; this RVOL version surfaces small/mid-caps breaking
        out of routine.

        Args:
            market: Scanner market segment (default `"america"`).
            min_rvol: Minimum relative volume (default 3.0). 2.0 for
                wider, 5.0 for outliers only.
            min_market_cap: Min market cap (default $300M).
            min_price: Min last price (default $2).
            direction: `"up"`, `"down"`, or `"any"` (default).
            limit: Max rows (default 30, cap 100).

        Returns:
            `{criteria, count, rows: [{symbol, close, change,
            change_abs, volume, rvol_10d, avg_volume_10d, market_cap,
            sector, industry, rsi, atr}, ...]}`, sorted by rvol desc.
        """
        limit = max(1, min(limit, 100))
        filters: list[dict[str, Any]] = [
            {"left": "relative_volume_10d_calc", "operation": "greater",
             "right": min_rvol},
            {"left": "market_cap_basic", "operation": "egreater",
             "right": min_market_cap},
            {"left": "close", "operation": "egreater", "right": min_price},
            {"left": "is_primary", "operation": "equal", "right": True},
            {"left": "type", "operation": "in_range",
             "right": ["stock", "dr", "fund"]},
        ]
        d = direction.lower()
        if d == "up":
            filters.append({"left": "change", "operation": "greater", "right": 0})
        elif d == "down":
            filters.append({"left": "change", "operation": "less", "right": 0})
        body = {
            "filter": filters,
            "options": {"lang": "en"},
            "markets": [market],
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": _RVOL_CONTEXT,
            "sort": {"sortBy": "relative_volume_10d_calc",
                     "sortOrder": "desc"},
            "range": [0, limit],
        }
        data = await tv_proxy.post_json(
            "scanner", f"/{market}/scan", json=body,
        )
        rows: list[dict[str, Any]] = []
        for raw in data.get("data") or []:
            values = raw.get("d") or []
            row = dict(zip(_RVOL_CONTEXT, values, strict=False))
            rows.append({
                "symbol": raw.get("s"),
                "name": row.get("name"),
                "description": row.get("description"),
                "close": row.get("close"),
                "change": row.get("change"),
                "change_abs": row.get("change_abs"),
                "volume": row.get("volume"),
                "rvol_10d": row.get("relative_volume_10d_calc"),
                "avg_volume_10d": row.get("average_volume_10d_calc"),
                "market_cap": row.get("market_cap_basic"),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "rsi": row.get("RSI"),
                "atr": row.get("ATR"),
            })
        return {
            "criteria": {
                "min_rvol": min_rvol,
                "min_market_cap": min_market_cap,
                "min_price": min_price,
                "direction": d,
            },
            "count": len(rows),
            "rows": rows,
        }


def _register_screener_columns(mcp: FastMCP, tv_proxy: TvProxyClient) -> None:

    @mcp.tool(
        annotations={
            "title": "Scanner Column Catalog",
            "readOnlyHint": True, "destructiveHint": False,
            "idempotentHint": True, "openWorldHint": True,
        }
    )
    async def screener_columns(
        query: str | None = None,
        *,
        market: str = "america",
        type_filter: str | None = None,
        timeframe: str | None = None,
        limit: int = 200,
    ) -> dict:
        """Search the TV scanner column catalog.

        3500+ columns per market — fundamentals, technicals, candle
        patterns, session prints, analyst data, forward fields. Use
        this before composing filters for `screener` or column lists
        for `perf_matrix` / `fundamentals_matrix` — guessing names
        breaks scans silently.

        Args:
            query: Case-insensitive substring match on column name
                (e.g. `"dividend"`, `"earnings"`, `"margin"`).
            market: Scanner market segment (default `"america"`).
                Crypto / forex / futures segments expose different
                column sets.
            type_filter: Restrict by column type — `"number"`,
                `"percent"`, `"time"`, `"bool"`, `"set"`, `"string"`,
                `"fundamental_price"`.
            timeframe: Many indicators appear at multiple bar
                resolutions. `None` or `"D"` = daily (no suffix).
                `"5"`, `"60"`, `"1W"`, `"1M"` etc. = that specific
                resolution. `"all"` = every variant.
            limit: Max rows (default 200, cap 1000). `truncated=True`
                when the match set was clipped.

        Returns:
            `{market, total_in_catalog, count, truncated,
            types_in_result: {type: count, ...},
            columns: [{name, type}, ...]}`, sorted alphabetically.
        """
        limit = max(1, min(limit, 1000))
        fields = await _load_metainfo(tv_proxy, market)
        total = len(fields)
        q = (query or "").lower().strip()
        if timeframe is None:
            tf_mode = "daily_only"
        elif timeframe.lower() == "all":
            tf_mode = "all"
        elif timeframe.upper() == "D":
            tf_mode = "daily_only"
        else:
            tf_mode = "specific"
            tf_value = timeframe.upper()

        matched: list[dict[str, Any]] = []
        type_counts: dict[str, int] = {}
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = f.get("n")
            if not name:
                continue
            if tf_mode == "daily_only":
                if "|" in name:
                    continue
            elif tf_mode == "specific":
                if "|" not in name or name.split("|", 1)[1].upper() != tf_value:
                    continue
            if q and q not in name.lower():
                continue
            t = f.get("t")
            if type_filter and t != type_filter:
                continue
            type_counts[t] = type_counts.get(t, 0) + 1
            matched.append({"name": name, "type": t})
        matched.sort(key=lambda x: x["name"])
        truncated = len(matched) > limit
        matched = matched[:limit]
        return {
            "market": market,
            "total_in_catalog": total,
            "count": len(matched),
            "truncated": truncated,
            "types_in_result": dict(sorted(type_counts.items())),
            "columns": matched,
        }
