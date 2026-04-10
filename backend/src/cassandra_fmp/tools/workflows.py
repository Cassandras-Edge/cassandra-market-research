"""Workflow tools that orchestrate multiple FMP endpoints for research questions."""

from __future__ import annotations

import asyncio
import statistics
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

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
from cassandra_fmp.tools.macro import _fetch_movers_with_mcap, _fetch_sectors

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient


# --- Helpers ---


def _pct_change(new: float | None, old: float | None) -> float | None:
    """Safe percentage change."""
    if new is None or old is None or old == 0:
        return None
    return round((new / old - 1) * 100, 2)


def _classify_signal(
    score: float, thresholds: tuple[float, float] = (-0.5, 0.5)
) -> str:
    """Convert a numeric score to a label."""
    if score <= thresholds[0]:
        return "bearish"
    if score >= thresholds[1]:
        return "bullish"
    return "neutral"


def _filter_recent(
    items: list[dict], days: int, date_field: str = "date"
) -> list[dict]:
    """Filter list by recency."""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    return [i for i in items if (_date_only(i.get(date_field)) or "") >= cutoff]


def _median(values: list[float]) -> float | None:
    clean = [v for v in values if v is not None and isinstance(v, (int, float))]
    if not clean:
        return None
    return round(statistics.median(clean), 4)


def _pre_revenue_flag(ratios: dict) -> dict:
    """Return pre-revenue/pre-profit flag when multiples are extreme."""
    pe = ratios.get("priceToEarningsRatioTTM")
    ps = ratios.get("priceToSalesRatioTTM")
    reasons = []
    if isinstance(pe, (int, float)) and (pe > 500 or pe < 0):
        reasons.append("pre-profit" if pe < 0 else "extreme P/E")
    if isinstance(ps, (int, float)) and ps > 500:
        reasons.append("pre-revenue")
    if reasons:
        return {"pre_revenue_flag": ", ".join(reasons) + " — valuation multiples not meaningful"}
    return {}


def _calc_performance(current: float, history: list[dict], days: int) -> float | None:
    if not current or len(history) < days:
        return None
    old_price = history[min(days - 1, len(history) - 1)].get("close")
    if not old_price or old_price == 0:
        return None
    return round((current / old_price - 1) * 100, 2)


def _build_extended_hours(
    premarket_data: list | None,
    afterhours_data: list | None,
    last_close: float | None,
) -> dict:
    """Build extended_hours dict from pre/post-market trade data.

    Returns a dict with an "extended_hours" key (suitable for **unpacking),
    or an empty dict if no data is available.
    """
    extended: dict = {}
    pre = _safe_first(premarket_data)
    if pre and pre.get("price"):
        ts = pre.get("timestamp")
        entry: dict = {
            "price": pre["price"],
            "size": pre.get("tradeSize"),
            "timestamp": _ms_to_str(ts),
        }
        if last_close and last_close > 0:
            entry["change_pct"] = round((pre["price"] / last_close - 1) * 100, 2)
        extended["premarket"] = entry
    post = _safe_first(afterhours_data)
    if post and post.get("price"):
        ts = post.get("timestamp")
        entry = {
            "price": post["price"],
            "size": post.get("tradeSize"),
            "timestamp": _ms_to_str(ts),
        }
        if last_close and last_close > 0:
            entry["change_pct"] = round((post["price"] / last_close - 1) * 100, 2)
        extended["afterhours"] = entry
    if extended:
        return {"extended_hours": extended}
    return {}


THESIS_MAP: dict[str, dict[str, Any]] = {
    "trained_on_it": {
        "tickers": ["DDOG", "NET", "ESTC", "MDB", "TWLO", "CRWD", "SNOW", "CFLT", "GTLB", "HCP"],
        "label": '"Trained On It" Moat (§3)',
        "key_question": "Does AI/agentic adoption show up in usage metrics?",
    },
    "bifurcation_infra": {
        "tickers": ["DDOG", "NET", "CRWD", "ESTC", "MDB", "CFLT", "SNOW", "ZS", "PANW", "CYBR"],
        "label": "Software Bifurcation — Infrastructure (§2)",
        "key_question": "Is usage-based revenue accelerating while seat-based peers decelerate?",
    },
    "bifurcation_prod": {
        "tickers": ["CRM", "WDAY", "ADBE", "TEAM", "PATH"],
        "label": "Software Bifurcation — Productivity [SHORT SIDE] (§2)",
        "key_question": "Is seat-based compression showing up in NRR or guidance?",
    },
    "ai_infra": {
        "tickers": [
            "NVDA", "AMD", "AVGO", "MRVL", "MU", "ALAB", "CRDO", "ASML", "TSM",
            "VRT", "ETN", "BE", "PWR", "GEV", "FCX", "SCCO", "DELL", "SMCI", "HPE",
        ],
        "label": "AI Infrastructure Bottleneck (§1)",
        "key_question": "Are supply constraints / pricing power holding?",
    },
    "spender": {
        "tickers": ["MSFT", "META", "GOOG", "GOOGL", "AMZN", "AAPL"],
        "label": "Spenders vs Suppliers — Spender Side (§5)",
        "key_question": "Does capex guidance increase again? Any AI ROI proof?",
    },
    "agentic": {
        "tickers": ["DDOG", "NET", "CRWD", "ESTC", "MDB", "TWLO", "SNOW"],
        "label": "Agentic AI Tailwind (§7)",
        "key_question": "Any explicit commentary on agent-driven usage growth?",
    },
}


def _score_beat_rate(rate: float | None, avg_surprise: float | None) -> float:
    """Score beat rate in -1..1 range."""
    if rate is None:
        return 0.0
    base = (rate - 50) / 50
    surprise_boost = min((avg_surprise or 0) / 30, 0.5)
    return min(max(base + surprise_boost, -1.0), 1.0)


def _score_price_setup(from_high_pct: float | None) -> float:
    """Far from highs can improve setup asymmetry."""
    if from_high_pct is None:
        return 0.0
    if from_high_pct < -40:
        return 1.0
    if from_high_pct < -25:
        return 0.7
    if from_high_pct < -15:
        return 0.3
    if from_high_pct < -5:
        return 0.0
    return -0.3


def _score_analyst(momentum: dict | None) -> float:
    """Net 90d upgrades/downgrades signal."""
    if not isinstance(momentum, dict):
        return 0.0
    net_90d = (momentum.get("upgrades_90d") or 0) - (momentum.get("downgrades_90d") or 0)
    if net_90d > 2:
        return 0.8
    if net_90d > 0:
        return 0.3
    if net_90d == 0:
        return 0.0
    if net_90d > -2:
        return -0.3
    return -0.8


def _score_insider(signal: dict | None) -> float:
    """Cluster buying and net buying/selling signal."""
    if not isinstance(signal, dict):
        return 0.0
    if signal.get("cluster_buying"):
        return 1.0
    s = signal.get("signal", "neutral")
    if s == "net_buying":
        return 0.5
    if s == "neutral":
        return 0.0
    return -0.4


def _match_theses(ticker: str) -> list[dict]:
    matches = []
    for thesis in THESIS_MAP.values():
        if ticker in thesis["tickers"]:
            matches.append({
                "thesis": thesis["label"],
                "key_question": thesis["key_question"],
            })
    return matches


def _default_key_questions(ticker: str, thesis_alignment: list[dict]) -> list[str]:
    questions: list[str] = []
    for thesis in thesis_alignment:
        key_question = thesis.get("key_question")
        if key_question and key_question not in questions:
            questions.append(key_question)
    questions.extend([
        f"What changed in the demand outlook for {ticker} since last quarter?",
        "Did management guide above or below current Street expectations?",
        "Are margin and cash-flow trends reinforcing or weakening the setup?",
    ])
    deduped = []
    for q in questions:
        if q not in deduped:
            deduped.append(q)
    return deduped[:5]


def _default_bull_triggers(ticker: str, thesis_alignment: list[dict], setup: dict) -> list[str]:
    triggers = []
    for thesis in thesis_alignment[:2]:
        key_question = thesis.get("key_question")
        if key_question:
            triggers.append(f"Positive evidence on: {key_question}")
    beat_rate = (setup.get("surprise_history") or {}).get("beat_rate")
    if isinstance(beat_rate, (int, float)) and beat_rate >= 75:
        triggers.append(f"Beat history remains strong (beat rate {beat_rate:.0f}%) with confident guidance")
    triggers.extend([
        "Forward guidance (quarter/FY) above consensus with stable-to-improving demand commentary",
        f"Management highlights durable growth drivers for {ticker} with no material execution flags",
    ])
    deduped = []
    for t in triggers:
        if t not in deduped:
            deduped.append(t)
    return deduped[:5]


def _default_bear_triggers(ticker: str, thesis_alignment: list[dict], setup: dict) -> list[str]:
    triggers = []
    for thesis in thesis_alignment[:2]:
        key_question = thesis.get("key_question")
        if key_question:
            triggers.append(f"Negative evidence on: {key_question}")
    beat_rate = (setup.get("surprise_history") or {}).get("beat_rate")
    if isinstance(beat_rate, (int, float)) and beat_rate < 50:
        triggers.append(f"Weak earnings track record persists (beat rate {beat_rate:.0f}%)")
    triggers.extend([
        "Guide-down or guidance below consensus with cautious demand commentary",
        f"Execution concerns, share-loss risk, or margin compression thesis strengthens for {ticker}",
    ])
    deduped = []
    for t in triggers:
        if t not in deduped:
            deduped.append(t)
    return deduped[:5]


# --- Registration ---


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:

    # ================================================================
    # 1. stock_brief — "Give me a quick read on this stock"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Stock Brief", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def stock_brief(symbol: str) -> dict:
        """Quick comprehensive stock snapshot: profile, price action, valuation, analyst consensus, insider signals, and top headlines.

        Replaces the 4-tool chain (overview + price + news + consensus) with a
        single call. Returns momentum across timeframes, SMA positioning,
        valuation multiples, analyst consensus + upside, insider signal, and
        a heuristic quick-take signal.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()
        sym = {"symbol": symbol}

        (
            profile_data, quote_data, ratios_data, history_data,
            grades_data, targets_data, insider_data, news_data,
            premarket_data, afterhours_data,
        ) = await asyncio.gather(
            _safe_call(client.company.get_profile, symbol=symbol, ttl=TTL_DAILY, default=None),
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            _safe_call(client.company.get_financial_ratios_ttm, symbol=symbol, ttl=TTL_HOURLY, default=None),
            _safe_call(
                client.company.get_historical_prices,
                symbol=symbol,
                from_date=date.today() - timedelta(days=365),
                to_date=date.today(),
                ttl=TTL_12H,
                default=None,
            ),
            _safe_call(client.intelligence.get_grades_consensus, symbol=symbol, ttl=TTL_6H, default=None),
            _safe_call(client.company.get_price_target_consensus, symbol=symbol, ttl=TTL_6H, default=None),
            _safe_call(client.institutional.search_insider_trading, symbol=symbol, limit=50, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.intelligence.get_stock_symbol_news, symbol=symbol, limit=15, ttl=TTL_REALTIME, default=[]),
            _safe_call(client.market.get_pre_post_market, ttl=TTL_REALTIME, default=[]),
            _safe_call(client.company.get_aftermarket_trade, symbol=symbol, ttl=TTL_REALTIME, default=None),
        )

        profile = _as_dict(profile_data)
        quote = _as_dict(quote_data)
        ratios = _as_dict(ratios_data)
        grades = _as_dict(grades_data)
        targets = _as_dict(targets_data)
        historical = _as_list(history_data, list_key="historical")
        insider_list = _as_list(insider_data)
        news_list_raw = _as_list(news_data)
        news_cross_symbol_count = 0
        news_list = []
        for item in news_list_raw:
            item_symbol = (item.get("symbol") or "").upper().strip()
            if item_symbol and item_symbol != symbol:
                news_cross_symbol_count += 1
                continue
            news_list.append(item)
        premarket_list = [
            item for item in _as_list(premarket_data)
            if (item.get("symbol") or "").upper() == symbol and (item.get("session") or "").lower() == "pre"
        ]

        if not profile and not quote:
            return {"error": f"No data found for symbol '{symbol}'"}

        current_price = quote.get("price")
        year_high = quote.get("yearHigh")
        year_low = quote.get("yearLow")
        sma_50 = quote.get("priceAvg50")
        sma_200 = quote.get("priceAvg200")

        # Performance across timeframes
        momentum = {}
        for label, days in [("1w", 5), ("1m", 21), ("3m", 63), ("ytd", None)]:
            if days is None:
                # YTD
                jan1 = date(date.today().year, 1, 1).isoformat()
                ytd_prices = [h for h in historical if (_date_only(h.get("date")) or "") >= jan1]
                if ytd_prices and current_price:
                    oldest = ytd_prices[-1].get("close") if ytd_prices else None
                    momentum["ytd"] = _pct_change(current_price, oldest)
            elif current_price:
                momentum[label] = _calc_performance(current_price, historical, days)

        momentum["sma_50"] = sma_50
        momentum["sma_200"] = sma_200
        momentum["above_50"] = current_price > sma_50 if current_price and sma_50 else None
        momentum["above_200"] = current_price > sma_200 if current_price and sma_200 else None

        from_high_pct = _pct_change(current_price, year_high) if current_price and year_high else None

        # Analyst consensus
        consensus_target = targets.get("targetConsensus")
        upside_pct = _pct_change(consensus_target, current_price) if consensus_target and current_price else None

        # Insider signal (30d)
        cutoff_30 = (date.today() - timedelta(days=30)).isoformat()
        buys_30, sells_30 = 0, 0
        cluster_buyers = set()
        for t in insider_list:
            trade_date = _date_only(t.get("filingDate") or t.get("transactionDate")) or ""
            if trade_date < cutoff_30:
                continue
            tx = (t.get("transactionType") or "").lower()
            shares = t.get("securitiesTransacted") or 0
            if "purchase" in tx or "p-purchase" in tx:
                buys_30 += shares
                cluster_buyers.add(t.get("reportingName", ""))
            elif "sale" in tx or "s-sale" in tx:
                sells_30 += shares

        net_30 = buys_30 - sells_30
        insider_signal = "net_buying" if net_30 > 0 else "net_selling" if net_30 < 0 else "neutral"
        cluster_buying = len(cluster_buyers) >= 3

        # News (top 5)
        news_items = []
        for n in news_list[:5]:
            title = n.get("title", "")
            news_items.append({
                "date": n.get("publishedDate"),
                "title": title,
                "source": n.get("site") or n.get("source"),
            })

        # Quick take heuristic
        score = 0
        factors = []
        if upside_pct is not None and upside_pct > 10:
            score += 1
            factors.append(f"analyst upside {upside_pct:.0f}%")
        elif upside_pct is not None and upside_pct < -10:
            score -= 1
            factors.append(f"analyst downside {upside_pct:.0f}%")
        if momentum.get("above_50") and momentum.get("above_200"):
            score += 0.5
            factors.append("above both SMAs")
        elif not momentum.get("above_50") and not momentum.get("above_200"):
            score -= 0.5
            factors.append("below both SMAs")
        if insider_signal == "net_buying":
            score += 0.5
            factors.append("insider buying")
        elif insider_signal == "net_selling":
            score -= 0.3
        consensus_label = grades.get("consensus", "")
        if consensus_label in ("Buy", "Strong Buy"):
            score += 0.5
            factors.append(f"consensus: {consensus_label}")
        elif consensus_label in ("Sell", "Strong Sell"):
            score -= 0.5
            factors.append(f"consensus: {consensus_label}")

        signal = _classify_signal(score)

        result = {
            "symbol": symbol,
            "company_name": profile.get("companyName"),
            "sector": profile.get("sector"),
            "industry": profile.get("industry"),
            "price": {
                "current": current_price,
                "market_cap": quote.get("marketCap"),
                "change_pct": quote.get("changePercentage"),
                "52w_high": year_high,
                "52w_low": year_low,
                "from_high_pct": from_high_pct,
                **_build_extended_hours(premarket_list, [_as_dict(afterhours_data)], current_price),
            },
            "momentum": momentum,
            "valuation": {
                "pe": ratios.get("priceToEarningsRatioTTM"),
                "ps": ratios.get("priceToSalesRatioTTM"),
                "ev_ebitda": ratios.get("enterpriseValueMultipleTTM"),
                "peg": ratios.get("priceToEarningsGrowthRatioTTM"),
                "dividend_yield": ratios.get("dividendYieldTTM"),
                **_pre_revenue_flag(ratios),
            },
            "analyst": {
                "consensus": grades.get("consensus"),
                "strong_buy": grades.get("strongBuy"),
                "buy": grades.get("buy"),
                "hold": grades.get("hold"),
                "sell": grades.get("sell"),
                "target": consensus_target,
                "upside_pct": upside_pct,
            },
            "insider": {
                "net_30d": net_30,
                "signal": insider_signal,
                "cluster_buying": cluster_buying,
            },
            "news": news_items,
            "quick_take": {
                "signal": signal,
                "key_factors": factors,
            },
        }

        _warnings = []
        if not profile:
            _warnings.append("profile unavailable")
        if not quote:
            _warnings.append("quote unavailable")
        if not ratios:
            _warnings.append("ratios unavailable")
        if not historical:
            _warnings.append("historical prices unavailable")
        if not grades:
            _warnings.append("analyst grades unavailable")
        if not targets:
            _warnings.append("price targets unavailable")
        if news_cross_symbol_count:
            _warnings.append(
                f"filtered out {news_cross_symbol_count} cross-symbol news item(s); news feed may be mixed/unfiltered"
            )
        if _warnings:
            result["_warnings"] = _warnings

        return result

    # ================================================================
    # 2. market_context — "What's the market doing?"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Market Context", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def market_context() -> dict:
        """Full market environment: rates, yield curve, sector rotation, breadth, movers, and economic calendar.

        Combines treasury rates, risk premium, sector performance, biggest
        movers, and upcoming macro events into a single environment snapshot.
        Calculates yield curve spread, rotation signal, breadth signal, and
        overall regime classification.
        """
        today_dt = date.today()
        end_dt = today_dt + timedelta(days=7)

        (
            rates_data, erp_data, calendar_data,
            sectors_list, (gainers_list, losers_list, actives_list),
        ) = await asyncio.gather(
            _safe_call(client.economics.get_treasury_rates, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.economics.get_market_risk_premium, ttl=TTL_DAILY, default=[]),
            _safe_call(client.economics.get_economic_calendar, start_date=today_dt, end_date=end_dt, ttl=TTL_HOURLY, default=[]),
            _fetch_sectors(client),
            _fetch_movers_with_mcap(client),
        )

        latest_rate = _as_dict(rates_data)
        erp_list = _as_list(erp_data)
        calendar_list = _as_list(calendar_data)

        if not latest_rate and not sectors_list:
            return {"error": "No market data available"}

        # Rates
        y10 = latest_rate.get("year10")
        y2 = latest_rate.get("year2")
        spread_bps = round((y10 - y2) * 100) if y10 is not None and y2 is not None else None
        inverted = (spread_bps or 0) < 0

        us_erp = None
        for entry in erp_list:
            if entry.get("country") == "United States":
                us_erp = entry.get("totalEquityRiskPremium")
                break
        cost_of_equity = round(y10 + us_erp, 2) if y10 is not None and us_erp is not None else None

        # Sector rotation (sectors_list already sorted desc by change_pct)
        leaders = [{"sector": s.get("sector"), "pct": s.get("change_pct")} for s in sectors_list[:3]]
        laggards = [{"sector": s.get("sector"), "pct": s.get("change_pct")} for s in sectors_list[-3:]]

        # Rotation signal: risk_on if growth/tech leading, risk_off if defensives leading
        growth_sectors = {"Technology", "Consumer Cyclical", "Communication Services"}
        defensive_sectors = {"Utilities", "Consumer Defensive", "Healthcare"}
        leader_names = {s["sector"] for s in leaders}
        laggard_names = {s["sector"] for s in laggards}

        growth_leading = len(leader_names & growth_sectors)
        defensive_leading = len(leader_names & defensive_sectors)
        growth_lagging = len(laggard_names & growth_sectors)
        defensive_lagging = len(laggard_names & defensive_sectors)

        if growth_leading > defensive_leading and defensive_lagging >= 1:
            rotation_signal = "risk_on"
        elif defensive_leading > growth_leading and growth_lagging >= 1:
            rotation_signal = "risk_off"
        elif defensive_leading >= 2:
            rotation_signal = "risk_off"
        elif growth_leading >= 2:
            rotation_signal = "risk_on"
        else:
            rotation_signal = "mixed"

        # Breadth (from filtered movers — more meaningful with mcap floor)
        gainer_pcts = [abs(g.get("change_pct") or 0) for g in gainers_list[:10]]
        loser_pcts = [abs(l.get("change_pct") or 0) for l in losers_list[:10]]
        avg_gainer = round(statistics.mean(gainer_pcts), 2) if gainer_pcts else 0
        avg_loser = round(statistics.mean(loser_pcts), 2) if loser_pcts else 0
        if avg_gainer > avg_loser * 1.2:
            breadth_signal = "bullish"
        elif avg_loser > avg_gainer * 1.2:
            breadth_signal = "bearish"
        else:
            breadth_signal = "neutral"

        # Calendar: US high-impact, split today vs this_week
        high_impact_kw = [
            "fed", "fomc", "interest rate", "cpi", "consumer price",
            "nonfarm", "non-farm", "unemployment", "gdp", "pce",
            "retail sales", "ism", "pmi",
        ]
        today_str = today_dt.isoformat()
        today_events, week_events = [], []
        for evt in calendar_list:
            country = (evt.get("country") or "").upper()
            if country != "US":
                continue
            name = (evt.get("event") or "").lower()
            if not any(kw in name for kw in high_impact_kw):
                continue
            event_date = _date_only(evt.get("date"))
            formatted = {
                "date": event_date,
                "event": evt.get("event"),
                "estimate": evt.get("estimate"),
                "previous": evt.get("previous"),
                "impact": evt.get("impact"),
            }
            if event_date == today_str:
                today_events.append(formatted)
            else:
                week_events.append(formatted)

        # Environment classification
        themes = []
        if inverted:
            themes.append("yield curve inverted")
        if rotation_signal == "risk_on":
            themes.append("risk-on rotation")
        elif rotation_signal == "risk_off":
            themes.append("risk-off rotation")
        if breadth_signal == "bullish":
            themes.append("strong breadth")
        elif breadth_signal == "bearish":
            themes.append("weak breadth")

        bullish_count = sum(1 for t in [rotation_signal, breadth_signal] if t in ("risk_on", "bullish"))
        bearish_count = sum(1 for t in [rotation_signal, breadth_signal] if t in ("risk_off", "bearish"))
        if inverted:
            bearish_count += 1
        if bullish_count > bearish_count:
            regime = "risk_on"
        elif bearish_count > bullish_count:
            regime = "risk_off"
        else:
            regime = "neutral"

        result = {
            "date": today_str,
            "rates": {
                "10y": y10,
                "2y": y2,
                "spread_bps": spread_bps,
                "inverted": inverted,
                "erp": us_erp,
                "cost_of_equity": cost_of_equity,
            },
            "rotation": {
                "leaders": leaders,
                "laggards": laggards,
                "signal": rotation_signal,
            },
            "breadth": {
                "avg_gainer_pct": avg_gainer,
                "avg_loser_pct": avg_loser,
                "signal": breadth_signal,
            },
            "movers": {
                "gainers": gainers_list[:5],
                "losers": losers_list[:5],
                "most_active": actives_list[:5],
            },
            "calendar": {
                "today": today_events,
                "this_week": week_events,
            },
            "environment": {
                "regime": regime,
                "themes": themes,
            },
        }

        _warnings = []
        if not latest_rate:
            _warnings.append("treasury rates unavailable")
        if not erp_list:
            _warnings.append("risk premium unavailable")
        if not sectors_list:
            _warnings.append("sector data unavailable")
        if not gainers_list and not losers_list:
            _warnings.append("movers data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    # ================================================================
    # 3. earnings_setup — "Should I play this earnings?"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Earnings Setup", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def earnings_setup(symbol: str) -> dict:
        """Pre-earnings positioning analysis: consensus estimates, historical beat/miss rate, analyst momentum, price drift, and insider signals.

        Orchestrates 8 endpoints to answer "should I play this earnings?"
        Returns days until earnings, consensus EPS/revenue, beat rate from
        last 4-8 quarters, analyst upgrade/downgrade momentum, pre-earnings
        price drift, insider net activity, and a heuristic setup signal.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL", "DDOG")
        """
        symbol = symbol.upper().strip()
        sym = {"symbol": symbol}

        (
            profile_data, quote_data, earnings_data, grades_data,
            history_data, insider_data, insider_stats_data, float_data,
        ) = await asyncio.gather(
            _safe_call(client.company.get_profile, symbol=symbol, ttl=TTL_DAILY, default=None),
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            _safe_call(client.company.get_earnings, symbol=symbol, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.intelligence.get_grades, symbol=symbol, ttl=TTL_6H, default=[]),
            _safe_call(
                client.company.get_historical_prices,
                symbol=symbol,
                from_date=date.today() - timedelta(days=90),
                to_date=date.today(),
                ttl=TTL_12H,
                default=None,
            ),
            _safe_call(client.institutional.search_insider_trading, symbol=symbol, limit=50, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.institutional.get_insider_statistics, symbol=symbol, ttl=TTL_HOURLY, default=None),
            _safe_call(client.company.get_share_float, symbol=symbol, ttl=TTL_DAILY, default=None),
        )

        profile = _as_dict(profile_data)
        quote = _as_dict(quote_data)
        earnings_list = _as_list(earnings_data)
        grades_list = _as_list(grades_data)
        historical = _as_list(history_data, list_key="historical")
        insider_list = _as_list(insider_data)
        float_info = _as_dict(float_data)

        if not quote and not earnings_list:
            return {"error": f"No data found for symbol '{symbol}'"}

        current_price = quote.get("price")
        today_dt = date.today()
        today_str = today_dt.isoformat()

        # --- Parse earnings: split future vs historical ---
        future_earnings = []
        historical_earnings = []
        for e in earnings_list:
            edate = _date_only(e.get("date")) or ""
            if edate > today_str and e.get("epsActual") is None:
                future_earnings.append(e)
            elif e.get("epsActual") is not None and e.get("epsEstimated") is not None:
                historical_earnings.append(e)

        # Sort: future ascending, historical descending
        future_earnings.sort(key=lambda e: _date_only(e.get("date")) or "")
        historical_earnings.sort(key=lambda e: _date_only(e.get("date")) or "", reverse=True)

        # Next earnings
        next_earnings = future_earnings[0] if future_earnings else None
        earnings_date = _date_only(next_earnings.get("date")) if next_earnings else None
        days_until = None
        if earnings_date:
            earnings_dt = _to_date(earnings_date)
            if earnings_dt is not None:
                days_until = (earnings_dt - today_dt).days

        # Consensus
        consensus = {}
        if next_earnings:
            consensus = {
                "eps": next_earnings.get("epsEstimated"),
                "revenue": next_earnings.get("revenueEstimated"),
                "analyst_count": next_earnings.get("numberOfAnalysts"),
            }

        # Surprise history (last 8 actuals)
        last_8 = historical_earnings[:8]
        surprise_list = []
        for e in last_8:
            actual = e.get("epsActual")
            estimated = e.get("epsEstimated")
            if actual is not None and estimated is not None and estimated != 0:
                surprise_pct = round((actual - estimated) / abs(estimated) * 100, 2)
                beat = actual > estimated
            else:
                surprise_pct = None
                beat = None
            surprise_list.append({
                "date": e.get("date"),
                "actual": actual,
                "estimate": estimated,
                "surprise_pct": surprise_pct,
                "beat": beat,
            })

        beats = [s for s in surprise_list if s.get("beat") is True]
        beat_rate = round(len(beats) / len(surprise_list) * 100) if surprise_list else None
        avg_surprise = round(statistics.mean([s["surprise_pct"] for s in surprise_list if s.get("surprise_pct") is not None]), 2) if any(s.get("surprise_pct") is not None for s in surprise_list) else None

        # Analyst momentum from /stable/grades
        recent_30d = _filter_recent(grades_list, 30)
        recent_90d = _filter_recent(grades_list, 90)

        upgrades_30 = len([g for g in recent_30d if (g.get("action") or "").lower() in ("upgrade", "initiate")])
        downgrades_30 = len([g for g in recent_30d if (g.get("action") or "").lower() == "downgrade"])
        upgrades_90 = len([g for g in recent_90d if (g.get("action") or "").lower() in ("upgrade", "initiate")])
        downgrades_90 = len([g for g in recent_90d if (g.get("action") or "").lower() == "downgrade"])

        recent_actions = []
        for g in grades_list[:5]:
            recent_actions.append({
                "firm": g.get("gradingCompany"),
                "action": g.get("action"),
                "new_grade": g.get("newGrade"),
                "date": g.get("date"),
            })

        analyst_net_30 = upgrades_30 - downgrades_30
        if analyst_net_30 > 0:
            analyst_signal = "positive"
        elif analyst_net_30 < 0:
            analyst_signal = "negative"
        else:
            analyst_signal = "neutral"

        # Price drift (5d, 20d)
        drift_5d = _calc_performance(current_price, historical, 5)
        drift_20d = _calc_performance(current_price, historical, 20)

        # Distance from 52w high
        year_high = quote.get("yearHigh")
        from_52w_high = _pct_change(current_price, year_high) if current_price and year_high else None

        # Insider signal (30d)
        cutoff_30 = (today_dt - timedelta(days=30)).isoformat()
        buys_30, sells_30 = 0, 0
        cluster_buyers = set()
        notable_insiders = []
        for t in insider_list:
            trade_date = _date_only(t.get("filingDate") or t.get("transactionDate")) or ""
            if trade_date < cutoff_30:
                continue
            tx = (t.get("transactionType") or "").lower()
            shares = t.get("securitiesTransacted") or 0
            name = t.get("reportingName", "")
            if "purchase" in tx or "p-purchase" in tx:
                buys_30 += shares
                cluster_buyers.add(name)
                notable_insiders.append({"name": name, "type": "buy", "shares": shares, "date": trade_date})
            elif "sale" in tx or "s-sale" in tx:
                sells_30 += shares
                notable_insiders.append({"name": name, "type": "sell", "shares": shares, "date": trade_date})

        net_30 = buys_30 - sells_30
        cluster = len(cluster_buyers) >= 3

        # --- Setup signal heuristic ---
        setup_score = 0
        key_factors = []

        # Beat history weight
        if beat_rate is not None:
            if beat_rate >= 75:
                setup_score += 1
                key_factors.append(f"beat rate {beat_rate}%")
            elif beat_rate <= 25:
                setup_score -= 1
                key_factors.append(f"miss rate {100 - beat_rate}%")

        # Analyst momentum
        if analyst_signal == "positive":
            setup_score += 0.5
            key_factors.append(f"{upgrades_30} upgrades in 30d")
        elif analyst_signal == "negative":
            setup_score -= 0.5
            key_factors.append(f"{downgrades_30} downgrades in 30d")

        # Insider signal
        if net_30 > 0 or cluster:
            setup_score += 0.5
            key_factors.append("insider buying" + (" (cluster)" if cluster else ""))
        elif net_30 < 0 and abs(net_30) > buys_30 * 2:
            setup_score -= 0.3
            key_factors.append("heavy insider selling")

        # Price drift
        if drift_20d is not None:
            if drift_20d > 5:
                setup_score += 0.3
                key_factors.append(f"positive drift +{drift_20d}%")
            elif drift_20d < -5:
                setup_score -= 0.3
                key_factors.append(f"negative drift {drift_20d}%")

        signal = _classify_signal(setup_score)

        result = {
            "symbol": symbol,
            "company_name": profile.get("companyName"),
            "current_price": current_price,
            "earnings_date": earnings_date,
            "days_until_earnings": days_until,
            "consensus": consensus,
            "surprise_history": {
                "last_quarters": surprise_list[:4],
                "beat_rate": beat_rate,
                "avg_surprise": avg_surprise,
            },
            "analyst_momentum": {
                "upgrades_30d": upgrades_30,
                "downgrades_30d": downgrades_30,
                "upgrades_90d": upgrades_90,
                "downgrades_90d": downgrades_90,
                "recent_actions": recent_actions,
                "signal": analyst_signal,
            },
            "price_action": {
                "drift_5d_pct": drift_5d,
                "drift_20d_pct": drift_20d,
                "from_52w_high_pct": from_52w_high,
            },
            "insider_signal": {
                "net_shares_30d": net_30,
                "cluster_buying": cluster,
                "notable": notable_insiders[:5],
            },
            "setup_summary": {
                "signal": signal,
                "key_factors": key_factors,
            },
        }

        _warnings = []
        if not earnings_list:
            _warnings.append("earnings data unavailable")
        if not grades_list:
            _warnings.append("analyst grades unavailable")
        if not historical:
            _warnings.append("historical prices unavailable")
        if not insider_list:
            _warnings.append("insider data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    # ================================================================
    # 4. earnings_preview — "How is this setup into earnings?"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Earnings Preview", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def earnings_preview(
        symbol: str,
        days_ahead: int = 30,
    ) -> dict:
        """Pre-earnings setup report with scoring and thesis alignment.

        Combines earnings_setup + stock_brief data, scores beat rate / price setup /
        analyst momentum / insider signals, and maps to thesis triggers.

        Args:
            symbol: Stock ticker symbol (e.g. "ESTC", "NVDA")
            days_ahead: Horizon for in-window earnings checks
        """
        symbol = symbol.upper().strip()

        if days_ahead < 1:
            return {"error": f"Invalid days_ahead '{days_ahead}'. Must be >= 1."}

        setup, brief = await asyncio.gather(
            earnings_setup(symbol=symbol),
            stock_brief(symbol=symbol),
        )

        setup_error = setup.get("error") if isinstance(setup, dict) else None
        brief_error = brief.get("error") if isinstance(brief, dict) else None

        if setup_error and brief_error:
            return {
                "error": f"No usable data for '{symbol}'",
                "details": {"earnings_setup": setup_error, "stock_brief": brief_error},
            }

        warnings: list[str] = []
        for msg in (setup.get("_warnings") or []) if isinstance(setup, dict) else []:
            warnings.append(f"earnings_setup: {msg}")
        for msg in (brief.get("_warnings") or []) if isinstance(brief, dict) else []:
            warnings.append(f"stock_brief: {msg}")
        if setup_error:
            warnings.append("earnings_setup unavailable; using neutral defaults for earnings components")
        if brief_error:
            warnings.append("stock_brief unavailable; using neutral defaults for price components")

        setup_data = setup if isinstance(setup, dict) else {}
        brief_data = brief if isinstance(brief, dict) else {}

        earnings_date = setup_data.get("earnings_date")
        days_until = setup_data.get("days_until_earnings")
        in_window = isinstance(days_until, int) and 0 <= days_until <= days_ahead
        if days_until is None:
            warnings.append("next earnings date unavailable; in_window set to false")
        elif not in_window:
            warnings.append(f"earnings date is outside requested horizon ({days_ahead} days)")

        surprise_history = setup_data.get("surprise_history") if isinstance(setup_data.get("surprise_history"), dict) else {}
        analyst_momentum = setup_data.get("analyst_momentum") if isinstance(setup_data.get("analyst_momentum"), dict) else {}
        insider_signal = setup_data.get("insider_signal") if isinstance(setup_data.get("insider_signal"), dict) else {}
        price = brief_data.get("price") if isinstance(brief_data.get("price"), dict) else {}
        momentum = brief_data.get("momentum") if isinstance(brief_data.get("momentum"), dict) else {}
        valuation = brief_data.get("valuation") if isinstance(brief_data.get("valuation"), dict) else {}
        analyst = brief_data.get("analyst") if isinstance(brief_data.get("analyst"), dict) else {}

        beat_rate = surprise_history.get("beat_rate")
        avg_surprise = surprise_history.get("avg_surprise")
        from_high_pct = price.get("from_high_pct")

        if beat_rate is None:
            warnings.append("beat history unavailable; beat_history score set to 0.0")
        if from_high_pct is None:
            warnings.append("price setup unavailable; price_setup score set to 0.0")
        if analyst_momentum.get("upgrades_90d") is None and analyst_momentum.get("downgrades_90d") is None:
            warnings.append("analyst momentum unavailable; analyst score set to 0.0")
        if insider_signal.get("signal") is None and insider_signal.get("cluster_buying") is None:
            warnings.append("insider signal unavailable; insider score set to 0.0")

        signals = {
            "beat_history": round(_score_beat_rate(beat_rate, avg_surprise), 4),
            "price_setup": round(_score_price_setup(from_high_pct), 4),
            "analyst": round(_score_analyst(analyst_momentum), 4),
            "insider": round(_score_insider(insider_signal), 4),
        }

        composite = round(
            signals["beat_history"] * 0.35
            + signals["price_setup"] * 0.25
            + signals["analyst"] * 0.20
            + signals["insider"] * 0.20,
            4,
        )
        if composite > 0.3:
            setup_signal = "BULLISH"
        elif composite < -0.3:
            setup_signal = "BEARISH"
        else:
            setup_signal = "NEUTRAL"

        thesis_alignment = _match_theses(symbol)
        key_questions = _default_key_questions(symbol, thesis_alignment)
        bull_triggers = _default_bull_triggers(symbol, thesis_alignment, setup_data)
        bear_triggers = _default_bear_triggers(symbol, thesis_alignment, setup_data)

        result = {
            "symbol": symbol,
            "ticker": symbol,  # backward-compatible key
            "company_name": brief_data.get("company_name") or setup_data.get("company_name"),
            "earnings_date": earnings_date,
            "days_until": days_until,
            "days_ahead": days_ahead,
            "in_window": in_window,
            "setup_signal": setup_signal,
            "composite_score": composite,
            "price_context": {
                "current": price.get("current"),
                "from_52w_high": from_high_pct,
                "sma_50": momentum.get("sma_50"),
                "sma_200": momentum.get("sma_200"),
                "above_50": momentum.get("above_50"),
                "above_200": momentum.get("above_200"),
                "pe": valuation.get("pe"),
                "ps": valuation.get("ps"),
                "ev_ebitda": valuation.get("ev_ebitda"),
                "analyst_consensus": analyst.get("consensus"),
                "analyst_target": analyst.get("target"),
                "analyst_upside_pct": analyst.get("upside_pct"),
            },
            "consensus": setup_data.get("consensus") if isinstance(setup_data.get("consensus"), dict) else {},
            "beat_history": {
                "rate": beat_rate,
                "avg_surprise": avg_surprise,
                "last_4q": surprise_history.get("last_quarters") or [],
            },
            "signals": signals,
            "thesis_alignment": thesis_alignment,
            "position": None,
            "key_questions": key_questions,
            "bull_triggers": bull_triggers,
            "bear_triggers": bear_triggers,
        }

        deduped_warnings = []
        for msg in warnings:
            if msg not in deduped_warnings:
                deduped_warnings.append(msg)
        if deduped_warnings:
            result["_warnings"] = deduped_warnings

        return result

    # ================================================================
    # 5. fair_value_estimate — "What's this stock worth?"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Fair Value Estimate", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def fair_value_estimate(symbol: str) -> dict:
        """Multi-method fair value estimate with peer context.

        Calculates fair value using PE-based, PS-based, simplified DCF, and
        analyst target methods. Compares current multiples to peer medians.
        Returns a blended fair value, upside/downside, and valuation rating.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()
        sym = {"symbol": symbol}

        # Step 1: Fetch base data for target
        (
            quote_data, income_data, balance_data, cashflow_data,
            metrics_data, ratios_data, estimates_data, targets_data,
            peers_data,
        ) = await asyncio.gather(
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            _safe_call(client.fundamental.get_income_statement, symbol=symbol, limit=4, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.fundamental.get_balance_sheet, symbol=symbol, limit=1, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.fundamental.get_cash_flow, symbol=symbol, limit=1, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.company.get_key_metrics_ttm, symbol=symbol, ttl=TTL_HOURLY, default=None),
            _safe_call(client.company.get_financial_ratios_ttm, symbol=symbol, ttl=TTL_HOURLY, default=None),
            _safe_call(client.company.get_analyst_estimates, symbol=symbol, period="quarter", limit=4, ttl=TTL_6H, default=[]),
            _safe_call(client.company.get_price_target_consensus, symbol=symbol, ttl=TTL_6H, default=None),
            _safe_call(client.company.get_company_peers, symbol=symbol, ttl=TTL_DAILY, default=[]),
        )

        quote = _as_dict(quote_data)
        income_list = _as_list(income_data)
        balance = _as_dict(balance_data)
        cashflow = _as_dict(cashflow_data)
        metrics = _as_dict(metrics_data)
        ratios = _as_dict(ratios_data)
        estimates_list = _as_list(estimates_data)
        targets = _as_dict(targets_data)
        peers_list = _as_list(peers_data)

        if not quote:
            return {"error": f"No data found for symbol '{symbol}'"}

        current_price = quote.get("price")
        market_cap = quote.get("marketCap")

        # Fundamentals
        latest_income = income_list[0] if income_list else {}
        ttm_revenue = latest_income.get("revenue")
        ttm_net_income = latest_income.get("netIncome")
        ttm_fcf = cashflow.get("freeCashFlow")
        total_debt = balance.get("totalDebt") or 0
        cash = balance.get("cashAndCashEquivalents") or 0
        net_debt = total_debt - cash

        # EV
        ev = (market_cap or 0) + total_debt - cash if market_cap else None

        # Shares outstanding (derive from market cap / price)
        shares = round(market_cap / current_price) if market_cap and current_price and current_price > 0 else None

        # Forward estimates: sum next 4 quarters for forward EPS/revenue
        estimates_list.sort(key=lambda e: e.get("date", ""))
        fwd_eps = sum((e.get("epsAvg") or e.get("estimatedEpsAvg") or 0) for e in estimates_list[:4]) if estimates_list else None
        fwd_revenue = sum((e.get("revenueAvg") or e.get("estimatedRevenueAvg") or 0) for e in estimates_list[:4]) if estimates_list else None

        # Growth rates
        rev_growth_fwd = _pct_change(fwd_revenue, ttm_revenue) if fwd_revenue and ttm_revenue else None
        eps_growth_fwd = None
        if fwd_eps and latest_income.get("epsDiluted") and latest_income["epsDiluted"] != 0:
            eps_growth_fwd = _pct_change(fwd_eps, latest_income["epsDiluted"])

        # 3-year revenue CAGR
        rev_cagr_3y = None
        if len(income_list) >= 4:
            start_rev = income_list[3].get("revenue")
            end_rev = income_list[0].get("revenue")
            if start_rev and end_rev and start_rev > 0:
                try:
                    rev_cagr_3y = round(((end_rev / start_rev) ** (1 / 3) - 1) * 100, 2)
                except (ZeroDivisionError, ValueError, OverflowError):
                    pass

        # Step 2: Fetch peer data
        peer_symbols = [p.get("symbol") for p in peers_list if p.get("symbol")][:5]

        async def _fetch_peer(s: str) -> dict:
            r, m = await asyncio.gather(
                _safe_call(client.company.get_financial_ratios_ttm, symbol=s, ttl=TTL_HOURLY, default=None),
                _safe_call(client.company.get_key_metrics_ttm, symbol=s, ttl=TTL_HOURLY, default=None),
            )
            rd = _as_dict(r)
            md = _as_dict(m)
            return {
                "symbol": s,
                "pe": rd.get("priceToEarningsRatioTTM"),
                "ps": rd.get("priceToSalesRatioTTM"),
                "ev_ebitda": rd.get("enterpriseValueMultipleTTM"),
                "p_fcf": rd.get("priceToFreeCashFlowRatioTTM"),
            }

        peer_metrics = await asyncio.gather(*[_fetch_peer(s) for s in peer_symbols]) if peer_symbols else []

        # Peer medians
        peer_pe_med = _median([p["pe"] for p in peer_metrics])
        peer_ps_med = _median([p["ps"] for p in peer_metrics])
        peer_ev_ebitda_med = _median([p["ev_ebitda"] for p in peer_metrics])
        peer_pfcf_med = _median([p["p_fcf"] for p in peer_metrics])

        # Current multiples
        cur_pe = ratios.get("priceToEarningsRatioTTM")
        cur_ps = ratios.get("priceToSalesRatioTTM")
        cur_ev_ebitda = ratios.get("enterpriseValueMultipleTTM")
        cur_pfcf = ratios.get("priceToFreeCashFlowRatioTTM")

        # Premium/discount
        def _prem(cur, med):
            if cur is None or med is None or med == 0:
                return None
            return round((cur / med - 1) * 100, 2)

        # Step 3: Fair value calculations
        # PE-based FV
        pe_fv = round(fwd_eps * peer_pe_med, 2) if fwd_eps and peer_pe_med else None

        # PS-based FV
        ps_fv = None
        if fwd_revenue and peer_ps_med and shares and shares > 0:
            ps_fv = round(fwd_revenue * peer_ps_med / shares, 2)

        # DCF-simplified: (ttm_fcf × (1 + growth) × terminal_multiple) / shares
        dcf_fv = None
        growth_rate = (rev_growth_fwd or 0) / 100
        terminal_multiple = 15  # conservative
        if ttm_fcf and ttm_fcf > 0 and shares and shares > 0:
            dcf_fv = round(ttm_fcf * (1 + growth_rate) * terminal_multiple / shares, 2)

        # Analyst target
        analyst_fv = targets.get("targetConsensus")

        # Blended (equal weight of available methods)
        methods = [v for v in [pe_fv, ps_fv, dcf_fv, analyst_fv] if v is not None]
        blended_fv = round(statistics.mean(methods), 2) if methods else None

        upside_pct = _pct_change(blended_fv, current_price) if blended_fv and current_price else None

        # Valuation rating
        if upside_pct is not None:
            if upside_pct > 15:
                rating = "undervalued"
            elif upside_pct < -15:
                rating = "overvalued"
            else:
                rating = "fairly_valued"
        else:
            rating = "insufficient_data"

        # Quality metrics
        roe = ratios.get("returnOnEquityTTM")
        net_margin = ratios.get("netProfitMarginTTM")
        de_ratio = ratios.get("debtToEquityRatioTTM")

        quality_score = 0
        if roe is not None and roe > 0.15:
            quality_score += 1
        if net_margin is not None and net_margin > 0.10:
            quality_score += 1
        if de_ratio is not None and de_ratio < 1.5:
            quality_score += 1

        # Key drivers
        key_drivers = []
        if upside_pct is not None:
            key_drivers.append(f"blended FV implies {upside_pct:+.1f}% {'upside' if upside_pct > 0 else 'downside'}")
        if pe_fv and current_price:
            key_drivers.append(f"PE-based: ${pe_fv:.0f} vs ${current_price:.0f}")
        if rev_growth_fwd:
            key_drivers.append(f"fwd revenue growth: {rev_growth_fwd:.1f}%")
        confidence = "high" if len(methods) >= 3 else "medium" if len(methods) >= 2 else "low"

        result = {
            "symbol": symbol,
            "current_price": current_price,
            "fundamentals": {
                "market_cap": market_cap,
                "ev": ev,
                "shares": shares,
                "ttm_revenue": ttm_revenue,
                "ttm_net_income": ttm_net_income,
                "ttm_fcf": ttm_fcf,
                "debt": total_debt,
                "cash": cash,
                "net_debt": net_debt,
            },
            "growth": {
                "rev_growth_fwd_pct": rev_growth_fwd,
                "eps_growth_fwd_pct": eps_growth_fwd,
                "rev_cagr_3y_pct": rev_cagr_3y,
            },
            "multiples": {
                "current": {"pe": cur_pe, "ps": cur_ps, "ev_ebitda": cur_ev_ebitda, "p_fcf": cur_pfcf},
                "peer_median": {"pe": peer_pe_med, "ps": peer_ps_med, "ev_ebitda": peer_ev_ebitda_med, "p_fcf": peer_pfcf_med},
                "premium_pct": {
                    "pe": _prem(cur_pe, peer_pe_med),
                    "ps": _prem(cur_ps, peer_ps_med),
                    "ev_ebitda": _prem(cur_ev_ebitda, peer_ev_ebitda_med),
                    "p_fcf": _prem(cur_pfcf, peer_pfcf_med),
                },
            },
            "fair_value": {
                "pe_based": pe_fv,
                "ps_based": ps_fv,
                "dcf_simplified": dcf_fv,
                "analyst_target": analyst_fv,
                "blended": blended_fv,
                "upside_pct": upside_pct,
            },
            "quality": {
                "roe": roe,
                "net_margin": net_margin,
                "debt_equity": de_ratio,
                "score": quality_score,
            },
            "summary": {
                "rating": rating,
                "confidence": confidence,
                "key_drivers": key_drivers,
            },
        }

        _warnings = []
        if not income_list:
            _warnings.append("income statement unavailable")
        if not balance:
            _warnings.append("balance sheet unavailable")
        if not cashflow:
            _warnings.append("cash flow unavailable")
        if not estimates_list:
            _warnings.append("analyst estimates unavailable")
        if not peer_symbols:
            _warnings.append("peer data unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    # ================================================================
    # 6. earnings_postmortem — "What just happened in earnings?"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Earnings Postmortem", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def earnings_postmortem(symbol: str, quarter: int | None = None, year: int | None = None) -> dict:
        """Post-earnings synthesis: beat/miss, trend comparison, analyst reaction, market response, and guidance tone.

        Analyzes the most recent (or specified) earnings report. Returns EPS
        and revenue surprise, YoY/QoQ comparisons, post-earnings price reaction,
        analyst rating changes since the print, and guidance tone from transcript.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            quarter: Specific quarter (1-4) to analyze. Omit for most recent.
            year: Specific year to analyze. Omit for most recent.
        """
        symbol = symbol.upper().strip()
        sym = {"symbol": symbol}
        if quarter is not None and quarter not in (1, 2, 3, 4):
            return {"error": f"Invalid quarter '{quarter}'. Must be 1, 2, 3, or 4."}

        def _to_int(value: Any) -> int | None:
            try:
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        def _period_from_report(report: dict) -> tuple[int, int] | None:
            fiscal_y = _to_int(report.get("fiscalYear") or report.get("year"))
            fiscal_q = _to_int(report.get("quarter"))
            if fiscal_y is not None and fiscal_q in (1, 2, 3, 4):
                return fiscal_y, fiscal_q
            fiscal_end = report.get("fiscalDateEnding", "")
            try:
                fd = datetime.strptime(fiscal_end, "%Y-%m-%d").date()
                return fd.year, (fd.month - 1) // 3 + 1
            except (ValueError, TypeError):
                return None

        (
            earnings_data, income_data, grades_data,
            targets_data, history_data, quote_data,
            transcript_dates_data,
        ) = await asyncio.gather(
            _safe_call(client.company.get_earnings, symbol=symbol, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.fundamental.get_income_statement, symbol=symbol, period="quarter", limit=8, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.intelligence.get_grades, symbol=symbol, ttl=TTL_6H, default=[]),
            _safe_call(client.company.get_price_target_consensus, symbol=symbol, ttl=TTL_6H, default=None),
            _safe_call(
                client.company.get_historical_prices,
                symbol=symbol,
                from_date=date.today() - timedelta(days=90),
                to_date=date.today(),
                ttl=TTL_12H,
                default=None,
            ),
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            _safe_call(client.transcripts.get_available_dates, symbol=symbol, ttl=TTL_REALTIME, default=[]),
        )

        earnings_list = _as_list(earnings_data)
        income_list = _as_list(income_data)
        grades_list = _as_list(grades_data)
        targets = _as_dict(targets_data)
        historical = _as_list(history_data, list_key="historical")
        quote = _as_dict(quote_data)
        transcript_dates = _as_list(transcript_dates_data)

        if not earnings_list:
            return {"error": f"No earnings data found for '{symbol}'"}

        # Find the target earnings report
        today_date = date.today()
        actual_earnings = [
            e
            for e in earnings_list
            if e.get("epsActual") is not None and (report_date := _to_date(e.get("date"))) and report_date <= today_date
        ]
        actual_earnings.sort(key=lambda e: _date_only(e.get("date")) or "", reverse=True)

        transcript_period_by_date: dict[str, tuple[int, int]] = {}
        for t in transcript_dates:
            t_date = _date_only(t.get("date"))
            t_year = _to_int(t.get("fiscalYear") or t.get("year"))
            t_quarter = _to_int(t.get("quarter") or t.get("period"))
            if t_date and t_year is not None and t_quarter in (1, 2, 3, 4):
                transcript_period_by_date[t_date] = (t_year, t_quarter)

        target_report = None
        if quarter is not None and year is not None:
            # Prefer transcript date mapping to avoid fiscal calendar mismatches.
            for e in actual_earnings:
                report_date = _date_only(e.get("date")) or ""
                report_period = transcript_period_by_date.get(report_date) or _period_from_report(e)
                if report_period == (year, quarter):
                    target_report = e
                    break
        if target_report is None:
            target_report = actual_earnings[0] if actual_earnings else None

        if not target_report:
            return {"error": f"No completed earnings found for '{symbol}'"}

        earnings_date = _date_only(target_report.get("date")) or ""
        earnings_dt = _to_date(earnings_date)
        if earnings_dt is not None:
            days_since_report = (today_date - earnings_dt).days
            if days_since_report < 3:
                return {
                    "error": (
                        f"Earnings for '{symbol}' were reported {days_since_report} day(s) ago; "
                        "FMP postmortem fields can lag for very recent prints."
                    ),
                    "symbol": symbol,
                    "earnings_date": earnings_date,
                    "redirect_to": "Use real-time/news-first checks (e.g., Perplexity or market_news(symbol=...)) and retry after 48-72 hours.",
                }

        actual_eps = target_report.get("epsActual")
        est_eps = target_report.get("epsEstimated")
        actual_rev = target_report.get("revenueActual")
        est_rev = target_report.get("revenueEstimated")

        # Surprises
        eps_surprise = round((actual_eps - est_eps) / abs(est_eps) * 100, 2) if actual_eps is not None and est_eps and est_eps != 0 else None
        rev_surprise = round((actual_rev - est_rev) / abs(est_rev) * 100, 2) if actual_rev is not None and est_rev and est_rev != 0 else None
        beat = (actual_eps or 0) > (est_eps or 0) if actual_eps is not None and est_eps is not None else None

        # YoY and QoQ from income statements
        income_list.sort(key=lambda i: _date_only(i.get("date")) or "", reverse=True)

        # Find matching quarter in income statements by date proximity
        target_fiscal = _date_only(target_report.get("fiscalDateEnding")) or ""
        current_q = None
        for i, inc in enumerate(income_list):
            inc_date = _date_only(inc.get("date")) or ""
            if inc_date == target_fiscal or (target_fiscal and inc_date[:7] == target_fiscal[:7]):
                current_q = i
                break
        if current_q is None and income_list:
            current_q = 0  # fallback to latest

        yoy, qoq = {}, {}
        if current_q is not None:
            cq = income_list[current_q]
            # YoY = same quarter last year (4 quarters back)
            if current_q + 4 < len(income_list):
                yq = income_list[current_q + 4]
                yoy["revenue_growth_pct"] = _pct_change(cq.get("revenue"), yq.get("revenue"))
                yoy["earnings_growth_pct"] = _pct_change(cq.get("netIncome"), yq.get("netIncome"))
                # Margin delta in bps
                cur_gm = (cq.get("grossProfit") or 0) / cq["revenue"] if cq.get("revenue") else None
                prev_gm = (yq.get("grossProfit") or 0) / yq["revenue"] if yq.get("revenue") else None
                if cur_gm is not None and prev_gm is not None:
                    yoy["gross_margin_delta_bps"] = round((cur_gm - prev_gm) * 10000)
                cur_om = (cq.get("operatingIncome") or 0) / cq["revenue"] if cq.get("revenue") else None
                prev_om = (yq.get("operatingIncome") or 0) / yq["revenue"] if yq.get("revenue") else None
                if cur_om is not None and prev_om is not None:
                    yoy["op_margin_delta_bps"] = round((cur_om - prev_om) * 10000)

            # QoQ = prior quarter (1 quarter back)
            if current_q + 1 < len(income_list):
                pq = income_list[current_q + 1]
                qoq["revenue_growth_pct"] = _pct_change(cq.get("revenue"), pq.get("revenue"))
                qoq["earnings_growth_pct"] = _pct_change(cq.get("netIncome"), pq.get("netIncome"))

        # Post-earnings price reaction
        day_of_pct = None
        post_5d_pct = None
        if earnings_date and historical:
            # Find the earnings date and day after in history
            hist_by_date = {
                d: h for h in historical if (d := _date_only(h.get("date")))
            }
            # Day of = close on earnings date vs previous close
            dates_sorted = sorted(hist_by_date.keys())
            ed_idx = None
            for i, d in enumerate(dates_sorted):
                if d and d >= earnings_date:
                    ed_idx = i
                    break

            if ed_idx is not None and ed_idx > 0:
                ed_close = hist_by_date[dates_sorted[ed_idx]].get("close")
                prev_close = hist_by_date[dates_sorted[ed_idx - 1]].get("close")
                day_of_pct = _pct_change(ed_close, prev_close)

                # 5d post
                if ed_idx + 5 <= len(dates_sorted):
                    post5_close = hist_by_date[dates_sorted[min(ed_idx + 5, len(dates_sorted) - 1)]].get("close")
                    post_5d_pct = _pct_change(post5_close, prev_close)

        # Reaction quality
        reaction_quality = None
        if eps_surprise is not None and day_of_pct is not None:
            if abs(day_of_pct) > abs(eps_surprise) * 0.5:
                reaction_quality = "strong"
            elif abs(day_of_pct) < abs(eps_surprise) * 0.2:
                reaction_quality = "muted"
            elif (eps_surprise > 0 and day_of_pct < 0) or (eps_surprise < 0 and day_of_pct > 0):
                reaction_quality = "inverse"
            else:
                reaction_quality = "proportional"

        # Analyst reaction post-print
        post_grades = [g for g in grades_list if (_date_only(g.get("date")) or "") >= earnings_date]
        upgrades_since = len([g for g in post_grades if (g.get("action") or "").lower() in ("upgrade", "initiate")])
        downgrades_since = len([g for g in post_grades if (g.get("action") or "").lower() == "downgrade"])
        recent_analyst = []
        for g in post_grades[:5]:
            recent_analyst.append({
                "firm": g.get("gradingCompany"),
                "action": g.get("action"),
                "new_grade": g.get("newGrade"),
                "date": _date_only(g.get("date")),
            })

        # Guidance tone from transcript (if available)
        guidance = {"has_transcript": False, "tone": None, "snippet": None}
        transcript_year = None
        transcript_quarter = None

        matched_transcript: dict | None = next(
            (t for t in transcript_dates if (_date_only(t.get("date")) or "") == earnings_date),
            None,
        )
        if matched_transcript is None and earnings_date:
            try:
                earnings_dt = datetime.strptime(earnings_date, "%Y-%m-%d").date()
                nearest_match: tuple[int, dict] | None = None
                for t in transcript_dates:
                    t_date = _date_only(t.get("date")) or ""
                    try:
                        td = datetime.strptime(t_date, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                    diff = abs((td - earnings_dt).days)
                    if diff <= 7 and (nearest_match is None or diff < nearest_match[0]):
                        nearest_match = (diff, t)
                if nearest_match is not None:
                    matched_transcript = nearest_match[1]
            except ValueError:
                pass

        if matched_transcript is not None:
            transcript_year = _to_int(matched_transcript.get("fiscalYear") or matched_transcript.get("year"))
            transcript_quarter = _to_int(matched_transcript.get("quarter") or matched_transcript.get("period"))
        else:
            report_period = _period_from_report(target_report)
            if report_period is not None:
                transcript_year, transcript_quarter = report_period

        if transcript_year is not None and transcript_quarter in (1, 2, 3, 4):
            transcript_data = await _safe_call(
                client.transcripts.get_transcript,
                symbol=symbol,
                year=transcript_year,
                quarter=transcript_quarter,
                ttl=TTL_REALTIME,
                default=[],
            )
            transcript_list = _as_list(transcript_data)
            if transcript_list:
                content = " ".join(t.get("content", "") for t in transcript_list)
                guidance["has_transcript"] = True
                # Simple sentiment scan
                content_lower = content.lower()
                positive_kw = ["strong", "growth", "record", "accelerat", "momentum", "confident", "optimistic", "exceed", "above expectations"]
                negative_kw = ["headwind", "challenge", "decline", "soft", "pressure", "cautious", "uncertain", "below expectations", "weakness"]
                pos_count = sum(1 for kw in positive_kw if kw in content_lower)
                neg_count = sum(1 for kw in negative_kw if kw in content_lower)
                if pos_count > neg_count * 1.5:
                    guidance["tone"] = "positive"
                elif neg_count > pos_count * 1.5:
                    guidance["tone"] = "negative"
                else:
                    guidance["tone"] = "mixed"
                # Snippet: first 300 chars
                guidance["snippet"] = content[:300] if content else None

        # Summary
        headline_parts = []
        if beat is True:
            headline_parts.append(f"beat by {eps_surprise:.1f}%" if eps_surprise else "EPS beat")
        elif beat is False:
            headline_parts.append(f"missed by {abs(eps_surprise or 0):.1f}%")
        if rev_surprise is not None:
            headline_parts.append(f"rev {'beat' if rev_surprise > 0 else 'miss'} {abs(rev_surprise):.1f}%")
        headline = ", ".join(headline_parts) if headline_parts else "results reported"

        beat_quality = "solid" if (eps_surprise or 0) > 5 and (rev_surprise or 0) > 2 else "narrow" if beat else "miss"

        key_positives = []
        key_concerns = []
        if (eps_surprise or 0) > 0:
            key_positives.append(f"EPS beat +{eps_surprise:.1f}%")
        if (rev_surprise or 0) > 0:
            key_positives.append(f"revenue beat +{rev_surprise:.1f}%")
        if yoy.get("revenue_growth_pct") and yoy["revenue_growth_pct"] > 0:
            key_positives.append(f"YoY revenue +{yoy['revenue_growth_pct']:.1f}%")
        if upgrades_since > downgrades_since:
            key_positives.append(f"{upgrades_since} analyst upgrades since report")
        if guidance.get("tone") == "positive":
            key_positives.append("positive guidance tone")

        if (eps_surprise or 0) < 0:
            key_concerns.append(f"EPS miss {eps_surprise:.1f}%")
        if (rev_surprise or 0) < 0:
            key_concerns.append(f"revenue miss {rev_surprise:.1f}%")
        if downgrades_since > upgrades_since:
            key_concerns.append(f"{downgrades_since} analyst downgrades since report")
        if guidance.get("tone") == "negative":
            key_concerns.append("negative guidance tone")

        result = {
            "symbol": symbol,
            "earnings_date": earnings_date,
            "results": {
                "actual_eps": actual_eps,
                "est_eps": est_eps,
                "surprise_pct": eps_surprise,
                "actual_rev": actual_rev,
                "est_rev": est_rev,
                "rev_surprise_pct": rev_surprise,
                "beat": beat,
            },
            "yoy": yoy,
            "qoq": qoq,
            "guidance": guidance,
            "analyst_reaction": {
                "upgrades_since": upgrades_since,
                "downgrades_since": downgrades_since,
                "recent_actions": recent_analyst,
            },
            "market_reaction": {
                "day_of_pct": day_of_pct,
                "post_5d_pct": post_5d_pct,
                "reaction_quality": reaction_quality,
            },
            "summary": {
                "headline": headline,
                "beat_quality": beat_quality,
                "key_positives": key_positives,
                "key_concerns": key_concerns,
            },
        }

        _warnings = []
        if not earnings_list:
            _warnings.append("earnings data unavailable")
        if not income_list:
            _warnings.append("income statement unavailable")
        if not grades_list:
            _warnings.append("analyst grades unavailable")
        if not historical:
            _warnings.append("historical prices unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    # ================================================================
    # 7. ownership_deep_dive — "Who owns this stock?"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Ownership Deep Dive", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def ownership_deep_dive(symbol: str) -> dict:
        """Comprehensive ownership analysis orchestrating multiple ownership endpoints.

        Combines ownership structure, insider activity, institutional ownership,
        and short interest into a unified analysis with ownership insights and signals.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()

        # Import ownership tools from the ownership module
        from cassandra_fmp.tools.ownership import (
            _resolve_latest_symbol_institutional_period,
            _short_interest_dates,
            _fetch_finra_short_interest,
        )

        sym_params = {"symbol": symbol}
        period_task = _resolve_latest_symbol_institutional_period(client, symbol)

        # Fetch all ownership-related endpoints in parallel
        (
            float_data, profile_data, quote_data,
            insider_trades_data, insider_stats_data,
            institutional_period_data,
            finra_tasks,
        ) = await asyncio.gather(
            _safe_call(client.company.get_share_float, symbol=symbol, ttl=TTL_DAILY, default=None),
            _safe_call(client.company.get_profile, symbol=symbol, ttl=TTL_DAILY, default=None),
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
            _safe_call(client.institutional.search_insider_trading, symbol=symbol, limit=100, ttl=TTL_HOURLY, default=[]),
            _safe_call(client.institutional.get_insider_statistics, symbol=symbol, ttl=TTL_HOURLY, default=None),
            period_task,
            asyncio.gather(*[_fetch_finra_short_interest(symbol, d) for d in _short_interest_dates()]),
        )
        year, quarter, institutional_summary_data, institutional_holders_data = institutional_period_data

        float_info = _as_dict(float_data)
        profile = _as_dict(profile_data)
        quote = _as_dict(quote_data)
        insider_trades = _as_list(insider_trades_data)
        insider_stats = _as_dict(insider_stats_data)
        institutional_summary = _as_dict(institutional_summary_data)
        institutional_holders = _as_list(institutional_holders_data)

        # Process FINRA short interest
        finra = None
        for r in finra_tasks:
            if r is not None:
                finra = r
                break

        if not float_info and not profile:
            return {"error": f"No data found for symbol '{symbol}'"}

        # --- Ownership Structure ---
        outstanding_shares = float_info.get("outstandingShares") or 0
        float_shares = float_info.get("floatShares") or 0
        insider_shares = outstanding_shares - float_shares if outstanding_shares > float_shares else 0
        insider_pct = round(insider_shares / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0

        institutional_shares = institutional_summary.get("numberOf13Fshares") or 0
        institutional_pct = round(institutional_shares / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0

        shares_short = (finra or {}).get("currentShortPositionQuantity") or 0
        short_pct_float = round(shares_short / float_shares * 100, 2) if float_shares > 0 else 0
        short_pct_outstanding = round(shares_short / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0

        retail_implied_shares = max(0, float_shares - institutional_shares)
        retail_implied_pct = round(retail_implied_shares / outstanding_shares * 100, 2) if outstanding_shares > 0 else 0

        # --- Insider Activity Analysis ---
        today = date.today()
        cutoff_30 = (today - timedelta(days=30)).isoformat()
        cutoff_90 = (today - timedelta(days=90)).isoformat()

        buys_30, sells_30, buys_90, sells_90 = 0, 0, 0, 0
        cluster_buyers = set()
        notable_insider_trades = []

        for t in insider_trades:
            trade_date = _date_only(t.get("filingDate") or t.get("transactionDate")) or ""
            tx_type = (t.get("transactionType") or "").lower()
            shares = t.get("securitiesTransacted") or 0
            price = t.get("price") or 0
            name = t.get("reportingName", "")
            title = t.get("typeOfOwner", "")

            is_buy = "purchase" in tx_type or "p-purchase" in tx_type
            is_sell = "sale" in tx_type or "s-sale" in tx_type

            if trade_date >= cutoff_90:
                if is_buy:
                    buys_90 += shares
                elif is_sell:
                    sells_90 += shares

                if trade_date >= cutoff_30:
                    if is_buy:
                        buys_30 += shares
                        cluster_buyers.add(name)
                    elif is_sell:
                        sells_30 += shares

            # C-suite trades
            title_lower = title.lower() if title else ""
            if any(t in title_lower for t in ["ceo", "cfo", "coo", "director", "officer"]):
                if (is_buy or is_sell) and trade_date >= cutoff_90:
                    notable_insider_trades.append({
                        "name": name,
                        "title": title,
                        "type": "buy" if is_buy else "sell",
                        "shares": shares,
                        "price": price,
                        "date": trade_date,
                        "value": round(shares * price, 2) if shares and price else None,
                    })

        net_30 = buys_30 - sells_30
        net_90 = buys_90 - sells_90
        cluster_buying = len(cluster_buyers) >= 3
        insider_signal = "net_buying" if net_30 > 0 else "net_selling" if net_30 < 0 else "neutral"

        # --- Institutional Ownership Details ---
        top_institutional = []
        for h in institutional_holders[:10]:
            shares_held = h.get("sharesNumber") or h.get("shares") or 0
            pct = round(shares_held / outstanding_shares * 100, 2) if outstanding_shares > 0 else None
            change = h.get("changeInSharesNumber") or h.get("changeInShares") or 0
            top_institutional.append({
                "holder": h.get("investorName") or h.get("name") or h.get("holder"),
                "shares": shares_held,
                "ownership_pct": pct,
                "change_in_shares": change,
                "change_type": "increased" if change > 0 else "decreased" if change < 0 else "unchanged",
            })

        institutional_investors = institutional_summary.get("investorsHolding") or 0
        institutional_change = institutional_summary.get("investorsHoldingChange") or 0

        # --- Short Interest Context ---
        short_interest_context = {
            "shares_short": shares_short,
            "pct_of_float": short_pct_float,
            "pct_of_outstanding": short_pct_outstanding,
            "settlement_date": (finra or {}).get("settlementDate"),
            "days_to_cover": (finra or {}).get("daysToCoverQuantity"),
            "change_pct": (finra or {}).get("changePercent"),
        }

        # --- Ownership Insights & Signals ---
        insights = []
        ownership_score = 0
        risk_factors = []

        # High insider ownership is positive
        if insider_pct > 20:
            insights.append(f"high insider ownership ({insider_pct:.1f}%) suggests alignment with shareholders")
            ownership_score += 1
        elif insider_pct < 1:
            risk_factors.append(f"very low insider ownership ({insider_pct:.1f}%)")
            ownership_score -= 0.5

        # Insider buying signal
        if cluster_buying or net_30 > 0:
            insights.append(f"insider buying activity: {buys_30:,} shares bought (30d)" + (" - cluster buying" if cluster_buying else ""))
            ownership_score += 0.5
        elif net_30 < 0 and abs(net_30) > buys_30 * 2:
            risk_factors.append(f"heavy insider selling: {sells_30:,} shares sold (30d)")
            ownership_score -= 0.5

        # Institutional concentration
        institutional_pct_float = round(institutional_shares / float_shares * 100, 2) if float_shares > 0 else 0
        if institutional_pct_float > 100:
            insights.append(f"institutional ownership exceeds float ({institutional_pct_float:.1f}% of float) - high conviction")
            ownership_score += 0.5
        elif institutional_pct < 20:
            risk_factors.append(f"low institutional ownership ({institutional_pct:.1f}%)")
            ownership_score -= 0.3

        # Short interest analysis
        if short_pct_float > 20:
            risk_factors.append(f"high short interest ({short_pct_float:.1f}% of float) - squeeze potential or bearish sentiment")
            ownership_score -= 0.5
        elif short_pct_float > 10:
            insights.append(f"elevated short interest ({short_pct_float:.1f}% of float)")

        # Float lock-up check
        locked_pct = insider_pct + institutional_pct
        if locked_pct > 80:
            insights.append(f"tight float: {locked_pct:.1f}% locked by insiders + institutions")
            ownership_score += 0.3

        # Retail dominance check
        if retail_implied_pct > 50:
            insights.append(f"retail-dominated float ({retail_implied_pct:.1f}% implied retail)")

        signal = _classify_signal(ownership_score, thresholds=(-0.5, 0.5))

        result = {
            "symbol": symbol,
            "company_name": profile.get("companyName"),
            "current_price": quote.get("price"),
            "market_cap": quote.get("marketCap"),
            "reporting_period": f"Q{quarter} {year}",
            "ownership_structure": {
                "outstanding_shares": outstanding_shares,
                "float_shares": float_shares,
                "insider_pct": insider_pct,
                "institutional_pct": institutional_pct,
                "short_pct_float": short_pct_float,
                "short_pct_outstanding": short_pct_outstanding,
                "retail_implied_pct": retail_implied_pct,
            },
            "insider_activity": {
                "net_shares_30d": net_30,
                "net_shares_90d": net_90,
                "signal": insider_signal,
                "cluster_buying": cluster_buying,
                "notable_trades": notable_insider_trades[:10],
            },
            "institutional_ownership": {
                "total_shares": institutional_shares,
                "investors_count": institutional_investors,
                "investors_change_qoq": institutional_change,
                "top_holders": top_institutional,
            },
            "short_interest": short_interest_context,
            "ownership_analysis": {
                "signal": signal,
                "score": round(ownership_score, 2),
                "key_insights": insights,
                "risk_factors": risk_factors,
            },
        }

        _warnings = []
        if not float_info:
            _warnings.append("float data unavailable")
        if not insider_trades:
            _warnings.append("insider trades unavailable")
        if not institutional_summary:
            _warnings.append("institutional summary unavailable")
        if finra is None:
            _warnings.append("FINRA short interest unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    # ================================================================
    # 8. industry_analysis — "What's happening in this industry?"
    # ================================================================

    @mcp.tool(
        annotations={"title": "Industry Analysis", "readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def industry_analysis(industry: str, limit: int = 10) -> dict:
        """Industry analysis orchestrating performance data, top stocks by market cap, median valuation multiples, growth comparison, and rotation signal.

        Orchestrates: industry performance, top stocks in industry by market cap (via screener),
        median valuation multiples, growth comparison, rotation signal (money flow in/out),
        valuation spread (cheapest vs most expensive).

        Args:
            industry: Industry name (e.g. "Software - Application", "Biotechnology")
            limit: Number of top stocks to return (default 10)
        """
        industry = industry.strip()
        limit = min(max(limit, 1), 50)
        today_dt = date.today()
        today_str = today_dt.isoformat()

        # Fetch industry performance and top stocks in parallel
        (
            nyse_perf_data, nasdaq_perf_data,
            nyse_pe_data, nasdaq_pe_data,
            screener_data,
        ) = await asyncio.gather(
            _safe_call(client.market.get_industry_performance_snapshot, date=today_dt, exchange="NYSE", ttl=TTL_REALTIME, default=[]),
            _safe_call(client.market.get_industry_performance_snapshot, date=today_dt, exchange="NASDAQ", ttl=TTL_REALTIME, default=[]),
            _safe_call(client.market.get_industry_pe_snapshot, date=today_dt, exchange="NYSE", ttl=TTL_DAILY, default=[]),
            _safe_call(client.market.get_industry_pe_snapshot, date=today_dt, exchange="NASDAQ", ttl=TTL_DAILY, default=[]),
            _safe_call(client.market.get_company_screener, industry=industry, limit=limit * 3, ttl=TTL_HOURLY, default=[]),
        )

        nyse_perf = _as_list(nyse_perf_data)
        nasdaq_perf = _as_list(nasdaq_perf_data)
        nyse_pe = _as_list(nyse_pe_data)
        nasdaq_pe = _as_list(nasdaq_pe_data)
        screener_list = _as_list(screener_data)

        # Industry performance (average across exchanges)
        perf_entries = [e for e in nyse_perf + nasdaq_perf if (e.get("industry") or "").lower() == industry.lower()]
        industry_change = None
        industry_sector = None
        if perf_entries:
            changes = []
            for e in perf_entries:
                val = e.get("averageChange")
                if val is None:
                    val = e.get("changePercentage")
                if val is not None:
                    changes.append(val)
            if changes:
                industry_change = round(sum(changes) / len(changes), 4)
            industry_sector = perf_entries[0].get("sector")

        # Industry PE (average across exchanges)
        pe_entries = [e for e in nyse_pe + nasdaq_pe if (e.get("industry") or "").lower() == industry.lower()]
        industry_pe = None
        if pe_entries:
            pe_vals = [e.get("pe") for e in pe_entries if e.get("pe") is not None and e.get("pe") > 0]
            if pe_vals:
                industry_pe = round(sum(pe_vals) / len(pe_vals), 2)

        if not screener_list and industry_change is None:
            return {"error": f"No data found for industry '{industry}'"}

        # Sort screener results by market cap descending and take top N
        screener_list.sort(key=lambda s: s.get("marketCap") or 0, reverse=True)
        top_stocks = screener_list[:limit]

        # Extract symbols for parallel data fetch
        symbols = [s.get("symbol") for s in top_stocks if s.get("symbol")]

        # Fetch ratios and income statements for top stocks
        async def _fetch_stock_data(sym: str) -> tuple[str, dict, dict, dict]:
            ratios, income, quote = await asyncio.gather(
                _safe_call(client.company.get_financial_ratios_ttm, symbol=sym, ttl=TTL_HOURLY, default=None),
                _safe_call(client.fundamental.get_income_statement, symbol=sym, limit=4, ttl=TTL_HOURLY, default=[]),
                _safe_call(client.company.get_quote, symbol=sym, ttl=TTL_REALTIME, default=None),
            )
            return sym, _as_dict(ratios), _as_list(income), _as_dict(quote)

        stock_data_results = await asyncio.gather(*[_fetch_stock_data(s) for s in symbols]) if symbols else []

        # Build stock_data map
        stock_data_map: dict[str, tuple[dict, list, dict]] = {}
        for sym, ratios, income, quote in stock_data_results:
            stock_data_map[sym] = (ratios, income, quote)

        # Build top stocks list with valuation and growth
        top_stocks_list = []
        pe_values = []
        ps_values = []
        rev_growth_values = []

        for stock in top_stocks:
            sym = stock.get("symbol")
            if not sym:
                continue

            ratios, income_list, quote = stock_data_map.get(sym, ({}, [], {}))

            pe = ratios.get("priceToEarningsRatioTTM")
            ps = ratios.get("priceToSalesRatioTTM")
            peg = ratios.get("priceToEarningsGrowthRatioTTM")
            roe = ratios.get("returnOnEquityTTM")

            # Calculate revenue growth (3-year CAGR if available)
            rev_cagr_3y = None
            if len(income_list) >= 4:
                start_rev = income_list[3].get("revenue")
                end_rev = income_list[0].get("revenue")
                if start_rev and end_rev and start_rev > 0:
                    try:
                        rev_cagr_3y = round(((end_rev / start_rev) ** (1 / 3) - 1) * 100, 2)
                    except (ZeroDivisionError, ValueError, OverflowError):
                        pass

            top_stocks_list.append({
                "symbol": sym,
                "name": stock.get("companyName") or stock.get("name"),
                "market_cap": stock.get("marketCap"),
                "price": quote.get("price") or stock.get("price"),
                "change_pct": quote.get("changePercentage"),
                "valuation": {
                    "pe": pe,
                    "ps": ps,
                    "peg": peg,
                },
                "growth": {
                    "rev_cagr_3y_pct": rev_cagr_3y,
                },
                "quality": {
                    "roe": roe,
                },
            })

            # Collect for median calculations
            if pe is not None and pe > 0:
                pe_values.append(pe)
            if ps is not None and ps > 0:
                ps_values.append(ps)
            if rev_cagr_3y is not None:
                rev_growth_values.append(rev_cagr_3y)

        # Median multiples
        median_pe = _median(pe_values)
        median_ps = _median(ps_values)
        median_rev_growth = _median(rev_growth_values)

        # Valuation spread (cheapest vs most expensive by PE)
        cheapest_stock = None
        most_expensive_stock = None
        if pe_values:
            min_pe = min(pe_values)
            max_pe = max(pe_values)
            for s in top_stocks_list:
                if s["valuation"]["pe"] == min_pe:
                    cheapest_stock = {"symbol": s["symbol"], "name": s["name"], "pe": min_pe}
                if s["valuation"]["pe"] == max_pe:
                    most_expensive_stock = {"symbol": s["symbol"], "name": s["name"], "pe": max_pe}

        # Rotation signal (compare industry performance to market)
        # Fetch sector performance for context
        sectors = await _fetch_sectors(client)
        market_avg = None
        if sectors:
            sector_changes = [s.get("change_pct") for s in sectors if s.get("change_pct") is not None]
            if sector_changes:
                market_avg = round(sum(sector_changes) / len(sector_changes), 4)

        rotation_signal = "neutral"
        rotation_score = 0
        if industry_change is not None and market_avg is not None:
            rotation_score = industry_change - market_avg
            if rotation_score > 0.5:
                rotation_signal = "money_flowing_in"
            elif rotation_score < -0.5:
                rotation_signal = "money_flowing_out"

        # Key insights
        insights = []
        if industry_change is not None:
            insights.append(f"industry performance: {industry_change:+.2f}%")
        if market_avg is not None and rotation_score != 0:
            insights.append(f"vs market: {rotation_score:+.2f}% ({rotation_signal.replace('_', ' ')})")
        if median_pe:
            insights.append(f"median P/E: {median_pe:.1f}")
        if median_rev_growth:
            insights.append(f"median revenue growth: {median_rev_growth:.1f}%")

        result = {
            "industry": industry,
            "sector": industry_sector,
            "date": today_str,
            "overview": {
                "performance_pct": industry_change,
                "median_pe": industry_pe or median_pe,
                "market_avg_pct": market_avg,
            },
            "top_stocks": top_stocks_list,
            "industry_medians": {
                "pe": median_pe,
                "ps": median_ps,
                "rev_growth_3y_pct": median_rev_growth,
            },
            "valuation_spread": {
                "cheapest": cheapest_stock,
                "most_expensive": most_expensive_stock,
            },
            "rotation": {
                "signal": rotation_signal,
                "industry_vs_market_pct": rotation_score if market_avg is not None else None,
            },
            "summary": {
                "key_insights": insights,
            },
        }

        _warnings = []
        if not perf_entries:
            _warnings.append("industry performance unavailable")
        if not pe_entries and industry_pe is None:
            _warnings.append("industry PE unavailable")
        if not screener_list:
            _warnings.append("stock screener data unavailable")
        if not sectors:
            _warnings.append("sector performance unavailable for rotation signal")
        if _warnings:
            result["_warnings"] = _warnings

        return result
