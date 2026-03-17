"""Analyst consensus, valuation, and estimate revision tools."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import TYPE_CHECKING

from cassandra_fmp.tools._helpers import TTL_6H, TTL_DAILY, TTL_HOURLY, TTL_REALTIME, _as_dict, _as_list, _date_only, _safe_call

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient


def _percentile(values: list[float], target: float | None) -> float | None:
    """Calculate percentile rank of target value in a list."""
    if target is None or not values:
        return None
    clean = [v for v in values if v is not None and isinstance(v, (int, float))]
    if not clean:
        return None
    below = sum(1 for v in clean if v < target)
    return round(below / len(clean) * 100, 1)


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    @mcp.tool(
        annotations={
            "title": "Valuation History",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def valuation_history(
        symbol: str,
        period: str = "annual",
        limit: int = 10,
    ) -> dict:
        """Get historical valuation multiples with percentile analysis.

        Returns current TTM multiples, historical time series, and percentile
        positioning (min, p25, median, p75, max, current_percentile) for
        P/E, P/S, P/B, EV/EBITDA, EV/Revenue, EV/FCF.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            period: "annual" or "quarter" (default "annual")
            limit: Number of historical periods to fetch (default 10)
        """
        symbol = symbol.upper().strip()

        # Fetch historical metrics and current TTM ratios
        key_metrics_data, ratios_ttm_data = await asyncio.gather(
            _safe_call(
                client.fundamental.get_key_metrics,
                symbol=symbol,
                period=period,
                limit=limit,
                ttl=TTL_HOURLY,
                default=[],
            ),
            _safe_call(
                client.company.get_financial_ratios_ttm,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=[],
            ),
        )

        key_metrics = _as_list(key_metrics_data)
        ratios_ttm = _as_dict(ratios_ttm_data)

        if not key_metrics and not ratios_ttm:
            return {"error": f"No valuation data found for '{symbol}'"}

        # Extract current TTM multiples
        current_ttm = {
            "pe_ttm": ratios_ttm.get("priceToEarningsRatioTTM"),
            "ps_ttm": ratios_ttm.get("priceToSalesRatioTTM"),
            "pb_ttm": ratios_ttm.get("priceToBookRatioTTM"),
            "ev_ebitda_ttm": ratios_ttm.get("enterpriseValueMultipleTTM"),
            "ev_revenue_ttm": None,  # Calculated below
            "ev_fcf_ttm": None,  # Calculated below
        }

        # Some EV ratios might be in key-metrics-ttm
        if key_metrics:
            latest = key_metrics[0]
            if current_ttm["ev_revenue_ttm"] is None:
                current_ttm["ev_revenue_ttm"] = latest.get("evToSales")
            if current_ttm["ev_fcf_ttm"] is None:
                current_ttm["ev_fcf_ttm"] = latest.get("evToFreeCashFlow")

        # Build historical series
        # key-metrics uses earningsYield (1/PE), evToSales, evToEBITDA
        # pbRatio is absent from key-metrics
        historical = []
        for m in key_metrics:
            # Compute PE from earningsYield if available
            earnings_yield = m.get("earningsYield")
            pe = round(1.0 / earnings_yield, 2) if earnings_yield and earnings_yield != 0 else m.get("peRatio")

            historical.append({
                "date": _date_only(m.get("date")),
                "period": m.get("period"),
                "pe": pe,
                "ps": m.get("evToSales") or m.get("priceToSalesRatio"),
                "pb": m.get("pbRatio"),  # may be None from key-metrics
                "ev_ebitda": m.get("evToEBITDA") or m.get("enterpriseValueOverEBITDA"),
                "ev_revenue": m.get("evToSales"),
                "ev_fcf": m.get("evToFreeCashFlow"),
            })

        # Calculate percentiles for each metric
        def _calc_percentiles(values: list[float | None], current: float | None) -> dict:
            clean = [v for v in values if v is not None and isinstance(v, (int, float))]
            if not clean:
                return {}
            clean.sort()
            n = len(clean)
            return {
                "min": round(clean[0], 2),
                "p25": round(clean[n // 4], 2) if n > 1 else clean[0],
                "median": round(clean[n // 2], 2),
                "p75": round(clean[3 * n // 4], 2) if n > 2 else clean[-1],
                "max": round(clean[-1], 2),
                "current_percentile": _percentile(clean, current),
            }

        pe_values = [h["pe"] for h in historical]
        ps_values = [h["ps"] for h in historical]
        pb_values = [h["pb"] for h in historical]
        ev_ebitda_values = [h["ev_ebitda"] for h in historical]
        ev_fcf_values = [h["ev_fcf"] for h in historical]

        percentiles = {
            "pe": _calc_percentiles(pe_values, current_ttm["pe_ttm"]),
            "ps": _calc_percentiles(ps_values, current_ttm["ps_ttm"]),
            "pb": _calc_percentiles(pb_values, current_ttm["pb_ttm"]),
            "ev_ebitda": _calc_percentiles(ev_ebitda_values, current_ttm["ev_ebitda_ttm"]),
            "ev_fcf": _calc_percentiles(ev_fcf_values, current_ttm["ev_fcf_ttm"]),
        }

        result = {
            "symbol": symbol,
            "period_type": period,
            "current_ttm": current_ttm,
            "historical": historical,
            "percentiles": percentiles,
        }

        _warnings = []
        if not ratios_ttm:
            _warnings.append("TTM ratios unavailable")
        if not key_metrics:
            _warnings.append("historical key metrics unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Analyst Consensus",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def analyst_consensus(symbol: str) -> dict:
        """Get Wall Street analyst consensus: price targets, ratings, and buy/sell distribution.

        Returns analyst count, consensus/high/low price targets, upside percentage,
        buy/hold/sell breakdown, and FMP's own rating.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()

        targets_data, grades_data, rating_data, quote_data = await asyncio.gather(
            _safe_call(client.company.get_price_target_consensus, symbol=symbol, ttl=TTL_6H, default=None),
            _safe_call(client.intelligence.get_grades_consensus, symbol=symbol, ttl=TTL_6H, default=None),
            _safe_call(client.intelligence.get_ratings_snapshot, symbol=symbol, ttl=TTL_6H, default=[]),
            _safe_call(client.company.get_quote, symbol=symbol, ttl=TTL_REALTIME, default=None),
        )

        targets = _as_dict(targets_data)
        grades = _as_dict(grades_data)
        rating = _as_dict(rating_data)
        quote = _as_dict(quote_data)

        if not targets and not grades and not rating:
            return {"error": f"No analyst data found for '{symbol}'"}

        current_price = quote.get("price")

        # Calculate upside from consensus target
        consensus_target = targets.get("targetConsensus")
        upside_pct = None
        if current_price and consensus_target:
            upside_pct = round((consensus_target / current_price - 1) * 100, 2)

        result = {
            "symbol": symbol,
            "current_price": current_price,
            "price_targets": {
                "consensus": consensus_target,
                "high": targets.get("targetHigh"),
                "low": targets.get("targetLow"),
                "median": targets.get("targetMedian"),
                "upside_pct": upside_pct,
            },
            "analyst_grades": {
                "strong_buy": grades.get("strongBuy"),
                "buy": grades.get("buy"),
                "hold": grades.get("hold"),
                "sell": grades.get("sell"),
                "strong_sell": grades.get("strongSell"),
                "consensus": grades.get("consensus"),
            },
            "fmp_rating": {
                "rating": rating.get("rating"),
                "overall_score": rating.get("overallScore"),
                "dcf_score": rating.get("discountedCashFlowScore"),
                "roe_score": rating.get("returnOnEquityScore"),
                "roa_score": rating.get("returnOnAssetsScore"),
                "de_score": rating.get("debtToEquityScore"),
                "pe_score": rating.get("priceToEarningsScore"),
                "pb_score": rating.get("priceToBookScore"),
            },
        }

        # Flag partial data
        errors = []
        if not targets:
            errors.append("price target data unavailable")
        if not grades:
            errors.append("analyst grades unavailable")
        if not rating:
            errors.append("FMP rating unavailable")
        if errors:
            result["_warnings"] = errors

        return result

    @mcp.tool(
        annotations={
            "title": "Discounted Cash Flow",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def discounted_cash_flow(symbol: str, mode: str = "unlevered") -> dict:
        """Get FMP-native DCF values (unlevered, levered, or both)."""
        symbol = symbol.upper().strip()
        mode = mode.lower().strip()
        if mode not in {"unlevered", "levered", "both"}:
            return {"error": "Invalid mode. Use 'unlevered', 'levered', or 'both'."}

        async def _unlevered():
            data = await _safe_call(
                client.fundamental.get_discounted_cash_flow,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=[],
            )
            return _as_list(data)

        async def _levered():
            data = await _safe_call(
                client.fundamental.get_levered_dcf,
                symbol=symbol,
                ttl=TTL_HOURLY,
                default=[],
            )
            return _as_list(data)

        unlevered_list: list[dict] = []
        levered_list: list[dict] = []

        if mode == "unlevered":
            unlevered_list = await _unlevered()
        elif mode == "levered":
            levered_list = await _levered()
        else:
            unlevered_list, levered_list = await asyncio.gather(_unlevered(), _levered())

        if not unlevered_list and not levered_list:
            return {"error": f"No DCF data found for '{symbol}'"}

        def _format(entries: list[dict]) -> list[dict]:
            formatted = []
            for row in entries:
                dcf_value = (
                    row.get("dcf")
                    or row.get("dcfValue")
                    or row.get("leveredDCF")
                    or row.get("leveredDcf")
                    or row.get("discountedCashFlow")
                )
                stock_price = row.get("stockPrice") or row.get("price")
                upside_pct = None
                if dcf_value is not None and stock_price not in (None, 0):
                    upside_pct = round((dcf_value / stock_price - 1) * 100, 2)
                formatted.append(
                    {
                        "date": _date_only(row.get("date")),
                        "dcf_value": dcf_value,
                        "stock_price": stock_price,
                        "upside_pct": upside_pct,
                    }
                )
            return formatted

        result = {
            "symbol": symbol,
            "mode": mode,
            "unlevered": _format(unlevered_list),
            "levered": _format(levered_list),
        }
        warnings = []
        if mode in {"unlevered", "both"} and not unlevered_list:
            warnings.append("unlevered DCF unavailable")
        if mode in {"levered", "both"} and not levered_list:
            warnings.append("levered DCF unavailable")
        if warnings:
            result["_warnings"] = warnings
        return result

    @mcp.tool(
        annotations={
            "title": "Peer Comparison",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def peer_comparison(symbol: str, peer_symbols: list[str] | None = None) -> dict:
        """Compare a stock's valuation and growth metrics against its peers.

        Finds FMP-identified peers (or uses custom peer list), then compares P/E, P/S,
        EV/EBITDA, forward P/E, forward P/S, revenue/EPS growth, PEG ratio, dividend yield,
        and margins. Shows target's premium/discount to peer median and rank within the group.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
            peer_symbols: Optional custom peer list (skips FMP auto-peers if provided)
        """
        symbol = symbol.upper().strip()

        # Step 1: Get peer list (either custom or FMP auto-peers)
        warnings: list[str] = []
        peer_source = "custom" if peer_symbols else "auto"
        auto_peer_count = None

        if peer_symbols:
            # Use custom peer list
            peer_symbols = [p.upper().strip() for p in peer_symbols if p and p.strip()]
            # Remove target symbol if accidentally included
            peer_symbols = [p for p in peer_symbols if p != symbol]
            if not peer_symbols:
                return {"error": "Custom peer_symbols was empty after normalization"}
        else:
            # /stable/stock-peers returns a flat list of peer companies
            peers_data = await _safe_call(
                client.company.get_company_peers,
                symbol=symbol,
                ttl=TTL_DAILY,
                default=[],
            )

            peers_list = _as_list(peers_data)
            if not peers_list:
                return {"error": f"No peer data found for '{symbol}'"}

            # Extract peer symbols from the flat list
            peer_symbols = [
                p.get("symbol").upper().strip()
                for p in peers_list
                if isinstance(p.get("symbol"), str) and p.get("symbol").strip()
            ]
            peer_symbols = [p for p in peer_symbols if p != symbol]
            if not peer_symbols:
                return {"error": f"No peers identified for '{symbol}'"}

            auto_peer_count = len(peer_symbols)
            if auto_peer_count < 5:
                warnings.append(
                    f"Auto peer set has only {auto_peer_count} symbol(s); provide peer_symbols for a higher-quality direct-comp set."
                )

            # Limit to 10 peers to avoid excessive API calls
            peer_symbols = peer_symbols[:10]

        # Step 2: Fetch ratios, key metrics, forward estimates, and income statements for target + all peers
        all_symbols = [symbol] + peer_symbols

        async def _fetch_metrics(sym: str) -> dict:
            ratios, metrics, estimates, income = await asyncio.gather(
                _safe_call(client.company.get_financial_ratios_ttm, symbol=sym, ttl=TTL_HOURLY, default=[]),
                _safe_call(client.company.get_key_metrics_ttm, symbol=sym, ttl=TTL_HOURLY, default=[]),
                _safe_call(
                    client.company.get_analyst_estimates,
                    symbol=sym,
                    period="quarter",
                    limit=1,
                    ttl=TTL_6H,
                    default=[],
                ),
                _safe_call(
                    client.fundamental.get_income_statement,
                    symbol=sym,
                    period="annual",
                    limit=2,
                    ttl=TTL_HOURLY,
                    default=[],
                ),
            )
            r = _as_dict(ratios)
            m = _as_dict(metrics)
            est = _as_dict(estimates)
            inc_list = _as_list(income)

            # Calculate 1Y growth rates
            revenue_growth_1y = None
            eps_growth_1y = None
            if len(inc_list) >= 2:
                latest = inc_list[0]
                prior = inc_list[1]
                if latest.get("revenue") and prior.get("revenue") and prior["revenue"] > 0:
                    revenue_growth_1y = round((latest["revenue"] / prior["revenue"] - 1) * 100, 2)
                if latest.get("epsDiluted") and prior.get("epsDiluted") and prior["epsDiluted"] > 0:
                    eps_growth_1y = round((latest["epsDiluted"] / prior["epsDiluted"] - 1) * 100, 2)

            # Forward P/E and P/S
            forward_pe = None
            forward_ps = None
            est_eps_avg = est.get("epsAvg")
            if est_eps_avg is None:
                est_eps_avg = est.get("estimatedEpsAvg")
            est_revenue_avg = est.get("revenueAvg")
            if est_revenue_avg is None:
                est_revenue_avg = est.get("estimatedRevenueAvg")
            if est_eps_avg and est_eps_avg > 0 and m.get("marketCapTTM"):
                # Approximate: market_cap / (forward_eps * shares)
                # We don't have shares easily, so use ratio of current P/E
                pe_current = r.get("priceToEarningsRatioTTM")
                eps_current = r.get("earningsYield")  # Inverse of P/E
                if pe_current:
                    forward_pe = pe_current * (est_eps_avg / (1 / eps_current if eps_current else 1))
            if est_revenue_avg and est_revenue_avg > 0 and m.get("marketCapTTM"):
                forward_ps = m["marketCapTTM"] / est_revenue_avg

            # PEG ratio
            peg = None
            pe_ttm = r.get("priceToEarningsRatioTTM")
            if pe_ttm and eps_growth_1y and eps_growth_1y > 0:
                peg = round(pe_ttm / eps_growth_1y, 2)

            # Dividend yield
            div_yield = r.get("dividendYielTTM") or r.get("dividendYieldTTM")

            return {
                "symbol": sym,
                "pe_ttm": pe_ttm,
                "ps_ttm": r.get("priceToSalesRatioTTM"),
                "ev_ebitda_ttm": r.get("enterpriseValueMultipleTTM"),
                "pb_ttm": r.get("priceToBookRatioTTM"),
                "forward_pe": forward_pe,
                "forward_ps": forward_ps,
                "revenue_growth_1y": revenue_growth_1y,
                "eps_growth_1y": eps_growth_1y,
                "peg": peg,
                "dividend_yield": div_yield,
                "roe_ttm": r.get("returnOnEquityTTM"),
                "gross_margin_ttm": r.get("grossProfitMarginTTM"),
                "net_margin_ttm": r.get("netProfitMarginTTM"),
                "market_cap": m.get("marketCapTTM"),
            }

        all_metrics = await asyncio.gather(*[_fetch_metrics(s) for s in all_symbols])

        target_metrics = all_metrics[0]
        peer_metrics = [m for m in all_metrics[1:] if m.get("pe_ttm") is not None or m.get("ps_ttm") is not None]

        if not peer_metrics:
            return {
                "symbol": symbol,
                "target": target_metrics,
                "error": "Could not retrieve metrics for any peers",
            }

        # Step 3: Calculate peer medians and target premium/discount
        def _median(values: list[float]) -> float | None:
            clean = [v for v in values if v is not None and isinstance(v, (int, float))]
            if not clean:
                return None
            clean.sort()
            n = len(clean)
            if n % 2 == 0:
                return round((clean[n // 2 - 1] + clean[n // 2]) / 2, 4)
            return round(clean[n // 2], 4)

        def _premium_discount(target_val, median_val) -> float | None:
            if target_val is None or median_val is None or median_val == 0:
                return None
            return round((target_val / median_val - 1) * 100, 2)

        def _rank(target_val, all_vals: list, higher_better: bool = False) -> str | None:
            clean = [(v, i) for i, v in enumerate(all_vals) if v is not None]
            if not clean or target_val is None:
                return None
            clean.sort(key=lambda x: x[0], reverse=higher_better)
            for rank, (v, i) in enumerate(clean, 1):
                if i == 0:  # target is always index 0
                    return f"{rank}/{len(clean)}"
            return None

        comparison_metrics = [
            "pe_ttm", "ps_ttm", "ev_ebitda_ttm", "pb_ttm", "forward_pe", "forward_ps",
            "revenue_growth_1y", "eps_growth_1y", "peg", "dividend_yield",
            "roe_ttm", "gross_margin_ttm", "net_margin_ttm"
        ]
        higher_better = {"revenue_growth_1y", "eps_growth_1y", "dividend_yield", "roe_ttm", "gross_margin_ttm", "net_margin_ttm"}

        comparisons = {}
        for metric in comparison_metrics:
            peer_vals = [m.get(metric) for m in peer_metrics]
            all_vals = [target_metrics.get(metric)] + peer_vals
            med = _median(peer_vals)
            comparisons[metric] = {
                "target": target_metrics.get(metric),
                "peer_median": med,
                "premium_discount_pct": _premium_discount(target_metrics.get(metric), med),
                "rank": _rank(target_metrics.get(metric), all_vals, metric in higher_better),
            }

        result = {
            "symbol": symbol,
            "peers": peer_symbols,
            "peer_count": len(peer_metrics),
            "peer_source": peer_source,
            "auto_peer_count": auto_peer_count,
            "comparisons": comparisons,
            "peer_details": peer_metrics,
        }
        if warnings:
            result["_warnings"] = warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Estimate Revisions",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def estimate_revisions(symbol: str) -> dict:
        """Get analyst estimate momentum: forward estimates, recent grade changes, and earnings track record.

        Combines current consensus estimates, individual analyst upgrades/downgrades
        (last 90 days), and historical beat/miss rates to gauge sentiment direction.

        Args:
            symbol: Stock ticker symbol (e.g. "AAPL")
        """
        symbol = symbol.upper().strip()

        estimates_data, grades_data, earnings_data = await asyncio.gather(
            _safe_call(
                client.company.get_analyst_estimates,
                symbol=symbol,
                period="quarter",
                limit=4,
                ttl=TTL_6H,
                default=[],
            ),
            _safe_call(client.intelligence.get_grades, symbol=symbol, page=0, ttl=TTL_6H, default=[]),
            _safe_call(client.company.get_earnings, symbol=symbol, limit=8, ttl=TTL_HOURLY, default=[]),
        )

        estimates_list = _as_list(estimates_data)
        grades_list = _as_list(grades_data)
        earnings_list = _as_list(earnings_data)

        if not estimates_list and not grades_list and not earnings_list:
            return {"error": f"No estimate or analyst data found for '{symbol}'"}

        # --- Forward estimates ---
        estimates_list.sort(key=lambda e: _date_only(e.get("date")) or "")
        forward_estimates = []
        for e in estimates_list:
            eps_avg = e.get("epsAvg")
            if eps_avg is None:
                eps_avg = e.get("estimatedEpsAvg")
            revenue_avg = e.get("revenueAvg")
            if revenue_avg is None:
                revenue_avg = e.get("estimatedRevenueAvg")
            forward_estimates.append({
                "date": _date_only(e.get("date")),
                "eps_avg": eps_avg,
                "revenue_avg": revenue_avg,
                "num_analysts_eps": e.get("numAnalystsEps") or e.get("numberAnalystsEstimatedEps"),
            })

        # --- Recent analyst actions (last 90 days) ---
        cutoff = (date.today() - timedelta(days=90)).isoformat()
        recent_grades = [g for g in grades_list if (_date_only(g.get("date")) or "") >= cutoff]

        actions = []
        upgrades = downgrades = initiations = maintains = 0
        for g in recent_grades:
            action = (g.get("action") or "").lower()
            actions.append({
                "date": _date_only(g.get("date")),
                "firm": g.get("gradingCompany"),
                "action": action,
                "new_grade": g.get("newGrade"),
            })
            if action == "upgrade":
                upgrades += 1
            elif action == "downgrade":
                downgrades += 1
            elif action == "initiate":
                initiations += 1
            elif action == "maintain":
                maintains += 1

        # Net sentiment
        if upgrades > downgrades:
            net_sentiment = "bullish"
        elif downgrades > upgrades:
            net_sentiment = "bearish"
        else:
            net_sentiment = "neutral"

        analyst_actions = {
            "period": "90d",
            "actions": actions,
            "summary": {
                "upgrades": upgrades,
                "downgrades": downgrades,
                "initiations": initiations,
                "maintains": maintains,
                "net_sentiment": net_sentiment,
            },
        }

        # --- Earnings track record ---
        # Filter to actuals only (entries with epsActual field populated)
        actuals = [e for e in earnings_list if e.get("epsActual") is not None]
        track = []
        beats = 0
        surprise_pcts = []
        for e in actuals:
            eps_actual = e.get("epsActual")
            eps_est = e.get("epsEstimated")
            rev_actual = e.get("revenueActual")
            rev_est = e.get("revenueEstimated")

            eps_surprise_pct = None
            if eps_est and eps_est != 0:
                eps_surprise_pct = round((eps_actual - eps_est) / abs(eps_est) * 100, 2)
                surprise_pcts.append(eps_surprise_pct)
                if eps_actual > eps_est:
                    beats += 1

            rev_surprise_pct = None
            if rev_est and rev_est != 0:
                rev_surprise_pct = round((rev_actual - rev_est) / abs(rev_est) * 100, 2)

            track.append({
                "date": _date_only(e.get("date")),
                "eps_surprise_pct": eps_surprise_pct,
                "revenue_surprise_pct": rev_surprise_pct,
            })

        beat_rate = round(beats / len(actuals) * 100, 1) if actuals else None
        avg_surprise = round(sum(surprise_pcts) / len(surprise_pcts), 2) if surprise_pcts else None

        earnings_track_record = {
            "last_8_quarters": track,
            "beat_rate_eps": beat_rate,
            "avg_eps_surprise_pct": avg_surprise,
        }

        result = {
            "symbol": symbol,
            "forward_estimates": forward_estimates,
            "recent_analyst_actions": analyst_actions,
            "earnings_track_record": earnings_track_record,
        }

        _warnings = []
        if not estimates_list:
            _warnings.append("forward estimates unavailable")
        if not grades_list:
            _warnings.append("analyst grades unavailable")
        if not earnings_list:
            _warnings.append("earnings history unavailable")
        if _warnings:
            result["_warnings"] = _warnings

        return result
