"""Commitment of Traders (COT) report tools — direct FMP /stable calls."""

from __future__ import annotations

import os
import statistics
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from fastmcp import FastMCP

FMP_BASE = "https://financialmodelingprep.com/stable"


def register(mcp: FastMCP) -> None:
    api_key = os.environ.get("FMP_API_KEY", "")

    async def _fmp_get(path: str, params: dict | None = None) -> list | dict:
        p = {"apikey": api_key, **(params or {})}
        async with httpx.AsyncClient(timeout=15) as http:
            resp = await http.get(f"{FMP_BASE}/{path}", params=p)
            resp.raise_for_status()
            return resp.json()

    def _percentile_rank(values: list[float], current: float) -> float:
        """Percent of values that are <= current."""
        if not values:
            return 50.0
        count_below = sum(1 for v in values if v <= current)
        return round(count_below / len(values) * 100, 1)

    def _zscore(values: list[float], current: float) -> float | None:
        if len(values) < 5:
            return None
        mu = statistics.mean(values)
        sd = statistics.stdev(values)
        if sd == 0:
            return 0.0
        return round((current - mu) / sd, 2)

    def _compute_analytics(reports: list[dict]) -> dict:
        """Compute positioning analytics across the full history."""
        if len(reports) < 4:
            return {}

        noncomm_nets = [r["noncomm_net"] for r in reports]
        comm_nets = [r["comm_net"] for r in reports]
        oi_series = [r["open_interest"] for r in reports if r["open_interest"]]
        noncomm_long_pcts = [r["pct_oi_noncomm_long"] for r in reports if r["pct_oi_noncomm_long"] is not None]

        latest = reports[0]
        current_noncomm_net = latest["noncomm_net"]
        current_comm_net = latest["comm_net"]

        # Long/short ratio for non-commercials
        nc_long = latest.get("noncomm_long") or 0
        nc_short = latest.get("noncomm_short") or 0
        noncomm_ls_ratio = round(nc_long / nc_short, 2) if nc_short > 0 else None

        # 4-week and 12-week net change
        noncomm_net_4w = current_noncomm_net - noncomm_nets[min(3, len(noncomm_nets) - 1)] if len(noncomm_nets) > 1 else 0
        noncomm_net_12w = current_noncomm_net - noncomm_nets[min(11, len(noncomm_nets) - 1)] if len(noncomm_nets) > 1 else 0

        # Streak: how many weeks in the same direction
        streak = 0
        if len(noncomm_nets) >= 2:
            direction = 1 if noncomm_nets[0] > noncomm_nets[1] else -1
            for i in range(len(noncomm_nets) - 1):
                if direction == 1 and noncomm_nets[i] > noncomm_nets[i + 1]:
                    streak += 1
                elif direction == -1 and noncomm_nets[i] < noncomm_nets[i + 1]:
                    streak += 1
                else:
                    break
            streak *= direction

        # Crowding signal: extreme positioning
        pct_rank = _percentile_rank(noncomm_nets, current_noncomm_net)
        if pct_rank >= 90:
            crowding = "EXTREME_LONG"
        elif pct_rank >= 75:
            crowding = "ELEVATED_LONG"
        elif pct_rank <= 10:
            crowding = "EXTREME_SHORT"
        elif pct_rank <= 25:
            crowding = "ELEVATED_SHORT"
        else:
            crowding = "NEUTRAL"

        return {
            "noncomm_net_zscore": _zscore(noncomm_nets, current_noncomm_net),
            "noncomm_net_percentile": pct_rank,
            "comm_net_zscore": _zscore(comm_nets, current_comm_net),
            "comm_net_percentile": _percentile_rank(comm_nets, current_comm_net),
            "noncomm_ls_ratio": noncomm_ls_ratio,
            "noncomm_net_4w_change": noncomm_net_4w,
            "noncomm_net_12w_change": noncomm_net_12w,
            "noncomm_net_streak_weeks": streak,
            "crowding_signal": crowding,
            "oi_current": oi_series[0] if oi_series else None,
            "oi_percentile": _percentile_rank(oi_series, oi_series[0]) if oi_series else None,
            "history_weeks": len(reports),
        }

    @mcp.tool(
        annotations={
            "title": "COT Report",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def cot_report(
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        last_n: int | None = None,
    ) -> dict:
        """Get Commitment of Traders report with positioning analytics.

        Returns weekly positioning data plus computed analytics:
        z-scores, percentile ranks, long/short ratios, momentum (4w/12w
        net change), streak length, and a crowding signal (EXTREME_LONG,
        ELEVATED_LONG, NEUTRAL, ELEVATED_SHORT, EXTREME_SHORT).

        Analytics are computed over the full history returned, so request
        more history for more meaningful stats. Defaults to ~2 years.

        Common symbols: GC (gold), SI (silver), CL (crude oil), NG (nat gas),
        HG (copper), ZW (wheat), ZC (corn), ZS (soybeans), ES (S&P 500 futures),
        NQ (Nasdaq futures), 6E (Euro FX), 6J (Japanese Yen).

        Args:
            symbol: Futures symbol (e.g. 'GC' for gold, 'CL' for crude oil).
            from_date: Start date (YYYY-MM-DD). Defaults to ~2 years ago.
            to_date: End date (YYYY-MM-DD). Defaults to latest available.
            last_n: Only return the N most recent reports (analytics still
                computed over full history). Useful to keep output compact.
        """
        if not from_date:
            from_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")

        params: dict = {"symbol": symbol.upper()}
        params["from"] = from_date
        params["to"] = to_date

        try:
            data = await _fmp_get("commitment-of-traders-report", params)
        except httpx.HTTPError as e:
            return {"error": f"COT request failed: {e}"}

        if not isinstance(data, list) or not data:
            return {"error": f"No COT data found for '{symbol}'"}

        reports = []
        for row in data:
            reports.append({
                "date": row.get("date"),
                "name": row.get("name"),
                "sector": row.get("sector"),
                "open_interest": row.get("openInterestAll"),
                "noncomm_long": row.get("noncommPositionsLongAll"),
                "noncomm_short": row.get("noncommPositionsShortAll"),
                "noncomm_spread": row.get("noncommPositionsSpreadAll"),
                "comm_long": row.get("commPositionsLongAll"),
                "comm_short": row.get("commPositionsShortAll"),
                "noncomm_net": (row.get("noncommPositionsLongAll") or 0)
                - (row.get("noncommPositionsShortAll") or 0),
                "comm_net": (row.get("commPositionsLongAll") or 0)
                - (row.get("commPositionsShortAll") or 0),
                "change_open_interest": row.get("changeInOpenInterestAll"),
                "change_noncomm_long": row.get("changeInNoncommLongAll"),
                "change_noncomm_short": row.get("changeInNoncommShortAll"),
                "change_comm_long": row.get("changeInCommLongAll"),
                "change_comm_short": row.get("changeInCommShortAll"),
                "pct_oi_noncomm_long": row.get("pctOfOiNoncommLongAll"),
                "pct_oi_noncomm_short": row.get("pctOfOiNoncommShortAll"),
                "pct_oi_comm_long": row.get("pctOfOiCommLongAll"),
                "pct_oi_comm_short": row.get("pctOfOiCommShortAll"),
            })

        analytics = _compute_analytics(reports)

        # Optionally trim output but keep full history for analytics
        output_reports = reports[:last_n] if last_n else reports

        return {
            "symbol": symbol.upper(),
            "report_count": len(reports),
            "showing": len(output_reports),
            "analytics": analytics,
            "reports": output_reports,
        }

    @mcp.tool(
        annotations={
            "title": "COT Symbols",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def cot_symbols(
        sector: str | None = None,
    ) -> dict:
        """List available COT report symbols.

        Returns all symbols that have COT data, grouped by sector.
        Use this to discover valid symbols for cot_report.

        Args:
            sector: Optional sector filter (e.g. 'METALS', 'ENERGIES',
                'GRAINS', 'FINANCIALS', 'CURRENCIES', 'INDICES',
                'SOFTS', 'MEATS'). Case-insensitive.
        """
        try:
            data = await _fmp_get("commitment-of-traders-report")
        except httpx.HTTPError as e:
            return {"error": f"COT symbols request failed: {e}"}

        if not isinstance(data, list) or not data:
            return {"error": "No COT symbols available"}

        # Group by sector
        by_sector: dict[str, list[dict]] = {}
        for row in data:
            s = row.get("sector", "OTHER")
            if sector and s.upper() != sector.upper():
                continue
            by_sector.setdefault(s, []).append({
                "symbol": row.get("shortName") or row.get("symbol"),
                "name": row.get("name"),
            })

        return {
            "sector_filter": sector,
            "sectors": {k: sorted(v, key=lambda x: x["name"] or "") for k, v in sorted(by_sector.items())},
            "total_symbols": sum(len(v) for v in by_sector.values()),
        }
