"""Treasury auction data tools via US Treasury Fiscal Data API.

Provides auction results, demand metrics, and grading for Treasury notes,
bonds, bills, TIPS, and FRNs. Data source: api.fiscaldata.treasury.gov
(free, no API key required).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from cass_market_sdk.clients.treasury import TreasuryClient


# ---------------------------------------------------------------------------
# Grading thresholds (Notes & Bonds only)
# ---------------------------------------------------------------------------

# Tail (bps): high_yield - WI yield proxy. Lower is better (negative = stop-through).
# With FRED_API_KEY: uses FRED CMT yields (DGS2, DGS5, DGS10, etc.) as WI proxy.
# Without: falls back to avg_med_yield (within-auction median, less precise).
TAIL_THRESHOLDS = {"A": -1.0, "B": 0.5, "C": 2.0, "D": 4.0}  # <= A, <= B, etc.; > D = F

# Bid-to-cover ratio: higher is better
BTC_THRESHOLDS = {"A": 2.8, "B": 2.5, "C": 2.2, "D": 2.0}  # >= A, >= B, etc.; < D = F

# Dealer takedown %: lower is better (dealers = backstop, not real demand)
DEALER_THRESHOLDS = {"A": 8.0, "B": 13.0, "C": 20.0, "D": 30.0}  # <= A, <= B, etc.; > D = F

# Indirect bidders %: higher is better (proxy for foreign/institutional demand)
INDIRECT_THRESHOLDS = {"A": 75.0, "B": 68.0, "C": 60.0, "D": 55.0}  # >= A, etc.; < D = F

# Weights for composite grade
WEIGHTS = {"tail_bps": 0.25, "bid_to_cover": 0.25, "dealer_pct": 0.30, "indirect_pct": 0.20}

GRADE_VALUES = {"A": 4, "B": 3, "C": 2, "D": 1, "F": 0}

GRADEABLE_TYPES = {"Note", "Bond", "TIPS", "FRN"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_float(value: Any) -> float | None:
    """Parse a string value to float, handling 'null' and empty strings."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() == "null":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _grade_lower_is_better(value: float | None, thresholds: dict[str, float]) -> str:
    """Grade a metric where lower values are better (exclusive upper bound)."""
    if value is None:
        return "N/A"
    if value < thresholds["A"]:
        return "A"
    if value < thresholds["B"]:
        return "B"
    if value < thresholds["C"]:
        return "C"
    if value < thresholds["D"]:
        return "D"
    return "F"


def _grade_higher_is_better(value: float | None, thresholds: dict[str, float]) -> str:
    """Grade a metric where higher values are better (exclusive lower bound)."""
    if value is None:
        return "N/A"
    if value >= thresholds["A"]:
        return "A"
    if value >= thresholds["B"]:
        return "B"
    if value >= thresholds["C"]:
        return "C"
    if value >= thresholds["D"]:
        return "D"
    return "F"


def _compute_metrics(record: dict, wi_yield: float | None = None) -> dict:
    """Compute demand metrics from a raw auction record.

    Args:
        record: Raw auction dict from Fiscal Data API
        wi_yield: FRED CMT yield to use as WI proxy (if available)
    """
    high_yield = _safe_float(record.get("high_yield"))
    avg_med = _safe_float(record.get("avg_med_yield"))
    btc = _safe_float(record.get("bid_to_cover_ratio"))
    comp_accepted = _safe_float(record.get("comp_accepted"))
    primary_dealer = _safe_float(record.get("primary_dealer_accepted"))
    indirect = _safe_float(record.get("indirect_bidder_accepted"))
    direct = _safe_float(record.get("direct_bidder_accepted"))

    # WI proxy priority: FRED CMT yield > avg_med_yield fallback
    proxy = wi_yield
    wi_source = "fred_cmt"

    # TIPS guard: if FRED CMT (nominal) diverges >150bps from high_yield (real),
    # discard it — we're comparing apples to oranges
    if proxy is not None and high_yield is not None and abs(high_yield - proxy) > 1.5:
        proxy = None

    if proxy is None:
        proxy = avg_med
        wi_source = "avg_med_yield"

    # Tail (bps) = (high_yield - WI proxy) * 100
    tail_bps: float | None = None
    if high_yield is not None and proxy is not None and proxy > 0:
        tail_bps = round((high_yield - proxy) * 100, 1)

    # Bidder percentages (vs competitive accepted)
    dealer_pct: float | None = None
    indirect_pct: float | None = None
    direct_pct: float | None = None
    if comp_accepted and comp_accepted > 0:
        if primary_dealer is not None:
            dealer_pct = round(primary_dealer / comp_accepted * 100, 1)
        if indirect is not None:
            indirect_pct = round(indirect / comp_accepted * 100, 1)
        if direct is not None:
            direct_pct = round(direct / comp_accepted * 100, 1)

    return {
        "tail_bps": tail_bps,
        "wi_source": wi_source if tail_bps is not None else None,
        "bid_to_cover": btc,
        "dealer_pct": dealer_pct,
        "indirect_pct": indirect_pct,
        "direct_pct": direct_pct,
    }


def _grade_auction(metrics: dict) -> dict:
    """Grade an auction based on its demand metrics.

    Returns per-metric grades, weighted GPA, and composite letter grade.
    """
    grades: dict[str, str] = {}
    grades["tail"] = _grade_lower_is_better(metrics["tail_bps"], TAIL_THRESHOLDS)
    grades["bid_to_cover"] = _grade_higher_is_better(metrics["bid_to_cover"], BTC_THRESHOLDS)
    grades["dealer_pct"] = _grade_lower_is_better(metrics["dealer_pct"], DEALER_THRESHOLDS)
    grades["indirect_pct"] = _grade_higher_is_better(metrics["indirect_pct"], INDIRECT_THRESHOLDS)

    # Weighted GPA
    total_weight = 0.0
    weighted_sum = 0.0
    weight_map = {
        "tail": WEIGHTS["tail_bps"],
        "bid_to_cover": WEIGHTS["bid_to_cover"],
        "dealer_pct": WEIGHTS["dealer_pct"],
        "indirect_pct": WEIGHTS["indirect_pct"],
    }
    for metric, grade in grades.items():
        if grade in GRADE_VALUES:
            w = weight_map[metric]
            weighted_sum += GRADE_VALUES[grade] * w
            total_weight += w

    gpa = weighted_sum / total_weight if total_weight > 0 else 0.0

    if gpa >= 3.5:
        composite = "A"
    elif gpa >= 2.5:
        composite = "B"
    elif gpa >= 1.5:
        composite = "C"
    elif gpa >= 0.5:
        composite = "D"
    else:
        composite = "F"

    return {
        "composite_grade": composite,
        "gpa": round(gpa, 2),
        "metric_grades": grades,
    }


def _format_auction(record: dict, wi_yield: float | None = None) -> dict:
    """Transform a raw API record into a structured auction result."""
    security_type = record.get("security_type", "")
    is_bill = security_type == "Bill"

    metrics = _compute_metrics(record, wi_yield=wi_yield)

    result: dict = {
        "cusip": record.get("cusip"),
        "security_type": security_type,
        "security_term": record.get("security_term"),
        "auction_date": record.get("auction_date"),
        "issue_date": record.get("issue_date"),
    }

    # Yield field depends on security type
    if is_bill:
        result["high_discnt_rate"] = _safe_float(record.get("high_discnt_rate"))
        result["high_investment_rate"] = _safe_float(record.get("high_investment_rate"))
    else:
        result["high_yield"] = _safe_float(record.get("high_yield"))
        result["avg_med_yield"] = _safe_float(record.get("avg_med_yield"))

    result["offering_amt"] = _safe_float(record.get("offering_amt"))
    result["bid_to_cover"] = metrics["bid_to_cover"]

    # Bidder composition
    result["dealer_pct"] = metrics["dealer_pct"]
    result["indirect_pct"] = metrics["indirect_pct"]
    result["direct_pct"] = metrics["direct_pct"]

    # SOMA (Fed) participation
    soma = _safe_float(record.get("soma_accepted"))
    offering = _safe_float(record.get("offering_amt"))
    if soma is not None:
        result["soma_accepted"] = soma
        if offering and offering > 0:
            result["soma_pct"] = round(soma / offering * 100, 1)

    # Tail + grading (notes/bonds only)
    if security_type in GRADEABLE_TYPES:
        result["tail_bps"] = metrics["tail_bps"]
        if metrics.get("wi_source"):
            result["wi_source"] = metrics["wi_source"]
        result["grade"] = _grade_auction(metrics)
    elif is_bill:
        cmb = record.get("cash_management_bill_cmb", "")
        if cmb and str(cmb).strip().lower() == "yes":
            result["is_cmb"] = True

    return result


def _trend_direction(recent: list[float], prior: list[float], lower_is_better: bool) -> str:
    """Compare recent vs prior averages and return trend label."""
    if not recent or not prior:
        return "insufficient_data"
    r_avg = sum(recent) / len(recent)
    p_avg = sum(prior) / len(prior)
    threshold = 0.05 * abs(p_avg) if p_avg != 0 else 0.1
    diff = r_avg - p_avg
    if abs(diff) < threshold:
        return "stable"
    if lower_is_better:
        return "improving" if diff < 0 else "deteriorating"
    return "improving" if diff > 0 else "deteriorating"


def _compute_trends(auctions: list[dict], recent_n: int = 3) -> dict:
    """Compute trend direction from a list of graded, formatted auctions.

    Compares the most recent `recent_n` auctions against the rest.
    """
    graded = [a for a in auctions if "grade" in a]
    if len(graded) < recent_n + 1:
        return {"overall": "insufficient_data"}

    recent = graded[:recent_n]
    prior = graded[recent_n:]

    def _vals(items: list[dict], key: str) -> list[float]:
        return [a[key] for a in items if a.get(key) is not None]

    trends = {
        "tail_bps": _trend_direction(_vals(recent, "tail_bps"), _vals(prior, "tail_bps"), lower_is_better=True),
        "bid_to_cover": _trend_direction(_vals(recent, "bid_to_cover"), _vals(prior, "bid_to_cover"), lower_is_better=False),
        "dealer_pct": _trend_direction(_vals(recent, "dealer_pct"), _vals(prior, "dealer_pct"), lower_is_better=True),
        "indirect_pct": _trend_direction(_vals(recent, "indirect_pct"), _vals(prior, "indirect_pct"), lower_is_better=False),
    }

    improving = sum(1 for v in trends.values() if v == "improving")
    deteriorating = sum(1 for v in trends.values() if v == "deteriorating")
    if improving > deteriorating:
        trends["overall"] = "improving"
    elif deteriorating > improving:
        trends["overall"] = "deteriorating"
    else:
        trends["overall"] = "stable"

    return trends


# ---------------------------------------------------------------------------
# FRED CMT yield batch fetcher
# ---------------------------------------------------------------------------


async def _fetch_wi_yields(
    treasury_client: "TreasuryClient",
    records: list[dict],
) -> dict[str, float | None]:
    """Batch-fetch FRED CMT yields for graded auction records.

    Returns a dict mapping CUSIP → CMT yield (or None if unavailable).
    Only fetches for gradeable security types. Deduplicates by (term, date)
    to minimize API calls.
    """
    if not treasury_client.fred_api_key:
        return {}

    # Collect unique (term, date) pairs for gradeable auctions
    needs_fetch: dict[tuple[str, str], list[str]] = {}  # (term, date) → [cusips]
    for r in records:
        sec_type = r.get("security_type", "")
        if sec_type not in GRADEABLE_TYPES:
            continue
        term = r.get("security_term", "")
        auction_date = r.get("auction_date", "")
        cusip = r.get("cusip", "")
        if term and auction_date and cusip:
            key = (term, auction_date)
            needs_fetch.setdefault(key, []).append(cusip)

    if not needs_fetch:
        return {}

    # Fetch in parallel (deduplicated by term+date)
    async def _fetch_one(term: str, auction_date: str) -> tuple[str, str, float | None]:
        yield_val = await treasury_client.fetch_cmt_yield(term, auction_date)
        return term, auction_date, yield_val

    tasks = [_fetch_one(term, dt) for (term, dt) in needs_fetch]
    results = await asyncio.gather(*tasks)

    # Map back to CUSIPs
    yield_map: dict[tuple[str, str], float | None] = {}
    for term, dt, val in results:
        yield_map[(term, dt)] = val

    cusip_yields: dict[str, float | None] = {}
    for (term, dt), cusips in needs_fetch.items():
        val = yield_map.get((term, dt))
        for cusip in cusips:
            cusip_yields[cusip] = val

    return cusip_yields


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def register(mcp: "FastMCP", treasury_client: "TreasuryClient") -> None:

    @mcp.tool(
        annotations={
            "title": "Treasury Auctions",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def treasury_auctions(
        security_type: str | None = None,
        security_term: str | None = None,
        days_back: int = 30,
        limit: int = 20,
    ) -> dict:
        """Get recent US Treasury auction results with demand metrics.

        Returns auction data including yield, bid-to-cover ratio, bidder
        composition (dealer/indirect/direct percentages), tail, and SOMA
        participation. Notes and bonds include a demand grade (A-F).

        Bills show discount rate instead of yield and are not graded.

        Grade methodology (notes/bonds):
        - Tail (25%): high_yield minus WI proxy in bps. Uses FRED CMT yields when
          FRED_API_KEY is set, falls back to avg_med_yield. Negative = stop-through.
        - Bid-to-Cover (25%): total_tendered / total_accepted.
        - Dealer Takedown (30%): dealer % of competitive. Lower = stronger real demand.
        - Indirect Bidders (20%): proxy for foreign/institutional demand.

        Args:
            security_type: Filter by type: "Note", "Bond", "Bill", "TIPS", "FRN"
            security_term: Filter by term: e.g. "10-Year", "2-Year", "4-Week"
            days_back: Lookback period in days (default 30, max 365)
            limit: Max results to return (default 20, max 100)
        """
        days_back = min(max(days_back, 1), 365)
        limit = min(max(limit, 1), 100)

        raw = await treasury_client.fetch_auctions(
            days_back=days_back,
            security_type=security_type,
            security_term=security_term,
        )

        # Filter out upcoming auctions that haven't settled (no results yet)
        settled = [r for r in raw if _safe_float(r.get("bid_to_cover_ratio")) is not None
                   or _safe_float(r.get("high_yield")) is not None
                   or _safe_float(r.get("high_discnt_rate")) is not None]

        to_format = settled[:limit]

        # Batch-fetch FRED CMT yields for graded auctions
        wi_yields = await _fetch_wi_yields(treasury_client, to_format)

        auctions = [_format_auction(r, wi_yield=wi_yields.get(r.get("cusip")))
                     for r in to_format]
        notes_bonds = [a for a in auctions if a.get("grade")]
        bills = [a for a in auctions if a.get("security_type") == "Bill"]

        _warnings: list[str] = []

        result: dict = {
            "count": len(auctions),
            "period": f"last {days_back} days",
            "auctions": auctions,
        }

        if notes_bonds:
            grade_dist = {}
            for a in notes_bonds:
                g = a["grade"]["composite_grade"]
                grade_dist[g] = grade_dist.get(g, 0) + 1
            result["graded_summary"] = {
                "count": len(notes_bonds),
                "grade_distribution": grade_dist,
            }
            wi_src = "fred_cmt" if treasury_client.fred_api_key else "avg_med_yield"
            result["wi_source"] = wi_src
            if wi_src == "avg_med_yield":
                _warnings.append(
                    "FRED_API_KEY not configured; tail uses avg_med_yield (less precise). "
                    "Set FRED_API_KEY for FRED CMT yield WI proxy."
                )
        else:
            if auctions:
                _warnings.append("No graded auctions (notes/bonds) in results; only bills returned.")

        if bills:
            result["bill_count"] = len(bills)

        if _warnings:
            result["_warnings"] = _warnings

        return result

    @mcp.tool(
        annotations={
            "title": "Auction Demand Analysis",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def auction_analysis(
        security_term: str | None = None,
        days_back: int = 90,
    ) -> dict:
        """Analyze Treasury auction demand health with grading and trend detection.

        Fetches auction history, grades each note/bond auction, computes trend
        direction (improving/deteriorating/stable), and summarizes demand signals.
        Use for assessing Treasury market reception and investor appetite.

        For a specific maturity (e.g. "10-Year"), shows that maturity's auction
        history with trends. Without a filter, shows cross-maturity demand health.

        Args:
            security_term: Focus on specific maturity (e.g. "10-Year", "2-Year", "30-Year")
            days_back: Lookback period in days (default 90, max 365)
        """
        days_back = min(max(days_back, 1), 365)

        raw = await treasury_client.fetch_auctions(
            days_back=days_back,
            security_term=security_term,
        )

        # Filter out upcoming auctions that haven't settled yet
        settled = [r for r in raw if _safe_float(r.get("bid_to_cover_ratio")) is not None
                   or _safe_float(r.get("high_yield")) is not None
                   or _safe_float(r.get("high_discnt_rate")) is not None]

        # Batch-fetch FRED CMT yields for graded auctions
        wi_yields = await _fetch_wi_yields(treasury_client, settled)

        all_formatted = [_format_auction(r, wi_yield=wi_yields.get(r.get("cusip")))
                          for r in settled]
        notes_bonds = [a for a in all_formatted if a.get("grade")]
        bills = [a for a in all_formatted if a.get("security_type") == "Bill"]

        _warnings: list[str] = []

        # --- Graded auctions summary ---
        graded_summary: dict = {}
        if notes_bonds:
            grade_dist: dict[str, int] = {}
            gpas: list[float] = []
            for a in notes_bonds:
                g = a["grade"]["composite_grade"]
                grade_dist[g] = grade_dist.get(g, 0) + 1
                gpas.append(a["grade"]["gpa"])

            avg_gpa = round(sum(gpas) / len(gpas), 2)
            graded_summary = {
                "count": len(notes_bonds),
                "avg_gpa": avg_gpa,
                "grade_distribution": grade_dist,
            }

            # Per-maturity breakdown
            by_term: dict[str, list[dict]] = {}
            for a in notes_bonds:
                term = a.get("security_term", "Unknown")
                by_term.setdefault(term, []).append(a)

            maturity_breakdown = []
            for term, term_auctions in sorted(by_term.items()):
                term_gpas = [a["grade"]["gpa"] for a in term_auctions]
                term_avg = round(sum(term_gpas) / len(term_gpas), 2)
                latest = term_auctions[0]
                trends = _compute_trends(term_auctions)

                maturity_breakdown.append({
                    "term": term,
                    "auction_count": len(term_auctions),
                    "avg_gpa": term_avg,
                    "latest_grade": latest["grade"]["composite_grade"],
                    "latest_date": latest.get("auction_date"),
                    "trends": trends,
                })

            graded_summary["by_maturity"] = maturity_breakdown
        else:
            _warnings.append("No graded auctions (notes/bonds) found in period")

        if notes_bonds and not treasury_client.fred_api_key:
            _warnings.append(
                "FRED_API_KEY not configured; tail uses avg_med_yield (less precise). "
                "Set FRED_API_KEY for FRED CMT yield WI proxy."
            )

        # --- Bill summary ---
        bill_summary: dict = {}
        if bills:
            btcs = [b["bid_to_cover"] for b in bills if b.get("bid_to_cover")]
            avg_btc = round(sum(btcs) / len(btcs), 2) if btcs else None
            cmb_count = sum(1 for b in bills if b.get("is_cmb"))
            bill_summary = {
                "count": len(bills),
                "avg_bid_to_cover": avg_btc,
                "cmb_count": cmb_count,
            }

        # --- Overall demand signal ---
        signal = "neutral"
        if notes_bonds:
            avg_gpa = graded_summary.get("avg_gpa", 2.0)
            if avg_gpa >= 3.0:
                signal = "strong"
            elif avg_gpa >= 2.5:
                signal = "healthy"
            elif avg_gpa >= 1.5:
                signal = "soft"
            else:
                signal = "weak"

        result: dict = {
            "period": f"last {days_back} days",
            "total_auctions": len(all_formatted),
            "demand_signal": signal,
        }

        if graded_summary:
            result["notes_bonds"] = graded_summary
        if bill_summary:
            result["bills"] = bill_summary

        # Include recent auctions for context (last 10 graded)
        if notes_bonds:
            result["recent_graded"] = notes_bonds[:10]

        if _warnings:
            result["_warnings"] = _warnings

        return result
