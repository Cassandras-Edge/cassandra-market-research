"""Earnings call transcript tools."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from cassandra_fmp.tools._helpers import TTL_REALTIME, _as_list, _date_only, _safe_call, _to_date

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    @mcp.tool(
        annotations={
            "title": "Earnings Transcript",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def earnings_transcript(
        symbol: str,
        year: int | str | None = None,
        quarter: int | str | None = None,
        latest_expected: bool = False,
        max_chars: int | str = 100000,
        offset: int | str = 0,
    ) -> dict:
        symbol = symbol.upper().strip()

        def _to_int(v: int | str | None) -> int | None:
            if v is None:
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        year = _to_int(year)
        quarter = _to_int(quarter)
        max_chars = _to_int(max_chars) or 100000
        offset = _to_int(offset) or 0
        requested_year = year
        requested_quarter = quarter

        if quarter is not None and quarter not in (1, 2, 3, 4):
            return {"error": f"Invalid quarter '{quarter}'. Must be 1, 2, 3, or 4."}

        transcript_data = []
        if year is not None and quarter is not None:
            transcript_data = await _safe_call(
                client.transcripts.get_transcript,
                symbol=symbol,
                year=year,
                quarter=quarter,
                ttl=TTL_REALTIME,
                default=[],
            )
        else:
            dates_data = await _safe_call(
                client.transcripts.get_available_dates,
                symbol=symbol,
                ttl=TTL_REALTIME,
                default=[],
            )
            dates_list = _as_list(dates_data)
            if not dates_list:
                return {"error": f"No earnings transcripts available for '{symbol}'"}

            filtered_dates: list[tuple[int, int]] = []
            for d in dates_list:
                y_raw = d.get("fiscalYear") or d.get("year")
                q_raw = d.get("quarter")
                if q_raw is None:
                    q_raw = d.get("period")
                if y_raw is None or q_raw is None:
                    continue
                try:
                    y = int(y_raw)
                    q = int(q_raw)
                except (TypeError, ValueError):
                    continue
                if year is not None and y != year:
                    continue
                if quarter is not None and q != quarter:
                    continue
                filtered_dates.append((y, q))

            if not filtered_dates:
                filters = []
                if year is not None:
                    filters.append(f"year={year}")
                if quarter is not None:
                    filters.append(f"quarter={quarter}")
                if filters:
                    return {"error": f"No earnings transcripts available for '{symbol}' matching {', '.join(filters)}"}
                return {"error": f"No valid transcript dates found for '{symbol}'"}

            filtered_dates.sort(reverse=True)
            year, quarter = filtered_dates[0]
            transcript_data = await _safe_call(
                client.transcripts.get_transcript,
                symbol=symbol,
                year=year,
                quarter=quarter,
                ttl=TTL_REALTIME,
                default=[],
            )

        transcript_list = _as_list(transcript_data)
        if not transcript_list:
            return {"error": f"No transcript found for '{symbol}' Q{quarter} {year}"}

        full_content = []
        for segment in transcript_list:
            content = segment.get("content") or ""
            if content:
                full_content.append(content)
        transcript_text = "\n\n".join(full_content)
        if not transcript_text:
            return {"error": f"Transcript for '{symbol}' Q{quarter} {year} is empty"}

        total_chars = len(transcript_text)
        offset = max(0, min(offset, total_chars))
        max_chars = max(1, max_chars)

        start = offset
        if start > 0 and start < total_chars and transcript_text[start - 1] != "\n":
            nl = transcript_text.find("\n", start)
            start = nl + 1 if nl != -1 else start

        end = min(start + max_chars, total_chars)
        if end < total_chars:
            nl = transcript_text.rfind("\n", start, end)
            if nl != -1 and nl > start:
                end = nl + 1

        chunk = transcript_text[start:end]
        truncated = end < total_chars

        result = {
            "symbol": symbol,
            "year": year,
            "quarter": quarter,
            "date": _date_only(transcript_list[0].get("date")) if transcript_list else None,
            "content": chunk,
            "length_chars": len(chunk),
            "total_chars": total_chars,
            "offset": start,
            "truncated": truncated,
        }
        if truncated:
            result["next_offset"] = end
            result["_warnings"] = [
                f"Transcript truncated ({len(chunk)}/{total_chars} chars shown). "
                f"Call again with offset={end} to continue reading."
            ]

        if latest_expected and requested_year is None and requested_quarter is None:
            latest_completed_earnings_date = None
            earnings_data = await _safe_call(
                client.company.get_earnings,
                symbol=symbol,
                ttl=TTL_REALTIME,
                default=[],
            )
            earnings_list = _as_list(earnings_data)
            today_date = date.today()
            completed: list[tuple[date, dict]] = []
            for e in earnings_list:
                if e.get("epsActual") is None:
                    continue
                earnings_date = _to_date(e.get("date"))
                if earnings_date and earnings_date <= today_date:
                    completed.append((earnings_date, e))
            if completed:
                completed.sort(key=lambda x: x[0], reverse=True)
                latest_completed_earnings_date = _date_only(completed[0][1].get("date"))

            transcript_date = _to_date(result.get("date"))
            met = True
            latest_completed_date = _to_date(latest_completed_earnings_date)
            if latest_completed_date and transcript_date:
                met = latest_completed_date <= transcript_date

            result["latest_expected"] = True
            result["latest_expected_met"] = met
            if latest_completed_earnings_date:
                result["latest_completed_earnings_date"] = latest_completed_earnings_date
            if not met:
                warnings = result.get("_warnings", [])
                warnings.append(
                    "Latest completed earnings appears newer than available transcript; "
                    "the new transcript may not be posted yet."
                )
                result["_warnings"] = warnings

        return result
