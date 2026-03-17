"""SEC EDGAR tools — full-text search, NPORT-P holdings parsing, and filing section extraction."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import date, timedelta
from typing import TYPE_CHECKING

import httpx

import cassandra_fmp.tools.edgar_client as _edgar_init  # noqa: F401 — triggers set_identity()
from edgar import Company, find as edgar_find

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fmp_data import AsyncFMPDataClient

logger = logging.getLogger(__name__)

EDGAR_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "")
EDGAR_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

OPENAI_API_KEY = os.environ.get("OPENAI_KEY", "")
OPENAI_MODEL = "gpt-5.2"

# Structured output schema for LLM sub-section routing
_ROUTER_RESPONSE_SCHEMA = {
    "name": "section_router",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "relevant": {
                "type": "array",
                "description": "List of relevant sub-section identifiers (format: 'section_key/sub_section_header').",
                "items": {"type": "string"},
            },
            "reasoning": {
                "type": "string",
                "description": "One sentence summary.",
            },
        },
        "required": ["relevant", "reasoning"],
        "additionalProperties": False,
    },
}

# Minimum chars for a block to be considered content (not a page number / footer)
_MIN_CONTENT_CHARS = 50

# Regex for standalone page numbers (e.g. "\n\n16\n\n" or "\n\nF-12\n\n")
_PAGE_NUMBER_RE = re.compile(r"\n\n\s*(?:F-?)?\d{1,3}\s*\n\n")

# Max filings to parse holdings for (each involves SEC fetch + XML parse)
_MAX_PARSE_FILINGS = 5

# Section key mapping for 10-K filings (friendly name -> edgartools key)
_TENK_SECTIONS = {
    "business": "business",
    "risk_factors": "risk_factors",
    "mda": "mda",
    "financials": "financial_statements",
    "legal": "legal_proceedings",
    "properties": "properties",
    "cybersecurity": "cybersecurity",
    "executive_compensation": "executive_compensation",
}

# Section key mapping for 10-Q filings
_TENQ_SECTIONS = {
    "financials": "Part I, Item 1",
    "mda": "Part I, Item 2",
    "market_risk": "Part I, Item 3",
    "controls": "Part I, Item 4",
    "legal": "Part II, Item 1",
    "risk_factors": "Part II, Item 1A",
}


async def _edgar_search(
    query: str,
    forms: str | None,
    start_date: str | None,
    end_date: str | None,
    entity: str | None,
    limit: int,
) -> dict | None:
    """Execute EFTS search and return raw JSON."""
    params: dict[str, str | int] = {
        "q": f'"{query}"',
        "dateRange": "custom",
        "startdt": start_date or (date.today() - timedelta(days=730)).isoformat(),
        "enddt": end_date or date.today().isoformat(),
    }
    if forms:
        params["forms"] = forms
    if entity:
        params["entity"] = entity
    # EFTS caps at 100 per page
    params["from"] = 0
    params["size"] = min(limit, 100)

    try:
        async with httpx.AsyncClient(timeout=15) as http:
            resp = await http.get(
                EDGAR_EFTS_URL,
                params=params,
                headers={"User-Agent": EDGAR_USER_AGENT},
            )
            resp.raise_for_status()
            return resp.json()
    except (httpx.HTTPError, ValueError):
        return None


def _build_filing_url(cik: str, accession: str) -> str:
    """Build SEC EDGAR filing URL from CIK and accession number."""
    cik_clean = cik.lstrip("0") or "0"
    accession_clean = accession.replace("-", "")
    return f"{EDGAR_ARCHIVES_BASE}/{cik_clean}/{accession_clean}/{accession}-index.htm"


_ENTITY_PATTERN = re.compile(
    r"^(.+?)\s*(?:\(([A-Z0-9.]+)\))?\s*\(CIK\s+([\d]+)\)\s*$"
)


def _parse_entity(display_name: str) -> tuple[str, str, str | None]:
    """Parse EFTS entity display name into (name, cik, ticker)."""
    m = _ENTITY_PATTERN.match(display_name.strip())
    if m:
        return m.group(1).strip(), m.group(3).zfill(10), m.group(2)
    return display_name.strip(), "", None


def _parse_nport_holdings(accession: str) -> dict | None:
    """Parse NPORT-P filing holdings using edgartools (sync, runs in thread).

    Returns dict with fund_name, total_assets, holdings_count, and holdings list,
    or None on failure.
    """
    try:
        filing = edgar_find(accession)
        if filing is None:
            return None
        report = filing.obj()
        if report is None:
            return None

        # Get fund metadata
        fund_name = None
        total_assets = None
        net_assets = None
        try:
            fund_name = report.general_info.series_name or report.general_info.name
        except (AttributeError, TypeError):
            pass
        try:
            total_assets = float(report.fund_info.total_assets) if report.fund_info.total_assets else None
        except (AttributeError, TypeError, ValueError):
            pass
        try:
            net_assets = float(report.fund_info.net_assets) if report.fund_info.net_assets else None
        except (AttributeError, TypeError, ValueError):
            pass

        # Get securities (non-derivative holdings)
        df = report.securities_data()
        if df is None or df.empty:
            return {
                "fund_name": fund_name,
                "total_assets": total_assets,
                "net_assets": net_assets,
                "holdings_count": 0,
                "holdings": [],
            }

        # Convert DataFrame to compact list of dicts
        holdings = []
        for _, row in df.iterrows():
            holding: dict = {"name": row.get("name") or row.get("title")}
            if row.get("ticker"):
                holding["ticker"] = row["ticker"]
            if row.get("cusip"):
                holding["cusip"] = row["cusip"]
            val = row.get("value_usd")
            if val is not None:
                try:
                    holding["value_usd"] = float(val)
                except (TypeError, ValueError):
                    pass
            bal = row.get("balance")
            if bal is not None:
                try:
                    holding["shares"] = float(bal)
                except (TypeError, ValueError):
                    pass
            pct = row.get("pct_value")
            if pct is not None:
                try:
                    holding["pct"] = round(float(pct), 4)
                except (TypeError, ValueError):
                    pass
            cat = row.get("asset_category")
            if cat:
                holding["category"] = str(cat)
            country = row.get("investment_country")
            if country:
                holding["country"] = str(country)
            holdings.append(holding)

        # Sort by value descending
        holdings.sort(key=lambda h: h.get("value_usd") or 0, reverse=True)

        return {
            "fund_name": fund_name,
            "total_assets": total_assets,
            "net_assets": net_assets,
            "holdings_count": len(holdings),
            "holdings": holdings,
        }
    except Exception:
        logger.exception("Failed to parse NPORT-P filing %s", accession)
        return None


def _extract_filing_section(symbol: str, form: str, section_key: str, accession: str | None) -> str | None:
    """Extract a section from a filing using edgartools (sync, runs in thread).

    Returns plain text of the section, or None if not found.
    """
    from cassandra_fmp.tools.edgar_client import EDGAR_IDENTITY as _ident
    if not _ident:
        raise RuntimeError("EDGAR_IDENTITY or EDGAR_USER_AGENT env var required")

    if accession:
        filing = edgar_find(accession)
    else:
        company = Company(symbol)
        filings = company.get_filings(form=form)
        filing = filings.latest()

    if filing is None:
        return None

    typed_obj = filing.obj()
    if typed_obj is None:
        return None

    # Use __getitem__ which returns plain text
    return typed_obj[section_key]


def _clean_section_text(text: str) -> str:
    """Clean raw section text from edgartools.

    Removes standalone page numbers, excess whitespace, and other artifacts.
    """
    # Remove standalone page numbers (e.g. "\n\n16\n\n")
    text = _PAGE_NUMBER_RE.sub("\n\n", text)
    # Collapse runs of 3+ newlines into double newlines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _split_sub_sections(section_key: str, text: str) -> dict[str, str]:
    """Split a section into sub-sections by detecting header blocks.

    Headers are short paragraph blocks (< 100 chars) that look like titles —
    no trailing punctuation, no bullet markers, no leading whitespace indentation.
    Returns dict of "section_key/header" -> full sub-section text.
    If no sub-headers found, returns the whole section under "section_key/_full".
    """
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    if not blocks:
        return {f"{section_key}/_full": text}

    # Characters that indicate a block is a list item, not a header
    _BULLET_CHARS = {"•", "·", "−", "–", "—", "-", "*", "►", "▪", "○", "●"}

    # Detect header indices
    header_indices: list[tuple[int, str]] = []
    for i, block in enumerate(blocks):
        stripped = block.rstrip()
        is_short = len(stripped) < 100
        # Headers don't end with sentence-ending punctuation
        ends_with_punct = stripped[-1] in ".;:,)" if stripped else True
        # Headers aren't bullet/list items
        first_char = stripped[0] if stripped else ""
        is_bullet = first_char in _BULLET_CHARS or (
            len(stripped) > 1 and stripped[0].isdigit() and stripped[1] in ".)"
        )
        # Headers aren't page numbers / footers (very short + has digits)
        is_footer = len(stripped) < _MIN_CONTENT_CHARS and any(c.isdigit() for c in stripped)
        # Headers shouldn't contain newlines (multi-line blocks are paragraphs)
        is_multiline = "\n" in block.strip()
        if is_short and not ends_with_punct and not is_bullet and not is_footer and not is_multiline and len(stripped) > 5:
            header_indices.append((i, block))

    if len(header_indices) <= 1:
        # No meaningful sub-structure
        return {f"{section_key}/_full": text}

    # Build sub-sections from header to next header
    sub_sections: dict[str, str] = {}
    for pos, (idx, header) in enumerate(header_indices):
        next_idx = header_indices[pos + 1][0] if pos + 1 < len(header_indices) else len(blocks)
        content_blocks = blocks[idx:next_idx]
        content = "\n\n".join(content_blocks)
        if len(content) >= _MIN_CONTENT_CHARS:
            # Clean header for use as key
            clean_header = header.strip().replace("\n", " ")
            sub_sections[f"{section_key}/{clean_header}"] = content

    return sub_sections or {f"{section_key}/_full": text}


async def _llm_route_sub_sections(
    sub_sections: dict[str, str],
    query: str,
) -> tuple[list[str], str]:
    """Use LLM to identify which sub-sections are relevant to a query.

    Sends all full text to the LLM and asks it to return the sub-section
    identifiers that contain relevant content. Returns (relevant_keys, reasoning).
    Falls back to all sub-sections if no API key or on error.
    """
    all_keys = list(sub_sections.keys())

    if not OPENAI_API_KEY:
        return all_keys, "no OPENAI_KEY set, returning all sub-sections"

    # Build the full context with all sub-sections labelled
    full_text = ""
    for key, text in sub_sections.items():
        full_text += f"\n\n=== {key} ===\n{text}"

    system_prompt = (
        "You are a SEC filing section router. Given a user query and a set of "
        "labelled sub-sections from a filing, return which sub-section identifiers "
        "contain content relevant to the query.\n\n"
        "IMPORTANT RULES:\n"
        "- Match SEMANTICALLY, not just by keyword. 'supply chain' matches sections "
        "about logistics, sourcing, inventory, vendors, manufacturing, distribution.\n"
        "- Consider synonyms and related concepts. 'tariffs' matches import duties, "
        "trade policy, customs, Section 301, anti-dumping. 'export restrictions' "
        "matches sanctions, ITAR, EAR, trade controls, embargoes.\n"
        "- When in doubt, INCLUDE the section. It is much worse to miss a relevant "
        "section than to include a borderline one.\n"
        "- You MUST return identifiers EXACTLY as they appear in the Identifiers list. "
        "Do not modify capitalization, spacing, or punctuation.\n"
        "- If no sections are relevant, return an empty list — but this should be rare "
        "for a well-formed query against a real filing."
    )

    user_prompt = (
        f"Query: \"{query}\"\n"
        f"Identifiers: {all_keys}\n\n"
        f"Return which identifiers contain content relevant to the query. "
        f"Copy identifiers exactly from the list above.\n"
        f"{full_text}"
    )

    try:
        async with httpx.AsyncClient(timeout=90) as http:
            resp = await http.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": _ROUTER_RESPONSE_SCHEMA,
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            relevant = parsed.get("relevant", [])
            reasoning = parsed.get("reasoning", "")
            # Validate — only return keys that actually exist
            valid = [k for k in relevant if k in sub_sections]
            dropped = [k for k in relevant if k not in sub_sections]
            if dropped:
                logger.warning(
                    "LLM returned %d keys that don't match any sub-section: %s",
                    len(dropped), dropped[:5],
                )
            if not valid and relevant:
                # LLM tried but all keys failed validation — this is a bug
                reasoning = (
                    f"FALLBACK: LLM returned {len(relevant)} keys but none matched "
                    f"(mismatched: {dropped[:3]}). Returning all sections."
                )
            return valid or all_keys, reasoning
    except httpx.HTTPStatusError as exc:
        logger.exception("LLM routing HTTP error: %s", exc.response.status_code)
        return all_keys, f"LLM routing failed (HTTP {exc.response.status_code}), returning all sub-sections"
    except httpx.TimeoutException:
        logger.exception("LLM routing timed out after 90s")
        return all_keys, "LLM routing timed out, returning all sub-sections"
    except Exception:
        logger.exception("LLM sub-section routing failed")
        return all_keys, "LLM routing failed, returning all sub-sections"


def register(mcp: FastMCP, client: AsyncFMPDataClient) -> None:
    del client  # unused — SEC EDGAR is a free public API

    @mcp.tool(
        annotations={
            "title": "SEC EDGAR Filing Search (NPORT-P fund holdings reverse lookup)",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def sec_filings_search(
        query: str,
        forms: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        entity: str | None = None,
        traded_only: bool = False,
        parse_holdings: bool = True,
        limit: int = 50,
    ) -> dict:
        """Search SEC EDGAR filings by full-text content. Best for NPORT-P reverse lookups.

        PRIMARY USE CASE — NPORT-P reverse lookup (forms="NPORT-P"):
        Find which publicly-traded funds hold a private or public company.
        This is the only way to answer "who holds X?" for private companies
        like SpaceX, Anthropic, xAI, Stripe, etc. NPORT-P filings contain
        structured holdings data, so matches are high-signal (actual positions).
        When parse_holdings=True (default), automatically parses the actual
        portfolio holdings from each NPORT-P filing — returns fund names,
        position sizes, values, and portfolio weights instead of just filing links.
        Examples:
        - "which funds hold SpaceX?" → query="SpaceX", forms="NPORT-P"
        - "who has Anthropic exposure?" → query="Anthropic", forms="NPORT-P"
        - Publicly-traded funds only: add traded_only=True to filter to
          closed-end funds with tickers (DXYZ, ECAT, BSTZ, BST, etc.) that
          investors can actually buy on the stock market. Without this flag,
          results include mutual fund trusts (Fidelity, Vanguard) that are
          not directly tradeable as stocks.

        SECONDARY USE CASES:
        - Cross-filing research: query="SpaceX" (no form filter) searches all
          10-K, 8-K, S-1, Form D, etc. These are noisier — a mention could be
          a holding, customer, competitor, or passing reference.
        - Form D private offerings: query="AI", forms="D"

        Args:
            query: Search term (e.g. "SpaceX", "Anthropic"). Exact phrase match.
            forms: Comma-separated form types. Use "NPORT-P" for fund holdings
                reverse lookup (recommended default). Other options: "10-K",
                "8-K", "D", "S-1", or omit for all forms.
            start_date: Filter filings from this date (YYYY-MM-DD). Default: ~2 years back.
            end_date: Filter filings to this date (YYYY-MM-DD). Default: today.
            entity: Filter by filer entity name (e.g. "Fidelity").
            traded_only: If True, only return entities/filings with a stock ticker
                (publicly-traded closed-end funds like DXYZ, BSTZ). Filters out
                mutual fund trusts without tickers. Default: False.
            parse_holdings: If True and forms includes "NPORT-P", automatically
                parse the actual holdings from each filing using edgartools.
                Returns structured position data (name, value, shares, weight).
                Set to False for metadata-only results. Default: True.
            limit: Max results to return (1-100, default 50).
        """
        if not EDGAR_USER_AGENT:
            return {"error": "EDGAR_USER_AGENT env var is required (e.g. 'YourApp/1.0 (you@example.com)')"}

        query = query.strip()
        if not query:
            return {"error": "query is required"}

        limit = max(1, min(limit, 100))

        raw = await _edgar_search(query, forms, start_date, end_date, entity, limit)
        if raw is None:
            return {"error": "SEC EDGAR EFTS request failed"}

        # Total hits
        total_hits = 0
        hits_data = raw.get("hits", {})
        if isinstance(hits_data, dict):
            total_hits = hits_data.get("total", {}).get("value", 0)
            hit_list = hits_data.get("hits", [])
        else:
            hit_list = []

        # Parse aggregations for entity and form breakdowns
        aggregations = raw.get("aggregations", {})

        # Form breakdown
        form_breakdown: dict[str, int] = {}
        form_agg = aggregations.get("form_filter", {})
        for bucket in form_agg.get("buckets", []):
            form_breakdown[bucket.get("key", "unknown")] = bucket.get("doc_count", 0)

        # Entity breakdown
        entity_breakdown: list[dict] = []
        entity_agg = aggregations.get("entity_filter", {})
        for bucket in entity_agg.get("buckets", []):
            display = bucket.get("key", "")
            name, cik, ticker = _parse_entity(display)
            entry: dict = {
                "entity": name,
                "cik": cik,
                "count": bucket.get("doc_count", 0),
            }
            if ticker:
                entry["ticker"] = ticker
            entity_breakdown.append(entry)

        # Parse individual filings from hits
        filings: list[dict] = []
        for hit in hit_list:
            src = hit.get("_source", {})
            display_names = src.get("display_names") or []
            entity_display = display_names[0] if display_names else ""
            name, cik, ticker = _parse_entity(entity_display)

            # CIK may also come from the ciks array
            if not cik:
                ciks = src.get("ciks") or []
                cik = ciks[0] if ciks else ""

            accession = src.get("adsh", "")

            filing: dict = {
                "entity": name,
                "cik": cik,
                "form": src.get("form", ""),
                "filed": src.get("file_date", ""),
                "period": src.get("period_ending", ""),
                "accession": accession,
            }
            if ticker:
                filing["ticker"] = ticker
            if cik and accession:
                filing["url"] = _build_filing_url(cik, accession)
            filings.append(filing)

        # Filter to publicly-traded entities only (those with tickers)
        if traded_only:
            entity_breakdown = [e for e in entity_breakdown if e.get("ticker")]
            filings = [f for f in filings if f.get("ticker")]

        # Parse NPORT-P holdings if requested
        is_nport = forms and "NPORT" in forms.upper()
        if parse_holdings and is_nport:
            parse_count = min(len(filings), _MAX_PARSE_FILINGS)
            filings_to_parse = filings[:parse_count]

            # Parse in parallel using thread pool (edgartools is sync)
            parse_tasks = [
                asyncio.to_thread(_parse_nport_holdings, f["accession"])
                for f in filings_to_parse
                if f.get("accession")
            ]
            if parse_tasks:
                parsed_results = await asyncio.gather(*parse_tasks, return_exceptions=True)
                for i, parsed in enumerate(parsed_results):
                    if isinstance(parsed, dict) and i < len(filings_to_parse):
                        filings_to_parse[i]["fund_name"] = parsed.get("fund_name")
                        filings_to_parse[i]["total_assets"] = parsed.get("total_assets")
                        filings_to_parse[i]["net_assets"] = parsed.get("net_assets")
                        filings_to_parse[i]["holdings_count"] = parsed.get("holdings_count")
                        filings_to_parse[i]["holdings"] = parsed.get("holdings")

        result: dict = {
            "query": query,
            "total_hits": total_hits,
        }
        if forms:
            result["forms_filter"] = forms
        if entity:
            result["entity_filter"] = entity
        if traded_only:
            result["traded_only"] = True
        if form_breakdown:
            result["form_breakdown"] = form_breakdown
        if entity_breakdown:
            result["entity_breakdown"] = entity_breakdown
        result["showing"] = len(filings)
        result["filings"] = filings

        return result

    # Default narrative sections to fetch for each form type
    _DEFAULT_10K_SECTIONS = ["business", "risk_factors", "mda", "legal", "cybersecurity"]
    _DEFAULT_10Q_SECTIONS = ["mda", "risk_factors", "legal"]

    @mcp.tool(
        annotations={
            "title": "SEC Filing Section Extraction",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        }
    )
    async def filing_sections(
        symbol: str,
        queries: list[str],
        form: str = "10-K",
        sections: list[str] | None = None,
        accession: str | None = None,
        max_chars: int = 50000,
    ) -> str | dict:
        """Search SEC filing content by topic using LLM-powered section routing.

        Fetches the filing ONCE and runs LLM routing for each query in the
        batch. This is the only way to search filing content — always pass
        all your topics in a single call.

        The LLM router matches semantically, not by keyword — "tariffs" also
        matches "import duties", "trade policy", "customs", etc.

        Examples:
        - symbol="WMT", queries=["tariffs trade policy", "supply chain logistics", "export restrictions sanctions"]
        - symbol="AAPL", queries=["China geopolitical risks", "AI strategy and investments"]
        - symbol="NVDA", queries=["export controls China", "data center revenue"]
        - symbol="TSLA", queries=["margins pricing competition"]

        Args:
            symbol: Stock ticker (e.g. "AAPL").
            queries: List of search topics. The filing is fetched once and
                each query is routed independently against the same extracted
                sections. Each query can contain multiple related keywords
                (e.g. "tariffs import duties trade policy").
            form: Filing type — "10-K" (default) or "10-Q".
            sections: Override which sections to fetch. If omitted, fetches all
                major narrative sections. 10-K options: business, risk_factors,
                mda, financials, legal, properties, cybersecurity,
                executive_compensation. 10-Q: financials, mda, market_risk,
                controls, legal, risk_factors.
            accession: Specific filing accession number. If omitted, uses the
                latest filing of the given form type.
            max_chars: Max characters per query result (default 50000).
        """
        symbol = symbol.upper().strip()
        form = form.upper().strip()
        max_chars = max(1000, min(max_chars, 100000))

        if not queries:
            return {"error": "queries is required — provide a list of topics to search for"}
        queries = [q.strip() for q in queries if q and q.strip()]
        if not queries:
            return {"error": "queries must contain at least one non-empty topic"}

        # Validate form type
        if form not in ("10-K", "10-Q"):
            return {"error": f"Unsupported form type '{form}'. Use '10-K' or '10-Q'."}

        # Resolve which sections to fetch
        section_map = _TENK_SECTIONS if form == "10-K" else _TENQ_SECTIONS
        if sections is None:
            sections = _DEFAULT_10K_SECTIONS if form == "10-K" else _DEFAULT_10Q_SECTIONS
        else:
            invalid = [s for s in sections if s not in section_map]
            if invalid:
                return {
                    "error": f"Invalid section(s): {invalid}. Valid: {list(section_map.keys())}",
                }

        # Extract all sections in parallel (edgartools is sync, use thread pool)
        extract_tasks = [
            asyncio.to_thread(
                _extract_filing_section,
                symbol, form, section_map[s], accession,
            )
            for s in sections
        ]
        extracted = await asyncio.gather(*extract_tasks, return_exceptions=True)

        raw_sections: dict[str, str] = {}
        warnings: list[str] = []
        for i, result_or_err in enumerate(extracted):
            section_name = sections[i]
            if isinstance(result_or_err, Exception):
                warnings.append(f"{section_name}: extraction failed ({result_or_err})")
            elif result_or_err is None:
                warnings.append(f"{section_name}: not found in filing")
            else:
                raw_sections[section_name] = result_or_err

        if not raw_sections:
            return {
                "error": f"No sections could be extracted from {symbol} {form}",
                "_warnings": warnings,
            }

        # Clean and split all sections into sub-sections by header (once)
        all_sub_sections: dict[str, str] = {}
        for section_key, text in raw_sections.items():
            cleaned = _clean_section_text(text)
            all_sub_sections.update(_split_sub_sections(section_key, cleaned))

        # Route each query against the same sub-sections in parallel
        route_tasks = [
            _llm_route_sub_sections(all_sub_sections, q)
            for q in queries
        ]
        route_results = await asyncio.gather(*route_tasks, return_exceptions=True)

        # Build output — one section per query
        output_lines: list[str] = [
            f"{symbol} {form} — {len(queries)} queries, {len(all_sub_sections)} sub-sections",
        ]
        if warnings:
            output_lines.append(f"Warnings: {'; '.join(warnings)}")
        output_lines.append("")

        for idx, query in enumerate(queries):
            route_result = route_results[idx]
            if isinstance(route_result, Exception):
                output_lines.append(f"=== Query: {query} ===")
                output_lines.append(f"ERROR: routing failed ({route_result})")
                output_lines.append("")
                continue

            relevant_keys, reasoning = route_result

            output_lines.append(f"=== Query: {query} ===")
            output_lines.append(
                f"Routing: {len(relevant_keys)}/{len(all_sub_sections)} sub-sections selected"
            )
            if reasoning:
                output_lines.append(f"Router: {reasoning}")
            output_lines.append("")

            total_chars = 0
            for key in relevant_keys:
                if key not in all_sub_sections:
                    continue
                text = all_sub_sections[key]
                truncated = (
                    text[:max_chars - total_chars]
                    if total_chars + len(text) > max_chars
                    else text
                )
                if not truncated:
                    continue
                output_lines.append(truncated)
                output_lines.append("")
                total_chars += len(truncated)
                if total_chars >= max_chars:
                    output_lines.append(f"[truncated at {max_chars} chars]")
                    break

        return "\n".join(output_lines)
