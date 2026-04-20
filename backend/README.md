# FMP MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io/) server that provides financial data from [Financial Modeling Prep](https://financialmodelingprep.com/) for AI-assisted investment research.

Built with [FastMCP 2.0](https://github.com/jlowin/fastmcp) and Python.

## Tools

### Workflow Tools (start here)

High-level tools that orchestrate multiple API calls into single research-ready responses:

| Tool | Description |
|------|-------------|
| `stock_brief` | Quick comprehensive snapshot: profile, price action, valuation, analyst consensus, insider signals, headlines |
| `market_context` | Full market environment: rates, yield curve, sector rotation, breadth, movers, economic calendar |
| `earnings_setup` | Pre-earnings positioning: consensus estimates, beat/miss history, analyst momentum, price drift, insider signals |
| `earnings_preview` | Pre-earnings setup scorecard: composite signal, thesis alignment, and bull/bear triggers |
| `fair_value_estimate` | Multi-method valuation: DCF, earnings-based, peer multiples, analyst targets, blended estimate |
| `earnings_postmortem` | Post-earnings synthesis: beat/miss, trend comparison, analyst reaction, market response, guidance tone |

### Atomic Tools (deeper dives)

| Tool | Description |
|------|-------------|
| `company_overview` | Company profile, quote, key metrics, and analyst ratings |
| `financial_statements` | Income statement, balance sheet, cash flow (annual/quarterly) |
| `analyst_consensus` | Analyst grades, price targets, and forward estimates |
| `price_history` | Historical daily prices with technical context |
| `stock_search` | Search for stocks by name or ticker |
| `insider_activity` | Insider trading activity and transaction statistics |
| `institutional_ownership` | Top institutional holders and position changes |
| `stock_news` | Recent news and press releases |
| `treasury_rates` | Current Treasury yields and yield curve |
| `economic_calendar` | Upcoming economic events and releases |
| `market_overview` | Sector performance, gainers, losers, most active |
| `earnings_transcript` | Earnings call transcripts with pagination support |
| `revenue_segments` | Revenue breakdown by product and geography |
| `peer_comparison` | Peer group valuation and performance comparison |
| `dividends_info` | Dividend history, yield, growth, and payout analysis |
| `earnings_calendar` | Upcoming earnings dates with optional symbol filter |
| `etf_lookup` | ETF holdings or stock ETF exposure (dual-mode with auto-detect) |
| `estimate_revisions` | Analyst sentiment momentum: forward estimates, grade changes, beat rate |
| `fmp_coverage_gaps` | Docs parity introspection: endpoint families not yet implemented in this MCP server |

## Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- An [FMP API key](https://financialmodelingprep.com/developer/docs/)

### Install

```bash
uv sync
```

### Configure

Set your API key as an environment variable:

```bash
export FMP_API_KEY=your_api_key_here
```

Or create a `.env` file:

```
FMP_API_KEY=your_api_key_here
```

### Run

```bash
uv run fastmcp run server.py
```

### Claude Desktop / Claude Code

Add to your MCP config:

```json
{
  "mcpServers": {
    "fmp": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/fmp", "fastmcp", "run", "server.py"],
      "env": {
        "FMP_API_KEY": "your_api_key_here"
      }
    }
  }
}
```

## Testing

```bash
uv run pytest tests/ -v
```

All tools are tested with mocked API responses using [respx](https://github.com/lundberg/respx).

Live e2e tests (real API) can be run in pooled parallel mode:

```bash
uv run pytest tests/test_live.py -m live_full -n 4 -q
```

## Architecture

```
server.py          # FastMCP entry point, registers all tool modules
tools/_helpers.py  # Shared SDK helpers: safe calls, TTL cache, model dumping/normalization
tools/
  overview.py      # company_overview, stock_search
  financials.py    # financial_statements, revenue_segments
  valuation.py     # analyst_consensus, peer_comparison, estimate_revisions
  market.py        # price_history, dividends_info, earnings_calendar, etf_lookup
  ownership.py     # insider_activity, institutional_ownership
  news.py          # stock_news
  macro.py         # treasury_rates, economic_calendar, market_overview
  transcripts.py   # earnings_transcript (with pagination)
  workflows.py     # stock_brief, market_context, earnings_setup, earnings_preview, fair_value_estimate, earnings_postmortem
```

Key design decisions:
- **Module pattern**: Each tool file exports `register(mcp, client)` to keep tools organized
- **SDK-first FMP client**: Uses `AsyncFMPDataClient` (`fmp-data==2.2.0`) directly for typed endpoint coverage
- **Parallel fetches**: Workflow tools use `asyncio.gather()` to call multiple endpoints concurrently
- **Graceful degradation**: `_safe_call()` returns defaults on error so composite tools return partial data instead of failing entirely
- **In-memory TTL cache**: Avoids redundant API calls with configurable TTLs per data type (60s for quotes, 24h for profiles)

## License

MIT
