# CLAUDE.md — Cassandra FMP

## What This Is

Financial Market Data MCP server. Wraps FMP (Financial Modeling Prep), Polygon.io, FRED, and Treasury Fiscal Data APIs behind a unified FastMCP interface with cassandra-mcp-auth for API key validation and ACL enforcement.

## Repo Structure

```
cassandra-fmp/
├── backend/
│   ├── src/cassandra_fmp/
│   │   ├── main.py           # CLI entrypoint
│   │   ├── config.py         # Settings from env vars
│   │   ├── mcp_server.py     # FastMCP server with auth
│   │   ├── auth.py           # Re-export from cassandra-mcp-auth
│   │   ├── acl.py            # Re-export from cassandra-mcp-auth
│   │   ├── clients/
│   │   │   ├── polygon.py    # Polygon.io API client
│   │   │   └── treasury.py   # Treasury Fiscal Data + FRED client
│   │   └── tools/            # Tool modules (register pattern)
│   ├── tests/
│   ├── pyproject.toml
│   └── Dockerfile
├── .woodpecker.yaml
└── CLAUDE.md
```

## Auth

Uses FastMCP sidecar pattern (Pattern 2):
- `McpKeyAuthProvider` validates `Bearer mcp_...` tokens via ACL `/keys/validate`
- API keys for upstream services (FMP, Polygon, FRED) are deployment-level env vars, NOT per-user credentials
- ACL policy baked into Docker image via `AUTH_YAML_CONTENT` build arg

## Deploy

Backend auto-deploys on push to main via Woodpecker CI. BuildKit builds image, pushes to local registry, kubectl restarts the deployment. ArgoCD syncs the Helm chart from `cassandra-k8s/apps/cassandra-fmp/`.

```bash
# Manual local run
cd backend
FMP_API_KEY=<key> uv run cassandra-fmp

# Manual deploy (if needed)
cd backend && docker build -t 172.20.0.161:30500/cassandra-fmp:latest . && docker push 172.20.0.161:30500/cassandra-fmp:latest
```

## Env Vars

| Variable | Required | Description |
|----------|----------|-------------|
| `FMP_API_KEY` | Yes | Financial Modeling Prep API key |
| `POLYGON_API_KEY` | No | Polygon.io API key (enables options + economy tools) |
| `FRED_API_KEY` | No | FRED API key (enables CMT yield data in treasury tools) |
| `AUTH_URL` | Yes (prod) | ACL service URL for key validation |
| `AUTH_SECRET` | Yes (prod) | Shared secret for ACL service |
| `AUTH_YAML_PATH` | No | Path to baked-in ACL YAML (default: /app/acl.yaml) |
| `HOST` | No | Bind address (default: 0.0.0.0) |
| `MCP_PORT` | No | Port (default: 3003) |

## Tool Modules

All tools are read-only financial data queries. Each module exports a `register(mcp, client, ...)` function:

- `overview` — quote, company_overview, stock_search, company_executives, sec_filings
- `financials` — financial_statements, financial_health, ratio_history
- `valuation` — valuation_history, discounted_cash_flow, peer_comparison
- `market` — intraday_prices, price_history, technical_indicators, sector/industry performance
- `ownership` — institutional_ownership, insider_activity, short_interest, ownership_structure
- `news` — market_news
- `macro` — index performance/constituents, sector valuation, market overview
- `transcripts` — earnings_transcript, earnings_calendar
- `assets` — crypto, forex, commodity quotes, ETF lookup
- `workflows` — stock_brief, market_context, earnings_setup/preview/postmortem, fair_value_estimate, ownership_deep_dive, industry_analysis
- `edgar` — SEC filings search, filing_sections (LLM-filtered), fund holdings (NPORT-P)
- `auctions` — Treasury auction data
- `meta` — fmp_coverage_gaps
- `options` — options_chain (requires Polygon)
- `economy` — economy_indicators (requires Polygon)
