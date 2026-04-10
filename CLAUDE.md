# CLAUDE.md — Cassandra Market Research

## What This Is

Financial Market Data MCP server. Wraps FMP (Financial Modeling Prep), ThetaData (options), Polygon.io (Fed/short volume only), FRED, and Treasury Fiscal Data APIs behind a unified FastMCP interface with cassandra-mcp-auth for API key validation and ACL enforcement.

## Repo Structure

```
cassandra-market-research/
├── backend/
│   ├── src/cassandra_fmp/
│   │   ├── main.py           # CLI entrypoint
│   │   ├── config.py         # Settings from env vars
│   │   ├── mcp_server.py     # FastMCP server with auth
│   │   ├── auth.py           # Re-export from cassandra-mcp-auth
│   │   ├── acl.py            # Re-export from cassandra-mcp-auth
│   │   ├── clients/
│   │   │   ├── polygon.py    # Polygon.io API client (Fed data + short volume)
│   │   │   ├── thetadata.py  # ThetaData REST client (options chain + history)
│   │   │   └── treasury.py   # Treasury Fiscal Data + FRED client
│   │   └── tools/            # Tool modules (register pattern)
│   ├── tests/
│   ├── pyproject.toml
│   └── Dockerfile
├── theta-terminal/           # ThetaTerminal sidecar image (Java + socat proxy)
│   ├── Dockerfile
│   └── entrypoint.sh
├── .woodpecker.yaml
└── CLAUDE.md
```

## Auth

Uses FastMCP sidecar pattern (Pattern 2):
- `McpKeyAuthProvider` validates `Bearer mcp_...` tokens via ACL `/keys/validate`
- API keys for upstream services (FMP, Polygon, FRED) are deployment-level env vars, NOT per-user credentials
- ThetaData credentials live in the `theta-terminal` k8s Secret and are passed to ThetaTerminal.jar at startup, NOT to this backend
- ACL policy baked into Docker image via `AUTH_YAML_CONTENT` build arg

## Deploy

Backend auto-deploys on push to main via Woodpecker CI. BuildKit builds two images:
- `market-research:latest` from `backend/` — the FastMCP server
- `theta-terminal:latest` from `theta-terminal/` — the ThetaTerminal Java sidecar (downloads jar from `https://download-stable.thetadata.us` at build time)

Both push to the local registry; ArgoCD syncs `cassandra-k8s/apps/market-research/` and `cassandra-k8s/apps/theta-terminal/`.

```bash
# Manual local run (against a local ThetaTerminal at default port)
cd backend
FMP_API_KEY=<key> THETA_TERMINAL_URL=http://127.0.0.1:25510 uv run cassandra-fmp
```

## Env Vars

| Variable | Required | Description |
|----------|----------|-------------|
| `FMP_API_KEY` | Yes | Financial Modeling Prep API key |
| `THETA_TERMINAL_URL` | Yes (for options) | URL of the ThetaTerminal v3 REST endpoint (e.g. `http://theta-terminal.production.svc.cluster.local:25503`) |
| `POLYGON_API_KEY` | No | Polygon.io API key — enables MACD, Polygon short-volume enrichment, and `economy_indicators` (Fed series). NOT options. |
| `FRED_API_KEY` | No | FRED API key (enables CMT yield data in treasury tools) |
| `AUTH_URL` | Yes (prod) | ACL service URL for key validation |
| `AUTH_SECRET` | Yes (prod) | Shared secret for ACL service |
| `AUTH_YAML_PATH` | No | Path to baked-in ACL YAML (default: /app/acl.yaml) |
| `HOST` | No | Bind address (default: 0.0.0.0) |
| `MCP_PORT` | No | Port (default: 3003) |

## ThetaData / ThetaTerminal v3

All options tools talk to a separate `theta-terminal` Deployment running `ThetaTerminalv3.jar` (from `download-unstable.thetadata.us` — v3 is still in beta). The terminal binds the REST API to `127.0.0.1:25503` only, so the sidecar image runs `socat TCP-LISTEN:25511 → TCP:127.0.0.1:25503` to expose it on a ClusterIP Service.

v3 credentials are written to a `creds.txt` file (email on line 1, password on line 2) at startup from `THETA_USERNAME`/`THETA_PASSWORD` k8s Secret env vars — positional-arg credentials are no longer accepted.

v3's `snapshot/greeks/first_order`, `snapshot/quote`, `snapshot/open_interest`, and `snapshot/ohlc` all accept `expiration=*` to fetch every contract on every expiration in a single call, so `options_chain` no longer fans out per-expiration. It still defaults to the next ~6 future expirations (client-side filter) when no `expiry_from`/`expiry_to` is supplied, to keep the result size bounded for liquid underlyings like SPY.

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
- `options` — options_chain, historical_options, historical_option_iv, historical_option_greeks, historical_option_oi, option_quote_at_time (requires ThetaData v3)
- `economy` — economy_indicators (requires Polygon)
