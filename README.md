# cassandra-market-research

Financial market data MCP server. Unifies [FMP](https://site.financialmodelingprep.com/), [ThetaData](https://www.thetadata.net/) (options), [Polygon.io](https://polygon.io/) (Fed + short volume), [FRED](https://fred.stlouisfed.org/), and [Treasury Fiscal Data](https://fiscaldata.treasury.gov/) behind a single FastMCP interface.

Covers stocks, SEC filings, macro series, options chains, earnings, and fiscal data.

## Architecture

```
MCP client → market-research.cassandrasedge.com (CF Tunnel)
  → FastMCP backend (port 3003)
    ├─ McpKeyAuthProvider → /keys/validate (auth service)
    ├─ FMP client           → api.financialmodelingprep.com
    ├─ Polygon client       → api.polygon.io
    ├─ FRED/Treasury client → Treasury Fiscal Data + FRED
    └─ ThetaData client     → theta-terminal sidecar
                                ↓
                          ThetaTerminal.jar (Java)
                                ↓
                          ThetaData REST API
```

The ThetaTerminal sidecar runs as a separate k8s deployment because it's a stateful Java process that logs into ThetaData and proxies REST calls; credentials live in the `theta-terminal` k8s Secret.

## Repo Layout

```text
cassandra-market-research/
├── backend/
│   ├── src/cassandra_fmp/
│   │   ├── main.py
│   │   ├── mcp_server.py
│   │   ├── config.py
│   │   ├── clients/
│   │   │   ├── polygon.py
│   │   │   ├── thetadata.py
│   │   │   └── treasury.py
│   │   └── tools/                  # Tool modules (register pattern)
│   ├── tests/
│   └── Dockerfile
├── theta-terminal/                 # ThetaTerminal sidecar image
│   ├── Dockerfile                  # Downloads ThetaTerminal.jar at build time
│   └── entrypoint.sh
├── .woodpecker.yaml
└── CLAUDE.md
```

## Auth

Uses the shared FastMCP sidecar pattern:
- `McpKeyAuthProvider` validates `Bearer mcp_...` tokens via auth service `/keys/validate`
- Upstream API keys (FMP, Polygon, FRED) are **deployment-level** env vars, not per-user credentials
- ThetaData credentials live in the `theta-terminal` k8s Secret, consumed by ThetaTerminal.jar, never by this backend
- ACL policy baked into the Docker image via the `AUTH_YAML_CONTENT` build arg

## Env Vars

| Variable | Required | Description |
|----------|----------|-------------|
| `FMP_API_KEY` | Yes | Financial Modeling Prep API key |
| `POLYGON_API_KEY` | No | Polygon.io API key (Fed data + short volume) |
| `FRED_API_KEY` | No | FRED API key for macro series |
| `THETA_TERMINAL_URL` | Yes (for options) | ThetaTerminal REST endpoint (e.g. `http://theta-terminal.production.svc.cluster.local:25510`) |
| `AUTH_URL` / `AUTH_SECRET` | Yes | Auth service wiring |

## Dev

```bash
cd backend
uv sync
FMP_API_KEY=<key> THETA_TERMINAL_URL=http://127.0.0.1:25510 uv run cassandra-fmp
```

## Deploy

Auto-deploys on push to main via Woodpecker CI. BuildKit builds two images and pushes to the local registry:
- `market-research:latest` from `backend/`
- `theta-terminal:latest` from `theta-terminal/`

ArgoCD then syncs `cassandra-k8s/apps/market-research/` and `cassandra-k8s/apps/theta-terminal/`.

Part of the [Cassandra](https://github.com/Cassandras-Edge) stack.
