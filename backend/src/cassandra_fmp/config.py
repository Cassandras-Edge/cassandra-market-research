"""Settings loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    fmp_api_key: str
    polygon_api_key: str | None
    fred_api_key: str | None
    auth_url: str
    auth_secret: str
    auth_yaml_path: str
    host: str
    mcp_port: int


def load_settings() -> Settings:
    fmp_api_key = os.environ.get("FMP_API_KEY", "")
    # FMP_API_KEY can come from env or service credentials (fetched at server init)

    return Settings(
        fmp_api_key=fmp_api_key,
        polygon_api_key=os.environ.get("POLYGON_API_KEY") or None,
        fred_api_key=os.environ.get("FRED_API_KEY") or None,
        auth_url=os.environ.get("AUTH_URL", ""),
        auth_secret=os.environ.get("AUTH_SECRET", ""),
        auth_yaml_path=os.environ.get("AUTH_YAML_PATH", "/app/acl.yaml"),
        host=os.environ.get("HOST", "0.0.0.0"),
        mcp_port=int(os.environ.get("MCP_PORT", "3003")),
    )
