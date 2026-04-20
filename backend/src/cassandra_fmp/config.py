"""Settings loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    auth_url: str
    auth_secret: str
    auth_yaml_path: str
    host: str
    mcp_port: int
    base_url: str
    workos_client_id: str
    workos_authkit_domain: str
    theta_terminal_url: str
    tv_proxy_http_url: str
    tv_proxy_ws_url: str
    tv_proxy_mcp_key: str


def load_settings() -> Settings:
    return Settings(
        auth_url=os.environ.get("AUTH_URL", ""),
        auth_secret=os.environ.get("AUTH_SECRET", ""),
        auth_yaml_path=os.environ.get("AUTH_YAML_PATH", "/app/acl.yaml"),
        host=os.environ.get("HOST", "0.0.0.0"),
        mcp_port=int(os.environ.get("MCP_PORT", "3003")),
        base_url=os.environ.get("BASE_URL", ""),
        workos_client_id=os.environ.get("WORKOS_CLIENT_ID", ""),
        workos_authkit_domain=os.environ.get("WORKOS_AUTHKIT_DOMAIN", ""),
        theta_terminal_url=os.environ.get("THETA_TERMINAL_URL", ""),
        tv_proxy_http_url=os.environ.get("TV_PROXY_HTTP_URL", ""),
        tv_proxy_ws_url=os.environ.get("TV_PROXY_WS_URL", ""),
        tv_proxy_mcp_key=os.environ.get("TV_PROXY_MCP_KEY", ""),
    )
