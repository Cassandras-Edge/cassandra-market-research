"""Cassandra FMP entrypoint."""

from __future__ import annotations

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

logger = logging.getLogger(__name__)


def cli() -> None:
    from cassandra_fmp.config import load_settings  # noqa: PLC0415
    from cassandra_fmp.mcp_server import create_mcp_server  # noqa: PLC0415

    settings = load_settings()
    logger.info("Starting Cassandra FMP on %s:%d", settings.host, settings.mcp_port)
    mcp = create_mcp_server(settings)
    mcp.run(
        transport="streamable-http",
        host=settings.host,
        port=settings.mcp_port,
    )
