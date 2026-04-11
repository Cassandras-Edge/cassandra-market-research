"""Compatibility shim — re-exports from cass_market_sdk."""

from cass_market_sdk.clients.fmp import IMPLEMENTED_FAMILIES  # noqa: F401

__all__ = ["IMPLEMENTED_FAMILIES"]
