"""Compatibility shim — re-exports from cass_market_sdk.

All logic now lives in the SDK. This module preserves the underscore-prefixed
names that existing tool modules import.
"""

from __future__ import annotations

from cass_market_sdk.cache import (
    TTL_REALTIME,
    TTL_HOURLY,
    TTL_6H,
    TTL_12H,
    TTL_DAILY,
    cached_call,
)
from cass_market_sdk.helpers import (
    to_date as _to_date,
    date_only as _date_only,
    ms_to_str as _ms_to_str,
    ts_to_epoch as _ts_to_epoch,
    latest_price as _latest_price,
)
from cass_market_sdk.clients.fmp import (
    safe_call as _safe_call_impl,
    safe_endpoint_call as _safe_endpoint_call_impl,
    dump as _dump,
    as_list as _as_list,
    as_dict as _as_dict,
    safe_first as _safe_first,
)
from cass_market_sdk.cache import _freeze, _cache_key, _CACHE  # noqa: F401

from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any

from fmp_data import AsyncFMPDataClient
from fmp_data.models import Endpoint


async def _safe_call(
    fn: Callable[..., Awaitable[Any]],
    *args: Any,
    default: Any = None,
    ttl: int = 0,
    **kwargs: Any,
) -> Any:
    """Call an async SDK method safely with optional TTL cache."""
    return await cached_call(fn, *args, default=default, ttl=ttl, **kwargs)


async def _safe_endpoint_call(
    client: AsyncFMPDataClient,
    endpoint: Endpoint[Any],
    *,
    default: Any = None,
    ttl: int = 0,
    **params: Any,
) -> Any:
    """Fallback safe call for SDK endpoint constants."""
    return await _safe_call(client.request_async, endpoint, default=default, ttl=ttl, **params)


# Re-export everything under the old names for backward compatibility
__all__ = [
    "TTL_REALTIME", "TTL_HOURLY", "TTL_6H", "TTL_12H", "TTL_DAILY",
    "_to_date", "_date_only", "_ms_to_str", "_ts_to_epoch", "_latest_price",
    "_dump", "_as_list", "_as_dict", "_safe_first",
    "_safe_call", "_safe_endpoint_call",
    "_freeze", "_cache_key",
]
