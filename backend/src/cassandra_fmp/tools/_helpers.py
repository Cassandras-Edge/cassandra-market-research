"""Shared helpers for fmp-data based tool modules."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any

from fmp_data import AsyncFMPDataClient
from fmp_data.models import Endpoint

TTL_REALTIME = 60
TTL_HOURLY = 3600
TTL_6H = 21600
TTL_12H = 43200
TTL_DAILY = 86400

_CACHE: dict[tuple[Any, ...], tuple[float, Any]] = {}


def _freeze(value: Any) -> Any:
    """Convert complex values into hashable cache-key components."""
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, set):
        return tuple(sorted(_freeze(v) for v in value))
    if isinstance(value, tuple):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _to_date(value: Any) -> date | None:
    """Coerce date-like values (date/datetime/ISO string) to date."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    value_str = str(value)
    if "T" in value_str:
        value_str = value_str.split("T", 1)[0]
    else:
        value_str = value_str.split(" ", 1)[0]
    try:
        return date.fromisoformat(value_str)
    except ValueError:
        return None


def _date_only(value: Any) -> str | None:
    """Return YYYY-MM-DD string for date-like values."""
    date_value = _to_date(value)
    return date_value.isoformat() if date_value else None


def _ms_to_str(ts_ms: int | float | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str | None:
    """Convert millisecond epoch timestamp to formatted string."""
    if not ts_ms:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime(fmt)
    except (OSError, ValueError, OverflowError):
        return None


def _cache_key(fn: Callable[..., Awaitable[Any]], args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
    return (
        getattr(fn, "__module__", ""),
        getattr(fn, "__qualname__", getattr(fn, "__name__", "unknown")),
        _freeze(args),
        _freeze(kwargs),
    )


async def _safe_call(
    fn: Callable[..., Awaitable[Any]],
    *args: Any,
    default: Any = None,
    ttl: int = 0,
    **kwargs: Any,
) -> Any:
    """Call an async SDK method safely with optional TTL cache."""
    key: tuple[Any, ...] | None = None
    if ttl > 0:
        key = _cache_key(fn, args, kwargs)
        cached = _CACHE.get(key)
        if cached is not None:
            cached_at, cached_data = cached
            if time.monotonic() - cached_at < ttl:
                return cached_data

    try:
        data = await fn(*args, **kwargs)
    except Exception:
        return default

    if ttl > 0 and key is not None:
        _CACHE[key] = (time.monotonic(), data)
    return data


async def _safe_endpoint_call(
    client: AsyncFMPDataClient,
    endpoint: Endpoint[Any],
    *,
    default: Any = None,
    ttl: int = 0,
    **params: Any,
) -> Any:
    """Fallback safe call for SDK endpoint constants lacking async convenience methods."""
    return await _safe_call(client.request_async, endpoint, default=default, ttl=ttl, **params)


def _dump(obj: Any) -> Any:
    """Dump pydantic model(s) with by_alias=True to preserve camelCase keys."""
    if obj is None:
        return None
    if isinstance(obj, list):
        return [item.model_dump(by_alias=True) if hasattr(item, "model_dump") else item for item in obj]
    if hasattr(obj, "model_dump"):
        return obj.model_dump(by_alias=True)
    return obj


def _as_list(obj: Any, *, list_key: str | None = None) -> list:
    """Normalize object into a list."""
    value = _dump(obj)
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        if list_key is not None and isinstance(value.get(list_key), list):
            return value[list_key]
        return [value]
    return []


def _as_dict(obj: Any) -> dict:
    """Normalize object into a dict."""
    value = _dump(obj)
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return value[0] if value and isinstance(value[0], dict) else {}
    return {}


def _safe_first(obj: Any) -> dict:
    """Return first dict from obj if present, otherwise empty dict."""
    return _as_dict(obj)


def _ts_to_epoch(ts: Any) -> float:
    """Normalise a timestamp to epoch seconds.

    Handles: ``int``/``float`` epoch-seconds, ``int``/``float``
    epoch-milliseconds (heuristic: > 1e12), ``datetime`` objects, and
    ``None`` (returns 0).
    """
    if ts is None:
        return 0.0
    if isinstance(ts, datetime):
        return ts.timestamp()
    val = float(ts)
    # Values > 1 trillion are almost certainly milliseconds
    if val > 1e12:
        return val / 1000
    return val


def _latest_price(
    quote: dict,
    premarket_list: list,
    afterhours: dict | None,
) -> dict:
    """Pick the most up-to-date price from quote + extended-hours data.

    Returns ``{"price": float, "source": str, "change_pct": float|None}``
    where source is "quote", "premarket", or "afterhours".
    """
    regular_price = quote.get("price")
    regular_ts = _ts_to_epoch(quote.get("timestamp"))

    best_price = regular_price
    best_ts = regular_ts
    best_source = "quote"
    best_change_pct = quote.get("changePercentage")

    # Premarket (already filtered to matching symbol by caller)
    pre = _as_dict(premarket_list) if premarket_list else {}
    if pre.get("price") and pre.get("timestamp"):
        pre_ts = _ts_to_epoch(pre["timestamp"])
        if pre_ts > best_ts:
            best_price = pre["price"]
            best_ts = pre_ts
            best_source = "premarket"
            if regular_price and regular_price > 0:
                best_change_pct = round((pre["price"] / regular_price - 1) * 100, 2)

    # After-hours
    post = _as_dict(afterhours) if afterhours else {}
    if post.get("price") and post.get("timestamp"):
        post_ts = _ts_to_epoch(post["timestamp"])
        if post_ts > best_ts:
            best_price = post["price"]
            best_ts = post_ts
            best_source = "afterhours"
            if regular_price and regular_price > 0:
                best_change_pct = round((post["price"] / regular_price - 1) * 100, 2)

    result: dict = {"price": best_price, "source": best_source}
    if best_change_pct is not None:
        result["change_pct"] = best_change_pct
    return result
