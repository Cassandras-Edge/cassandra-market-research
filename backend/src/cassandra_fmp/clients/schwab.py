from __future__ import annotations

from typing import Any

import httpx


class SchwabClient:
    def __init__(self, *, base_url: str, broker_secret: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._broker_secret = broker_secret
        self._client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def get_quote(self, email: str, symbol: str) -> dict[str, Any] | None:
        if not email:
            return None
        resp = await self._client.get(
            f"{self._base_url}/users/{email}/quote/{symbol.upper().strip()}",
            headers={"X-Auth-Secret": self._broker_secret},
        )
        if resp.status_code >= 400:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
