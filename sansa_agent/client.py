# sansa_agent/client.py
"""
HTTP client for the Sansa agent API.

All requests are authenticated with a bearer token after registration.
Enrollment and registration are unauthenticated (the enrollment token is
the auth for register).
"""
from __future__ import annotations

from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class SansaClient:
    def __init__(self, *, base_url: str, credential: str | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.credential = credential

    def _headers(self) -> dict[str, str]:
        if not self.credential:
            return {}
        return {"Authorization": f"Bearer {self.credential}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
            retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
            reraise=True,
        ):
            with attempt:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.request(
                        method, url, json=json, headers=self._headers()
                    )
        if response.status_code >= 400:
            raise SansaClientError(
                f"{method} {path} -> {response.status_code}: {response.text[:200]}"
            )
        return response.json()

    async def register(
        self, *, enrollment_token: str, agent_metadata: dict[str, Any]
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/api/v1/agents/register",
            json={
                "enrollment_token": enrollment_token,
                "agent_metadata": agent_metadata,
            },
        )

    async def heartbeat(
        self, *, agent_id: str, agent_metadata: dict[str, Any]
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/v1/agents/{agent_id}/heartbeat",
            json={"agent_metadata": agent_metadata},
        )

    async def sync(
        self, *, agent_id: str, files: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/v1/agents/{agent_id}/sync",
            json={"files": files},
        )


class SansaClientError(Exception):
    """Sansa returned an error, or the network failed after retries."""