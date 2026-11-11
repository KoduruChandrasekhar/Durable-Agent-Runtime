"""Async client for keel-server's REST API."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx

FINAL = ("completed", "failed", "cancelled")


class KeelAPIError(Exception):
    def __init__(self, status: int, code: str, message: str, retry_after: float | None = None):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.retry_after = retry_after


class KeelClient:
    def __init__(self, url: str | None = None, api_key: str | None = None, timeout: float = 30.0,
                 tenant: str | None = None):
        self.url = (url or os.environ.get("KEEL_URL", "http://localhost:8080")).rstrip("/")
        headers = {}
        key = api_key or os.environ.get("KEEL_API_KEY")
        if key:
            headers["Authorization"] = f"Bearer {key}"
        if tenant:
            headers["X-Keel-Tenant"] = tenant
        self.http = httpx.AsyncClient(base_url=self.url, headers=headers, timeout=timeout,
                                      limits=httpx.Limits(max_connections=256, max_keepalive_connections=64))

    async def aclose(self) -> None:
        await self.http.aclose()

    async def __aenter__(self) -> KeelClient:
        return self

    async def __aexit__(self, *a: Any) -> None:
        await self.aclose()

    async def _req(self, method: str, path: str, **kw: Any) -> Any:
        r = await self.http.request(method, path, **kw)
        if r.status_code >= 300:
            try:
                err = r.json().get("error", {})
            except ValueError:
                err = {}
            ra = r.headers.get("Retry-After")
            raise KeelAPIError(r.status_code, err.get("code", "error"), err.get("message", r.text[:200]),
                               float(ra) if ra else None)
        return r.json() if r.content else None

    async def start_run(self, workflow: str, input: Any = None, *, version: str | None = None,  # noqa: A002
                        budget_usd: float | None = None, priority: int = 0, start_key: str | None = None,
                        dry_run: bool = False, tenant: str | None = None, retry_429: bool = True) -> dict:
        body: dict[str, Any] = {"workflow": workflow, "input": input if input is not None else {}, "priority": priority}
        for k, v in (("version", version), ("budget_usd", budget_usd), ("start_key", start_key), ("tenant", tenant)):
            if v is not None:
                body[k] = v
        if dry_run:
            body["dry_run"] = True
        while True:
            try:
                return await self._req("POST", "/v1/runs", json=body)
            except KeelAPIError as e:
                if e.status == 429 and retry_429:
                    await asyncio.sleep(e.retry_after or 1)
                    continue
                raise

    async def get_run(self, run_id: str) -> dict:
        return await self._req("GET", f"/v1/runs/{run_id}")

    async def list_runs(self, **filters: Any) -> dict:
        return await self._req("GET", "/v1/runs", params={k: v for k, v in filters.items() if v is not None})

    async def events(self, run_id: str, after_seq: int = 0, reveal_pii: bool = False) -> list[dict]:
        params: dict[str, Any] = {"after_seq": after_seq}
        if reveal_pii:
            params["reveal_pii"] = "true"
        return (await self._req("GET", f"/v1/runs/{run_id}/events", params=params))["events"]

    async def stream(self, run_id: str) -> AsyncIterator[tuple[str, dict]]:
        async with self.http.stream("GET", f"/v1/runs/{run_id}/events", headers={"Accept": "text/event-stream"},
                                    timeout=None) as r:
            kind = ""
            async for line in r.aiter_lines():
                if line.startswith("event: "):
                    kind = line[7:]
                elif line.startswith("data: "):
                    yield kind, json.loads(line[6:])
                    if kind == "end":
                        return

    async def signal(self, run_id: str, name: str, payload: Any = None, signal_id: str | None = None) -> dict:
        body: dict[str, Any] = {"payload": payload}
        if signal_id:
            body["signal_id"] = signal_id
        return await self._req("POST", f"/v1/runs/{run_id}/signals/{name}", json=body)

    async def cancel(self, run_id: str) -> dict:
        return await self._req("POST", f"/v1/runs/{run_id}/cancel")

    async def fork(self, run_id: str, from_seq: int, overrides: dict | None = None, dry_run: bool = True) -> dict:
        return await self._req("POST", f"/v1/runs/{run_id}/fork",
                               json={"from_seq": from_seq, "overrides": overrides or {}, "dry_run": dry_run})

    async def approvals(self, status: str = "pending", limit: int = 500) -> list[dict]:
        return (await self._req("GET", "/v1/approvals", params={"status": status, "limit": limit}))["approvals"]

    async def decide(self, approval_id: str, decision: str, edited_args: dict | None = None, comment: str = "") -> dict:
        body: dict[str, Any] = {"decision": decision, "comment": comment}
        if edited_args is not None:
            body["edited_args"] = edited_args
        return await self._req("POST", f"/v1/approvals/{approval_id}", json=body)

    async def workflows(self) -> list[dict]:
        return (await self._req("GET", "/v1/workflows"))["workflows"]

    async def put_tenant(self, tenant_id: str, **cfg: Any) -> dict:
        return await self._req("PUT", f"/v1/tenants/{tenant_id}", json=cfg)

    async def create_api_key(self, tenant: str, principal: str, scopes: list[str]) -> str:
        r = await self._req("POST", "/v1/api-keys", json={"tenant": tenant, "principal": principal, "scopes": scopes})
        return r["api_key"]

    async def wait(self, run_id: str, timeout: float = 120.0, statuses: tuple[str, ...] = FINAL,
                   poll: float = 0.25) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            r = await self.get_run(run_id)
            if r["status"] in statuses:
                return r
            if time.monotonic() > deadline:
                raise TimeoutError(f"run {run_id} still {r['status']} after {timeout}s")
            await asyncio.sleep(poll)
