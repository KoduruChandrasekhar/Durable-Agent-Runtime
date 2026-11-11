"""Clients for the refund agent's external systems.

``HttpSystems`` talks to the mock FastAPI services; ``MemorySystems`` keeps
the same semantics in-process (idempotency keys on payments and CRM, no keys
on email, fail-after-commit chaos) for unit tests and run_with_crashes.
"""

from __future__ import annotations

import os
import random
import time
import uuid
from typing import Any

import httpx
from keel.telemetry import inject_headers


class SystemError_(Exception):
    """Upstream failed (5xx/timeout): the outcome is unknown, retry with the same key."""

    retryable = True


class NotFound(Exception):
    retryable = False


class BadRequest(Exception):
    retryable = False


class HttpSystems:
    def __init__(self, orders: str | None = None, payments: str | None = None, email: str | None = None,
                 crm: str | None = None, timeout: float = 5.0):
        base = os.environ.get("KEEL_MOCKS_URL", "http://localhost:9000")
        self.orders = orders or os.environ.get("KEEL_ORDERS_URL", base + "/orders")
        self.payments = payments or os.environ.get("KEEL_PAYMENTS_URL", base + "/payments")
        self.email = email or os.environ.get("KEEL_EMAIL_URL", base + "/email")
        self.crm = crm or os.environ.get("KEEL_CRM_URL", base + "/crm")
        self.http = httpx.AsyncClient(timeout=timeout, limits=httpx.Limits(max_connections=400, max_keepalive_connections=100))

    async def _call(self, method: str, url: str, **kw: Any) -> Any:
        try:
            kw["headers"] = inject_headers(kw.get("headers"))  # type: ignore[arg-type]
            r = await self.http.request(method, url, **kw)
        except httpx.HTTPError as e:  # timeout or reset: we do not know whether it happened
            raise SystemError_(f"{method} {url}: {type(e).__name__}") from None
        if r.status_code == 404:
            raise NotFound(r.text[:200])
        if r.status_code >= 500:
            raise SystemError_(f"{method} {url}: {r.status_code}")
        if r.status_code >= 400:
            raise BadRequest(f"{method} {url}: {r.status_code} {r.text[:200]}")
        return r.json()

    async def get_order(self, order_id: str) -> dict:
        return await self._call("GET", f"{self.orders}/orders/{order_id}")

    async def refund(self, order_id: str, amount_inr: int, reason: str, key: str) -> dict:
        return await self._call("POST", f"{self.payments}/refunds", json={"order_id": order_id, "amount_inr": amount_inr,
                                                                          "reason": reason},
                                headers={"Idempotency-Key": key})

    async def refund_status(self, refund_id: str) -> dict:
        return await self._call("GET", f"{self.payments}/refunds/{refund_id}")

    async def send_email(self, to: str, subject: str, body: str, key: str) -> dict:
        return await self._call("POST", f"{self.email}/messages", json={"to": to, "subject": subject, "body": body},
                                headers={"X-Keel-Key": key})

    async def find_email(self, key: str) -> list[dict]:
        return await self._call("GET", f"{self.email}/messages", params={"key": key})

    async def add_note(self, ticket_id: str, note: str, key: str) -> dict:
        return await self._call("POST", f"{self.crm}/notes", json={"ticket_id": ticket_id, "note": note},
                                headers={"Idempotency-Key": key})


class MemorySystems:
    def __init__(self, orders: list[dict] | None = None, *, fail_after_commit: float = 0.0, seed: int = 0,
                 settle_after_ms: int = 0, clock: Any = None):
        self.orders = {o["order_id"]: dict(o) for o in (orders or [])}
        self.refunds: dict[str, dict] = {}      # idempotency key -> refund
        self.emails: list[dict] = []
        self.notes: dict[str, dict] = {}
        self.fail_after_commit = fail_after_commit
        self.rng = random.Random(seed)
        self.settle_after_ms = settle_after_ms
        self.clock = clock

    def _now(self) -> float:
        return self.clock.now_ms() / 1000 if self.clock is not None else time.time()

    def _chaos(self) -> None:
        if self.fail_after_commit and self.rng.random() < self.fail_after_commit:
            raise SystemError_("committed, response dropped")

    async def get_order(self, order_id: str) -> dict:
        if order_id not in self.orders:
            raise NotFound(order_id)
        return dict(self.orders[order_id])

    async def refund(self, order_id: str, amount_inr: int, reason: str, key: str) -> dict:
        if key not in self.refunds:
            self.refunds[key] = {"refund_id": f"rf_{len(self.refunds) + 1:05d}", "order_id": order_id,
                                 "amount_inr": amount_inr, "idempotency_key": key, "created_at": self._now()}
            self._chaos()
        return {**self.refunds[key], "status": "pending"}

    async def refund_status(self, refund_id: str) -> dict:
        for r in self.refunds.values():
            if r["refund_id"] == refund_id:
                settled = (self._now() - r["created_at"]) * 1000 >= self.settle_after_ms
                return {**r, "status": "settled" if settled else "pending"}
        raise NotFound(refund_id)

    async def send_email(self, to: str, subject: str, body: str, key: str) -> dict:
        mid = f"msg_{uuid.UUID(int=self.rng.getrandbits(128)).hex[:10]}"
        self.emails.append({"message_id": mid, "to": to, "subject": subject, "body": body, "keel_key": key})
        self._chaos()
        return {"message_id": mid, "to": to}

    async def find_email(self, key: str) -> list[dict]:
        return [e for e in self.emails if e["keel_key"] == key]

    async def add_note(self, ticket_id: str, note: str, key: str) -> dict:
        if key not in self.notes:
            self.notes[key] = {"note_id": f"note_{len(self.notes) + 1}", "ticket_id": ticket_id, "note": note}
            self._chaos()
        return self.notes[key]


_current: Any = None


def set_systems(s: Any) -> None:
    global _current
    _current = s


def systems() -> Any:
    global _current
    if _current is None:
        _current = HttpSystems()
    return _current
