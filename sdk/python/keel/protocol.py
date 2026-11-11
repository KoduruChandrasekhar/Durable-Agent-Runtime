"""Transport-neutral types for the worker protocol, the clock abstraction and
the Backend interface implemented by the gRPC client (real server), the
in-memory LocalServer (tests) and the simulation."""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class Event:
    seq: int
    type: str
    step_id: str
    payload: dict
    epoch: int
    created_at_ms: int = 0


@dataclass
class NewEvent:
    type: str
    step_id: str = ""
    payload: dict = field(default_factory=dict)


@dataclass
class Wait:
    kind: str  # timer | signal | approval | child
    id: str
    name: str = ""
    fire_at_ms: int = 0


@dataclass
class RunTask:
    run_id: str
    epoch: int
    workflow: str
    version: str
    history: list[Event]
    input: Any
    tenant_id: str = "default"
    cancel_requested: bool = False
    server_now_ms: int = 0
    lease_ttl_ms: int = 30_000
    dry_run: bool = False
    parent_run_id: str = ""
    next_seq: int = 1
    created_at_ms: int = 0
    overrides: dict = field(default_factory=dict)


@dataclass
class AppendResult:
    ok: bool
    next_seq: int = 0
    error_code: str = ""
    cancel_requested: bool = False
    server_now_ms: int = 0


@dataclass
class HeartbeatResult:
    ok: bool
    lease_lost: bool = False
    cancel_requested: bool = False
    server_now_ms: int = 0


@dataclass
class SuspendResult:
    ok: bool
    lease_lost: bool = False
    woken: bool = False


@dataclass
class FinishResult:
    ok: bool
    lease_lost: bool = False
    error_code: str = ""
    new_run_id: str = ""


@dataclass
class PolicyDecision:
    decision: str  # allow | deny | require_approval
    reason: str = ""
    rule_ids: list[str] = field(default_factory=list)
    approvers: list[str] = field(default_factory=list)
    expires_in_ms: int = 0
    on_expiry: str = "reject"
    policy_version: str = ""
    lease_lost: bool = False

    def to_json(self) -> dict:
        return {"decision": self.decision, "reason": self.reason, "rule_ids": self.rule_ids,
                "approvers": self.approvers, "expires_in_ms": self.expires_in_ms,
                "on_expiry": self.on_expiry, "policy_version": self.policy_version}

    @classmethod
    def from_json(cls, d: dict) -> PolicyDecision:
        return cls(decision=d.get("decision", "allow"), reason=d.get("reason", ""),
                   rule_ids=list(d.get("rule_ids") or []), approvers=list(d.get("approvers") or []),
                   expires_in_ms=int(d.get("expires_in_ms") or 0), on_expiry=d.get("on_expiry") or "reject",
                   policy_version=d.get("policy_version", ""))


@dataclass
class WorkflowDescriptor:
    name: str
    version: str
    input_schema: dict | None = None


class TransientBackendError(Exception):
    """The server could not be reached; the call may be retried."""


# --------------------------------------------------------------------- clocks


class Clock(Protocol):
    def now_ms(self) -> int: ...

    async def sleep(self, seconds: float) -> None: ...


class RealClock:
    """Wall clock corrected towards the server's database clock.

    Lease and timer math happens on the server with now(); the worker only
    uses this clock to stamp fire times and to decide whether a recorded retry
    time has passed.
    """

    def __init__(self) -> None:
        self.offset_ms = 0

    def observe_server_time(self, server_now_ms: int) -> None:
        if server_now_ms > 0:
            local = int(time.time() * 1000)
            self.offset_ms = server_now_ms - local

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.offset_ms

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


class VirtualClock:
    """A clock that only moves when the test driver advances it, so a
    workflow that sleeps for seven days finishes in milliseconds."""

    def __init__(self, start_ms: int = 1_767_225_600_000) -> None:  # 2026-01-01T00:00:00Z
        self._now = start_ms
        self._waiters: list[tuple[int, int, asyncio.Future]] = []
        self._seq = itertools.count()

    def observe_server_time(self, server_now_ms: int) -> None:
        pass

    def now_ms(self) -> int:
        return self._now

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        fut = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiters, (self._now + int(seconds * 1000), next(self._seq), fut))
        await fut

    def next_wakeup_ms(self) -> int | None:
        while self._waiters and self._waiters[0][2].done():
            heapq.heappop(self._waiters)
        return self._waiters[0][0] if self._waiters else None

    def advance_to(self, t_ms: int) -> None:
        if t_ms > self._now:
            self._now = t_ms
        while self._waiters and self._waiters[0][0] <= self._now:
            _, _, fut = heapq.heappop(self._waiters)
            if not fut.done():
                fut.set_result(None)

    def advance(self, seconds: float) -> None:
        self.advance_to(self._now + int(seconds * 1000))


# --------------------------------------------------------------------- backend


class Backend(Protocol):
    clock: Any

    async def poll(self, worker_id: str, workflows: list[str], descriptors: list[WorkflowDescriptor],
                   max_wait_ms: int) -> RunTask | None: ...

    async def append(self, run_id: str, epoch: int, expected_seq: int, events: list[NewEvent]) -> AppendResult: ...

    async def heartbeat(self, run_id: str, epoch: int, worker_id: str) -> HeartbeatResult: ...

    async def suspend(self, run_id: str, epoch: int, waits: list[Wait], available_at_ms: int,
                      reason: str) -> SuspendResult: ...

    async def finish(self, run_id: str, epoch: int, expected_seq: int, status: str, result: Any = None,
                     error: Any = None, continue_input: Any = None) -> FinishResult: ...

    async def evaluate_policy(self, run_id: str, epoch: int, tool: str, risk: str, args: dict, step_id: str,
                              approved_rules: list[str]) -> PolicyDecision: ...
