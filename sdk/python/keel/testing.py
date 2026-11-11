"""In-memory server with a virtual clock for unit tests and simulation.

``LocalServer`` is a small Python version of keel-server's run, lease,
append, wait and timer rules. The conformance suite runs the same scenarios
against it and the Go server so the two cannot drift apart.

``LocalEnv`` wires a LocalServer to in-process workers running the real SDK.
Time only moves when every task is blocked, so a workflow that sleeps for
seven days finishes in milliseconds. ``run_with_crashes`` re-runs a workflow
crashing the worker before and after every write in turn.
"""

from __future__ import annotations

import asyncio
import copy
import json
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import determinism
from .errors import WorkerCrash
from .executor import RunExecutor, Services
from .guard import SecretStore
from .llm import MockLLM
from .policy import PolicyEngine
from .protocol import (
    AppendResult,
    Event,
    FinishResult,
    HeartbeatResult,
    NewEvent,
    PolicyDecision,
    RunTask,
    SuspendResult,
    TransientBackendError,
    VirtualClock,
    Wait,
    WorkflowDescriptor,
)
from .tools import DEFAULT_REGISTRY, ToolRegistry
from .workflow import WORKFLOWS, WorkflowDef, WorkflowRegistry, workflow_def

SERVER_ONLY = {"RunStarted", "RunResumed", "TimerFired", "SignalReceived", "ApprovalResolved", "ChildRunCompleted",
               "RunCompleted", "RunFailed", "RunCancelled", "RunForked", "RunContinuedAsNew"}
FINAL = ("completed", "failed", "cancelled")


@dataclass
class RunState:
    run_id: str
    tenant_id: str
    workflow: str
    version: str
    status: str
    input: Any
    created_at: int
    available_at: int
    result: Any = None
    error: Any = None
    next_seq: int = 1
    lease_owner: str | None = None
    lease_epoch: int = 0
    lease_expires_at: int | None = None
    wake_at: int | None = None
    waiting_on: list[Wait] = field(default_factory=list)
    wait_reason: str | None = None
    last_release: str | None = None
    cancel_requested: bool = False
    priority: int = 0
    budget_usd: float | None = None
    spent_usd: float = 0.0
    tainted: bool = False
    facts: dict = field(default_factory=dict)
    dry_run: bool = False
    overrides: dict = field(default_factory=dict)
    start_key: str | None = None
    parent_run_id: str | None = None
    chain_root: str = ""
    continued_as: str | None = None
    first_claimed_at: int | None = None
    finished_at: int | None = None
    forked_from: str | None = None


@dataclass
class Tenant:
    tenant_id: str
    weight: int = 1
    max_running: int | None = None
    max_pending: int = 50_000


class LocalServer:
    def __init__(self, *, policy: PolicyEngine | str | Path | None = None, seed: int = 0,
                 lease_ttl_ms: int = 30_000, clock: VirtualClock | None = None):
        self.clock = clock or VirtualClock()
        self.rng = random.Random(seed)
        self.lease_ttl_ms = lease_ttl_ms
        self.runs: dict[str, RunState] = {}
        self.events: dict[str, list[Event]] = {}
        self.signals: list[dict] = []
        self.approvals: dict[str, dict] = {}
        self.outbox: list[dict] = []
        self.tenants: dict[str, Tenant] = {"default": Tenant("default")}
        self.workflows: dict[str, set[str]] = {}
        self.down = False
        self.stats = {"claims": 0, "appends": 0, "fencing_rejections": 0, "seq_conflicts": 0,
                      "lease_expirations": 0, "timers_fired": 0}
        if isinstance(policy, PolicyEngine):
            self.policy = policy
        elif isinstance(policy, Path) or (isinstance(policy, str) and policy.endswith((".yaml", ".yml"))):
            self.policy = PolicyEngine.load(policy)
        elif isinstance(policy, str):
            self.policy = PolicyEngine.parse(policy)
        else:
            self.policy = PolicyEngine.empty()

    # ------------------------------------------------------------------ helpers

    def _now(self) -> int:
        return self.clock.now_ms()

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _check_up(self) -> None:
        if self.down:
            raise TransientBackendError("server unavailable")

    def _append_server(self, r: RunState, evs: list[NewEvent]) -> None:
        log = self.events.setdefault(r.run_id, [])
        for e in evs:
            log.append(Event(seq=r.next_seq, type=e.type, step_id=e.step_id,
                             payload=json.loads(json.dumps(e.payload)), epoch=r.lease_epoch, created_at_ms=self._now()))
            r.next_seq += 1

    def _counts(self, tenant: str) -> tuple[int, int]:
        running = pending = 0
        for r in self.runs.values():
            if r.tenant_id == tenant:
                running += r.status == "running"
                pending += r.status == "pending"
        return running, pending

    # ------------------------------------------------------------------ client API

    def start_run(self, workflow: str, input: Any = None, *, version: str | None = None,  # noqa: A002
                  tenant: str = "default", budget_usd: float | None = None, priority: int = 0,
                  start_key: str | None = None, dry_run: bool = False, overrides: dict | None = None,
                  parent_run_id: str | None = None, run_id: str | None = None, chain_root: str | None = None,
                  extra: dict | None = None) -> tuple[str, bool]:
        self._check_up()
        if start_key:
            for r in self.runs.values():
                if r.tenant_id == tenant and r.start_key == start_key:
                    return r.run_id, False
        t = self.tenants.setdefault(tenant, Tenant(tenant))
        if parent_run_id is None and self._counts(tenant)[1] >= t.max_pending:
            raise RuntimeError("too_many_pending")
        if version is None:
            vs = sorted(self.workflows.get(workflow, set()), key=lambda v: (len(v), v))
            if not vs:
                raise ValueError(f"unknown workflow {workflow!r}")
            version = vs[-1]
        rid = run_id or self._uuid()
        if rid in self.runs:
            return rid, False
        now = self._now()
        r = RunState(run_id=rid, tenant_id=tenant, workflow=workflow, version=version, status="pending",
                     input=json.loads(json.dumps(input)), created_at=now, available_at=now, budget_usd=budget_usd,
                     priority=priority, start_key=start_key, dry_run=dry_run, overrides=overrides or {},
                     parent_run_id=parent_run_id, chain_root=chain_root or rid)
        self.runs[rid] = r
        payload = {"workflow": workflow, "version": version, "input": r.input, "budget_usd": budget_usd,
                   "priority": priority, "tenant_id": tenant}
        if parent_run_id:
            payload["parent_run_id"] = parent_run_id
        if dry_run:
            payload["dry_run"] = True
        payload.update(extra or {})
        self._append_server(r, [NewEvent("RunStarted", "", payload)])
        return rid, True

    def signal(self, run_id: str, name: str, payload: Any = None, signal_id: str | None = None) -> bool:
        """Returns False if the signal id was a duplicate."""
        self._check_up()
        r = self.runs[run_id]
        if r.status in FINAL:
            raise RuntimeError("conflict: run is finished")
        sid = signal_id or self._uuid()
        if any(s["run_id"] == run_id and s["signal_id"] == sid for s in self.signals):
            return False
        self.signals.append({"id": len(self.signals) + 1, "run_id": run_id, "name": name,
                             "payload": json.loads(json.dumps(payload)), "signal_id": sid, "consumed_at": None})
        if r.status == "waiting":
            evs = self._check_waits(r, r.waiting_on)
            if evs:
                self._wake(r, evs)
        return True

    def decide(self, approval_id: str, decision: str, edited_args: dict | None = None, by: str = "tester",
               comment: str = "") -> bool:
        self._check_up()
        decision = {"approve": "approved", "reject": "rejected"}.get(decision, decision)
        a = self.approvals[approval_id]
        if a["status"] != "pending":
            return False
        a.update(status=decision, edited_args=edited_args, decided_by=by, comment=comment, decided_at=self._now())
        self._wake_if_waiting(a["run_id"])
        return True

    def cancel(self, run_id: str) -> None:
        self._check_up()
        r = self.runs[run_id]
        if r.status in FINAL:
            raise RuntimeError("conflict: run is finished")
        r.cancel_requested = True
        if r.status == "waiting":
            self._wake(r, [])

    def fork(self, run_id: str, from_seq: int = 0, overrides: dict | None = None, dry_run: bool = True) -> str:
        src = self.runs[run_id]
        copied, tainted, facts, max_epoch = [], False, {}, 0
        for e in self.events[run_id]:
            if from_seq and e.seq > from_seq:
                break
            if e.type in ("RunCompleted", "RunFailed", "RunCancelled", "RunContinuedAsNew"):
                break
            copied.append(e)
            max_epoch = max(max_epoch, e.epoch)
            if e.type == "RunTainted":
                tainted = True
            if e.type == "FactRecorded":
                facts.update(e.payload.get("facts") or {})
        nid = self._uuid()
        now = self._now()
        r = RunState(run_id=nid, tenant_id=src.tenant_id, workflow=src.workflow, version=src.version,
                     status="pending", input=src.input, created_at=now, available_at=now, budget_usd=src.budget_usd,
                     priority=src.priority, dry_run=dry_run, overrides=overrides or {}, chain_root=nid,
                     tainted=tainted, facts=facts, lease_epoch=max_epoch, forked_from=run_id)
        self.runs[nid] = r
        self.events[nid] = [Event(seq=e.seq, type=e.type, step_id=e.step_id, payload=copy.deepcopy(e.payload),
                                  epoch=e.epoch, created_at_ms=e.created_at_ms) for e in copied]
        r.next_seq = len(copied) + 1
        self._append_server(r, [NewEvent("RunForked", "", {"source_run_id": run_id, "from_seq": len(copied),
                                                            "overrides": overrides, "dry_run": dry_run})])
        return nid

    def unpark(self, run_id: str) -> None:
        r = self.runs[run_id]
        if r.status == "waiting" and r.wait_reason == "parked":
            self._wake(r, [])

    def pending_approvals(self) -> list[dict]:
        return [a for a in self.approvals.values() if a["status"] == "pending"]

    def history(self, run_id: str) -> list[Event]:
        return list(self.events.get(run_id, []))

    # ------------------------------------------------------------------ worker protocol

    def register(self, worker_id: str, workflows: list[str]) -> None:
        for key in workflows:
            name, _, ver = key.partition("@")
            self.workflows.setdefault(name, set()).add(ver)

    async def poll(self, worker_id: str, workflows: list[str], descriptors: list[WorkflowDescriptor],
                   max_wait_ms: int = 0) -> RunTask | None:
        self._check_up()
        self.register(worker_id, workflows)
        now = self._now()
        keys = set(workflows)
        ready = [r for r in self.runs.values()
                 if r.status == "pending" and r.available_at <= now and f"{r.workflow}@{r.version}" in keys]
        if not ready:
            return None
        # Fair scheduling: the tenant with the lowest running/weight and spare capacity first.
        by_tenant: dict[str, list[RunState]] = {}
        for r in ready:
            by_tenant.setdefault(r.tenant_id, []).append(r)
        cands = []
        for t, rs in by_tenant.items():
            ten = self.tenants.setdefault(t, Tenant(t))
            running, _ = self._counts(t)
            if ten.max_running is not None and running >= ten.max_running:
                continue
            cands.append((running / max(1, ten.weight), self.rng.random(), t, rs))
        if not cands:
            return None
        cands.sort(key=lambda c: (c[0], c[1]))
        rs = cands[0][3]
        rs.sort(key=lambda r: (-r.priority, r.available_at, r.run_id))
        r = rs[0]
        reclaimed = r.first_claimed_at is not None
        last = r.last_release
        r.status = "running"
        r.lease_owner = worker_id
        r.lease_epoch += 1
        r.lease_expires_at = now + self.lease_ttl_ms
        r.first_claimed_at = r.first_claimed_at or now
        r.wake_at, r.waiting_on, r.wait_reason = None, [], None
        self.stats["claims"] += 1
        if reclaimed:
            self._append_server(r, [NewEvent("RunResumed", "", {"worker_id": worker_id, "epoch": r.lease_epoch,
                                                                  "reason": last})])
        return RunTask(run_id=r.run_id, epoch=r.lease_epoch, workflow=r.workflow, version=r.version,
                       history=list(self.events[r.run_id]), input=copy.deepcopy(r.input), tenant_id=r.tenant_id,
                       cancel_requested=r.cancel_requested, server_now_ms=now, lease_ttl_ms=self.lease_ttl_ms,
                       dry_run=r.dry_run, parent_run_id=r.parent_run_id or "", next_seq=r.next_seq,
                       created_at_ms=r.created_at, overrides=copy.deepcopy(r.overrides))

    async def append(self, run_id: str, epoch: int, expected_seq: int, events: list[NewEvent]) -> AppendResult:
        self._check_up()
        for e in events:
            if e.type in SERVER_ONLY or not e.type:
                raise ValueError(f"workers cannot append {e.type!r}")
        r = self.runs.get(run_id)
        if r is None or r.lease_epoch != epoch or r.status != "running":
            self.stats["fencing_rejections"] += 1
            return AppendResult(False, error_code="lease_lost")
        if r.next_seq != expected_seq:
            # A retried append whose first attempt landed: same epoch wrote those seqs.
            log = self.events[run_id]
            tail = log[expected_seq - 1:]
            if (r.next_seq == expected_seq + len(events) and len(tail) == len(events)
                    and all(t.epoch == epoch and t.type == e.type and t.step_id == e.step_id for t, e in zip(tail, events, strict=True))):
                return AppendResult(True, next_seq=r.next_seq, cancel_requested=r.cancel_requested,
                                    server_now_ms=self._now())
            self.stats["seq_conflicts"] += 1
            return AppendResult(False, next_seq=r.next_seq, error_code="seq_conflict")
        self._append_server(r, events)
        self.stats["appends"] += 1
        for e in events:
            self._derived(r, e)
        return AppendResult(True, next_seq=r.next_seq, cancel_requested=r.cancel_requested, server_now_ms=self._now())

    def _derived(self, r: RunState, e: NewEvent) -> None:
        p = e.payload
        if e.type == "ApprovalRequested":
            aid = p["approval_id"]
            if aid not in self.approvals:
                self.approvals[aid] = {"approval_id": aid, "run_id": r.run_id, "tenant_id": r.tenant_id,
                                       "step_id": e.step_id, "action": p.get("action"), "reason": p.get("reason", ""),
                                       "rule_ids": p.get("rule_ids", []), "approvers": p.get("approvers", []),
                                       "status": "pending", "on_expiry": p.get("on_expiry") or "reject",
                                       "expires_at": p.get("expires_at_ms") or None, "edited_args": None,
                                       "decided_by": "", "comment": "", "summary": p.get("summary")}
        elif e.type == "ChildRunStarted":
            self.start_run(p["workflow"], p.get("input"), version=p.get("version") or None, tenant=r.tenant_id,
                           budget_usd=p.get("budget_usd"), parent_run_id=r.run_id, run_id=p["child_run_id"],
                           dry_run=r.dry_run)
        elif e.type == "OutboxMessage":
            key = p.get("dedupe_key") or f"{r.run_id}:{e.step_id}"
            if not any(o["dedupe_key"] == key for o in self.outbox):
                self.outbox.append({"dedupe_key": key, "destination": p.get("destination"), "message": p.get("message")})
        elif e.type == "RunTainted":
            r.tainted = True
        elif e.type == "FactRecorded":
            r.facts.update(p.get("facts") or {})

    async def heartbeat(self, run_id: str, epoch: int, worker_id: str) -> HeartbeatResult:
        self._check_up()
        r = self.runs.get(run_id)
        if r is None or r.lease_epoch != epoch or r.status != "running":
            self.stats["fencing_rejections"] += 1
            return HeartbeatResult(False, lease_lost=True)
        r.lease_expires_at = self._now() + self.lease_ttl_ms
        return HeartbeatResult(True, cancel_requested=r.cancel_requested, server_now_ms=self._now())

    async def suspend(self, run_id: str, epoch: int, waits: list[Wait], available_at_ms: int, reason: str) -> SuspendResult:
        self._check_up()
        r = self.runs.get(run_id)
        if r is None or r.lease_epoch != epoch or r.status != "running":
            self.stats["fencing_rejections"] += 1
            return SuspendResult(False, lease_lost=True)
        now = self._now()
        r.lease_owner, r.lease_expires_at = None, None
        if reason == "handback":
            r.status, r.available_at, r.last_release = "pending", now, "handback"
            return SuspendResult(True)
        if reason == "parked":
            r.status, r.waiting_on, r.wake_at, r.wait_reason, r.last_release = "waiting", [], None, "parked", "parked"
            return SuspendResult(True)
        if not waits:
            r.status, r.available_at, r.last_release = "pending", max(now, available_at_ms or now), "retry"
            return SuspendResult(True)
        if r.cancel_requested:
            self._wake(r, [])
            return SuspendResult(True, woken=True)
        evs = self._check_waits(r, waits)
        if evs:
            r.last_release = "wait"
            self._wake(r, evs, release="wait")
            return SuspendResult(True, woken=True)
        timers = [w.fire_at_ms for w in waits if w.kind == "timer" and w.fire_at_ms]
        r.status, r.waiting_on, r.wake_at = "waiting", list(waits), min(timers) if timers else None
        r.wait_reason = ",".join(dict.fromkeys(w.kind for w in waits))
        r.last_release = "wait"
        return SuspendResult(True)

    def _check_waits(self, r: RunState, waits: list[Wait]) -> list[NewEvent]:
        now = self._now()
        evs: list[NewEvent] = []
        for w in waits:
            if w.kind == "timer":
                if w.fire_at_ms <= now:
                    evs.append(NewEvent("TimerFired", w.id, {"timer_id": w.id, "fire_at_ms": w.fire_at_ms,
                                                             "fired_at_ms": now}))
                    self.stats["timers_fired"] += 1
            elif w.kind == "signal":
                for s in self.signals:
                    if s["run_id"] == r.run_id and s["name"] == w.name and s["consumed_at"] is None:
                        s["consumed_at"], s["consumed_by"] = now, w.id
                        evs.append(NewEvent("SignalReceived", w.id, {"wait_id": w.id, "name": w.name,
                                                                     "payload": s["payload"], "signal_id": s["signal_id"]}))
                        break
            elif w.kind == "approval":
                a = self.approvals.get(w.id)
                if a is not None and a["status"] != "pending":
                    p = {"approval_id": w.id, "decision": a["status"], "decided_by": a.get("decided_by", ""),
                         "comment": a.get("comment", ""), "on_expiry": a.get("on_expiry", "reject")}
                    if a.get("edited_args"):
                        p["edited_args"] = a["edited_args"]
                    evs.append(NewEvent("ApprovalResolved", a["step_id"], p))
            elif w.kind == "child":
                for c in self.runs.values():
                    if c.chain_root == w.id and c.status in FINAL and c.continued_as is None:
                        evs.append(NewEvent("ChildRunCompleted", w.id, {
                            "child_run_id": w.id, "final_run_id": c.run_id, "status": c.status,
                            "result": c.result, "error": c.error}))
                        break
        return evs

    def _wake(self, r: RunState, evs: list[NewEvent], release: str = "wake") -> None:
        self._append_server(r, evs)
        r.status, r.available_at = "pending", self._now()
        r.waiting_on, r.wake_at, r.wait_reason = [], None, None
        r.lease_owner, r.lease_expires_at = None, None
        r.last_release = release

    def _wake_if_waiting(self, run_id: str) -> None:
        r = self.runs.get(run_id)
        if r is not None and r.status == "waiting":
            evs = self._check_waits(r, r.waiting_on)
            if evs:
                self._wake(r, evs)

    async def finish(self, run_id: str, epoch: int, expected_seq: int, status: str, result: Any = None,
                     error: Any = None, continue_input: Any = None) -> FinishResult:
        self._check_up()
        r = self.runs.get(run_id)
        if r is None or r.lease_epoch != epoch or r.status != "running":
            self.stats["fencing_rejections"] += 1
            return FinishResult(False, lease_lost=True, error_code="lease_lost")
        if expected_seq and r.next_seq != expected_seq:
            self.stats["seq_conflicts"] += 1
            return FinishResult(False, error_code="seq_conflict")
        new_id = ""
        if status == "completed":
            evs = [NewEvent("RunCompleted", "", {"result": result})]
        elif status == "failed":
            evs = [NewEvent("RunFailed", "", {"error": error})]
        elif status == "cancelled":
            evs = [NewEvent("RunCancelled", "", {"error": error})]
        elif status == "continued":
            budget = None if r.budget_usd is None else r.budget_usd - r.spent_usd
            new_id, _ = self.start_run(r.workflow, continue_input, version=r.version, tenant=r.tenant_id,
                                       budget_usd=budget, priority=r.priority, parent_run_id=r.parent_run_id,
                                       chain_root=r.chain_root, dry_run=r.dry_run, overrides=r.overrides,
                                       extra={"continued_from": r.run_id})
            r.continued_as = new_id
            result = {"continued_as_new": new_id}
            status = "completed"
            evs = [NewEvent("RunContinuedAsNew", "", {"new_run_id": new_id}),
                   NewEvent("RunCompleted", "", {"result": result})]
        else:
            raise ValueError(status)
        self._append_server(r, evs)
        r.status, r.result, r.error = status, result, error
        r.lease_owner, r.lease_expires_at, r.finished_at = None, None, self._now()
        if r.parent_run_id and not r.continued_as:
            p = self.runs.get(r.parent_run_id)
            if p is not None and p.status == "waiting" and any(w.kind == "child" and w.id == r.chain_root for w in p.waiting_on):
                evs2 = self._check_waits(p, p.waiting_on)
                if evs2:
                    self._wake(p, evs2)
        return FinishResult(True, new_run_id=new_id)

    async def evaluate_policy(self, run_id: str, epoch: int, tool: str, risk: str, args: dict, step_id: str,
                              approved_rules: list[str]) -> PolicyDecision:
        self._check_up()
        r = self.runs.get(run_id)
        if r is None or r.lease_epoch != epoch or r.status != "running":
            return PolicyDecision("deny", lease_lost=True)
        return self.policy.evaluate(tool, risk, r.tenant_id, args, self.run_facts(run_id, step_id), approved_rules)

    def run_facts(self, run_id: str, step_id: str = "") -> dict:
        r = self.runs[run_id]
        counts: dict[str, set] = {}
        for e in self.events[run_id]:
            if e.type == "StepStarted" and e.payload.get("kind") == "tool" and e.step_id != step_id:
                counts.setdefault(e.payload.get("tool", ""), set()).add(e.step_id)
        facts = {"tainted": r.tainted, "spent_usd": r.spent_usd, "input": r.input, "facts": dict(r.facts),
                 "tool_counts": {k: len(v) for k, v in counts.items()}, "dry_run": r.dry_run, "tenant": r.tenant_id}
        if r.budget_usd is not None:
            facts["budget_usd"] = r.budget_usd
        return facts

    # ------------------------------------------------------------------ background loops

    def tick(self) -> None:
        """Sweeper, timers and approval expiry, as the leader runs them."""
        now = self._now()
        for r in list(self.runs.values()):
            if r.status == "running" and r.lease_expires_at is not None and r.lease_expires_at < now:
                r.status, r.lease_owner, r.lease_expires_at = "pending", None, None
                r.available_at, r.last_release = now, "lease_expired"
                self.stats["lease_expirations"] += 1
        for r in list(self.runs.values()):
            if r.status == "waiting" and r.wake_at is not None and r.wake_at <= now:
                evs = self._check_waits(r, r.waiting_on)
                if evs:
                    self._wake(r, evs)
                else:
                    r.wake_at = None
        for a in list(self.approvals.values()):
            if a["status"] == "pending" and a.get("expires_at") and a["expires_at"] <= now:
                a.update(status="expired", decided_by="system:expiry", decided_at=now)
                self._wake_if_waiting(a["run_id"])

    def next_event_ms(self) -> int | None:
        now = self._now()
        ts: list[int] = []
        for r in self.runs.values():
            if r.status == "waiting" and r.wake_at is not None:
                ts.append(r.wake_at)
            elif r.status == "pending" and r.available_at > now:
                ts.append(r.available_at)
            elif r.status == "running" and r.lease_expires_at is not None:
                ts.append(r.lease_expires_at + 1)
        for a in self.approvals.values():
            if a["status"] == "pending" and a.get("expires_at"):
                ts.append(a["expires_at"])
        return min(ts) if ts else None

    def runnable_now(self, keys: set[str]) -> bool:
        now = self._now()
        return any(r.status == "pending" and r.available_at <= now and f"{r.workflow}@{r.version}" in keys
                   for r in self.runs.values())


# ====================================================================== workers and driver


class CrashingBackend:
    """Wraps a backend and simulates the worker process dying before or
    after its n-th write call (append, suspend or finish)."""

    def __init__(self, inner: Any, crash_at: int, when: str):
        self.inner = inner
        self.clock = inner.clock
        self.crash_at = crash_at
        self.when = when
        self.writes = 0
        self.crashed = False

    def _maybe(self, phase: str) -> None:
        if self.crashed:
            raise WorkerCrash()
        if self.writes == self.crash_at and self.when == phase:
            self.crashed = True
            raise WorkerCrash()

    async def _write(self, call: Callable[[], Awaitable[Any]]) -> Any:
        if self.crashed:
            raise WorkerCrash()
        self.writes += 1
        self._maybe("before")
        res = await call()
        self._maybe("after")
        return res

    async def poll(self, *a: Any, **k: Any) -> Any:
        if self.crashed:
            raise WorkerCrash()
        return await self.inner.poll(*a, **k)

    async def append(self, *a: Any, **k: Any) -> Any:
        return await self._write(lambda: self.inner.append(*a, **k))

    async def suspend(self, *a: Any, **k: Any) -> Any:
        return await self._write(lambda: self.inner.suspend(*a, **k))

    async def finish(self, *a: Any, **k: Any) -> Any:
        return await self._write(lambda: self.inner.finish(*a, **k))

    async def heartbeat(self, *a: Any, **k: Any) -> Any:
        if self.crashed:
            raise WorkerCrash()
        return await self.inner.heartbeat(*a, **k)

    async def evaluate_policy(self, *a: Any, **k: Any) -> Any:
        if self.crashed:
            raise WorkerCrash()
        return await self.inner.evaluate_policy(*a, **k)


class CountingBackend:
    def __init__(self, inner: Any):
        self.inner = inner
        self.clock = inner.clock
        self.writes = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def append(self, *a: Any, **k: Any) -> Any:
        self.writes += 1
        return await self.inner.append(*a, **k)

    async def suspend(self, *a: Any, **k: Any) -> Any:
        self.writes += 1
        return await self.inner.suspend(*a, **k)

    async def finish(self, *a: Any, **k: Any) -> Any:
        self.writes += 1
        return await self.inner.finish(*a, **k)


class LocalWorker:
    def __init__(self, server: LocalServer, registry: WorkflowRegistry, services: Services, worker_id: str,
                 backend: Any = None, max_concurrent: int = 100):
        self.server = server
        self.registry = registry
        self.services = services
        self.worker_id = worker_id
        self.backend = backend or server
        self.max_concurrent = max_concurrent
        self.running: dict[str, tuple[asyncio.Task, RunExecutor]] = {}
        self.alive = True
        self.frozen_until: int | None = None
        self.outcomes: list[tuple[str, str]] = []

    @property
    def keys(self) -> list[str]:
        return [d.key for d in self.registry.all()]

    def _reap(self) -> None:
        for rid, (t, _ex) in list(self.running.items()):
            if t.done():
                del self.running[rid]
                if t.cancelled():
                    continue
                exc = t.exception()
                if isinstance(exc, WorkerCrash):
                    self.kill()
                elif exc is not None:
                    raise exc
                else:
                    self.outcomes.append((rid, t.result()))

    async def poll_once(self) -> int:
        self._reap()
        if not self.alive or (self.frozen_until is not None and self.server.clock.now_ms() < self.frozen_until):
            return 0
        spawned = 0
        while len(self.running) < self.max_concurrent:
            try:
                task = await self.backend.poll(self.worker_id, self.keys, [], 0)
            except WorkerCrash:
                self.kill()
                return spawned
            if task is None:
                break
            wd = self.registry.get(task.workflow, task.version)
            assert wd is not None
            ex = RunExecutor(self.backend, task, wd, self.services, self.worker_id)
            t = asyncio.create_task(ex.execute())
            self.running[task.run_id] = (t, ex)
            spawned += 1
        return spawned

    def kill(self) -> None:
        """kill -9: every in-flight run is abandoned without another RPC."""
        self.alive = False
        for _t, ex in self.running.values():
            ex.crash = WorkerCrash()
            if ex.root_task is not None and not ex.root_task.done():
                ex.root_task.cancel()
        for t, _ in self.running.values():
            t.cancel()


async def quiesce(rounds: int = 3) -> None:
    """Yield until no task is ready to run (everything is blocked on the
    virtual clock or on the driver)."""
    loop = asyncio.get_running_loop()
    idle = 0
    while idle < rounds:
        await asyncio.sleep(0)
        if getattr(loop, "_ready", None):
            idle = 0
        else:
            idle += 1


class LocalEnv:
    """A complete in-process deployment: LocalServer + workers + mock LLM."""

    def __init__(self, workflows: list[Any] | None = None, *, tools: ToolRegistry | None = None,
                 llm: Any = None, brain: Callable[[Any], dict] | None = None, policy: Any = None, seed: int = 0,
                 dev_mode: bool = True, secrets: dict | SecretStore | None = None, default_model: str = "mock",
                 workers: int = 1, max_concurrent: int = 100, lease_ttl_ms: int = 30_000,
                 inline_backoff_ms: int = 5000, batch_local_steps: bool = True):
        if dev_mode:
            determinism.install()
        self.server = LocalServer(policy=policy, seed=seed, lease_ttl_ms=lease_ttl_ms)
        self.clock = self.server.clock
        self.registry = WorkflowRegistry()
        for w in workflows or WORKFLOWS.all():
            self.registry.add(w if isinstance(w, WorkflowDef) else workflow_def(w))
        if llm is None and brain is not None:
            llm = MockLLM(brain, clock=self.clock)
        self.services = Services(tools=tools or DEFAULT_REGISTRY, llm=llm,
                                 secrets=secrets if isinstance(secrets, SecretStore) else SecretStore(secrets or {}, env_prefix="\0"),
                                 rng=random.Random(seed + 1), dev_mode=dev_mode, default_model=default_model,
                                 inline_backoff_ms=inline_backoff_ms, batch_local_steps=batch_local_steps)
        self.max_concurrent = max_concurrent
        self._worker_seq = 0
        self.workers: list[LocalWorker] = []
        self._crash_plan: tuple[int, str] | None = None
        self.backends: list[Any] = []
        for _ in range(workers):
            self.add_worker()
        self.server.register("init", [d.key for d in self.registry.all()])
        self.auto_approve: Callable[[dict], tuple[str, dict | None]] | None = None

    def add_worker(self) -> LocalWorker:
        self._worker_seq += 1
        backend: Any = CountingBackend(self.server)
        if self._crash_plan is not None:
            backend = CrashingBackend(self.server, *self._crash_plan)
            self._crash_plan = None
        self.backends.append(backend)
        w = LocalWorker(self.server, self.registry, self.services, f"worker-{self._worker_seq}", backend,
                        self.max_concurrent)
        self.workers.append(w)
        return w

    def arm_crash(self, at_write: int, when: str = "after") -> None:
        """The current worker dies before/after its n-th write; a replacement starts."""
        self._crash_plan = (at_write, when)
        for w in self.workers:
            w.alive = False
        self.workers = [w for w in self.workers if w.running]
        self.add_worker()

    @property
    def writes(self) -> int:
        return sum(getattr(b, "writes", 0) for b in self.backends)

    # client conveniences
    def start(self, wf: Any, input: Any = None, **kw: Any) -> str:  # noqa: A002
        if isinstance(wf, str):
            name = wf
            kw.setdefault("version", (self.registry.latest(name) or workflow_def(wf)).version)
        else:
            d = workflow_def(wf)
            name = d.name
            kw.setdefault("version", d.version)
        from .serde import to_jsonable
        rid, _ = self.server.start_run(name, to_jsonable(input), **kw)
        return rid

    def run_state(self, rid: str) -> RunState:
        return self.server.runs[rid]

    def events(self, rid: str) -> list[Event]:
        return self.server.history(rid)

    def approvals(self, status: str = "pending") -> list[dict]:
        return [a for a in self.server.approvals.values() if a["status"] == status]

    def approve(self, approval_id: str, edited_args: dict | None = None, by: str = "tester") -> bool:
        return self.server.decide(approval_id, "approved", edited_args, by)

    def reject(self, approval_id: str, by: str = "tester", comment: str = "") -> bool:
        return self.server.decide(approval_id, "rejected", None, by, comment)

    def signal(self, rid: str, name: str, payload: Any = None, signal_id: str | None = None) -> bool:
        return self.server.signal(rid, name, payload, signal_id)

    async def run_until_idle(self, *, max_virtual_s: float = 400 * 86400, max_iterations: int = 200_000,
                             stop_when: Callable[[], bool] | None = None) -> None:
        """Run everything until no work is left (advancing virtual time to the
        next timer/lease/expiry), or until ``stop_when()`` is true before a
        time jump."""
        start = self.clock.now_ms()
        def auto() -> int:
            n = 0
            if self.auto_approve is not None:
                for a in self.server.pending_approvals():
                    decision, edits = self.auto_approve(a)
                    if decision:
                        self.server.decide(a["approval_id"], decision, edits, "auto")
                        n += 1
            return n

        for _ in range(max_iterations):
            auto()
            # Claim and run until nothing new can start at this instant.
            for _round in range(10_000):
                if not any(w.alive for w in self.workers):
                    self.add_worker()
                spawned = 0
                for w in list(self.workers):
                    spawned += await w.poll_once()
                await quiesce()
                for w in self.workers:
                    w._reap()
                if spawned == 0 and any(w.alive for w in self.workers):
                    break
            if auto():
                continue
            if stop_when is not None and stop_when():
                return
            cands = [t for t in (self.clock.next_wakeup_ms(), self.server.next_event_ms()) if t is not None]
            if not cands:
                return
            nxt = max(min(cands), self.clock.now_ms())
            if nxt - start > max_virtual_s * 1000:
                return
            self.clock.advance_to(nxt)
            self.server.tick()
            await quiesce()
        raise RuntimeError("run_until_idle did not settle")

    async def run_until_waiting(self, rid: str, kind: str = "") -> None:
        """Run until ``rid`` waits (optionally on a given kind) or finishes."""
        def waiting() -> bool:
            r = self.server.runs[rid]
            return r.status in FINAL or (r.status == "waiting" and kind in (r.wait_reason or ""))
        await self.run_until_idle(stop_when=waiting)

    async def run(self, wf: Any, input: Any = None, **kw: Any) -> RunState:  # noqa: A002
        rid = self.start(wf, input, **kw)
        await self.run_until_idle()
        return self.server.runs[rid]


# ====================================================================== crash testing


@dataclass
class CrashReport:
    baseline_status: str
    baseline_result: Any
    crash_points: int
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


async def run_with_crashes(wf: Any, input: Any = None, *,  # noqa: A002
                           env_factory: Callable[[], LocalEnv],
                           check: Callable[[LocalEnv, str], None] | None = None,
                           compare: Callable[[Any, Any], bool] | None = None,
                           start_kwargs: dict | None = None,
                           setup: Callable[[LocalEnv], None] | None = None,
                           after_start: Callable[[LocalEnv, str], None] | None = None) -> CrashReport:
    """Re-run ``wf`` crashing the worker before and after every write in turn
    and check every crashed run reaches the same result as an uninterrupted
    one (and that ``check`` holds, e.g. exactly one refund)."""
    kw = start_kwargs or {}
    env = env_factory()
    if setup:
        setup(env)
    rid = env.start(wf, input, **kw)
    if after_start:
        after_start(env, rid)
    await env.run_until_idle()
    base = env.run_state(rid)
    if check:
        check(env, rid)
    total = env.writes
    report = CrashReport(base.status, base.result, total * 2)
    same = compare or (lambda a, b: a == b)
    for k in range(1, total + 1):
        for when in ("before", "after"):
            env = env_factory()
            if setup:
                setup(env)
            env.arm_crash(k, when)
            rid = env.start(wf, input, **kw)
            if after_start:
                after_start(env, rid)
            try:
                await env.run_until_idle()
            except Exception as e:  # noqa: BLE001
                report.failures.append(f"crash {when} write {k}: driver error {e!r}")
                continue
            st = env.run_state(rid)
            if st.status != base.status or not same(st.result, base.result):
                report.failures.append(f"crash {when} write {k}: got {st.status} {st.result!r}, "
                                       f"expected {base.status} {base.result!r}")
                continue
            if check:
                try:
                    check(env, rid)
                except AssertionError as e:
                    report.failures.append(f"crash {when} write {k}: invariant failed: {e}")
    return report


def check_log_invariants(server: LocalServer) -> list[str]:
    """Contiguous sequence numbers and non-decreasing epochs in every log."""
    problems = []
    for rid, log in server.events.items():
        for i, e in enumerate(log):
            if e.seq != i + 1:
                problems.append(f"{rid}: seq {e.seq} at position {i + 1}")
                break
        last = 0
        for e in log:
            if e.epoch < last:
                problems.append(f"{rid}: epoch {e.epoch} after {last} at seq {e.seq}")
                break
            last = e.epoch
    return problems
