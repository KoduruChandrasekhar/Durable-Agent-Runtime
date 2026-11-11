"""The journal: a run's event log folded into lookup tables for replay, plus
the nondeterminism checks.

Every operation a workflow requests gets an id at call time. Requests made
from the workflow's main task also get an *order* index; the k-th ordered
request during replay must match the k-th recorded one in id, kind and input
hash. Requests from concurrent branches carry explicit keys and are matched by
id instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import NonDeterminismError
from .protocol import Event


@dataclass
class StepRecord:
    step_id: str
    kind: str = ""
    order: int | None = None
    input_hash: str | None = None
    attempts: int = 0
    completed: bool = False
    result: Any = None
    reconciled: bool = False
    failures: list[dict] = field(default_factory=list)
    started_open: bool = False  # a StepStarted with no StepCompleted/StepFailed after it

    @property
    def final_error(self) -> dict | None:
        if self.failures and not self.completed and not self.started_open:
            last = self.failures[-1]
            if not last.get("retryable"):
                return last.get("error") or {"type": "Error", "message": "step failed"}
        return None

    @property
    def next_retry_at_ms(self) -> int | None:
        if self.failures and not self.completed and not self.started_open:
            last = self.failures[-1]
            if last.get("retryable"):
                return int(last.get("next_retry_at_ms") or 0)
        return None


class Journal:
    def __init__(self, history: list[Event]):
        self.steps: dict[str, StepRecord] = {}
        self.ordered: dict[int, tuple[str, str, str | None]] = {}
        self.max_order = 0
        self.values: dict[str, Any] = {}
        self.timers_started: dict[str, int] = {}
        self.timers_fired: set[str] = set()
        self.signals: dict[str, dict] = {}
        self.approvals_requested: dict[str, dict] = {}
        self.approvals_resolved: dict[str, dict] = {}
        self.policy: dict[str, list[dict]] = {}
        self.children_started: dict[str, dict] = {}
        self.children_completed: dict[str, dict] = {}
        self.comp_started: set[str] = set()
        self.comp_completed: dict[str, Any] = {}
        self.facts_recorded: set[str] = set()
        self.outbox: set[str] = set()
        self.tainted = False
        self.run_started: dict = {}
        self.forked: dict | None = None
        self.resumes = 0
        self.last_seq = 0
        self.length = len(history)
        for e in history:
            self._apply(e)

    def _ordered(self, order: Any, op_id: str, kind: str, input_hash: str | None) -> None:
        if order is None:
            return
        o = int(order)
        if o not in self.ordered:
            self.ordered[o] = (op_id, kind, input_hash)
        self.max_order = max(self.max_order, o)

    def _apply(self, e: Event) -> None:
        p = e.payload or {}
        sid = e.step_id
        self.last_seq = max(self.last_seq, e.seq)
        t = e.type
        if t == "RunStarted":
            self.run_started = p
        elif t == "RunResumed":
            self.resumes += 1
        elif t == "RunForked":
            self.forked = p
        elif t == "StepStarted":
            rec = self.steps.setdefault(sid, StepRecord(step_id=sid))
            rec.kind = p.get("kind", rec.kind)
            rec.attempts = max(rec.attempts, int(p.get("attempt") or rec.attempts + 1))
            if rec.order is None and p.get("order") is not None:
                rec.order = int(p["order"])
            if rec.input_hash is None:
                rec.input_hash = p.get("input_hash")
            rec.started_open = True
            self._ordered(p.get("order"), sid, rec.kind, p.get("input_hash"))
        elif t == "StepCompleted":
            rec = self.steps.setdefault(sid, StepRecord(step_id=sid))
            rec.completed = True
            rec.result = p.get("output")
            rec.reconciled = bool(p.get("reconciled"))
            rec.started_open = False
            rec.kind = rec.kind or p.get("kind", "")
        elif t == "StepFailed":
            rec = self.steps.setdefault(sid, StepRecord(step_id=sid))
            rec.failures.append(p)
            rec.started_open = False
        elif t == "ValueRecorded":
            self.values[sid] = p.get("value")
            self._ordered(p.get("order"), sid, "value:" + str(p.get("kind", "")), None)
        elif t == "TimerStarted":
            self.timers_started[sid] = int(p.get("fire_at_ms") or 0)
            self._ordered(p.get("order"), sid, "timer", None)
        elif t == "TimerFired":
            self.timers_fired.add(p.get("timer_id") or sid)
        elif t == "SignalReceived":
            self.signals[p.get("wait_id") or sid] = p
        elif t == "ApprovalRequested":
            self.approvals_requested[p.get("approval_id", "")] = p
        elif t == "ApprovalResolved":
            self.approvals_resolved[p.get("approval_id", "")] = p
        elif t == "PolicyDecided":
            self.policy.setdefault(sid, []).append(p)
        elif t == "ChildRunStarted":
            self.children_started[p.get("child_run_id", "")] = p
            self._ordered(p.get("order"), sid, "child", p.get("input_hash"))
        elif t == "ChildRunCompleted":
            self.children_completed[p.get("child_run_id") or sid] = p
        elif t == "CompensationStarted":
            self.comp_started.add(sid)
        elif t == "CompensationCompleted":
            self.comp_completed[sid] = p.get("result")
        elif t == "RunTainted":
            self.tainted = True
        elif t == "FactRecorded":
            self.facts_recorded.add(sid)
            self._ordered(p.get("order"), sid, "fact", None)
        elif t == "OutboxMessage":
            self.outbox.add(sid)

    # -------------------------------------------------------------- checks

    def check_ordered(self, order: int | None, op_id: str, kind: str, input_hash: str | None) -> bool:
        """Returns True if this ordered request is recorded (i.e. replaying)."""
        if order is None:
            return False
        rec = self.ordered.get(order)
        if rec is None:
            return False
        rid, rkind, rhash = rec
        if rid != op_id or rkind != kind:
            raise NonDeterminismError(
                f"replay diverged at request #{order}: the code asked for {kind} {op_id!r} but the "
                f"history recorded {rkind} {rid!r} (reordered or removed steps, or a nondeterministic decision)")
        if rhash and input_hash and rhash != input_hash:
            raise NonDeterminismError(
                f"replay diverged at request #{order} ({op_id}): the step input changed "
                f"(hash {input_hash} != recorded {rhash})")
        return True

    def check_keyed(self, op_id: str, kind: str, input_hash: str | None) -> None:
        rec = self.steps.get(op_id)
        if rec is None or not rec.kind:
            return
        if rec.kind != kind:
            raise NonDeterminismError(f"step {op_id!r} was recorded as {rec.kind}, now requested as {kind}")
        if rec.input_hash and input_hash and rec.input_hash != input_hash:
            raise NonDeterminismError(f"step {op_id!r}: the step input changed "
                                      f"(hash {input_hash} != recorded {rec.input_hash})")
