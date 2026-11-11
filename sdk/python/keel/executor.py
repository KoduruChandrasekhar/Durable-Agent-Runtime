"""Executes one run on a worker: rebuild the journal from the event log,
re-run the workflow from the top (recorded steps return instantly), then
execute new steps live, appending their events under the lease epoch.

The four cases for every step (Part 2 of the design):

* StepCompleted recorded           -> return the recorded result
* last attempt failed, retries left -> wait for the recorded retry time, run again
* StepStarted with no result        -> the previous worker crashed mid-step:
                                       re-run with the same idempotency key
                                       (or reconcile first)
* not in the journal                -> replay is over: StepStarted, run, StepCompleted
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import random
import time
import uuid as _uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from . import determinism, errors
from .errors import ControlFlow, LeaseLost, StepError, Suspend, WorkerCrash
from .guard import SecretStore
from .llm import ChatRequest, LLMReply, Message
from .protocol import Backend, NewEvent, PolicyDecision, RunTask, TransientBackendError, Wait
from .replay import Journal
from .retry import DEFAULT, FOREVER, RetryPolicy
from .serde import error_json, from_jsonable, stable_hash, to_jsonable
from .telemetry import METRICS, span
from .tools import DEFAULT_REGISTRY, ToolFunction, ToolRegistry, ToolResult, ToolSpec
from .workflow import KEEL_NS, Context, WorkflowDef

log = logging.getLogger("keel.executor")

LLM_RETRY = RetryPolicy(max_attempts=4, initial=1.0, max_interval=20.0)


@dataclass
class Services:
    """Everything a run needs besides the backend."""

    tools: ToolRegistry = field(default_factory=lambda: DEFAULT_REGISTRY)
    llm: Callable[[ChatRequest], Any] | None = None
    secrets: SecretStore = field(default_factory=SecretStore)
    default_model: str = "router:default"
    default_retry: RetryPolicy = DEFAULT
    llm_retry: RetryPolicy = LLM_RETRY
    rng: random.Random = field(default_factory=random.SystemRandom)
    inline_backoff_ms: int = 5000
    dev_mode: bool = False
    batch_local_steps: bool = True


def _sdk_context() -> contextvars.Context:
    """A copy of the current context (keeps the trace) marked as SDK code."""
    c = contextvars.copy_context()
    c.run(determinism.IN_WORKFLOW.set, False)
    return c


def _dry_run_stub(spec: ToolSpec) -> Callable[..., Any]:
    async def stub(**kwargs: Any) -> Any:
        kwargs.pop("key", None)
        kwargs.pop("secrets", None)
        return {"dry_run": True, "tool": spec.name, "args": kwargs}
    return stub


class RunExecutor:
    def __init__(self, backend: Backend, task: RunTask, wdef: WorkflowDef, services: Services, worker_id: str):
        self.backend = backend
        self.task = task
        self.wdef = wdef
        self.services = services
        self.worker_id = worker_id
        self.clock = backend.clock
        self.rng = services.rng
        self.journal = Journal(task.history)
        self.next_seq = task.next_seq
        self.epoch = task.epoch
        self.live = False
        self.appended = 0
        self.dead = False
        self.crash: BaseException | None = None
        self.cancel_requested = task.cancel_requested
        self.compensating = False
        self.suspending = False
        self.draining = False
        self.waits: list[Wait] = []
        self.retry_at_ms: int | None = None
        self.inflight = 0
        self._idle = asyncio.Event()
        self.root_task: asyncio.Task | None = None
        self._queue: list[NewEvent] = []
        self._queue_futs: list[asyncio.Future] = []
        self._flusher: asyncio.Task | None = None
        self._claimed_at = time.perf_counter()
        self.replay_ms: float | None = None
        self.ctx: Context | None = None
        self.outcome = ""
        self._requested_approvals: set[str] = set()
        self._versions: dict[str, int] = {}

    # ================================================================= plumbing

    def mark_dead(self) -> None:
        self.dead = True
        if self.root_task is not None and not self.root_task.done():
            self.root_task.cancel()

    def check_alive(self) -> None:
        if self.crash is not None:
            raise self.crash
        if self.dead:
            raise LeaseLost()
        if self.suspending and not self.compensating:
            raise Suspend("wait")

    def go_live(self) -> None:
        """Called right before the first operation that is not in the journal."""
        if not self.live:
            self.live = True
            self.replay_ms = (time.perf_counter() - self._claimed_at) * 1000
            METRICS.replay_duration(self.replay_ms / 1000, self.journal.length)
        if self.compensating:
            return
        if self.cancel_requested:
            raise errors.RunCancelled()
        if self.draining:
            self.suspending = True
            raise Suspend("handback")
        lim = self.wdef.limits.max_duration_s
        if lim and self.task.created_at_ms and self.clock.now_ms() - self.task.created_at_ms > lim * 1000:
            raise errors.LimitExceeded(f"run exceeded its wall-clock limit of {lim}s")

    def _enter(self) -> None:
        self.inflight += 1
        self._idle.clear()

    def _exit(self) -> None:
        self.inflight -= 1
        if self.inflight <= 0:
            self._idle.set()

    def enqueue(self, events: list[NewEvent], wait: bool = True) -> asyncio.Future | None:
        secrets = self.services.secrets
        for e in events:
            e.payload = secrets.redact(e.payload)
        self._queue.extend(events)
        if not wait:
            return None
        fut = asyncio.get_running_loop().create_future()
        self._queue_futs.append(fut)
        if self._flusher is None or self._flusher.done():
            self._flusher = asyncio.create_task(self._flush_loop(), context=_sdk_context())
        return fut

    async def commit(self, events: list[NewEvent]) -> None:
        """Append events (plus any buffered values) and wait until durable."""
        fut = self.enqueue(events)
        assert fut is not None
        await fut

    async def flush(self) -> None:
        if self._queue:
            await self.commit([])

    async def _flush_loop(self) -> None:
        # Group commit: everything queued while an append is in flight goes
        # out in the next single append.
        while self._queue_futs:
            batch, futs = self._queue, self._queue_futs
            self._queue, self._queue_futs = [], []
            try:
                if batch:
                    await self._append(batch)
            except BaseException as e:  # noqa: BLE001 - deliver to every waiter
                for f in futs + self._queue_futs:
                    if not f.done():
                        f.set_exception(e)
                self._queue_futs = []
                return
            for f in futs:
                if not f.done():
                    f.set_result(None)

    async def _append(self, batch: list[NewEvent]) -> None:
        if self.crash is not None:
            raise self.crash
        if self.dead:
            raise LeaseLost()
        try:
            res = await self.backend.append(self.task.run_id, self.epoch, self.next_seq, batch)
        except WorkerCrash as c:
            self.crash = c
            raise
        except TransientBackendError:
            # The server stayed unreachable past the client's retry window:
            # abandon the run; its lease expires and another worker resumes it.
            self.mark_dead()
            raise LeaseLost() from None
        if not res.ok:
            self.mark_dead()
            raise LeaseLost()
        self.next_seq = res.next_seq
        self.appended += len(batch)
        if res.cancel_requested:
            self.cancel_requested = True
        self.clock.observe_server_time(res.server_now_ms) if hasattr(self.clock, "observe_server_time") else None

    def wait_on(self, waits: list[Wait]) -> None:
        if self.cancel_requested and not self.compensating:
            raise errors.RunCancelled()
        self.waits.extend(waits)
        self.suspending = True
        raise Suspend("wait")

    async def backoff_until(self, t_ms: int | None) -> None:
        if not t_ms:
            return
        wait = t_ms - self.clock.now_ms()
        if wait <= 0:
            return
        if wait <= self.services.inline_backoff_ms and not self.draining:
            with determinism.sdk_code():
                await self.clock.sleep(wait / 1000)
            return
        self.retry_at_ms = t_ms if self.retry_at_ms is None else min(self.retry_at_ms, t_ms)
        self.suspending = True
        raise Suspend("retry", available_at_ms=t_ms)

    def resolve_tool(self, t: Any) -> ToolSpec:
        if isinstance(t, ToolSpec):
            return t
        if isinstance(t, ToolFunction):
            return t.spec
        if isinstance(t, str):
            return self.services.tools.get(t)
        raise TypeError(f"not a tool: {t!r}")

    # ================================================================= steps

    async def run_step(self, sid: str, order: int | None, kind: str, fn: Callable[..., Any], args: tuple,
                       kwargs: dict, policy: RetryPolicy, *, hash_input: Any, record_input: Any, ret_type: Any = None,
                       timeout: float | None = None, batch: bool = False, extra_started: dict | None = None,
                       pre_events: list[NewEvent] | None = None, reconcile: Callable[[], Any] | None = None,
                       extra_completed: Callable[[Any], dict] | None = None, checked: bool = False) -> Any:
        self._enter()
        try:
            return await self._run_step(sid, order, kind, fn, args, kwargs, policy, hash_input, record_input,
                                        ret_type, timeout, batch and self.services.batch_local_steps,
                                        extra_started or {}, pre_events or [], reconcile, extra_completed, checked)
        finally:
            self._exit()

    async def _run_step(self, sid, order, kind, fn, args, kwargs, policy, hash_input, record_input, ret_type,
                        timeout, batch, extra_started, pre_events, reconcile, extra_completed, checked):
        self.check_alive()
        ih = stable_hash(hash_input)
        if not checked:
            if order is not None:
                self.journal.check_ordered(order, sid, kind, ih)
            else:
                self.journal.check_keyed(sid, kind, ih)
        rec = self.journal.steps.get(sid)
        if rec is not None and rec.completed:
            return from_jsonable(rec.result, ret_type)
        if rec is not None and rec.final_error is not None:
            raise StepError.from_record(sid, rec.final_error)
        self.go_live()
        attempt = rec.attempts if rec else 0
        reattempt = rec is not None
        after_crash = rec is not None and rec.started_open
        if rec is not None and rec.next_retry_at_ms:
            await self.backoff_until(rec.next_retry_at_ms)
        record = to_jsonable(record_input)
        while True:
            attempt += 1
            started = NewEvent("StepStarted", sid, {"kind": kind, "attempt": attempt, "order": order,
                                                    "input_hash": ih, "input": record, **extra_started})
            if not batch:
                await self.commit([*pre_events, started])
                pre_events = []
            t0 = time.perf_counter()
            reconciled = False
            try:
                with determinism.sdk_code(), span(f"step {sid}", run_id=self.task.run_id, step_id=sid,
                                                  kind=kind, attempt=attempt):
                    found = None
                    if reattempt and reconcile is not None:
                        found = await reconcile()
                    if found is not None:
                        result, reconciled = found, True
                    else:
                        res = fn(*args, **kwargs)
                        if inspect.isawaitable(res):
                            res = await (asyncio.wait_for(res, timeout) if timeout else res)
                        result = res
                out = to_jsonable(result)
            except ControlFlow:
                raise
            except Exception as e:
                dur = (time.perf_counter() - t0) * 1000
                retryable = policy.is_retryable(e) and attempt < policy.max_attempts
                delay_ms = 0
                if retryable:
                    delay_ms = int(policy.delay(attempt, self.rng) * 1000)
                    ra = getattr(e, "retry_after", None)
                    if ra:
                        delay_ms = max(delay_ms, int(float(ra) * 1000))
                next_at = self.clock.now_ms() + delay_ms if retryable else None
                failed = NewEvent("StepFailed", sid, {"kind": kind, "attempt": attempt, "retryable": retryable,
                                                      "next_retry_at_ms": next_at, "duration_ms": round(dur, 3),
                                                      "error": error_json(e)})
                await self.commit([*pre_events, started, failed] if batch else [failed])
                pre_events = []
                METRICS.step(kind, dur / 1000, failed=True)
                if not retryable:
                    raise StepError(sid, type(e).__name__, str(e)[:2000]) from e
                reattempt = True
                await self.backoff_until(next_at)
                continue
            dur = (time.perf_counter() - t0) * 1000
            payload = {"kind": kind, "attempt": attempt, "output": out, "duration_ms": round(dur, 3)}
            if reconciled:
                payload["reconciled"] = True
            if after_crash:
                payload["reexecuted_after_crash"] = True
                usage = getattr(result, "usage", None)
                if kind == "llm" and usage is not None:
                    # The interrupted call was paid for and its answer lost.
                    METRICS.wasted_tokens(usage.input_tokens + usage.output_tokens)
            if extra_completed is not None:
                payload.update(extra_completed(result))
            completed = NewEvent("StepCompleted", sid, payload)
            await self.commit([*pre_events, started, completed] if batch else [completed])
            METRICS.step(kind, dur / 1000)
            return from_jsonable(out, ret_type)

    # ================================================================= LLM

    def _model_for(self, requested: str) -> str:
        ov = self.task.overrides or {}
        models = ov.get("models") or {}
        return models.get(requested) or ov.get("model") or requested

    async def run_llm(self, sid: str, order: int | None, msgs: list[Message], requested: str, specs: list[ToolSpec],
                      *, temperature: float, max_tokens: int, response_format: dict | None, quarantined: bool,
                      stream: bool, retry: RetryPolicy | None, cacheable: bool | None) -> LLMReply:
        if self.services.llm is None:
            raise errors.LLMError("no_client", "this worker has no LLM client configured")
        trusts = [m.effective_trust() for m in msgs]
        sources = sorted({(m.source or m.role) for m, t in zip(msgs, trusts, strict=True) if t == "untrusted"}
                         | {f"mcp:{s.name}" for s in specs if s.source == "mcp"})
        taints = bool(sources) and not quarantined
        wire = [m.wire() for m in msgs]
        tool_names = [s.name for s in specs]
        hash_input = {"model": requested, "messages": wire, "tools": tool_names, "temperature": temperature,
                      "max_tokens": max_tokens, "response_format": response_format}
        record = {"model": requested, "tools": tool_names, "quarantined": quarantined,
                  "params": {"temperature": temperature, "max_tokens": max_tokens},
                  "response_format": response_format,
                  "messages": [{**w, "trust": t, **({"source": m.source} if m.source else {})}
                               for w, t, m in zip(wire, trusts, msgs, strict=True)]}
        pre: list[NewEvent] = []
        ctx = self.ctx
        if taints and not self.journal.tainted and ctx is not None and not ctx._taint_sent:
            ctx._taint_sent = True
            pre.append(NewEvent("RunTainted", sid, {"step_id": sid, "sources": sources}))
        llm = self.services.llm

        async def call() -> LLMReply:
            req = ChatRequest(run_id=self.task.run_id, tenant_id=self.task.tenant_id, step_key=sid,
                              model=self._model_for(requested), messages=wire,
                              tools=[s.openai_schema() for s in specs] or None, temperature=temperature,
                              max_tokens=max_tokens, response_format=response_format, stream=stream,
                              cacheable=cacheable, metadata={"workflow": self.task.workflow,
                                                             "version": self.task.version,
                                                             "dry_run": self.task.dry_run, "tainted": taints,
                                                             "quarantined": quarantined})
            reply = await llm(req)
            reply.tainted = taints
            return reply

        def completed_extra(r: LLMReply) -> dict:
            METRICS.llm(r)
            return {"cost_usd": r.cost_usd, "model": r.model, "provider": r.provider,
                    "tokens": {"input": r.usage.input_tokens, "output": r.usage.output_tokens},
                    "cache_hit": r.cache_hit, "tainted": taints}

        return await self.run_step(sid, order, "llm", call, (), {}, retry or self.services.llm_retry,
                                   hash_input=hash_input, record_input=record, ret_type=LLMReply,
                                   timeout=180.0, extra_started={"tainted": taints, "model": requested},
                                   pre_events=pre, extra_completed=completed_extra)

    # ================================================================= tools

    async def run_tool(self, spec: ToolSpec, args: dict, sid: str, order: int | None) -> Any:
        self._enter()
        try:
            return await self._run_tool(spec, args, sid, order)
        finally:
            self._exit()

    async def _run_tool(self, spec: ToolSpec, args: dict, sid: str, order: int | None) -> Any:
        self.check_alive()
        ctx = self.ctx
        assert ctx is not None
        # 1. Schema validation before anything runs.
        try:
            args = spec.validate(args)
        except errors.ToolValidationError:
            n = ctx._repairs.get(spec.name, 0) + 1
            ctx._repairs[spec.name] = n
            if n > ctx.limits.max_repairs:
                raise errors.LimitExceeded(
                    f"{spec.name}: arguments still invalid after {ctx.limits.max_repairs} repair attempts") from None
            raise
        ctx._repairs[spec.name] = 0
        ih = stable_hash({"tool": spec.name, "args": args})
        if order is not None:
            self.journal.check_ordered(order, sid, "tool", ih)
        else:
            self.journal.check_keyed(sid, "tool", ih)
        # 2. Loop detection: same tool, same arguments, three times in a row.
        ctx._tool_window.append((spec.name, ih))
        ctx._tool_window = ctx._tool_window[-3:]
        if len(ctx._tool_window) == 3 and len(set(ctx._tool_window)) == 1:
            raise errors.LoopDetected(f"{spec.name} called three times in a row with the same arguments")
        # 3. Per-tool limit per run.
        c = ctx._tool_counts.get(spec.name, 0) + 1
        ctx._tool_counts[spec.name] = c
        if spec.max_per_run is not None and c > spec.max_per_run:
            raise errors.ToolLimitExceeded(f"{spec.name} may run at most {spec.max_per_run} times per run")
        # 4. Policy, possibly a durable human approval.
        final_args = await self._authorize(spec, args, sid)
        # 5. Execute with the idempotency key run_id:step_id. The step's input
        # hash covers the arguments the model proposed; a human's edits are
        # recorded but must not change the hash replay compares against.
        return await self._exec_tool(spec, final_args, sid, order, args, spec.retry)

    async def _exec_tool(self, spec: ToolSpec, args: dict, sid: str, order: int | None, proposed: dict,
                         policy: RetryPolicy) -> Any:
        key = f"{self.task.run_id}:{sid}"
        kwargs = dict(args)
        if spec.takes_key:
            kwargs["key"] = key
        if spec.takes_secrets:
            kwargs["secrets"] = self.services.secrets.resolve(spec.secrets)
        fn = spec.fn
        if self.task.dry_run and spec.effect != "none":
            fn = spec.dry_run or _dry_run_stub(spec)
        reconcile = None
        if spec.effect == "reconcile" and spec.reconcile is not None and not self.task.dry_run:
            rfn = spec.reconcile

            async def reconcile() -> Any:
                res = rfn(key=key, **args)
                return await res if inspect.isawaitable(res) else res
        ret_type = None if (self.task.dry_run and spec.effect != "none" and spec.dry_run is None) else spec.output_type
        return await self.run_step(
            sid, order, "tool", fn, (), kwargs, policy, hash_input={"tool": spec.name, "args": proposed},
            record_input={"tool": spec.name, "args": args, "idempotency_key": key, "effect": spec.effect,
                          "risk": spec.risk, "dry_run": self.task.dry_run,
                          **({"proposed_args": proposed} if proposed != args else {})},
            ret_type=ret_type, timeout=spec.timeout,
            extra_started={"tool": spec.name, "idempotency_key": key, "effect": spec.effect, "risk": spec.risk},
            reconcile=reconcile, checked=True)

    async def _authorize(self, spec: ToolSpec, args: dict, sid: str) -> dict:
        approved_rules: list[str] = []
        current = args
        n = 0
        while True:
            recorded = self.journal.policy.get(sid, [])
            if n < len(recorded):
                d = PolicyDecision.from_json(recorded[n])
            else:
                self.go_live()
                await self.flush()  # facts must be durable before the server reads them
                try:
                    d = await self.backend.evaluate_policy(self.task.run_id, self.epoch, spec.name, spec.risk,
                                                           current, sid, approved_rules)
                except TransientBackendError:
                    self.mark_dead()
                    raise LeaseLost() from None
                if d.lease_lost:
                    self.mark_dead()
                    raise LeaseLost()
                await self.commit([NewEvent("PolicyDecided", sid, {**d.to_json(), "n": n, "tool": spec.name,
                                                                   "args": current})])
                METRICS.policy(d)
            n += 1
            if d.decision == "allow":
                return current
            if d.decision == "deny":
                raise errors.PolicyDenied(spec.name, d.reason, d.rule_ids)
            approval_id = str(_uuid.uuid5(KEEL_NS, f"{self.task.run_id}:{sid}:{n}"))
            res = self.journal.approvals_resolved.get(approval_id)
            if res is None:
                if approval_id not in self.journal.approvals_requested and approval_id not in self._requested_approvals:
                    self.go_live()
                    expires = self.clock.now_ms() + d.expires_in_ms if d.expires_in_ms else 0
                    summary = {"workflow": self.task.workflow, "version": self.task.version,
                               "tainted": self.journal.tainted or bool(self.ctx and self.ctx._taint_sent),
                               "input": self.task.input, "steps_so_far": sorted(self.journal.steps)[-12:]}
                    await self.commit([NewEvent("ApprovalRequested", sid, {
                        "approval_id": approval_id, "step_id": sid,
                        "action": {"tool": spec.name, "args": current, "risk": spec.risk,
                                   "schema": spec.json_schema()},
                        "reason": d.reason, "rule_ids": d.rule_ids, "approvers": d.approvers,
                        "expires_at_ms": expires, "on_expiry": d.on_expiry, "summary": summary})])
                    self._requested_approvals.add(approval_id)
                self.wait_on([Wait("approval", approval_id)])
            assert res is not None  # wait_on raised otherwise
            decision = res.get("decision")
            if decision == "expired":
                decision = {"reject": "rejected", "approve": "approved", "escalate": "escalate"}.get(
                    res.get("on_expiry") or "reject", "rejected")
            if decision == "approved":
                edited = res.get("edited_args")
                if edited:
                    try:
                        current = spec.validate({**current, **edited})
                    except errors.ToolValidationError as e:
                        raise errors.ApprovalRejected(spec.name, approval_id, "approved with invalid edits",
                                                      res.get("decided_by", ""), str(e.errors)) from None
                approved_rules = approved_rules + d.rule_ids  # deny rules still apply on re-evaluation
                continue
            if decision == "escalate":
                continue  # re-evaluate: a fresh approval request goes out
            raise errors.ApprovalRejected(spec.name, approval_id, str(decision), res.get("decided_by", ""),
                                          res.get("comment", ""))

    async def run_tool_batch(self, targets: list[tuple]) -> list[ToolResult]:
        recoverable = (errors.ToolValidationError, errors.PolicyDenied, errors.ApprovalRejected, StepError,
                       errors.ToolNotFound, errors.ToolLimitExceeded)

        async def one(call: Any, spec: ToolSpec | None, a: dict | None, sid: str | None, order: int | None,
                      err: Exception | None) -> ToolResult:
            if err is not None:
                return ToolResult(call_id=call.id, name=call.name, ok=False, error=str(err), error_type=type(err).__name__)
            try:
                out = await self.run_tool(spec, a or {}, sid or "", order)  # type: ignore[arg-type]
                return ToolResult(call_id=call.id, name=call.name, ok=True, output=to_jsonable(out))
            except recoverable as e:
                return ToolResult(call_id=call.id, name=call.name, ok=False, error=str(e), error_type=type(e).__name__)

        return list(await asyncio.gather(*(one(*t) for t in targets)))

    # ================================================================= values, waits, children

    def record_value(self, vid: str, order: int | None, kind: str, make: Callable[[], Any]) -> Any:
        self.check_alive()
        self.journal.check_ordered(order, vid, "value:" + kind, None)
        if vid in self.journal.values:
            return self.journal.values[vid]
        self.go_live()
        with determinism.sdk_code():
            v = make()
        self.enqueue([NewEvent("ValueRecorded", vid, {"kind": kind, "value": v, "order": order})], wait=False)
        self.journal.values[vid] = v
        return v

    def record_version(self, change_id: str, vmax: int, vmin: int) -> int:
        vid = f"version:{change_id}"
        if vid in self.journal.values:
            return int(self.journal.values[vid])
        if vid in self._versions:
            return self._versions[vid]
        ctx = self.ctx
        replaying_past_here = ctx is not None and self.journal.max_order > ctx._order
        v = vmin if replaying_past_here else vmax
        self._versions[vid] = v
        self.enqueue([NewEvent("ValueRecorded", vid, {"kind": "version", "value": v, "change_id": change_id})],
                     wait=False)
        return v

    def record_fact(self, fid: str, order: int | None, facts: dict) -> None:
        self.check_alive()
        self.journal.check_ordered(order, fid, "fact", None)
        if fid in self.journal.facts_recorded:
            return
        self.enqueue([NewEvent("FactRecorded", fid, {"facts": facts, "order": order})], wait=False)
        self.journal.facts_recorded.add(fid)

    async def durable_sleep(self, tid: str, order: int | None, seconds: float) -> None:
        self._enter()
        try:
            self.check_alive()
            self.journal.check_ordered(order, tid, "timer", None)
            if tid in self.journal.timers_fired:
                return
            fire_at = self.journal.timers_started.get(tid)
            if fire_at is None:
                self.go_live()
                fire_at = self.clock.now_ms() + int(seconds * 1000)
                await self.commit([NewEvent("TimerStarted", tid, {"timer_id": tid, "fire_at_ms": fire_at,
                                                                  "duration_s": seconds, "order": order})])
                self.journal.timers_started[tid] = fire_at
            self.wait_on([Wait("timer", tid, fire_at_ms=fire_at)])
        finally:
            self._exit()

    async def wait_signal(self, wid: str, name: str, order: int | None, tid: str, timeout: float | None) -> Any:
        self._enter()
        try:
            self.check_alive()
            got = self.journal.signals.get(wid)
            if got is not None:
                return got.get("payload")
            if tid in self.journal.timers_fired:
                raise errors.SignalTimeout(f"no {name!r} signal within {timeout}s")
            waits = [Wait("signal", wid, name=name)]
            if timeout is not None:
                fire_at = self.journal.timers_started.get(tid)
                if fire_at is None:
                    self.go_live()
                    fire_at = self.clock.now_ms() + int(timeout * 1000)
                    await self.commit([NewEvent("TimerStarted", tid, {"timer_id": tid, "fire_at_ms": fire_at,
                                                                      "duration_s": timeout, "for_signal": name})])
                    self.journal.timers_started[tid] = fire_at
                waits.append(Wait("timer", tid, fire_at_ms=fire_at))
            self.wait_on(waits)
        finally:
            self._exit()

    async def run_child(self, sid: str, order: int | None, child_id: str, wname: str, wver: str | None, inp: Any,
                        budget: float | None) -> Any:
        self._enter()
        try:
            self.check_alive()
            ih = stable_hash({"workflow": wname, "input": inp})
            self.journal.check_ordered(order, sid, "child", ih)
            done = self.journal.children_completed.get(child_id)
            if done is not None:
                if done.get("status") == "completed":
                    return done.get("result")
                raise errors.ChildRunFailed(child_id, done.get("status", "failed"), done.get("error"))
            if child_id not in self.journal.children_started:
                self.go_live()
                await self.commit([NewEvent("ChildRunStarted", sid, {
                    "child_run_id": child_id, "workflow": wname, "version": wver or "", "input": inp,
                    "budget_usd": budget, "order": order, "input_hash": ih})])
                self.journal.children_started[child_id] = {"workflow": wname}
            self.wait_on([Wait("child", child_id)])
        finally:
            self._exit()

    async def emit_outbox(self, oid: str, order: int | None, dest: str, msg: Any) -> None:
        self._enter()
        try:
            self.check_alive()
            if oid in self.journal.outbox:
                return
            self.go_live()
            await self.commit([NewEvent("OutboxMessage", oid, {"destination": dest, "message": msg,
                                                               "dedupe_key": f"{self.task.run_id}:{oid}",
                                                               "order": order})])
            self.journal.outbox.add(oid)
        finally:
            self._exit()

    # ================================================================= lifecycle

    async def execute(self) -> str:
        self.ctx = Context(self)
        hb = asyncio.create_task(self._heartbeat(), context=_sdk_context())
        try:
            with span("run", trace_run=self.task.run_id, run_id=self.task.run_id, workflow=self.task.workflow, version=self.task.version,
                      tenant=self.task.tenant_id, epoch=self.epoch):
                self.outcome = await self._execute()
            return self.outcome
        finally:
            hb.cancel()
            with suppress(BaseException):
                await hb
            if self._flusher is not None and not self._flusher.done():
                self._flusher.cancel()

    async def _invoke(self) -> Any:
        wd = self.wdef
        inp = from_jsonable(self.task.input, wd.input_type) if wd.input_type is not None else self.task.input
        params = len(inspect.signature(wd.fn).parameters)
        if params >= 2:
            return await wd.fn(self.ctx, inp)
        return await wd.fn(self.ctx)

    async def _execute(self) -> str:
        try:
            wf_context = contextvars.copy_context()
            wf_context.run(determinism.IN_WORKFLOW.set, self.services.dev_mode)
            self.root_task = asyncio.create_task(self._invoke(), context=wf_context)
            try:
                result = await self.root_task
            except asyncio.CancelledError:
                if self.dead:
                    return "lease_lost"
                if self.crash is not None:
                    raise self.crash from None
                raise
            return await self._guard(self._complete(result))
        except WorkerCrash:
            raise
        except LeaseLost:
            return "lease_lost"
        except Suspend as s:
            return await self._guard(self._suspend(s))
        except errors.RunCancelled:
            return await self._guard(self._fail(None, cancelled=True))
        except errors.ContinueAsNew as c:
            return await self._guard(self._continue(c.state))
        except errors.NonDeterminismError as e:
            return await self._guard(self._park(e))
        except Exception as e:  # the workflow failed
            return await self._guard(self._fail(e))

    async def _guard(self, coro: Any) -> str:
        try:
            return await coro
        except WorkerCrash:
            raise
        except (LeaseLost, TransientBackendError):
            self.dead = True
            return "lease_lost"
        except Suspend as s:  # e.g. a long backoff inside a compensation
            return await self._guard(self._suspend(s))

    async def _settle(self) -> None:
        """Let steps already in flight in other branches finish and record."""
        while self.inflight > 0 and not self.dead and self.crash is None:
            self._idle.clear()
            await self._idle.wait()

    async def _suspend(self, s: Suspend) -> str:
        await self._settle()
        self.suspending = True
        await self.flush()
        waits: list[Wait] = []
        seen = set()
        for w in self.waits:
            if (w.kind, w.id) not in seen:
                seen.add((w.kind, w.id))
                waits.append(w)
        reason = s.reason
        avail = 0
        if reason == "handback" and not waits:
            pass
        elif not waits:
            reason, avail = "retry", self.retry_at_ms or s.available_at_ms or self.clock.now_ms()
        elif self.retry_at_ms:
            waits.append(Wait("timer", f"retry:{self.retry_at_ms}", fire_at_ms=self.retry_at_ms))
            reason = "wait"
        else:
            reason = "wait"
        res = await self.backend.suspend(self.task.run_id, self.epoch, waits, avail, reason)
        if res.lease_lost:
            return "lease_lost"
        METRICS.run_outcome("suspended")
        return "suspended"

    async def _complete(self, result: Any) -> str:
        try:
            out = self.services.secrets.redact(to_jsonable(result))
        except errors.SerializationError as e:
            return await self._fail(e)
        await self.flush()
        res = await self.backend.finish(self.task.run_id, self.epoch, self.next_seq, "completed", result=out)
        if not res.ok:
            return "lease_lost"
        METRICS.run_outcome("completed")
        return "completed"

    async def _fail(self, exc: Exception | None, cancelled: bool = False) -> str:
        self.compensating = True
        self.suspending = False
        self.waits = []
        await self._settle()
        await self._run_compensations()
        await self.flush()
        err = self.services.secrets.redact(
            error_json(exc) if exc is not None else {"type": "Cancelled", "message": "run cancelled"})
        status = "cancelled" if cancelled else "failed"
        res = await self.backend.finish(self.task.run_id, self.epoch, self.next_seq, status, error=err)
        if not res.ok:
            return "lease_lost"
        METRICS.run_outcome(status)
        return status

    async def _run_compensations(self) -> None:
        ctx = self.ctx
        assert ctx is not None
        for comp in reversed(ctx._compensations):
            cid = f"compensate:{comp.index}"
            if cid in self.journal.comp_completed:
                continue
            if cid not in self.journal.comp_started:
                await self.commit([NewEvent("CompensationStarted", cid, {"label": comp.label, "index": comp.index})])
                self.journal.comp_started.add(cid)
            fn = comp.fn
            if isinstance(fn, ToolFunction):
                inv = fn(*comp.args, **comp.kwargs)
                spec = inv.spec
                args = spec.validate(inv.args)
                sid = f"{cid}:{spec.name}"
                self.journal.check_keyed(sid, "tool", stable_hash({"tool": spec.name, "args": args}))
                result = await self._exec_tool(spec, args, sid, None, args, FOREVER)
            else:
                result = await self.run_step(f"{cid}:{comp.label}", None, "local", fn, comp.args, comp.kwargs,
                                             FOREVER, hash_input={"args": comp.args, "kwargs": comp.kwargs},
                                             record_input={"args": comp.args, "kwargs": comp.kwargs})
            await self.commit([NewEvent("CompensationCompleted", cid, {"result": to_jsonable(result)})])
            self.journal.comp_completed[cid] = result

    async def _continue(self, state: Any) -> str:
        await self._settle()
        await self.flush()
        res = await self.backend.finish(self.task.run_id, self.epoch, self.next_seq, "continued",
                                        continue_input=self.services.secrets.redact(state))
        if not res.ok:
            return "lease_lost"
        METRICS.run_outcome("continued")
        return "continued"

    async def _park(self, e: Exception) -> str:
        log.error("run %s parked: %s", self.task.run_id, e)
        self.suspending = True
        await self._settle()
        await self.commit([NewEvent("RunParked", "", {"error": error_json(e)})])
        res = await self.backend.suspend(self.task.run_id, self.epoch, [], 0, "parked")
        METRICS.run_outcome("parked")
        return "lease_lost" if res.lease_lost else "parked"

    async def _heartbeat(self) -> None:
        interval = max(0.05, self.task.lease_ttl_ms / 3 / 1000)
        while True:
            await self.clock.sleep(interval)
            if self.dead or self.crash is not None:
                return
            try:
                r = await self.backend.heartbeat(self.task.run_id, self.epoch, self.worker_id)
            except WorkerCrash as c:
                self.crash = c
                if self.root_task is not None:
                    self.root_task.cancel()
                return
            except TransientBackendError:
                continue
            if r.lease_lost:
                self.mark_dead()
                return
            if r.cancel_requested:
                self.cancel_requested = True
