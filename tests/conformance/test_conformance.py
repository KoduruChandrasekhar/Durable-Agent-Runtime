"""Conformance: the same scenarios run against keel-server (Go) and
keel.testing.LocalServer (the simulation's server), and both must produce
identical traces. This keeps the simulation from drifting away from reality.

    KEEL_CONFORMANCE_GRPC=localhost:7233 KEEL_CONFORMANCE_URL=http://localhost:8080 pytest tests/conformance
(The Go side is skipped when those variables are unset.)
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import pytest

from keel.policy import PolicyEngine
from keel.protocol import NewEvent, Wait
from keel.testing import LocalServer

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
POLICY = open(os.path.join(ROOT, "deploy", "policy.yaml"), encoding="utf-8").read()


class LocalDriver:
    name = "local"

    def __init__(self) -> None:
        self.s = LocalServer(policy=POLICY)
        self.backend = self.s
        self.wf = f"conf-{uuid.uuid4().hex[:8]}"

    def now_ms(self) -> int:
        return self.s.clock.now_ms()

    async def start(self, inp: Any, **kw: Any) -> str:
        rid, _ = self.s.start_run(self.wf, inp, version="1", **kw)
        return rid

    async def claim(self) -> Any:
        return await self.s.poll("w", [f"{self.wf}@1"], [], 0)

    async def signal(self, rid: str, name: str, payload: Any, sid: str) -> bool:
        return self.s.signal(rid, name, payload, sid)

    async def decide(self, aid: str, decision: str, edits: dict | None = None) -> bool:
        return self.s.decide(aid, decision, edits, "conformance")

    async def cancel(self, rid: str) -> None:
        self.s.cancel(rid)

    async def fork(self, rid: str, seq: int) -> str:
        return self.s.fork(rid, seq, {"model": "x"}, True)

    async def status(self, rid: str) -> str:
        return self.s.runs[rid].status

    async def events(self, rid: str) -> list[tuple[str, str]]:
        return [(e.type, e.step_id) for e in self.s.events[rid]]

    async def close(self) -> None:
        pass


class GoDriver:
    name = "go"

    def __init__(self, grpc_addr: str, url: str) -> None:
        from keel.backend import GrpcBackend
        from keel.client import KeelClient
        self.backend = GrpcBackend(grpc_addr, token=os.environ.get("KEEL_WORKER_TOKEN"))
        self.c = KeelClient(url, api_key=os.environ.get("KEEL_CONFORMANCE_API_KEY"))
        self.wf = f"conf-{uuid.uuid4().hex[:8]}"

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    async def start(self, inp: Any, **kw: Any) -> str:
        r = await self.c.start_run(self.wf, inp, version="1", **{k: v for k, v in kw.items() if k in ("start_key", "budget_usd")})
        return r["run_id"]

    async def claim(self) -> Any:
        from keel.protocol import WorkflowDescriptor
        deadline = time.time() + 5
        while time.time() < deadline:
            t = await self.backend.poll("w", [f"{self.wf}@1"], [WorkflowDescriptor(self.wf, "1")], 1000)
            if t is not None:
                return t
        return None

    async def signal(self, rid: str, name: str, payload: Any, sid: str) -> bool:
        return not (await self.c.signal(rid, name, payload, sid))["duplicate"]

    async def decide(self, aid: str, decision: str, edits: dict | None = None) -> bool:
        return (await self.c.decide(aid, decision, edits))["changed"]

    async def cancel(self, rid: str) -> None:
        await self.c.cancel(rid)

    async def fork(self, rid: str, seq: int) -> str:
        return (await self.c.fork(rid, seq, {"model": "x"}, True))["run_id"]

    async def status(self, rid: str) -> str:
        return (await self.c.get_run(rid))["status"]

    async def events(self, rid: str) -> list[tuple[str, str]]:
        return [(e["type"], e.get("step_id") or "") for e in await self.c.events(rid)]

    async def close(self) -> None:
        await self.c.aclose()
        await self.backend.close()


@pytest.fixture(params=["local", "go"])
async def drv(request: Any) -> Any:
    if request.param == "go":
        g, u = os.environ.get("KEEL_CONFORMANCE_GRPC"), os.environ.get("KEEL_CONFORMANCE_URL")
        if not g or not u:
            pytest.skip("KEEL_CONFORMANCE_GRPC / KEEL_CONFORMANCE_URL not set")
        d: Any = GoDriver(g, u)
    else:
        d = LocalDriver()
    yield d
    await d.close()


def E(t: str, s: str = "", p: dict | None = None) -> NewEvent:
    return NewEvent(t, s, p or {"kind": "local"})


async def test_append_fencing_and_sequence(drv: Any) -> None:
    rid = await drv.start({"a": 1})
    t = await drv.claim()
    assert t.run_id == rid and t.epoch == 1 and t.next_seq == 2
    b = drv.backend
    r = await b.append(rid, 1, 2, [E("StepStarted", "s:1"), E("StepCompleted", "s:1")])
    assert r.ok and r.next_seq == 4
    dup = await b.append(rid, 1, 2, [E("StepStarted", "s:1"), E("StepCompleted", "s:1")])
    assert dup.ok and dup.next_seq == 4                       # retried append acknowledged once
    conflict = await b.append(rid, 1, 2, [E("StepStarted", "x:1")])
    assert not conflict.ok and conflict.error_code == "seq_conflict"
    stale = await b.append(rid, 0, 4, [E("StepStarted", "z:1")])
    assert not stale.ok and stale.error_code == "lease_lost"
    with pytest.raises(ValueError):
        await b.append(rid, 1, 4, [E("RunCompleted")])          # server-only event type
    hb = await b.heartbeat(rid, 1, "w")
    assert hb.ok and not hb.lease_lost
    f = await b.finish(rid, 1, 4, "completed", result={"ok": True})
    assert f.ok
    late = await b.finish(rid, 1, 5, "completed", result={})
    assert late.lease_lost
    assert await drv.status(rid) == "completed"
    assert await drv.events(rid) == [("RunStarted", ""), ("StepStarted", "s:1"), ("StepCompleted", "s:1"),
                                     ("RunCompleted", "")]


async def test_waits_wake_on_signal_timer_and_approval(drv: Any) -> None:
    rid = await drv.start({"customer_email": "a@x.com"})
    t = await drv.claim()
    b = drv.backend
    assert await drv.signal(rid, "go", {"v": 1}, "sig-1")
    assert not await drv.signal(rid, "go", {"v": 2}, "sig-1")   # duplicate id
    s = await b.suspend(rid, t.epoch, [Wait("signal", "signal:go:1", "go")], 0, "wait")
    assert s.ok and s.woken                                    # buffered signal consumed immediately
    t = await drv.claim()
    past = drv.now_ms() - 1000
    s = await b.suspend(rid, t.epoch, [Wait("timer", "timer:1", fire_at_ms=past)], 0, "wait")
    assert s.ok and s.woken                                    # a due timer fires at once
    t = await drv.claim()
    aid = str(uuid.uuid4())
    r = await b.append(rid, t.epoch, t.next_seq, [E("ApprovalRequested", "refund:1", {
        "approval_id": aid, "action": {"tool": "refund"}, "reason": "big", "rule_ids": ["big"], "approvers": []})])
    assert r.ok
    s = await b.suspend(rid, t.epoch, [Wait("approval", aid)], 0, "wait")
    assert s.ok and not s.woken and await drv.status(rid) == "waiting"
    assert await drv.decide(aid, "approved", {"amount": 1})
    assert not await drv.decide(aid, "rejected")
    assert await drv.status(rid) == "pending"
    t = await drv.claim()
    types = [ty for ty, _ in await drv.events(rid)]
    assert types == ["RunStarted", "SignalReceived", "RunResumed", "TimerFired", "RunResumed", "ApprovalRequested",
                     "ApprovalResolved", "RunResumed"]
    await b.finish(rid, t.epoch, t.next_seq, "completed", result=None)


async def test_cancel_wakes_a_waiting_run(drv: Any) -> None:
    rid = await drv.start({})
    t = await drv.claim()
    future = drv.now_ms() + 3_600_000
    await drv.backend.suspend(rid, t.epoch, [Wait("timer", "timer:1", fire_at_ms=future)], 0, "wait")
    assert await drv.status(rid) == "waiting"
    await drv.cancel(rid)
    t = await drv.claim()
    assert t.cancel_requested
    await drv.backend.finish(rid, t.epoch, t.next_seq, "cancelled", error={"type": "Cancelled"})
    assert await drv.status(rid) == "cancelled"


async def test_child_completion_wakes_parent_and_fork(drv: Any) -> None:
    parent = await drv.start({"p": 1})
    t = await drv.claim()
    child = str(uuid.uuid4())
    b = drv.backend
    await b.append(parent, t.epoch, t.next_seq, [E("ChildRunStarted", "child:1", {
        "child_run_id": child, "workflow": drv.wf, "version": "1", "input": {"c": 1}})])
    s = await b.suspend(parent, t.epoch, [Wait("child", child)], 0, "wait")
    assert s.ok and not s.woken
    ct = await drv.claim()
    assert ct.run_id == child and ct.parent_run_id == parent
    await b.finish(child, ct.epoch, ct.next_seq, "completed", result={"answer": 42})
    assert await drv.status(parent) == "pending"
    pt = await drv.claim()
    assert [ty for ty, _ in await drv.events(parent)][-2:] == ["ChildRunCompleted", "RunResumed"]
    await b.finish(parent, pt.epoch, pt.next_seq, "completed", result=None)
    fork = await drv.fork(parent, 2)
    assert [ty for ty, _ in await drv.events(fork)] == ["RunStarted", "ChildRunStarted", "RunForked"]


async def test_policy_engines_agree(drv: Any) -> None:
    rid = await drv.start({"customer_email": "a@x.com"})
    t = await drv.claim()
    b = drv.backend
    await b.append(rid, t.epoch, t.next_seq, [NewEvent("FactRecorded", "fact:1", {"facts": {
        "order_total_inr": 4000, "eligible": True, "order_id": "ORD-1"}})])
    cases = [
        ("issue_refund", "high", {"order_id": "ORD-1", "amount_inr": 3000}, "allow"),
        ("issue_refund", "high", {"order_id": "ORD-1", "amount_inr": 6000}, "deny"),
        ("issue_refund", "high", {"order_id": "ORD-2", "amount_inr": 100}, "deny"),
        ("send_email", "medium", {"to": "evil@x.com", "subject": "", "body": ""}, "deny"),
        ("send_email", "medium", {"to": "a@x.com", "subject": "", "body": ""}, "allow"),
        ("confirm_outline", "medium", {"title": "t", "sections": []}, "require_approval"),
    ]
    py = PolicyEngine.parse(POLICY)
    for tool_name, risk, args, want in cases:
        d = await b.evaluate_policy(rid, t.epoch, tool_name, risk, args, "x:1", [])
        assert d.decision == want, (drv.name, tool_name, args, d)
        if drv.name == "go":  # the Python engine must decide exactly like the Go one
            facts = {"tainted": False, "spent_usd": 0, "input": {"customer_email": "a@x.com"},
                     "facts": {"order_total_inr": 4000, "eligible": True, "order_id": "ORD-1"},
                     "tool_counts": {}, "dry_run": False}
            p = py.evaluate(tool_name, risk, "default", args, facts, [])
            assert (p.decision, sorted(p.rule_ids)) == (d.decision, sorted(d.rule_ids))
    await b.finish(rid, t.epoch, t.next_seq + 1, "completed", result=None)
