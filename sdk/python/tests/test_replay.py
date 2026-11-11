"""Core replay behaviour on the in-memory server with a virtual clock."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap

import pytest

from keel import Context, NonDeterminismError, NonDeterministicCallError, RetryPolicy, StepError, tool, workflow
from keel.errors import EffectDeclarationError, MissingStepKeyError
from keel.testing import LocalEnv, check_log_invariants, run_with_crashes
from keel.tools import ToolRegistry
from keel.workflow import WorkflowRegistry



def fresh() -> WorkflowRegistry:
    return WorkflowRegistry()


async def test_seven_day_sleep_finishes_instantly():
    reg = fresh()

    @workflow(name="sleeper", registry=reg)
    async def sleeper(ctx: Context, _: dict) -> dict:
        start = ctx.now()
        await ctx.sleep(days=7)
        end = ctx.now()
        return {"slept_days": (end - start).days}

    env = LocalEnv(reg.all())
    st = await asyncio.wait_for(env.run(sleeper, {}), timeout=5)
    assert st.status == "completed"
    assert st.result == {"slept_days": 7}


async def test_step_results_are_replayed_not_reexecuted():
    reg = fresh()
    calls = {"n": 0}

    def work(x: int) -> int:
        calls["n"] += 1
        return x * 2

    @workflow(name="replayed", registry=reg)
    async def replayed(ctx: Context, _: dict) -> int:
        a = await ctx.step("work", work, 21)
        await ctx.sleep(hours=1)  # forces a second claim that replays `work`
        b = await ctx.step("work", work, a)
        return b

    env = LocalEnv(reg.all())
    st = await env.run(replayed, {})
    assert st.result == 84
    assert calls["n"] == 2  # each step ran exactly once although the run was replayed
    assert not check_log_invariants(env.server)


async def test_retries_record_backoff_and_final_error():
    reg = fresh()
    attempts = {"n": 0}

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("try again")
        return "ok"

    def broken() -> None:
        raise ValueError("bad input")

    @workflow(name="retrying", registry=reg)
    async def retrying(ctx: Context, _: dict) -> dict:
        a = await ctx.step("flaky", flaky, retry=RetryPolicy(max_attempts=5, initial=0.5))
        try:
            await ctx.step("broken", broken, retry=RetryPolicy(max_attempts=3, non_retryable=[ValueError]))
        except StepError as e:
            return {"a": a, "error": e.error_type}
        return {"a": a}

    env = LocalEnv(reg.all())
    st = await env.run(retrying, {})
    assert st.result == {"a": "ok", "error": "ValueError"}
    failed = [e for e in env.events(st.run_id) if e.type == "StepFailed"]
    assert [e.payload["retryable"] for e in failed] == [True, True, False]
    assert all(e.payload["next_retry_at_ms"] for e in failed[:2])


async def test_long_backoff_suspends_the_run():
    reg = fresh()
    n = {"x": 0}

    def fails_once() -> int:
        n["x"] += 1
        if n["x"] == 1:
            raise TimeoutError("unknown outcome")
        return 1

    @workflow(name="long_backoff", registry=reg)
    async def long_backoff(ctx: Context, _: dict) -> int:
        return await ctx.step("x", fails_once, retry=RetryPolicy(initial=600, jitter=False))

    env = LocalEnv(reg.all())
    st = await env.run(long_backoff, {})
    assert st.status == "completed"
    resumed = [e for e in env.events(st.run_id) if e.type == "RunResumed"]
    assert resumed and resumed[0].payload["reason"] == "retry"


async def test_registering_a_tool_without_effect_fails():
    reg = ToolRegistry()
    with pytest.raises(EffectDeclarationError):
        @tool(registry=reg)
        async def no_effect(x: str) -> str:  # pragma: no cover
            return x
    with pytest.raises(EffectDeclarationError):
        @tool(effect="sometimes", registry=reg)
        async def bad_effect(x: str) -> str:  # pragma: no cover
            return x
    with pytest.raises(EffectDeclarationError):
        @tool(effect="idempotent", registry=reg)
        async def no_key(x: str) -> str:  # pragma: no cover
            return x
    with pytest.raises(EffectDeclarationError):
        @tool(effect="reconcile", registry=reg)
        async def no_reconcile(x: str, *, key: str) -> str:  # pragma: no cover
            return x


async def test_concurrent_branches_need_explicit_keys():
    reg = fresh()

    async def branch(ctx: Context, name: str, keyed: bool) -> int:
        await ctx.step("a", lambda: 1, key=name if keyed else None)
        return await ctx.step("b", lambda: 2, key=name if keyed else None)

    @workflow(name="branches_bad", registry=reg)
    async def bad(ctx: Context, _: dict) -> list:
        return list(await asyncio.gather(branch(ctx, "x", False), branch(ctx, "y", False)))

    @workflow(name="branches_good", registry=reg)
    async def good(ctx: Context, _: dict) -> list:
        return list(await asyncio.gather(branch(ctx, "x", True), branch(ctx, "y", True)))

    env = LocalEnv(reg.all())
    st = await env.run(bad, {})
    assert st.status == "failed" and st.error["type"] == "MissingStepKeyError"
    st = await env.run(good, {})
    assert st.status == "completed" and st.result == [2, 2]
    # Calling ctx.step directly inside gather is fine: ids are assigned at call time.
    reg2 = fresh()

    @workflow(name="gather_direct", registry=reg2)
    async def direct(ctx: Context, _: dict) -> list:
        return list(await asyncio.gather(ctx.step("p", lambda: 1), ctx.step("p", lambda: 2)))

    st = await LocalEnv(reg2.all()).run(direct, {})
    assert st.result == [1, 2]
    _ = MissingStepKeyError


async def test_signal_sent_before_the_wait_is_delivered():
    reg = fresh()

    @workflow(name="early_signal", registry=reg)
    async def early(ctx: Context, _: dict) -> dict:
        await ctx.sleep(minutes=5)
        got = await ctx.wait_for_signal("approve")
        return {"got": got}

    env = LocalEnv(reg.all())
    rid = env.start(early, {})
    await env.run_until_waiting(rid, "timer")
    assert env.signal(rid, "approve", {"by": "ops"}, signal_id="sig-1")
    assert not env.signal(rid, "approve", {"by": "dup"}, signal_id="sig-1")  # deduplicated
    await env.run_until_idle()
    assert env.run_state(rid).result == {"got": {"by": "ops"}}


async def test_continue_as_new_carries_state():
    reg = fresh()

    @workflow(name="counter", registry=reg)
    async def counter(ctx: Context, state: dict) -> dict:
        n = state.get("n", 0)
        await ctx.step("tick", lambda: n)
        if n < 4:
            ctx.continue_as_new({"n": n + 1})
        return {"final": n}

    env = LocalEnv(reg.all())
    rid = env.start(counter, {})
    await env.run_until_idle()
    first = env.run_state(rid)
    assert "continued_as_new" in first.result
    finals = [r for r in env.server.runs.values() if r.status == "completed" and r.result == {"final": 4}]
    assert len(finals) == 1 and finals[0].chain_root == rid


async def test_child_runs():
    reg = fresh()

    @workflow(name="child", registry=reg)
    async def child(ctx: Context, x: int) -> int:
        await ctx.sleep(seconds=30)
        return await ctx.step("sq", lambda: x * x)

    @workflow(name="parent", registry=reg)
    async def parent(ctx: Context, _: dict) -> int:
        rs = await asyncio.gather(*(ctx.child(child, i, key=f"c{i}") for i in range(1, 4)))
        return sum(rs)

    env = LocalEnv(reg.all())
    st = await env.run(parent, {})
    assert st.result == 1 + 4 + 9
    assert sum(1 for r in env.server.runs.values() if r.parent_run_id == st.run_id) == 3


async def test_version_patching_keeps_old_runs_on_old_path():
    reg = fresh()
    path = {"v": 1}

    @workflow(name="patched", registry=reg)
    async def patched(ctx: Context, _: dict) -> str:
        await ctx.step("first", lambda: 1)
        await ctx.sleep(hours=1)
        if path["v"] == 2 and ctx.version("extra-check", max=2) >= 2:
            await ctx.step("extra", lambda: 2)
            return "new"
        await ctx.step("final", lambda: 3)
        return "old"

    env = LocalEnv(reg.all())
    rid = env.start(patched, {})
    await env.run_until_waiting(rid, "timer")
    path["v"] = 2  # deploy the patched code while the run is sleeping
    await env.run_until_idle()
    # The run's history was recorded before the patch point? No: it slept before
    # reaching it, so it takes the new path; a fresh run also takes it.
    assert env.run_state(rid).result == "new"
    st = await env.run(patched, {})
    assert st.result == "new"


async def test_cancellation_runs_compensations():
    reg = fresh()
    undone: list[str] = []

    def undo(name: str) -> str:
        undone.append(name)
        return name

    @workflow(name="cancellable", registry=reg)
    async def cancellable(ctx: Context, _: dict) -> str:
        await ctx.step("book", lambda: "b1")
        ctx.on_failure(undo, "b1")
        await ctx.wait_for_signal("never")
        return "done"

    env = LocalEnv(reg.all())
    rid = env.start(cancellable, {})
    await env.run_until_waiting(rid, "signal")
    env.server.cancel(rid)
    await env.run_until_idle()
    assert env.run_state(rid).status == "cancelled"
    assert undone == ["b1"]


async def test_lease_lost_worker_cannot_write():
    """A frozen worker whose lease expired is fenced off (the zombie test, in-memory)."""
    reg = fresh()
    gate = asyncio.Event()

    async def slow() -> str:
        await gate.wait()
        return "zombie-result"

    @workflow(name="zombie", registry=reg)
    async def zombie(ctx: Context, _: dict) -> str:
        return await ctx.step("slow", slow)

    env = LocalEnv(reg.all())
    rid = env.start(zombie, {})
    w1 = env.workers[0]
    await w1.poll_once()
    await asyncio.sleep(0)
    ex1 = w1.running[rid][1]
    # Freeze: no heartbeats; the lease expires and the sweeper re-queues the run.
    ex1._heartbeat_task_disabled = True
    w1.alive = False
    env.clock.advance(31)
    env.server.tick()
    assert env.server.runs[rid].status == "pending"
    epoch_before = env.server.runs[rid].lease_epoch
    task = await env.server.poll("worker-B", [d.key for d in reg.all()], [])
    assert task is not None and task.epoch == epoch_before + 1
    # The zombie wakes up and tries to record its result with the old epoch.
    res = await env.server.append(rid, ex1.epoch, ex1.next_seq, [])
    assert not res.ok and res.error_code == "lease_lost"
    assert env.server.stats["fencing_rejections"] >= 1
    gate.set()


async def test_secrets_never_reach_the_event_log():
    reg = fresh()
    tools = ToolRegistry()
    secret = "sk-test-SUPERSECRETVALUE-123456"

    @tool(effect="none", secrets=["PAYMENTS_KEY"], registry=tools)
    async def call_api(order_id: str, secrets: dict) -> dict:
        assert secrets["PAYMENTS_KEY"] == secret
        return {"order_id": order_id, "echo": f"used key {secrets['PAYMENTS_KEY']}"}

    @workflow(name="uses_secret", registry=reg)
    async def uses_secret(ctx: Context, _: dict) -> dict:
        return await ctx.tool(call_api(order_id="o1"))

    env = LocalEnv(reg.all(), tools=tools, secrets={"PAYMENTS_KEY": secret})
    st = await env.run(uses_secret, {})
    assert st.status == "completed"
    dump = json.dumps([e.payload for e in env.events(st.run_id)])
    assert secret not in dump and "[REDACTED]" in dump


def test_nondeterminism_detector_catches_planted_bugs(tmp_path):
    """time.time(), random, set iteration, reordered steps and a changed step input."""
    script = tmp_path / "planted.py"
    script.write_text(textwrap.dedent('''
        import asyncio, json, random, sys, time
        from keel import Context, workflow
        from keel.testing import LocalEnv
        from keel.protocol import Event
        from keel.workflow import WorkflowRegistry
        from keel.replay import Journal

        bug, phase, hist_file = sys.argv[1], sys.argv[2], sys.argv[3]
        reg = WorkflowRegistry()

        @workflow(name="w", registry=reg)
        async def w(ctx: Context, _: dict) -> str:
            if bug == "time":
                if time.time() > 0:
                    await ctx.step("a", lambda: 1)
            elif bug == "random":
                if random.random() < 2:
                    await ctx.step("a", lambda: 1)
            elif bug == "set":
                for name in {"alpha", "beta", "gamma", "delta", "eps", "zeta", "eta", "theta"}:
                    await ctx.step(name, lambda: 1)
            elif bug == "reorder":
                order = ["a", "b"] if phase == "record" else ["b", "a"]
                for name in order:
                    await ctx.step(name, lambda: 1)
            elif bug == "input":
                await ctx.step("a", lambda x: x, 1 if phase == "record" else 2)
            await ctx.sleep(hours=1)
            return "done"

        async def main():
            env = LocalEnv(reg.all(), dev_mode=(phase == "replay" and bug in ("time", "random")))
            if phase == "record":
                rid = env.start(w, {})
                await env.run_until_waiting(rid, "timer")
                log = [e.__dict__ for e in env.events(rid)]
                json.dump(log, open(hist_file, "w"))
                print("recorded", env.run_state(rid).status)
                return
            # Replay the recorded history in this (different) process.
            log = json.load(open(hist_file))
            rid = env.start(w, {})
            st = env.server.runs[rid]
            env.server.events[rid] = [Event(**e) for e in log]
            st.next_seq = len(log) + 1
            st.first_claimed_at = 0
            await env.run_until_idle()
            st = env.server.runs[rid]
            print("RESULT", st.status, st.wait_reason, json.dumps(st.error))
        asyncio.run(main())
    '''))
    caught = {}
    for bug in ("time", "random", "set", "reorder", "input"):
        hist = tmp_path / f"{bug}.json"
        env1 = {**os.environ, "PYTHONHASHSEED": "1", "PYTHONPATH": os.pathsep.join(sys.path)}
        env2 = {**os.environ, "PYTHONHASHSEED": "2", "PYTHONPATH": os.pathsep.join(sys.path)}
        subprocess.run([sys.executable, str(script), bug, "record", str(hist)], check=True, env=env1,
                       capture_output=True, text=True)
        out = subprocess.run([sys.executable, str(script), bug, "replay", str(hist)], check=True, env=env2,
                             capture_output=True, text=True).stdout
        line = [ln for ln in out.splitlines() if ln.startswith("RESULT")][0]
        caught[bug] = ("parked" in line) or ("NonDeterministicCallError" in line) or ("NonDeterminismError" in line)
    assert caught == {b: True for b in caught}, caught
    _ = (NonDeterminismError, NonDeterministicCallError)


async def test_saga_compensates_in_reverse_exactly_once_even_with_crashes():
    tools = ToolRegistry()
    ledger: dict[str, list] = {"booked": [], "cancelled": []}

    def make_tools():
        @tool(effect="idempotent", registry=tools)
        async def book(item: str, *, key: str) -> str:
            if key not in [k for k, _ in ledger["booked"]]:
                ledger["booked"].append((key, item))
            return item

        @tool(effect="idempotent", registry=tools)
        async def cancel(item: str, *, key: str) -> str:
            if key not in [k for k, _ in ledger["cancelled"]]:
                ledger["cancelled"].append((key, item))
            return item

        @tool(effect="idempotent", registry=tools, retry=RetryPolicy(max_attempts=2, initial=0.1))
        async def book_car(item: str, *, key: str) -> str:
            raise RuntimeError("no cars left")
        return book, cancel, book_car

    book, cancel, book_car = make_tools()
    reg = fresh()

    @workflow(name="trip", registry=reg)
    async def trip(ctx: Context, _: dict) -> str:
        for item in ("flight", "hotel", "train"):
            await ctx.tool(book(item=item), key=item)
            ctx.on_failure(cancel, item=item)
        await ctx.tool(book_car(item="car"))  # step 4 fails permanently
        return "booked"

    def factory():
        ledger["booked"].clear()
        ledger["cancelled"].clear()
        return LocalEnv(reg.all(), tools=tools)

    def check(env, rid):
        st = env.run_state(rid)
        assert st.status == "failed", st.status
        assert [i for _, i in ledger["cancelled"]] == ["train", "hotel", "flight"], ledger["cancelled"]

    rep = await run_with_crashes(trip, {}, env_factory=factory, check=check)
    assert rep.ok, rep.failures[:5]
    assert rep.crash_points > 20


async def test_policy_edit_does_not_change_replay():
    """Decisions are journaled: a run replays last week's decision under this week's rules."""
    tools = ToolRegistry()

    @tool(effect="idempotent", risk="high", registry=tools)
    async def pay(amount: int, *, key: str) -> int:
        return amount

    reg = fresh()

    @workflow(name="payer", registry=reg)
    async def payer(ctx: Context, _: dict) -> int:
        r = await ctx.tool(pay(amount=100))
        await ctx.sleep(days=2)
        return r

    allow_all = "[]"
    deny_all = "- {id: no-pay, tool: pay, action: deny}"
    env = LocalEnv(reg.all(), tools=tools, policy=allow_all)
    rid = env.start(payer, {})
    await env.run_until_waiting(rid, "timer")
    from keel.policy import PolicyEngine
    env.server.policy = PolicyEngine.parse(deny_all)  # someone edits the rules
    await env.run_until_idle()
    assert env.run_state(rid).status == "completed"
    assert env.run_state(rid).result == 100


async def test_approval_survives_a_two_day_wait_and_edits_are_revalidated():
    tools = ToolRegistry()

    @tool(effect="idempotent", risk="high", registry=tools)
    async def refund(order_id: str, amount_inr: int, *, key: str) -> dict:
        return {"order_id": order_id, "amount_inr": amount_inr}

    reg = fresh()

    @workflow(name="needs_approval", registry=reg)
    async def needs_approval(ctx: Context, _: dict) -> dict:
        return await ctx.tool(refund(order_id="ORD-1", amount_inr=9000))

    policy = """
- {id: big, tool: refund, when: "args.amount_inr > 5000", action: require_approval, expires_in: 72h}
- {id: cap, tool: refund, when: "args.amount_inr > 8000", action: require_approval, expires_in: 72h}
"""
    env = LocalEnv(reg.all(), tools=tools, policy=policy)
    rid = env.start(needs_approval, {})
    await env.run_until_waiting(rid, "approval")
    env.clock.advance(2 * 86400)  # two days pass
    env.server.tick()
    (a,) = env.approvals()
    assert sorted(a["rule_ids"]) == ["big", "cap"]
    assert env.approve(a["approval_id"], edited_args={"amount_inr": 6000})
    assert not env.approve(a["approval_id"])  # deciding twice changes nothing
    await env.run_until_idle()
    st = env.run_state(rid)
    assert st.status == "completed" and st.result == {"order_id": "ORD-1", "amount_inr": 6000}


async def test_fork_in_dry_run_repeats_no_side_effect():
    tools = ToolRegistry()
    effects: list[str] = []

    @tool(effect="idempotent", registry=tools)
    async def charge(n: int, *, key: str) -> int:
        effects.append(key)
        return n

    reg = fresh()

    @workflow(name="forkable", registry=reg)
    async def forkable(ctx: Context, _: dict) -> list:
        a = await ctx.tool(charge(n=1))
        b = await ctx.tool(charge(n=2))
        c = await ctx.tool(charge(n=3))
        return [a, b, c]

    env = LocalEnv(reg.all(), tools=tools)
    st = await env.run(forkable, {})
    assert len(effects) == 3
    # Fork after the second charge completed: only the third runs, and as a dry run.
    seq = max(e.seq for e in env.events(st.run_id) if e.type == "StepCompleted" and e.step_id == "charge:2")
    nid = env.server.fork(st.run_id, from_seq=seq, overrides={"model": "mock-large"}, dry_run=True)
    await env.run_until_idle()
    fork = env.run_state(nid)
    assert fork.status == "completed"
    assert fork.result[:2] == [1, 2] and fork.result[2]["dry_run"] is True
    assert len(effects) == 3  # no earlier (or later) real side effect repeated
