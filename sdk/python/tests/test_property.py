"""Replay property test: random workflows (steps, sleeps, signals, parallel
branches, idempotent tools) crashed before and after every write reach the
same result as an uninterrupted run, with every effect applied exactly once."""

from __future__ import annotations

import asyncio

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from keel import Context, tool, workflow
from keel.testing import LocalEnv, check_log_invariants, run_with_crashes
from keel.tools import ToolRegistry
from keel.workflow import WorkflowRegistry

op = st.one_of(
    st.tuples(st.just("step"), st.integers(0, 9)),
    st.tuples(st.just("sleep"), st.integers(1, 7200)),
    st.tuples(st.just("tool"), st.integers(0, 9)),
    st.tuples(st.just("signal"), st.sampled_from(["a", "b"])),
    st.tuples(st.just("parallel"), st.integers(2, 3)),
    st.tuples(st.just("value"), st.just(0)),
)
programs = st.lists(op, min_size=1, max_size=7)

LEDGER: dict[str, int] = {}   # effects applied, by idempotency key
CALLS: dict[str, int] = {}    # times the tool body ran (at-least-once)
TOOLS = ToolRegistry()


@tool(effect="idempotent", registry=TOOLS)
async def effect(n: int, *, key: str) -> int:
    CALLS[key] = CALLS.get(key, 0) + 1
    LEDGER.setdefault(key, 1)  # the service deduplicates by key
    return n * 10


def build(program: list) -> tuple:
    reg = WorkflowRegistry()

    @workflow(name="generated", registry=reg)
    async def generated(ctx: Context, _: dict) -> list:
        out: list = []
        for i, (kind, arg) in enumerate(program):
            if kind == "step":
                out.append(await ctx.step("s", lambda a=arg: a + 1))
            elif kind == "sleep":
                await ctx.sleep(seconds=arg)
                out.append("slept")
            elif kind == "tool":
                out.append(await ctx.tool(effect(n=arg * 100 + i)))
            elif kind == "signal":
                out.append(await ctx.wait_for_signal(arg))
            elif kind == "parallel":
                async def branch(j: int) -> int:
                    a = await ctx.step("p", lambda: j, key=f"{i}-{j}")
                    await ctx.sleep(seconds=5 + j, key=f"{i}-{j}")
                    return await ctx.tool(effect(n=a), key=f"{i}-{j}")
                out.append(list(await asyncio.gather(*(branch(j) for j in range(arg)))))
            elif kind == "value":
                out.append(round(ctx.random(), 6) >= 0)
        return out

    return reg, generated


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(programs)
def test_random_workflows_survive_crashes_at_every_write(program):
    reg, generated = build(program)
    signals = sorted({arg for kind, arg in program if kind == "signal"})

    n_effects = sum(1 for kind, _ in program if kind == "tool") + sum(
        a for kind, a in program if kind == "parallel")

    def factory() -> LocalEnv:
        LEDGER.clear()
        CALLS.clear()
        return LocalEnv(reg.all(), tools=TOOLS)

    def after_start(env: LocalEnv, rid: str) -> None:
        for name in signals:  # sent before the run waits: must be buffered, not lost
            for k in range(sum(1 for kind, a in program if kind == "signal" and a == name)):
                env.signal(rid, name, {"sig": name, "k": k}, signal_id=f"{name}-{k}")

    def check(env: LocalEnv, rid: str) -> None:
        assert env.run_state(rid).status == "completed", env.run_state(rid).error
        assert len(LEDGER) == n_effects, (LEDGER, n_effects)
        assert not check_log_invariants(env.server)

    rep = asyncio.run(run_with_crashes(generated, {}, env_factory=factory, check=check, after_start=after_start))
    assert rep.ok, (program, rep.failures[:3])
