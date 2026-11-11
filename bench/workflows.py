"""Benchmark workflows (served by the demo worker)."""

from __future__ import annotations

from keel import Context, workflow


def _noop(i: int) -> int:
    return i


@workflow(name="bench_noop", version="1", max_steps=20000)
async def bench_noop(ctx: Context, inp: dict) -> int:
    """Steps that do no work: pure engine overhead. batch=False sends
    StepStarted and StepCompleted in separate appends."""
    batch = bool(inp.get("batch", True))
    for i in range(int(inp.get("steps", 10))):
        await ctx.step("noop", _noop, i, batch=batch)
    return int(inp.get("steps", 10))


@workflow(name="bench_sleeper", version="1")
async def bench_sleeper(ctx: Context, inp: dict) -> int:
    """Sleeps and wakes a few times: stresses durable timers."""
    for i in range(int(inp.get("wakes", 3))):
        await ctx.sleep(seconds=float(inp.get("sleep_s", 5)), key=f"s{i}")
        await ctx.step("tick", _noop, i)
    return int(inp.get("wakes", 3))


@workflow(name="bench_slow", version="1", max_steps=10000)
async def bench_slow(ctx: Context, inp: dict) -> int:
    """Long-running steps, for failover measurements (a worker is killed mid-run)."""
    import asyncio

    async def slow(i: int) -> int:
        await asyncio.sleep(float(inp.get("step_s", 1.0)))
        return i
    for i in range(int(inp.get("steps", 60))):
        await ctx.step("slow", slow, i)
    return 1


ALL = [bench_noop, bench_sleeper, bench_slow]
