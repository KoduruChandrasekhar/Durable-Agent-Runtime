"""Replay time vs history length (100, 1,000 and 10,000 events), measured on
the worker side where replay happens: the journal is rebuilt and the
workflow re-run until its first live step. Also the in-process cost of a
journaled no-op step vs a plain function call (the naive loop).

    python -m bench.replay_bench --json bench/results/bench_replay.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time

from keel import Context, workflow
from keel.executor import RunExecutor
from keel.testing import LocalEnv
from keel.workflow import WorkflowRegistry

REG = WorkflowRegistry()


def _noop(i: int) -> int:
    return i


@workflow(name="replay_bench", registry=REG, max_steps=100_000)
async def replay_bench(ctx: Context, inp: dict) -> int:
    for i in range(inp["steps"]):
        await ctx.step("s", _noop, i)
    await ctx.sleep(seconds=1)
    await ctx.step("after", _noop, 0)  # the first live step after replay
    return inp["steps"]


async def measure(steps: int, repeats: int) -> dict:
    env = LocalEnv(REG.all(), dev_mode=False)
    rid = env.start(replay_bench, {"steps": steps})
    await env.run_until_waiting(rid, "timer")
    events = len(env.events(rid))
    env.clock.advance(2)
    env.server.tick()
    times = []
    for _ in range(repeats):
        task = await env.server.poll("bench", ["replay_bench@1"], [], 0)
        wd = REG.get("replay_bench", "1")
        t0 = time.perf_counter()
        ex = RunExecutor(env.server, task, wd, env.services, "bench")
        await ex.execute()
        times.append(ex.replay_ms if ex.replay_ms is not None else (time.perf_counter() - t0) * 1000)
        # Put the run back so the next repeat replays the same history.
        r = env.server.runs[rid]
        env.server.events[rid] = [e for e in env.server.events[rid] if e.seq <= events + 1]
        r.status, r.next_seq, r.result = "pending", events + 2, None
    return {"steps": steps, "events": events, "replay_ms_p50": round(statistics.median(times), 3),
            "replay_ms_min": round(min(times), 3), "replay_ms_max": round(max(times), 3),
            "replay_us_per_event": round(statistics.median(times) * 1000 / events, 2)}


async def step_cost(n: int = 2000) -> dict:
    reg = WorkflowRegistry()

    @workflow(name="cost", registry=reg, max_steps=n + 10)
    async def cost(ctx: Context, _: dict) -> float:
        for i in range(n):
            await ctx.step("s", _noop, i)
        return 0.0

    env = LocalEnv(reg.all(), dev_mode=False)
    t0 = time.perf_counter()
    await env.run(cost, {})
    keel_us = (time.perf_counter() - t0) * 1e6 / n
    t0 = time.perf_counter()
    for i in range(n):
        _noop(i)
    naive_us = (time.perf_counter() - t0) * 1e6 / n
    return {"in_process_keel_step_us": round(keel_us, 1), "plain_call_us": round(naive_us, 3)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="bench/results/bench_replay.json")
    a = ap.parse_args()

    async def run() -> dict:
        pts = [await measure(s, 5 if s < 5000 else 3) for s in (50, 500, 5000)]
        return {"workload": "replay", "points": pts, "step_cost": await step_cost()}
    res = asyncio.run(run())
    print(json.dumps(res, indent=2))
    json.dump(res, open(a.json, "w"), indent=2)


if __name__ == "__main__":
    main()
