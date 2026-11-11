"""Engine benchmarks against the Docker Compose stack, with a mock LLM so
model latency cannot hide the engine's own overhead.

    python -m bench.run_bench noop --runs 200 --steps 10            # overhead per step, steps/s
    python -m bench.run_bench noop --runs 200 --steps 10 --no-batch # without append batching
    python -m bench.run_bench burst --runs 5000                     # queue wait during a burst
    python -m bench.run_bench failover --rounds 10                  # kill -9 -> RunResumed elsewhere
    python -m bench.run_bench scaling --workers 1 2 4 8             # steps/s vs workers
    python -m bench.run_bench sleepers --runs 2000                  # durable timers

Every result file records the machine, software versions and git commit.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psycopg

from chaos.common import API, DSN, containers, docker, hostname
from keel.client import KeelClient

OUT = Path("bench/results")


def env_info() -> dict:
    def cmd(*a: str) -> str:
        try:
            return subprocess.run(list(a), capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception:  # noqa: BLE001
            return ""
    return {"machine": platform.machine(), "os": platform.platform(), "cpus": os.cpu_count(),
            "python": sys.version.split()[0], "go": cmd("go", "version"), "docker": cmd("docker", "--version"),
            "docker_cpus": cmd("docker", "info", "--format", "{{.NCPU}}"),
            "docker_mem_gb": round(int(cmd("docker", "info", "--format", "{{.MemTotal}}") or 0) / 2**30, 1),
            "postgres": "16", "git_commit": cmd("git", "rev-parse", "--short", "HEAD") or "uncommitted",
            "at": time.strftime("%Y-%m-%dT%H:%M:%S")}


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def save(name: str, data: dict) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data["env"] = env_info()
    (OUT / f"bench_{name}.json").write_text(json.dumps(data, indent=2, default=str))
    print(json.dumps({k: v for k, v in data.items() if k not in ("env", "samples")}, indent=2, default=str))


def db_stats() -> dict:
    with psycopg.connect(DSN) as db:
        commits = db.execute("SELECT xact_commit FROM pg_stat_database WHERE datname = 'keel'").fetchone()[0]
        wal = db.execute("SELECT wal_bytes FROM pg_stat_wal").fetchone()[0]
    return {"commits": int(commits), "wal_bytes": int(wal), "t": time.time()}


async def start_many(c: KeelClient, workflow: str, inputs: list[dict], conc: int = 64) -> list[str]:
    sem = asyncio.Semaphore(conc)

    async def one(inp: dict) -> str:
        async with sem:
            return (await c.start_run(workflow, inp))["run_id"]
    return list(await asyncio.gather(*(one(i) for i in inputs)))


async def wait_final(ids: list[str], timeout: float) -> float:
    t0 = time.time()
    while time.time() - t0 < timeout:
        with psycopg.connect(DSN) as db:
            n = db.execute("SELECT count(*) FROM runs WHERE run_id = ANY(%s::uuid[]) AND status IN "
                           "('completed','failed','cancelled')", (ids,)).fetchone()[0]
        if n == len(ids):
            return time.time() - t0
        await asyncio.sleep(0.5)
    raise TimeoutError(f"{len(ids)} runs not final after {timeout}s")


def step_gaps(ids: list[str]) -> tuple[list[float], dict]:
    """Per-step engine overhead: time between consecutive StepCompleted
    events of a no-op workflow (server timestamps), plus run-level timings."""
    with psycopg.connect(DSN) as db:
        rows = db.execute("""SELECT run_id::text, extract(epoch from created_at) FROM events
                             WHERE run_id = ANY(%s::uuid[]) AND type IN ('StepCompleted','RunResumed','RunStarted')
                             ORDER BY run_id, seq""", (ids,)).fetchall()
        # Steady-state window: first to last step completion, so the time
        # spent submitting runs through the API does not count as engine time.
        span = db.execute("""SELECT extract(epoch from min(created_at)), extract(epoch from max(created_at)), count(*)
                             FROM events WHERE run_id = ANY(%s::uuid[]) AND type = 'StepCompleted'""", (ids,)).fetchone()
    gaps: list[float] = []
    last: dict[str, float] = {}
    for rid, ts in rows:
        ts = float(ts)
        if rid in last:
            gaps.append((ts - last[rid]) * 1000)
        last[rid] = ts
    return gaps, {"first_start": float(span[0]), "last_finish": float(span[1]), "events": int(span[2] or 0)}


async def bench_noop(a: argparse.Namespace, label: str = "") -> dict:
    async with KeelClient(API) as c:
        before = db_stats()
        t0 = time.time()
        ids = await start_many(c, "bench_noop", [{"steps": a.steps, "batch": not a.no_batch}] * a.runs)
        await wait_final(ids, a.timeout)
        wall = time.time() - t0
        after = db_stats()
    gaps, span = step_gaps(ids)
    total_steps = a.runs * a.steps
    dt_s = after["t"] - before["t"]
    res = {"workload": "noop", "label": label, "runs": a.runs, "steps_per_run": a.steps, "batching": not a.no_batch,
           "workers": len(containers("worker")), "wall_s": round(wall, 2),
           "steps_per_s": round(span["events"] / max(1e-6, span["last_finish"] - span["first_start"]), 1),
           "overhead_ms_p50": round(pct(gaps, 50), 2), "overhead_ms_p99": round(pct(gaps, 99), 2),
           "pg_commits_per_s": round((after["commits"] - before["commits"]) / dt_s, 1),
           "pg_wal_mb_per_s": round((after["wal_bytes"] - before["wal_bytes"]) / dt_s / 2**20, 2),
           "pg_commits_per_step": round((after["commits"] - before["commits"]) / total_steps, 2)}
    return res


async def bench_burst(a: argparse.Namespace) -> dict:
    async with KeelClient(API) as c:
        t0 = time.time()
        ids = await start_many(c, "bench_noop", [{"steps": 2}] * a.runs, conc=200)
        submit = time.time() - t0
        await wait_final(ids, a.timeout)
    with psycopg.connect(DSN) as db:
        waits = [float(r[0]) * 1000 for r in db.execute(
            "SELECT extract(epoch from first_claimed_at - created_at) FROM runs WHERE run_id = ANY(%s::uuid[])",
            (ids,)).fetchall()]
    return {"workload": "burst", "runs": a.runs, "submitted_in_s": round(submit, 2),
            "workers": len(containers("worker")),
            "queue_wait_ms_p50": round(pct(waits, 50), 1), "queue_wait_ms_p90": round(pct(waits, 90), 1),
            "queue_wait_ms_p99": round(pct(waits, 99), 1), "queue_wait_ms_max": round(max(waits), 1),
            "samples": sorted(waits)[:: max(1, len(waits) // 500)]}


async def bench_failover(a: argparse.Namespace) -> dict:
    """kill -9 a worker holding runs; measure kill -> RunResumed on another worker."""
    samples: list[float] = []
    async with KeelClient(API) as c:
        for rnd in range(a.rounds):
            ids = await start_many(c, "bench_slow", [{"steps": 40, "step_s": 0.5}] * a.runs)
            victim, held = None, []
            for _ in range(40):
                await asyncio.sleep(0.5)
                with psycopg.connect(DSN) as db:
                    for w in containers("worker"):
                        rows = db.execute("SELECT run_id::text FROM runs WHERE status='running' AND lease_owner LIKE %s",
                                          (hostname(w) + "%",)).fetchall()
                        if rows:
                            victim, held = w, [r[0] for r in rows]
                            break
                if victim:
                    break
            if not victim:
                continue
            t_kill = time.time()
            docker("kill", "--signal", "KILL", victim)
            for _ in range(240):
                await asyncio.sleep(0.5)
                with psycopg.connect(DSN) as db:
                    rows = db.execute("""SELECT run_id::text, min(extract(epoch from created_at)) FROM events
                        WHERE run_id = ANY(%s::uuid[]) AND type = 'RunResumed' AND payload->>'reason' = 'lease_expired'
                        AND created_at > to_timestamp(%s) GROUP BY 1""", (held, t_kill)).fetchall()
                if len(rows) == len(held):
                    break
            samples += [float(ts) - t_kill for _, ts in rows]
            docker("start", victim)
            await wait_final(ids, a.timeout)
            print(f"round {rnd + 1}: {len(rows)} runs failed over, median {statistics.median([float(t) - t_kill for _, t in rows]):.1f}s")
    return {"workload": "failover", "rounds": a.rounds, "lease_ttl_s": 30, "sweeper_interval_s": 1,
            "failover_s_p50": round(pct(samples, 50), 2), "failover_s_p99": round(pct(samples, 99), 2),
            "failover_s_min": round(min(samples), 2) if samples else None, "samples": samples}


async def bench_sleepers(a: argparse.Namespace) -> dict:
    async with KeelClient(API) as c:
        t0 = time.time()
        ids = await start_many(c, "bench_sleeper", [{"wakes": 3, "sleep_s": 5}] * a.runs, conc=128)
        await wait_final(ids, a.timeout)
    with psycopg.connect(DSN) as db:
        late = [float(r[0]) * 1000 for r in db.execute("""
            SELECT extract(epoch from e.created_at) - (e.payload->>'fire_at_ms')::bigint / 1000.0 FROM events e
            WHERE e.run_id = ANY(%s::uuid[]) AND e.type = 'TimerFired'""", (ids,)).fetchall()]
    return {"workload": "sleepers", "runs": a.runs, "timers": len(late), "wall_s": round(time.time() - t0, 1),
            "timer_lateness_ms_p50": round(pct(late, 50), 1), "timer_lateness_ms_p99": round(pct(late, 99), 1)}


def scale_workers(n: int) -> None:
    subprocess.run(["docker", "compose", "-f", "deploy/docker-compose.yml", "up", "-d", "--no-deps", "--scale",
                    f"worker={n}", "worker"], check=True, capture_output=True,
                   env={**os.environ, "MSYS_NO_PATHCONV": "1"})
    time.sleep(8)


async def bench_scaling(a: argparse.Namespace) -> dict:
    points = []
    for n in a.workers:
        scale_workers(n)
        r = await bench_noop(a, label=f"{n} workers")
        r["workers"] = n
        points.append(r)
        print(f"{n} workers: {r['steps_per_s']} steps/s, p99 {r['overhead_ms_p99']} ms, "
              f"{r['pg_commits_per_s']} commits/s, {r['pg_wal_mb_per_s']} MB/s WAL")
    scale_workers(a.restore_workers)
    return {"workload": "scaling", "points": points}


async def bench_sync_commit(a: argparse.Namespace) -> dict:
    res = {}
    for setting in ("on", "off"):
        with psycopg.connect(DSN, autocommit=True) as db:
            db.execute(f"ALTER SYSTEM SET synchronous_commit = {setting}")
            db.execute("SELECT pg_reload_conf()")
        time.sleep(1)
        res[setting] = await bench_noop(a, label=f"synchronous_commit={setting}")
    with psycopg.connect(DSN, autocommit=True) as db:
        db.execute("ALTER SYSTEM SET synchronous_commit = on")
        db.execute("SELECT pg_reload_conf()")
    return {"workload": "synchronous_commit", **res}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workload", choices=["noop", "burst", "failover", "sleepers", "scaling", "sync_commit"])
    ap.add_argument("--runs", type=int, default=200)
    ap.add_argument("--steps", type=int, default=10)
    ap.add_argument("--no-batch", action="store_true")
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    ap.add_argument("--restore-workers", type=int, default=4)
    ap.add_argument("--timeout", type=float, default=1800)
    ap.add_argument("--name", default="")
    a = ap.parse_args()
    fn: Any = {"noop": bench_noop, "burst": bench_burst, "failover": bench_failover, "sleepers": bench_sleepers,
               "scaling": bench_scaling, "sync_commit": bench_sync_commit}[a.workload]
    res = asyncio.run(fn(a))
    save(a.name or (a.workload + ("_nobatch" if a.workload == "noop" and a.no_batch else "")), res)


if __name__ == "__main__":
    main()
