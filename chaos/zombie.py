"""The zombie test: freeze a worker (cgroup freezer: `docker pause`, the
container equivalent of SIGSTOP) past its 30 s lease, then resume it. None of
its writes may be accepted, and its runs must finish elsewhere.

    python -m chaos.zombie --runs 60 --pause 60 --json bench/results/zombie.json
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import time

import psycopg

from demos.refund_agent.fixtures import generate
from keel.client import KeelClient

from .common import API, DSN, MOCKS, containers, docker, hostname, reset_mocks


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=60)
    ap.add_argument("--pause", type=float, default=60)
    ap.add_argument("--json", default="bench/results/zombie.json")
    a = ap.parse_args()
    import httpx
    reset_mocks()
    tickets = generate(a.runs, seed=99, now=dt.datetime.now(dt.UTC), settle_wait_s=5,
                       mix=(1.0, 0.0, 0.0, 0.0))
    httpx.post(f"{MOCKS}/orders/admin/seed", json={"orders": [t["order"] for t in tickets]}, timeout=30)
    # Slow the model so runs are mid-flight on every worker when we freeze one.
    httpx.post("http://127.0.0.1:8000/admin/chaos", json={"target": "primary", "latency_ms_min": 1500,
                                                         "latency_ms_max": 2500}, timeout=10)
    async with KeelClient(API) as c:
        ids = [(await c.start_run("refund_agent", t["ticket"], version="2"))["run_id"] for t in tickets]
        victim = None
        held: list[tuple[str, int]] = []
        for _ in range(60):
            await asyncio.sleep(0.5)
            with psycopg.connect(DSN) as db:
                for w in containers("worker"):
                    h = hostname(w)
                    rows = db.execute("SELECT run_id::text, lease_epoch FROM runs WHERE status = 'running' "
                                      "AND lease_owner LIKE %s", (h + "%",)).fetchall()
                    if len(rows) >= 3:
                        victim, held = w, rows
                        break
            if victim:
                break
        if not victim:
            print("no worker held enough runs")
            return 1
        print(f"freezing {victim} holding {len(held)} runs for {a.pause}s (lease is 30 s)")
        t_pause = time.time()
        docker("pause", victim, check=True)
        await asyncio.sleep(a.pause)
        docker("unpause", victim, check=True)
        t_resume = time.time()
        print("resumed; waiting for every run to finish")
        for _ in range(240):
            st = [(await c.get_run(r))["status"] for r in ids]
            if all(s in ("completed", "failed", "cancelled") for s in st):
                break
            await asyncio.sleep(1)
    httpx.post("http://127.0.0.1:8000/admin/chaos", json={"target": "primary", "latency_ms_min": 0, "latency_ms_max": 0})
    with psycopg.connect(DSN) as db:
        # Any event written with the frozen worker's epochs after the freeze began?
        zombie_writes = 0
        reclaimed = 0
        for rid, epoch in held:
            zombie_writes += db.execute("SELECT count(*) FROM events WHERE run_id = %s::uuid AND epoch = %s AND "
                                        "created_at > to_timestamp(%s) + interval '1 second'",
                                        (rid, epoch, t_pause)).fetchone()[0]
            reclaimed += db.execute("SELECT count(*) > 0 FROM events WHERE run_id = %s::uuid AND epoch > %s",
                                    (rid, epoch)).fetchone()[0]
        statuses = db.execute("SELECT status, count(*) FROM runs WHERE run_id = ANY(%s::uuid[]) GROUP BY 1",
                              (ids,)).fetchall()
    report = {"frozen_container": victim, "runs_held": len(held), "pause_s": a.pause,
              "writes_accepted_from_zombie": zombie_writes, "held_runs_finished_elsewhere": reclaimed,
              "final_statuses": dict(statuses), "resumed_after_s": round(t_resume - t_pause, 1),
              "passed": zombie_writes == 0 and reclaimed == len(held)}
    print(json.dumps(report, indent=2))
    json.dump(report, open(a.json, "w"), indent=2)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
