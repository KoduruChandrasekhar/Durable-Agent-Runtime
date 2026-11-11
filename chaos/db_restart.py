"""Restart PostgreSQL under load: workers and servers retry, no acknowledged
event is lost, every run still finishes.

    python -m chaos.db_restart --json bench/results/db_restart.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import psycopg
from keel.client import KeelClient

from .common import API, DSN, containers, docker


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=200)
    ap.add_argument("--steps", type=int, default=40)
    ap.add_argument("--json", default="bench/results/db_restart.json")
    a = ap.parse_args()
    async with KeelClient(API, timeout=60) as c:
        ids = [(await c.start_run("bench_slow", {"steps": a.steps, "step_s": 0.2}))["run_id"] for _ in range(a.runs)]
    await asyncio.sleep(4)
    with psycopg.connect(DSN) as db:
        acked_before = dict(db.execute("SELECT run_id::text, next_seq - 1 FROM runs WHERE run_id = ANY(%s::uuid[])",
                                       (ids,)).fetchall())
    pg = containers("postgres")[0]
    t0 = time.time()
    docker("restart", "-t", "2", pg)
    print(f"restarted {pg} in {time.time() - t0:.1f}s")
    done = 0
    for _ in range(600):
        await asyncio.sleep(1)
        try:
            with psycopg.connect(DSN) as db:
                done = db.execute("SELECT count(*) FROM runs WHERE run_id = ANY(%s::uuid[]) AND status = 'completed'",
                                  (ids,)).fetchone()[0]
            if done == len(ids):
                break
        except psycopg.OperationalError:
            continue
    with psycopg.connect(DSN) as db:
        lost = sum(1 for rid, n in acked_before.items()
                   if db.execute("SELECT count(*) FROM events WHERE run_id = %s::uuid AND seq <= %s", (rid, n)).fetchone()[0] < n)
        gaps = db.execute("""SELECT count(*) FROM (SELECT seq, row_number() OVER (PARTITION BY run_id ORDER BY seq) rn
                             FROM events WHERE run_id = ANY(%s::uuid[])) t WHERE seq <> rn""", (ids,)).fetchone()[0]
    report = {"runs": len(ids), "completed": done, "runs_missing_acknowledged_events": lost, "seq_gaps": gaps,
              "recovered_in_s": round(time.time() - t0, 1), "passed": done == len(ids) and lost == 0 and gaps == 0}
    print(json.dumps(report, indent=2))
    json.dump(report, open(a.json, "w"), indent=2)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
