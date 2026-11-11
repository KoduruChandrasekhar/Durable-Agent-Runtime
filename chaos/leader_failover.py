"""Kill the server replica holding the advisory lock: the background loops
(sweeper, timers, approval expiry, outbox) must move to the other replica
within seconds, and no timer may get stuck.

    python -m chaos.leader_failover --json bench/results/leader_failover.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

import httpx
import psycopg
from keel.client import KeelClient

from .common import DSN, containers, docker

REPLICAS = {"server": os.environ.get("KEEL_URL", "http://127.0.0.1:8080"),
            "server-standby": os.environ.get("KEEL_STANDBY_URL", "http://127.0.0.1:8082")}


def leader() -> str | None:
    for svc, url in REPLICAS.items():
        try:
            if httpx.get(url + "/ready", timeout=2).json().get("leader"):
                return svc
        except Exception:  # noqa: BLE001
            pass
    return None


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=100)
    ap.add_argument("--json", default="bench/results/leader_failover.json")
    a = ap.parse_args()
    for _ in range(30):
        if leader():
            break
        time.sleep(1)
    old = leader()
    print("leader:", old)
    other = [s for s in REPLICAS if s != old][0]
    api = REPLICAS[other]  # keep using the replica that survives
    async with KeelClient(api) as c:
        ids = [(await c.start_run("bench_sleeper", {"wakes": 3, "sleep_s": 4}))["run_id"] for _ in range(a.runs)]
        await asyncio.sleep(3)
        victim = containers(old)[0]
        t_kill = time.time()
        docker("kill", "--signal", "KILL", victim)
        takeover = None
        while time.time() - t_kill < 60:
            try:
                if httpx.get(api + "/ready", timeout=2).json().get("leader"):
                    takeover = time.time() - t_kill
                    break
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(0.2)
        print(f"{other} became leader {takeover:.1f}s after the kill" if takeover else "no takeover!")
        deadline = time.time() + 180
        while time.time() < deadline:
            with psycopg.connect(DSN) as db:
                n = db.execute("SELECT count(*) FROM runs WHERE run_id = ANY(%s::uuid[]) AND status = 'completed'",
                               (ids,)).fetchone()[0]
            if n == len(ids):
                break
            await asyncio.sleep(1)
    docker("start", victim)
    with psycopg.connect(DSN) as db:
        late = db.execute("""SELECT max(extract(epoch from created_at) - (payload->>'fire_at_ms')::bigint / 1000.0)
                             FROM events WHERE run_id = ANY(%s::uuid[]) AND type = 'TimerFired'""", (ids,)).fetchone()[0]
    report = {"old_leader": old, "new_leader": other, "takeover_s": round(takeover, 2) if takeover else None,
              "sleeper_runs": len(ids), "completed": n, "max_timer_lateness_s": round(float(late or 0), 2),
              "passed": bool(takeover) and takeover < 15 and n == len(ids)}
    print(json.dumps(report, indent=2))
    json.dump(report, open(a.json, "w"), indent=2)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
