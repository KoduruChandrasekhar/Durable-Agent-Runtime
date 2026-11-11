"""`make chaos-demo`: 1,000 concurrent refund-agent runs while a random worker
is killed every 3 seconds, 10% of LLM calls fail and 20% of payment and email
calls fail *after committing*. An approver bot approves 80% of approval
requests after a random delay and rejects the rest. At the end the invariant
checker prints the report.

    python -m chaos.demo --runs 1000 --json bench/results/chaos_report.json
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import random
import threading
import time
from collections import Counter

import httpx
from keel.client import KeelClient

from demos.refund_agent.fixtures import generate

from .common import API, MOCKS, Killer, configure_chaos, reset_mocks
from .invariants import check, print_report


async def approver_bot(c: KeelClient, stop: asyncio.Event, approve_rate: float, rng: random.Random,
                       stats: Counter, runs: dict) -> None:
    seen: set[str] = set()
    pending: list[tuple[float, str, str]] = []
    while not stop.is_set():
        try:
            for a in await c.approvals("pending"):
                if a["approval_id"] not in seen and a["run_id"] in runs:  # only this experiment's runs
                    seen.add(a["approval_id"])
                    decision = "approve" if rng.random() < approve_rate else "reject"
                    pending.append((time.time() + rng.uniform(0.5, 8.0), a["approval_id"], decision))
            now = time.time()
            for item in [p for p in pending if p[0] <= now]:
                pending.remove(item)
                await c.decide(item[1], item[2], comment="approver bot")
                stats[item[2]] += 1
        except Exception:  # noqa: BLE001 - the API may be briefly unavailable; keep going
            pass
        await asyncio.sleep(1.0)


async def run(args: argparse.Namespace) -> dict:
    rng = random.Random(args.seed)
    now = dt.datetime.now(dt.UTC)
    tickets = generate(args.runs, seed=args.seed, now=now, settle_wait_s=args.settle)
    print(f"seeding {len(tickets)} orders and resetting mocks ...")
    reset_mocks()
    with httpx.Client(timeout=60) as h:
        h.post(f"{MOCKS}/orders/admin/seed", json={"orders": [t["order"] for t in tickets]}).raise_for_status()
    configure_chaos(payments_fail_after_commit=args.fail_after_commit, email_fail_after_commit=args.fail_after_commit,
                    llm_error_rate=args.llm_error_rate)
    c = KeelClient(API, timeout=60)
    sem = asyncio.Semaphore(64)
    nonce = int(time.time())  # a new demo is a new set of runs; retries within it stay idempotent
    manifest: dict = {"runs": {}, "started_at": time.time(), "config": vars(args)}

    async def start(t: dict) -> None:
        async with sem:
            r = await c.start_run("refund_agent", t["ticket"], version=args.version,
                                  start_key=f"chaos-{args.seed}-{nonce}-{t['ticket']['ticket_id']}")
            manifest["runs"][r["run_id"]] = {"kind": t["kind"], "order_id": t["order"]["order_id"],
                                             "expect_refund": t["expect"]["refund"],
                                             "ticket_id": t["ticket"]["ticket_id"]}

    killer = Killer("worker", every=args.kill_every, restart_after=2.0, seed=args.seed)
    kt = threading.Thread(target=killer.run, daemon=True)
    t0 = time.time()
    await asyncio.gather(*(start(t) for t in tickets))
    print(f"started {len(manifest['runs'])} runs in {time.time() - t0:.1f}s; killing a worker every {args.kill_every}s")
    if args.kill_every > 0:
        kt.start()
    stop = asyncio.Event()
    decisions: Counter = Counter()
    bot = asyncio.create_task(approver_bot(c, stop, args.approve_rate, rng, decisions, manifest["runs"]))
    ids = list(manifest["runs"])
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        await asyncio.sleep(5)
        try:
            page = await asyncio.gather(*(c.get_run(r) for r in rng.sample(ids, min(200, len(ids)))))
            sample = Counter(p["status"] for p in page)
            done = await _count_final(c, ids)
        except Exception as e:  # noqa: BLE001
            print("progress check failed:", e)
            continue
        print(f"[{time.time() - t0:6.0f}s] final {done}/{len(ids)}  sample {dict(sample)}  kills={len(killer.events)} "
              f"approvals={dict(decisions)}", flush=True)
        if done == len(ids):
            break
    killer.stopped = True
    stop.set()
    await bot
    if kt.is_alive():
        kt.join(timeout=30)
    configure_chaos()
    manifest["finished_at"] = time.time()
    manifest["kills"] = len(killer.events)
    await c.aclose()
    with open(args.manifest, "w") as f:
        json.dump(manifest, f)
    report = check(manifest)
    report.update(wall_s=round(manifest["finished_at"] - t0, 1), worker_kills=len(killer.events),
                  approvals_decided=dict(decisions), config=vars(args))
    print_report(report)
    return report


async def _count_final(c: KeelClient, ids: list[str]) -> int:
    import psycopg

    from .common import DSN
    with psycopg.connect(DSN) as db:
        return db.execute("SELECT count(*) FROM runs WHERE run_id = ANY(%s::uuid[]) AND status IN "
                          "('completed','failed','cancelled')", (ids,)).fetchone()[0]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--version", default="2")
    ap.add_argument("--kill-every", type=float, default=3.0)
    ap.add_argument("--llm-error-rate", type=float, default=0.10)
    ap.add_argument("--fail-after-commit", type=float, default=0.20)
    ap.add_argument("--approve-rate", type=float, default=0.80)
    ap.add_argument("--settle", type=float, default=60.0, help="refund settle wait (1 minute in the demo)")
    ap.add_argument("--timeout", type=float, default=2400)
    ap.add_argument("--manifest", default="bench/results/chaos_manifest.json")
    ap.add_argument("--json", default="bench/results/chaos_report.json")
    a = ap.parse_args()
    report = asyncio.run(run(a))
    with open(a.json, "w") as f:
        json.dump(report, f, indent=2)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
