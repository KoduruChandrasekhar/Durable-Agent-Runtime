"""Seeds the refund demo after `docker compose up`: a few orders and tickets
of every kind (small refund, large refund needing approval, ineligible,
injection attempt), so the UI has runs and an approval to look at."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import time

import httpx
from keel.client import KeelClient

from demos.refund_agent.fixtures import generate


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mocks", default="http://127.0.0.1:9000")
    ap.add_argument("--api", default="http://127.0.0.1:8080")
    ap.add_argument("--n", type=int, default=12)
    a = ap.parse_args()
    tickets = generate(a.n, seed=2026, now=dt.datetime.now(dt.UTC), settle_wait_s=20)
    for _ in range(60):
        try:
            httpx.post(f"{a.mocks}/orders/admin/seed", json={"orders": [t["order"] for t in tickets]}, timeout=10).raise_for_status()
            break
        except Exception:  # noqa: BLE001 - services still starting
            time.sleep(2)
    async with KeelClient(a.api) as c:
        for _ in range(60):  # wait until a worker has announced the workflow
            try:
                if any(w["name"] == "refund_agent" for w in await c.workflows()):
                    break
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(2)
        for t in tickets:
            r = await c.start_run("refund_agent", t["ticket"], version="2", start_key="demo-" + t["ticket"]["ticket_id"])
            print(f"{t['kind']:10s} {t['ticket']['ticket_id']} -> run {r['run_id']}")
        r = await c.start_run("research_agent", {"topic": "durable execution", "pause_s": 30, "batch_size": 2},
                              start_key="demo-research")
        print(f"research   -> run {r['run_id']}")


if __name__ == "__main__":
    asyncio.run(main())
