"""The baseline: the same refund agent written the way many teams start. A
plain Python loop with try/except retries; tickets come from a queue with a
visibility timeout, so after a crash a ticket is redelivered and processed
from scratch. No journal, no idempotency keys (a fresh key per attempt, which
is what a naive retry does), no policy engine beyond the prompt.

Run it under exactly the same chaos as the runtime and compare:

    docker compose -f deploy/docker-compose.yml --profile naive up -d naive-worker
    python -m chaos.naive_baseline run --runs 1000 --json bench/results/naive_report.json
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import random
import socket
import threading
import time
import uuid
from collections import Counter

import httpx
import psycopg

from demos.refund_agent.agent import REFUND_WINDOW_DAYS, SYSTEM_PROMPT
from demos.refund_agent.fixtures import generate

DSN = os.environ.get("KEEL_DATABASE_URL", "postgresql://keel:keel@127.0.0.1:55433/keel")
MOCKS = os.environ.get("KEEL_MOCKS_URL", "http://127.0.0.1:9000")
LLM = os.environ.get("KEEL_MOCK_LLM_URL", "http://127.0.0.1:8000")
VISIBILITY_S = 90
MAX_DELIVERIES = 3

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS naive;
CREATE TABLE IF NOT EXISTS naive.queue (ticket_id TEXT PRIMARY KEY, payload JSONB NOT NULL, state TEXT NOT NULL,
  deliveries INT NOT NULL DEFAULT 0, visible_at TIMESTAMPTZ NOT NULL DEFAULT now(), done_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS naive.tokens (ticket_id TEXT, attempt TEXT, tokens INT, finished BOOL DEFAULT false);
"""


class Naive:
    def __init__(self) -> None:
        self.http = httpx.AsyncClient(timeout=10)
        self.tokens = 0

    async def llm(self, messages: list[dict], tools: list | None = None, json_mode: bool = False) -> dict:
        body: dict = {"model": "mock-small", "messages": messages, "temperature": 0}
        if tools:
            body["tools"] = tools
        if json_mode:
            body["response_format"] = {"type": "json_object"}
        for _ in range(3):  # try/except retries
            try:
                r = await self.http.post(f"{LLM}/v1/chat/completions", json=body)
                r.raise_for_status()
                d = r.json()
                self.tokens += d["usage"]["total_tokens"]
                return d["choices"][0]["message"]
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1)
        raise RuntimeError("llm failed")

    async def call(self, method: str, url: str, **kw: object) -> dict:
        last: Exception | None = None
        for _ in range(3):  # retry on any error, including "maybe it happened"
            try:
                r = await self.http.request(method, url, **kw)
                r.raise_for_status()
                return r.json()
            except Exception as e:  # noqa: BLE001
                last = e
                await asyncio.sleep(0.5)
        raise RuntimeError(f"{url}: {last}")

    async def process(self, t: dict) -> None:
        conv = "\n".join([*t.get("history", []), t["message"]])
        ext = await self.llm([{"role": "system", "content": "You extract fields ... order_id request_type reason"},
                              {"role": "user", "content": conv}], json_mode=True)
        try:
            fields = json.loads(ext["content"] or "{}")
        except ValueError:
            fields = {}
        order = await self.call("GET", f"{MOCKS}/orders/orders/{fields.get('order_id')}")
        delivered = dt.datetime.fromisoformat(order["delivered_at"])
        eligible = (order["customer_email"] == t["customer_email"] and order["status"] == "delivered"
                    and (dt.datetime.now(dt.UTC) - delivered).days <= REFUND_WINDOW_DAYS and not order["refunded"])
        facts = {"order_id": order["order_id"], "eligible": eligible, "amount_inr": order["amount_inr"],
                 "customer_name": t["customer_name"], "customer_email": t["customer_email"]}
        tools = [{"type": "function", "function": {"name": "issue_refund", "parameters": {"type": "object"}}}]
        msgs = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": "FACTS: " + json.dumps(facts)},
                {"role": "user", "content": conv}]
        reply = await self.llm(msgs, tools)
        for tc in reply.get("tool_calls") or []:
            args = json.loads(tc["function"]["arguments"])
            # A fresh key per attempt: every retry looks like a new refund.
            await self.call("POST", f"{MOCKS}/payments/refunds", json={"order_id": args["order_id"],
                            "amount_inr": int(str(args["amount_inr"]).strip("₹").replace(",", "")) if str(args["amount_inr"]).strip("₹").replace(",", "").isdigit() else order["amount_inr"]},
                            headers={"Idempotency-Key": str(uuid.uuid4())})
            await asyncio.sleep(t.get("settle_wait_s", 60))  # in-process wait: a crash here loses the context
        final = await self.llm(msgs + [{"role": "user", "content": "now reply"}], json_mode=True)
        try:
            body = json.loads(final["content"] or "{}")
        except ValueError:
            body = {"reply": final.get("content") or ""}
        to = body.get("email_to") or t["customer_email"]
        await self.call("POST", f"{MOCKS}/email/messages", json={"to": to, "subject": t["ticket_id"],
                        "body": str(body.get("reply", ""))}, headers={"X-Keel-Key": t["ticket_id"]})
        await self.call("POST", f"{MOCKS}/crm/notes", json={"ticket_id": t["ticket_id"], "note": "done"},
                        headers={"Idempotency-Key": str(uuid.uuid4())})


async def worker_main(concurrency: int = 150) -> None:
    me = f"{socket.gethostname()}-{os.getpid()}"
    with psycopg.connect(DSN, autocommit=True) as db:
        db.execute(SCHEMA)

    async def loop() -> None:
        while True:
            with psycopg.connect(DSN, autocommit=True) as db:
                row = db.execute("""UPDATE naive.queue SET deliveries = deliveries + 1, state = 'claimed',
                    visible_at = now() + interval '1 second' * %s WHERE ticket_id = (
                      SELECT ticket_id FROM naive.queue WHERE state IN ('queued','claimed') AND visible_at <= now()
                       AND deliveries < %s ORDER BY visible_at LIMIT 1 FOR UPDATE SKIP LOCKED)
                    RETURNING ticket_id, payload""", (VISIBILITY_S, MAX_DELIVERIES)).fetchone()
            if row is None:
                await asyncio.sleep(1)
                continue
            n = Naive()
            attempt = f"{me}-{uuid.uuid4().hex[:6]}"
            ok = False
            try:
                await n.process(row[1])
                ok = True
            except Exception:  # noqa: BLE001 - the naive agent gives up on this delivery
                pass
            with psycopg.connect(DSN, autocommit=True) as db:
                db.execute("INSERT INTO naive.tokens VALUES (%s, %s, %s, %s)", (row[0], attempt, n.tokens, ok))
                if ok:
                    db.execute("UPDATE naive.queue SET state = 'done', done_at = now() WHERE ticket_id = %s", (row[0],))
    await asyncio.gather(*(loop() for _ in range(concurrency)))


def run_main(a: argparse.Namespace) -> dict:
    from .common import Killer, configure_chaos, reset_mocks
    reset_mocks()
    tickets = generate(a.runs, seed=a.seed, now=dt.datetime.now(dt.UTC), settle_wait_s=a.settle)
    httpx.post(f"{MOCKS}/orders/admin/seed", json={"orders": [t["order"] for t in tickets]}, timeout=60).raise_for_status()
    with psycopg.connect(DSN, autocommit=True) as db:
        db.execute(SCHEMA)
        db.execute("TRUNCATE naive.queue, naive.tokens")
        with db.cursor() as cur:
            cur.executemany("INSERT INTO naive.queue (ticket_id, payload, state) VALUES (%s, %s, 'queued')",
                            [(t["ticket"]["ticket_id"], json.dumps(t["ticket"])) for t in tickets])
    configure_chaos(payments_fail_after_commit=a.fail_after_commit, email_fail_after_commit=a.fail_after_commit,
                    llm_error_rate=a.llm_error_rate)
    killer = Killer("naive-worker", every=a.kill_every, restart_after=2.0, seed=a.seed)
    kt = threading.Thread(target=killer.run, daemon=True)
    if a.kill_every > 0:
        kt.start()
    t0 = time.time()
    while time.time() - t0 < a.timeout:
        time.sleep(5)
        with psycopg.connect(DSN) as db:
            states = dict(db.execute("SELECT state, count(*) FROM naive.queue GROUP BY 1").fetchall())
            open_ = db.execute("SELECT count(*) FROM naive.queue WHERE state <> 'done' AND deliveries < %s",
                               (MAX_DELIVERIES,)).fetchone()[0]
        print(f"[{time.time() - t0:6.0f}s] {states} kills={len(killer.events)}", flush=True)
        if open_ == 0:
            break
    time.sleep(a.settle + 10)  # let in-flight deliveries finish their side effects
    killer.stopped = True
    configure_chaos()
    by_ticket = {t["ticket"]["ticket_id"]: t for t in tickets}
    with psycopg.connect(DSN) as db:
        done = db.execute("SELECT count(*) FROM naive.queue WHERE state = 'done'").fetchone()[0]
        tok = db.execute("SELECT coalesce(sum(tokens),0), coalesce(sum(tokens) FILTER (WHERE NOT finished),0) FROM naive.tokens").fetchone()
    # Killed attempts cannot report their tokens, so count what the model served.
    served = httpx.get(f"{LLM}/admin/stats", timeout=30).json()["primary"]["tokens"]
    with psycopg.connect(DSN) as db:
        completed_tokens = db.execute("SELECT coalesce(sum(tokens),0) FROM naive.tokens WHERE finished").fetchone()[0]
    ledger = httpx.get(f"{MOCKS}/payments/admin/ledger", timeout=60).json()
    messages = httpx.get(f"{MOCKS}/email/admin/messages", timeout=60).json()
    order_to_ticket = {t["order"]["order_id"]: t for t in tickets}
    per_order = Counter(r["order_id"] for r in ledger)
    per_ticket_email = Counter(m["subject"] for m in messages)
    unauthorized = sum(1 for r in ledger if (order_to_ticket.get(r["order_id"], {}).get("expect", {}).get("refund", 0) == 0)
                       or r["amount_inr"] > 5000)
    report = {"runs": a.runs, "completed": done, "lost_runs": a.runs - done,
              "refunds": len(ledger), "duplicate_refunds": sum(n - 1 for n in per_order.values() if n > 1),
              "max_refunds_per_order": max(per_order.values(), default=0),
              "emails": len(messages), "duplicate_emails": sum(n - 1 for n in per_ticket_email.values() if n > 1),
              "emails_to_wrong_address": sum(1 for m in messages if m["to"] != by_ticket.get(m["subject"], {}).get("ticket", {}).get("customer_email")),
              "unauthorized_high_risk_actions": unauthorized,
              "llm_tokens": served, "wasted_llm_tokens": served - completed_tokens,
              "wasted_llm_tokens_recorded_by_workers": tok[1], "worker_kills": len(killer.events),
              "wall_s": round(time.time() - t0, 1), "config": vars(a)}
    print(json.dumps(report, indent=2))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--concurrency", type=int, default=150)
    r = sub.add_parser("run")
    r.add_argument("--runs", type=int, default=1000)
    r.add_argument("--seed", type=int, default=7)
    r.add_argument("--kill-every", type=float, default=3.0)
    r.add_argument("--llm-error-rate", type=float, default=0.10)
    r.add_argument("--fail-after-commit", type=float, default=0.20)
    r.add_argument("--settle", type=float, default=60.0)
    r.add_argument("--timeout", type=float, default=2400)
    r.add_argument("--json", default="bench/results/naive_report.json")
    a = ap.parse_args()
    if a.cmd == "worker":
        asyncio.run(worker_main(a.concurrency))
    else:
        rep = run_main(a)
        json.dump(rep, open(a.json, "w"), indent=2)


if __name__ == "__main__":
    random.seed()
    main()
