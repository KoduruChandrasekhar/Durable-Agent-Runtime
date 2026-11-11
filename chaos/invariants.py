"""The invariant checker, run after every chaos experiment:

* every run is completed, failed or cancelled, or waiting with a valid reason
  (a timer, a signal, an approval)
* each order has at most one refund; each completed ticket has exactly one email
* no high-risk action ran without authorization (refunds the ticket did not
  call for, or above 5,000 INR, without a human approval)
* every run's event sequence numbers are contiguous
* no event was written with an older epoch after an event with a newer one

    python -m chaos.invariants --manifest bench/results/chaos_manifest.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from typing import Any

import httpx
import psycopg

from .common import DSN, MOCKS

VALID_WAITS = {"timer", "signal", "approval", "child", "approval,timer", "signal,timer"}


def check(manifest: dict, dsn: str = DSN, mocks: str = MOCKS) -> dict[str, Any]:
    runs: dict[str, dict] = manifest["runs"]
    ids = list(runs)
    with psycopg.connect(dsn) as db:
        rows = db.execute("""SELECT run_id::text, status, coalesce(wait_reason,''), error::text FROM runs
                             WHERE run_id = ANY(%s::uuid[])""", (ids,)).fetchall()
        status = {r[0]: (r[1], r[2], r[3]) for r in rows}
        gaps = db.execute("""SELECT count(*) FROM (SELECT seq, row_number() OVER (PARTITION BY run_id ORDER BY seq) rn
                             FROM events WHERE run_id = ANY(%s::uuid[])) t WHERE seq <> rn""", (ids,)).fetchone()[0]
        epoch_back = db.execute("""SELECT count(*) FROM (SELECT epoch, lag(epoch) OVER (PARTITION BY run_id ORDER BY seq) prev
                                   FROM events WHERE run_id = ANY(%s::uuid[])) t WHERE epoch < prev""", (ids,)).fetchone()[0]
        approvals = db.execute("""SELECT run_id::text, action->'args'->>'order_id', status FROM approvals
                                  WHERE run_id = ANY(%s::uuid[]) AND action->>'tool' = 'issue_refund'""", (ids,)).fetchall()
        resumes = db.execute("""SELECT count(*), count(*) FILTER (WHERE payload->>'reason' = 'lease_expired')
                                FROM events WHERE run_id = ANY(%s::uuid[]) AND type = 'RunResumed'""", (ids,)).fetchone()
        wasted = db.execute("""SELECT coalesce(sum((payload->'tokens'->>'input')::int + (payload->'tokens'->>'output')::int), 0),
                                      count(*) FROM events
                               WHERE run_id = ANY(%s::uuid[]) AND type = 'StepCompleted' AND payload->>'kind' = 'llm'
                                 AND (payload->>'reexecuted_after_crash')::bool
                                 AND coalesce(payload->>'cache_hit','') <> 'idempotent'""", (ids,)).fetchone()
        tokens = db.execute("""SELECT coalesce(sum((payload->'tokens'->>'input')::int + (payload->'tokens'->>'output')::int), 0)
                               FROM events WHERE run_id = ANY(%s::uuid[]) AND type = 'StepCompleted'
                               AND payload->>'kind' = 'llm'""", (ids,)).fetchone()[0]
    with httpx.Client(timeout=60) as c:
        ledger = c.get(f"{mocks}/payments/admin/ledger").json()
        messages = c.get(f"{mocks}/email/admin/messages").json()
    counts = Counter(s for s, _, _ in status.values())
    stuck = [rid for rid, (s, w, _) in status.items() if s in ("pending", "running") or (s == "waiting" and w not in VALID_WAITS)]
    missing = [rid for rid in ids if rid not in status]
    run_of_key = lambda k: (k or "").split(":")[0]  # noqa: E731 - key = run_id:step_id
    # Only this experiment's effects: other experiments' runs share the mock
    # ledgers (and their generated order ids); they are reported separately.
    foreign = [r for r in ledger if run_of_key(r["idempotency_key"]) not in runs]
    ledger = [r for r in ledger if run_of_key(r["idempotency_key"]) in runs]
    messages = [m for m in messages if run_of_key(m["keel_key"]) in runs]
    refunds_per_order = Counter(r["order_id"] for r in ledger)
    emails_per_run = Counter(run_of_key(m["keel_key"]) for m in messages)
    approved = {(rid, oid) for rid, oid, st in approvals if st == "approved"}
    unauthorized = []
    for r in ledger:
        rid = run_of_key(r["idempotency_key"])
        meta = runs.get(rid)
        if meta is None:
            continue
        authorized = (rid, r["order_id"]) in approved
        legit = meta["expect_refund"] > 0 and r["order_id"] == meta["order_id"] and r["amount_inr"] <= meta["expect_refund"]
        if not authorized and (not legit or r["amount_inr"] > 5000):
            unauthorized.append({"run": rid, "order": r["order_id"], "amount": r["amount_inr"], "kind": meta["kind"]})
    completed = [rid for rid, (s, _, _) in status.items() if s == "completed"]
    # Every email must talk about the ticket's own order (catches answers
    # crossing between customers, e.g. through a cache).
    import re
    wrong_order_emails = []
    for m in messages:
        meta = runs.get(run_of_key(m["keel_key"]))
        if meta is None:
            continue
        mentioned = set(re.findall(r"ORD-\d{5}", m.get("body") or ""))
        if mentioned - {meta["order_id"]}:
            wrong_order_emails.append({"run": run_of_key(m["keel_key"]), "order": meta["order_id"],
                                       "mentions": sorted(mentioned)})
    email_violations = [rid for rid in completed if emails_per_run.get(rid, 0) != 1]
    failed_errors = Counter(json.loads(e or "{}").get("type", "?") for s, _, e in status.values() if s == "failed")
    report = {
        "runs": len(ids), "by_status": dict(counts), "lost_runs": len(missing) + len(stuck),
        "stuck_runs": stuck[:10], "failed_by_error": dict(failed_errors),
        "refunds": len(ledger), "max_refunds_per_order": max(refunds_per_order.values(), default=0),
        "refunds_by_runs_outside_this_experiment": len(foreign),
        "duplicate_refunds": sum(n - 1 for n in refunds_per_order.values() if n > 1),
        "emails": len(messages), "max_emails_per_run": max(emails_per_run.values(), default=0),
        "duplicate_emails": sum(n - 1 for n in emails_per_run.values() if n > 1),
        "completed_without_exactly_one_email": len(email_violations),
        "emails_about_another_order": len(wrong_order_emails), "wrong_order_examples": wrong_order_emails[:5],
        "unauthorized_high_risk_actions": len(unauthorized), "unauthorized_examples": unauthorized[:5],
        "seq_gaps_or_duplicates": gaps, "epoch_regressions": epoch_back,
        "recoveries": resumes[0], "recoveries_after_crash": resumes[1],
        "llm_tokens": tokens, "wasted_llm_tokens": wasted[0], "llm_calls_reexecuted": wasted[1],
    }
    report["passed"] = (report["lost_runs"] == 0 and report["max_refunds_per_order"] <= 1
                        and report["completed_without_exactly_one_email"] == 0
                        and report["emails_about_another_order"] == 0
                        and report["unauthorized_high_risk_actions"] == 0 and gaps == 0 and epoch_back == 0)
    return report


def print_report(r: dict) -> None:
    ok = lambda b: "PASS" if b else "FAIL"  # noqa: E731
    print("\n===================== invariant report =====================")
    print(f"runs                         {r['runs']}  {r['by_status']}")
    print(f"lost runs                    {r['lost_runs']:<6} {ok(r['lost_runs'] == 0)}")
    print(f"max refunds per order        {r['max_refunds_per_order']:<6} {ok(r['max_refunds_per_order'] <= 1)}")
    print(f"emails per completed ticket  exactly 1 for all: {ok(r['completed_without_exactly_one_email'] == 0)}"
          f" (duplicates: {r['duplicate_emails']})")
    print(f"emails about another order   {r['emails_about_another_order']:<6} {ok(r['emails_about_another_order'] == 0)}")
    print(f"unauthorized high-risk acts  {r['unauthorized_high_risk_actions']:<6} {ok(r['unauthorized_high_risk_actions'] == 0)}")
    print(f"seq gaps / epoch regressions {r['seq_gaps_or_duplicates']} / {r['epoch_regressions']}"
          f"  {ok(r['seq_gaps_or_duplicates'] == 0 and r['epoch_regressions'] == 0)}")
    print(f"recoveries after a crash     {r['recoveries_after_crash']} (of {r['recoveries']} resumes)")
    print(f"wasted LLM tokens            {r['wasted_llm_tokens']} of {r['llm_tokens']}")
    print(f"failed runs by error         {r['failed_by_error']}")
    print(f"OVERALL                      {ok(r['passed'])}")
    print("============================================================\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--json", default="")
    a = ap.parse_args()
    r = check(json.load(open(a.manifest, encoding="utf-8")))
    print_report(r)
    if a.json:
        json.dump(r, open(a.json, "w"), indent=2)
    return 0 if r["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
