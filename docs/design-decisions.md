# Design decisions

Each decision lists the alternative and the measurement that decided it.
Numbers come from `bench/results/*.json` (machine: 8-core laptop, Windows 11,
Docker Desktop with 8 CPUs / 7.6 GB, PostgreSQL 16, Go 1.26, Python 3.13).

## Steps run inline on the worker that holds the run (not separate activity tasks)

*Alternative:* Temporal's split into workflow tasks and activity tasks that
any worker pool can pick up.

*Why inline:* one round trip per step instead of three (schedule activity,
poll activity, complete activity), and far less code — the whole server is
one table of runs with their leases. The cost: a long step keeps the run on
one worker. That is acceptable because the expensive work (model inference,
sandboxed code) happens in separate services (gateway, sandbox), so a worker
mostly waits on I/O and runs 150 runs concurrently.

*Measured:* engine overhead per step and steps/s in
[README results](../README.md#results). In-process (no network), a journaled
no-op step costs ~184 µs vs 0.07 µs for a plain function call — the price is
dominated by the durable append, not the SDK.

## PostgreSQL as the queue, the log and everything else

*Alternative:* Kafka/Redis for the queue, a separate store for the log.

*Why Postgres:* the claim, the lease, the append and its consequences
(approval rows, child runs, outbox messages) commit in **one transaction**.
`FOR UPDATE SKIP LOCKED` makes claims non-blocking; `LISTEN/NOTIFY` wakes
pollers with a 1 s polling backup. Switching is justified only when commits
per second are the bottleneck; the scaling experiment shows where that is.

*Measured:* fairness — with 3,000 runs queued for tenant A, all 10 of tenant
B's runs started by claim #20 (`TestFairSchedulingAcrossTenants`). The
per-tenant running cap holds under 12 concurrent claimers.

## Run-level leases with fencing tokens

*Alternative:* trusting lease expiry alone, or distributed locks.

*Why:* a frozen worker (GC pause, `SIGSTOP`, partition) can wake up after its
lease expired and believe it still owns the run. Every append checks
`lease_epoch` and `next_seq` in the same `UPDATE` that serializes appends, so
a stale writer is rejected. Lease math uses the database clock only.

*Measured:* 1,000 rounds of two stale and two current appenders racing on one
run: 0 stale writes, exactly one current write per round, no gaps
(`TestFencingConcurrentAppenders`). The zombie test pauses a real worker
past its lease (see README). Across 1,000 simulation seeds, 1,858 stale
writes were rejected and no invariant broke.

## Finding the bottleneck: three fixes, measured before and after

The first scaling run showed throughput *falling* as workers were added
(422 → 359 → 290 → 274 steps/s for 1/2/4/8 workers) with p99 step overhead
at 4–5 s regardless of `synchronous_commit` — contention, not fsync. Three
causes, each fixed and measured on the same workload:

1. **A hot counter row.** The trigger keeping per-tenant `running`/`pending`
   counts exact updated one row per tenant on every claim, start, wake and
   finish, so a busy tenant's status changes serialized on one row lock.
   Counters moved to 16 shards per tenant (migration `00002`); tenants with
   a running cap still get a strict check by locking their row only then.
2. **NOTIFY inside transactions.** PostgreSQL serializes commits that call
   `pg_notify` on a global lock. Notifications are now coalesced (at most
   one per 10 ms, sent outside transactions); pollers still re-check every
   second, so a lost notification costs at most a second.
3. **Timers fired one transaction at a time.** Now a pool of 16 parallel
   transactions.

| same workload, same measurement | before | after |
|---|---|---|
| steps/s, 300 runs × 10 steps, 4 workers | 327 | 484 |
| step overhead p99 under that load | 3.6 s | 1.05 s |
| 5,000-run burst: queue wait p50 / p99 | 49.2 s / 58.2 s | 5.5 s / 8.6 s |
| 2,000 sleepers (6,000 timers): lateness p50 / p99 | 40.1 s / 43.3 s | 5.3 s / 8.4 s |

A second mistake was in the benchmark itself: measuring steps/s from the
first run's creation counted the time spent submitting runs through the API.
Measured over the steady-state window (first to last step completion) with
longer runs, the engine does **1,225 steps/s with batched appends vs 601
without** (0.95 vs 2.04 commits per step), scaling 820 → 937 → 1,266 steps/s
from 1 to 4 workers and falling to 644 at 8 workers, where eight Python
workers, the server and PostgreSQL (~1,400 commits/s at the peak) compete for
the laptop's 8 CPUs. Intrinsic overhead at light load (10 runs): 13 ms p50,
75 ms p99 per step; a raw append is 8.2 ms p50 inside Docker Desktop.
`synchronous_commit=off` changed nothing measurable (491 vs 470 steps/s), so
the durable setting stays on. Next steps, in order of expected payoff:
grouping appends from many workers into one server-side transaction, then
PgBouncer, then sharding the log by run id.

## Retried appends are acknowledged, not rejected

*Problem found while building:* a gRPC retry after a lost response hits
`expected_seq` already advanced, which looks exactly like a conflict. Since
only the holder of the current epoch can have written those rows, the server
compares the rows at `expected_seq` (same epoch, types, step ids) and
acknowledges. The simulation exercises this constantly (7,777 dropped
responses over 1,000 seeds).

## Reserve-then-settle budgets

*Alternative:* checking "is there budget left?" before each call.

*Why:* parallel calls all see budget left and overspend together. The
gateway reserves the worst-case cost (input tokens × input price + max
tokens × output price, of the most expensive model in the fallback chain plus
the escalation model) atomically in the run's row and the tenant's daily row,
then settles the real cost.

*Measured:* 100 parallel calls against a run budgeted for ~10:
`accepted=10, rejected=90`, never overspent
(`TestBudgetReserveThenSettleNeverOverspends`).

## Taint tracking instead of prompt instructions

*Alternative:* telling the model to ignore malicious instructions.

*Why:* the mock model in the suite is deliberately gullible, as real models
sometimes are. Restricting what a tainted run may do (high-risk tools need a
human; deterministic facts and CEL rules bound arguments) does not depend on
the model resisting.

*Measured:* 120-case suite, 0% attack success for both agent versions. The
price is false escalations: 58.3% when raw order notes reach the model, 1.7%
when they go through quarantined extraction first (v2).

## CEL for policies

*Alternative:* OPA/Rego, or Python expressions.

*Why:* CEL is safe to evaluate (no loops, no side effects, cost-limited), has
a Go implementation (`cel-go`) and a Python one (`celpy`) — so the
simulation's server decides exactly like the real one. The conformance suite
checks both engines agree. Rules fail closed: a condition that errors counts
as matched. Decisions are journaled, so editing the policy file never
changes how an existing run replays (`test_policy_edit_does_not_change_replay`).

## Deterministic simulation with a conformance suite

*Alternative:* only chaos testing the real deployment.

*Why:* chaos on Docker explores a handful of interleavings per hour; the
simulation explores thousands per minute and replays any failure from its
seed. To keep it honest, the simulation's server (`keel.testing.LocalServer`)
runs the same scenarios as the Go server in `tests/conformance` and both must
produce identical traces.

*Measured:* 1,000 full seeds in 10 minutes on 7 processes: 0 failures across
16,625 mid-call crashes, 37,356 freezes past the lease, 17,679 server
outages, 7,777 dropped responses, 4,841 duplicated requests and 1,858 fencing
rejections. Re-running a seed reproduces the same history fingerprint.

*Bugs the tests found while building:* the nondeterminism detector caught
that a tool step's recorded input hash used a human's *edited* arguments
(so the run diverged on its next replay) — the hash now covers the proposed
arguments; the secret-scan test caught a secret echoed through a workflow's
return value into `RunCompleted` — results and errors are now redacted too;
the simulation caught that a driver stuck during a server outage never
advanced time (a harness bug, but exactly the class of liveness bug the
simulator exists for).

## Model routing and caching

*Measured on the refund suite (8 trials):*

| strategy | pass^1 | pass^8 | $ per successful task |
|---|---|---|---|
| always-large | 1.000 | 1.000 | 0.002629 |
| always-small | 0.938 | 0.583 | 0.000134 |
| routed (small + escalate on invalid output) | 0.969 | 0.750 | 0.000279 |

Routing cuts cost per successful task by 89% versus always-large, and
escalation lifts pass^8 from 0.583 (always-small) to 0.750 — but not to
always-large's 1.000: escalation repairs malformed output, not wrong
decisions. A learned router (stretch goal) would target that gap. Eval
trials use fixed seeds (CRC32 of task, trial, version, strategy), so two runs
of the suite produce identical success, pass^k and cost figures.

## Gateway calls are detached from the caller

When a worker dies mid-LLM-call, the replacement worker re-sends the step
with the same `run_id:step_id` idempotency key. Originally the gateway
cancelled the upstream call when the dead worker's connection closed, so the
answer was lost and paid for twice. Running the upstream call detached
(`context.WithoutCancel`, with the gateway's own deadline) lets it finish and
land in the idempotency cache. Measured in the chaos demo: wasted tokens
fell from 137,245 of 814,873 (17%) to 35,225 of 818,925 (4.3%).

## The semantic-cache bug the chaos demo found

The first 1,000-run chaos demo passed every invariant — and was wrong.
Investigating why only 3 of 105 large refunds asked for approval showed
1,976 of ~4,260 LLM calls had been served from the **semantic cache**,
including the quarantined extraction step: "The TV from order ORD-10996…"
was answered with the cached extraction of "…ORD-10012…", because two long
texts that differ only in an order id embed almost identically. The
guardrails held (the wrong refunds were denied by `refund-only-when-eligible`
and `refund-only-the-ticket-order`, so no money moved), but **994 of 1,000
confirmation emails talked about another customer's order**, and the
invariant checker only counted emails.

Fixes, each with its measurement:

* The cache accepts a semantic hit only when the identifiers in the text
  (anything containing a digit, email addresses) match exactly. On the
  labeled pairs (`bench/cache_pairs.py`, now including id-only and
  amount-only differences), at threshold 0.80 the wrong-hit rate drops from
  16% to 0% at the same paraphrase hit rate (18%).
* No semantic caching for tool-calling requests; the namespace is the whole
  conversation before the last user message (system prompt, facts, earlier
  turns), not just the system prompt.
* A regression test (`TestSemanticCacheNeverCrossesIdentifiers`) and a new
  invariant: no email may mention an order other than the ticket's.

With the built-in hashed n-gram embedding, useful semantic hit rates need a
real embedding model; the threshold is set from the measured curve (0.80,
zero wrong hits with the guard on).
