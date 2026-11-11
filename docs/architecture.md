# Architecture

The control plane decides who runs what and owns every write to the log; the
execution plane does the work and can lose any machine at any moment.

```
                 clients: apps · keel CLI · replay debugger + approvals UI
                                     │  REST (start, signal, approve, fork)
   ┌─────────────── control plane ───┴──────────────┐   ┌──────────── execution plane ─────────────┐
   │ keel-server (Go, N stateless replicas)          │   │ workers (Python SDK), many runs each     │
   │  · REST API            · gRPC WorkerService ◄───┼───┤  poll · replay · append · heartbeat      │
   │  · policy engine (CEL) · leader-only loops:     │   │        │                    │            │
   │    sweeper, timers, approval expiry, outbox     │   │  model gateway (Go)     tools / MCP /    │
   └───────────────┬─────────────────────────────────┘   │  budgets · routing ·    sandbox (Go +    │
                   │ one transaction per append           │  rate limits · cache    Docker)          │
            ┌──────┴───────┐                              └────────┬───────────────────┬────────────┘
            │ PostgreSQL   │ runs · events · signals ·             │                   │
            │              │ approvals · outbox · tenants   LLM providers        payments · email · CRM
            └──────────────┘                                (mock, local, hosted)  (mocks with chaos)
```

## Components

| Path | What it is |
|---|---|
| `proto/keel/v1/worker.proto` | Worker protocol: `PollRun`, `AppendEvents`, `Heartbeat`, `Suspend`, `Finish`, `EvaluatePolicy` |
| `server/internal/store` | The only code that touches PostgreSQL: fenced appends, fair claims, waits, fork, retention |
| `server/internal/workerapi` | gRPC service; long-poll woken by `LISTEN runs_ready` with a 1 s polling backup |
| `server/internal/api` | REST API, SSE event/token streams, PII masking, scopes, tenant isolation |
| `server/internal/scheduler` | Leader election (`pg_try_advisory_lock` on a dedicated session) and the background loops |
| `server/internal/policy` | CEL rules → allow / deny / require_approval; fails closed |
| `gateway/` | Model gateway: providers (OpenAI-compatible, Anthropic, mock), retries + retry budget, circuit breakers, fallback chains, reserve-then-settle budgets, token buckets (memory or Redis Lua), exact + semantic cache, routing + escalation, token streaming |
| `sandbox/` | Single-use locked-down containers, warm pool, reaper |
| `sdk/python/keel` | `@workflow`, `Context`, journal + replay, executor, worker, tools/MCP, guardrails, `LocalServer` + `run_with_crashes` |
| `demos/` | Refund agent (3 versions), research agent, data-analysis agent, mock services and the scripted mock LLM |
| `sim/` | Deterministic simulation (seeded faults, invariants, liveness) |
| `chaos/`, `bench/`, `evals/` | Chaos experiments + invariant checker, benchmarks + charts, eval harness + injection suite |
| `ui/` | Replay debugger and approvals inbox (React + TypeScript) |

## Data model

`runs` carries its own lease (`lease_owner`, `lease_epoch`, `lease_expires_at`),
`next_seq`, wait state (`waiting_on`, `wake_at`), budget (`budget_usd`,
`spent_usd`, `reserved_usd`), `tainted` and declared `facts`. A trigger keeps
per-tenant `running`/`pending` counters exact inside the same transaction as
every status change, which is what fair scheduling reads.

`events` is partitioned by the month the run was created, so all events of a
run live in one partition and `(run_id, seq)` stays unique; partitions of
finished runs can be dropped. Payloads over 64 KB move to a content-addressed
blob store and the row keeps `{"blob": "sha256:…"}`.

## Appending safely

```sql
UPDATE runs SET next_seq = next_seq + $n
 WHERE run_id = $1 AND lease_epoch = $2 AND next_seq = $3 AND status = 'running';
-- 0 rows: lease lost (epoch moved on) or sequence conflict -> roll back, worker stops
INSERT INTO events ... -- seq $3 .. $3+n-1, plus derived rows (approvals, child runs, outbox) in the same transaction
```

A retried append whose first attempt committed but whose response was lost is
acknowledged instead of rejected: only the holder of that epoch can have
written those rows.

## Run lifecycle

`pending → running → (waiting ↔ pending) → completed | failed | cancelled`.
A crash or an expired lease sends a running run back to `pending`; the next
claim increments the epoch and appends `RunResumed` with the reason, which
the debugger shows as a recovery marker. Waits (timers, signals, approvals,
child runs) release the worker; a condition already satisfied when the
worker suspends (a signal that arrived early, a decided approval, a finished
child, a due timer) wakes the run in the same transaction.

## Replay

The worker folds the history into a journal (step id → status/result/
failures/attempts; values; timers; signals; approvals; children; policy
decisions), runs the workflow from the top, and for each `ctx.` call:
returns the recorded result, waits for the recorded retry time and runs
again, re-runs a step whose `StepStarted` has no result (same idempotency
key; reconcile first for tools that declare it), or — when the journal has
nothing — appends `StepStarted`, runs it, and appends the outcome. The k-th
ordered request must match the k-th recorded one in id, kind and input hash;
a mismatch parks the run for an operator.
