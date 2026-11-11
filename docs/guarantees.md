# Guarantees and threat models

## What keel guarantees

* **Durability.** Once a step's completion is recorded, it is never lost and
  never executed again. (`StepCompleted` commits in the same transaction that
  checks the lease; replay returns the recorded result.)
* **At-least-once steps.** A crash between doing a step's work and recording
  it means the step runs again, with the same idempotency key
  `run_id:step_id`.
* **Exactly-once effects** for tools that declare `effect="idempotent"` (the
  service deduplicates by key) or `effect="reconcile"` (a lookup by key runs
  before any re-attempt). Tools with neither are rejected at registration,
  so there is no silent gap. `effect="none"` tools are read-only.
* **Single writer.** At any moment at most one worker can append to a run:
  every append, heartbeat, suspend and finish carries the lease epoch, and a
  stale epoch is rejected in the same `UPDATE` that serializes appends.
* **Progress.** While at least one worker and the database are up, every
  run that is not waiting eventually runs (sweeper every second, lease 30 s,
  `NOTIFY` plus a 1 s polling backup).

## What keel does not guarantee

* Ordering between different runs.
* Exactly-once effects on a service that offers neither idempotency keys nor
  a way to look up past actions. The runtime refuses such tools rather than
  pretend.
* That an LLM call is paid for once: a crash during a call means paying
  for it twice. The gateway's idempotency cache (same `run:step` key and
  request body within 10 minutes) removes most of that waste, and the rest is
  counted (`keel_wasted_llm_tokens_total`).
* Workflow code that reads the clock, randomness or I/O between steps
  replays deterministically. Dev mode, the lint and the nondeterminism
  detector catch it; the run is parked rather than silently diverging.

## Sandbox threat model

The sandbox stops code written by an agent from reaching the network
(`--network none`), the host filesystem (read-only root, 64 MB tmpfs `/tmp`),
secrets (empty environment via `env -i`, no mounts), other runs (one
container per execution, destroyed afterwards) and from exhausting the host
(512 MB memory, 1 CPU, 128 pids, a hard wall-clock timeout). All twelve
escape tests in `sandbox/tests/escape_test.py` are blocked.

Plain containers share the host kernel, so this is **not** a boundary
against kernel exploits. Set `KEEL_SANDBOX_RUNTIME=runsc` (gVisor) or move to
Firecracker microVMs for that. The sandbox service itself talks to the Docker
socket, which is effectively root on the host: run it against rootless
Docker or on a separate VM.

## Prompt-injection threat model

Any text the agent reads can try to take control of it: the customer's
message, order notes, retrieved articles, tool outputs, MCP tool
descriptions. keel assumes the model **will** be fooled sometimes (the mock
model in the suite is deliberately gullible) and puts the defense in the
system around it:

1. Everything entering the model's context is labeled `trusted`, `user`,
   `untrusted` or `extracted`. A run becomes **tainted** as soon as
   untrusted content (or an MCP tool definition) reaches a non-quarantined
   LLM call.
2. Tools carry a risk level. Once tainted, high-risk tools need a human
   approval; medium-risk tools get stricter argument rules (email only the
   ticket's customer).
3. Deterministic code computes eligibility and declares facts
   (`order_total_inr`, `eligible`, `order_id`); CEL rules deny refunds above
   the order total, for another order, for an ineligible order, or a second
   refund on the same ticket — whatever the model says.
4. Quarantined extraction (the dual-LLM pattern): a separate tool-less call
   turns untrusted text into a typed schema, and the privileged agent sees
   only those fields.
5. Approvals are durable, survive restarts, lift only the rule that asked,
   revalidate edited arguments, and are journaled as the audit trail.

Result on the 120-case suite: 0% attack success for both agent versions;
false escalations drop from 58.3% to 1.7% when order notes go through
quarantined extraction instead of straight into the prompt.

What this does not stop: an attacker who controls *trusted* inputs
(developer config, system prompts), a human approver who approves a bad
action, and injected text changing the wording of a reply (output checks
redact secrets and other customers' email addresses, but tone and content
are not policed).
