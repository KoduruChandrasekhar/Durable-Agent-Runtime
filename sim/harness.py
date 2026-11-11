"""Deterministic simulation testing (the FoundationDB / TigerBeetle idea).

The whole system runs in one process: an in-memory server with the same run,
lease, append and timer rules as keel-server (``keel.testing.LocalServer``),
N simulated workers running the *real* SDK and workflow code, a simulated
clock and a simulated network. One seeded random generator decides
everything: message delays, dropped requests, dropped responses (so retried
appends hit the server twice), worker crashes, worker freezes (a SIGSTOP past
the lease), server outages, when signals and approvals arrive.

Invariants are checked after every step of the driver; liveness is checked
once faults stop. A failure prints its seed, and re-running the seed
reproduces the exact same history.

    python -m sim.harness --seed 42 -v
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import random
import sys
from dataclasses import dataclass, field
from typing import Any

from keel import Context, RetryPolicy, tool, workflow
from keel.errors import WorkerCrash
from keel.executor import Services
from keel.guard import SecretStore
from keel.protocol import TransientBackendError
from keel.testing import LocalServer, LocalWorker, check_log_invariants, quiesce
from keel.tools import ToolRegistry
from keel.workflow import WorkflowRegistry

from demos.refund_agent import agent as refund
from demos.refund_agent import fixtures as fx
from demos.refund_agent import systems as refund_systems
from demos.refund_agent import tools as refund_tools

POLICY_PATH = "deploy/policy.yaml"

# ---------------------------------------------------------------- simulated workloads

SIM_TOOLS = ToolRegistry()
SIM_WF = WorkflowRegistry()
SAGA_LEDGER: dict[str, list] = {"booked": [], "cancelled": []}
FAIL_CAR: dict[str, bool] = {}


@tool(effect="idempotent", registry=SIM_TOOLS)
async def book(trip: str, item: str, *, key: str) -> str:
    if not any(k == key for k, *_ in SAGA_LEDGER["booked"]):
        SAGA_LEDGER["booked"].append((key, trip, item))
    return item


@tool(effect="idempotent", registry=SIM_TOOLS)
async def cancel(trip: str, item: str, *, key: str) -> str:
    if not any(k == key for k, *_ in SAGA_LEDGER["cancelled"]):
        SAGA_LEDGER["cancelled"].append((key, trip, item))
    return item


@tool(effect="idempotent", registry=SIM_TOOLS, retry=RetryPolicy(max_attempts=2, initial=1))
async def book_car(trip: str, *, key: str) -> str:
    if FAIL_CAR.get(trip):
        raise RuntimeError("no cars")
    return "car"


@workflow(name="sim_saga", registry=SIM_WF)
async def sim_saga(ctx: Context, inp: dict) -> str:
    for item in ("flight", "hotel", "train"):
        await ctx.tool(book(trip=inp["trip"], item=item), key=item)
        ctx.on_failure(cancel, trip=inp["trip"], item=item)
        await ctx.sleep(seconds=30, key=f"after-{item}")
    await ctx.tool(book_car(trip=inp["trip"]))
    return "booked"


@workflow(name="sim_signals", registry=SIM_WF)
async def sim_signals(ctx: Context, inp: dict) -> list:
    got = []
    for i in range(inp["n"]):
        got.append(await ctx.wait_for_signal("tick", timeout=86400))
        await ctx.step("record", lambda i=i: i)
    return got


@workflow(name="sim_child", registry=SIM_WF)
async def sim_child(ctx: Context, x: int) -> int:
    await ctx.sleep(seconds=10 * (x + 1))
    return await ctx.step("sq", lambda: x * x)


@workflow(name="sim_parent", registry=SIM_WF)
async def sim_parent(ctx: Context, inp: dict) -> int:
    rs = await asyncio.gather(*(ctx.child(sim_child, i, key=f"c{i}") for i in range(inp["n"])))
    return sum(rs)


for wf in refund.ALL[:2]:
    SIM_WF.add(wf.__keel_workflow__)  # type: ignore[attr-defined]


# ---------------------------------------------------------------- simulated network


class SimNetwork:
    """Per-worker view of the server: delays, drops requests or responses
    (the client retries, like the gRPC client does), and blocks while the
    worker is frozen."""

    def __init__(self, sim: Simulation, worker_id: str):
        self.sim = sim
        self.server = sim.server
        self.clock = sim.server.clock
        self.worker_id = worker_id

    def _maybe_crash(self, write: bool) -> None:
        sim = self.sim
        if sim.faults and write and sim.rng.random() < sim.p_crash_in_call:
            sim.stats["crashes_mid_call"] += 1
            sim.log(f"crash {self.worker_id} mid-call")
            raise WorkerCrash()

    async def _call(self, name: str, fn: Any, *args: Any, write: bool = True) -> Any:
        sim = self.sim
        for _attempt in range(6):
            self._maybe_crash(write)  # the process dies before the request leaves
            fz = sim.frozen_until.get(self.worker_id, 0)
            if fz > self.clock.now_ms():
                await self.clock.sleep((fz - self.clock.now_ms()) / 1000)
            if sim.faults and sim.rng.random() < sim.p_delay:
                await self.clock.sleep(sim.rng.uniform(0.01, 2.0))
            if sim.faults and sim.rng.random() < sim.p_drop_request:
                sim.stats["dropped_requests"] += 1
                await self.clock.sleep(0.2)
                continue
            try:
                res = await fn(*args)
            except TransientBackendError:
                await self.clock.sleep(0.5)
                continue
            self._maybe_crash(write)  # ...or after the server applied it
            if sim.faults and write and sim.rng.random() < sim.p_drop_response:
                sim.stats["dropped_responses"] += 1  # applied on the server, the client never hears
                await self.clock.sleep(0.2)
                continue
            if sim.faults and write and sim.rng.random() < sim.p_duplicate:
                sim.stats["duplicated"] += 1
                try:
                    await fn(*args)  # a duplicated request arriving late
                except TransientBackendError:
                    pass
            return res
        raise TransientBackendError(f"{name}: gave up")

    async def poll(self, worker_id: str, keys: list, descs: list, max_wait_ms: int = 0) -> Any:
        if self.sim.faults and self.sim.rng.random() < self.sim.p_drop_request:
            return None
        return await self.server.poll(worker_id, keys, descs, max_wait_ms)

    async def append(self, *a: Any) -> Any:
        return await self._call("append", self.server.append, *a)

    async def heartbeat(self, *a: Any) -> Any:
        return await self._call("heartbeat", self.server.heartbeat, *a, write=False)

    async def suspend(self, *a: Any) -> Any:
        return await self._call("suspend", self.server.suspend, *a)

    async def finish(self, *a: Any, **k: Any) -> Any:
        return await self._call("finish", lambda: self.server.finish(*a, **k))

    async def evaluate_policy(self, *a: Any) -> Any:
        return await self._call("policy", self.server.evaluate_policy, *a, write=False)


# ---------------------------------------------------------------- simulation


@dataclass
class SimResult:
    seed: int
    ok: bool
    violations: list[str]
    fingerprint: str
    runs: int
    final: int
    stats: dict = field(default_factory=dict)
    virtual_s: float = 0.0


class Simulation:
    def __init__(self, seed: int, *, n_refund: int = 6, n_saga: int = 3, n_signal: int = 2, n_parent: int = 1,
                 workers: int = 3, fault_rate: float = 1.0, fault_window_s: float = 3600.0, verbose: bool = False):
        self.seed = seed
        self.rng = random.Random(seed)
        self.verbose = verbose
        policy = open(POLICY_PATH, encoding="utf-8").read()
        self.server = LocalServer(policy=policy, seed=seed, lease_ttl_ms=30_000)
        self.clock = self.server.clock
        self.faults = True
        fr = fault_rate
        self.p_delay, self.p_drop_request, self.p_drop_response, self.p_duplicate = 0.10 * fr, 0.03 * fr, 0.03 * fr, 0.02 * fr
        self.p_crash, self.p_freeze, self.p_outage = 0.02 * fr, 0.01 * fr, 0.005 * fr
        self.p_crash_in_call = 0.03 * fr
        self.fault_window_ms = int(fault_window_s * 1000)
        self.frozen_until: dict[str, int] = {}
        self.stats = {"crashes": 0, "crashes_mid_call": 0, "freezes": 0, "outages": 0, "dropped_requests": 0, "dropped_responses": 0,
                      "duplicated": 0, "signals": 0, "approvals": 0}
        self.violations: list[str] = []
        self.workers: list[LocalWorker] = []
        self.n_workers = workers
        self._wseq = 0
        SAGA_LEDGER["booked"].clear()
        SAGA_LEDGER["cancelled"].clear()
        FAIL_CAR.clear()
        tickets = fx.generate(n_refund, seed=seed, settle_wait_s=60)
        self.mem = refund_systems.MemorySystems([t["order"] for t in tickets], fail_after_commit=0.2 * fr,
                                                seed=seed, clock=self.clock)
        refund_systems.set_systems(self.mem)
        self.services = Services(tools=refund_tools.TOOLS.merge(SIM_TOOLS), llm=fx.mock_llm(self.clock),
                                 secrets=SecretStore({}, env_prefix="\0"), rng=random.Random(seed + 7),
                                 default_model="router:support")
        self.server.register("sim", [d.key for d in SIM_WF.all()])
        self.runs: dict[str, dict] = {}
        for t in tickets:
            rid, _ = self.server.start_run("refund_agent", t["ticket"], version=self.rng.choice(["1", "2"]))
            self.runs[rid] = {"kind": "refund", **t}
        for i in range(n_saga):
            trip = f"trip-{i}"
            FAIL_CAR[trip] = self.rng.random() < 0.5
            rid, _ = self.server.start_run("sim_saga", {"trip": trip}, version="1")
            self.runs[rid] = {"kind": "saga", "trip": trip, "fails": FAIL_CAR[trip]}
        for _ in range(n_signal):
            n = self.rng.randint(1, 3)
            rid, _ = self.server.start_run("sim_signals", {"n": n}, version="1")
            self.runs[rid] = {"kind": "signals", "n": n, "sent": 0}
        for _ in range(n_parent):
            n = self.rng.randint(2, 4)
            rid, _ = self.server.start_run("sim_parent", {"n": n}, version="1")
            self.runs[rid] = {"kind": "parent", "n": n}
        for _ in range(workers):
            self._add_worker()

    def _add_worker(self) -> LocalWorker:
        self._wseq += 1
        wid = f"sim-worker-{self._wseq}"
        w = LocalWorker(self.server, SIM_WF, self.services, wid, backend=SimNetwork(self, wid), max_concurrent=20)
        self.workers.append(w)
        return w

    def log(self, msg: str) -> None:
        if self.verbose:
            print(f"[{(self.clock.now_ms() - 1_767_225_600_000) / 1000:9.1f}s] {msg}")

    # ------------------------------------------------------------ faults and the outside world

    def _inject(self) -> None:
        now = self.clock.now_ms()
        if now - 1_767_225_600_000 > self.fault_window_ms:
            if self.faults:
                self.faults = False
                self.server.down = False
                self.frozen_until.clear()
                self.log("faults stop")
            return
        alive = [w for w in self.workers if w.alive]
        r = self.rng.random()
        if r < self.p_crash and alive:
            w = self.rng.choice(alive)
            w.kill()
            self.stats["crashes"] += 1
            self.log(f"crash {w.worker_id} ({len(w.running)} runs)")
            self._add_worker()
        elif r < self.p_crash + self.p_freeze and alive:
            w = self.rng.choice(alive)
            self.frozen_until[w.worker_id] = now + self.rng.randint(35_000, 90_000)  # past the 30 s lease
            self.stats["freezes"] += 1
            self.log(f"freeze {w.worker_id}")
        elif r < self.p_crash + self.p_freeze + self.p_outage and not self.server.down:
            self.server.down = True
            self.stats["outages"] += 1
            self.outage_end = now + self.rng.randint(1_000, 20_000)
            self.log("server outage")
        if self.server.down and now >= getattr(self, "outage_end", 0):
            self.server.down = False
            self.log("server back")

    def _outside_world(self) -> None:
        """Humans and external systems: signals (sometimes early, sometimes
        duplicated) and approvals (80% approve after a random delay)."""
        if self.server.down:
            return
        for rid, meta in self.runs.items():
            st = self.server.runs[rid]
            if meta["kind"] == "signals" and meta["sent"] < meta["n"] and st.status not in ("completed", "failed", "cancelled"):
                if self.rng.random() < 0.05:
                    sid = f"{rid}-{meta['sent']}"
                    self.server.signal(rid, "tick", {"k": meta["sent"]}, signal_id=sid)
                    if self.rng.random() < 0.3:
                        self.server.signal(rid, "tick", {"k": "dup"}, signal_id=sid)  # duplicate delivery
                    meta["sent"] += 1
                    self.stats["signals"] += 1
        for a in self.server.pending_approvals():
            if self.rng.random() < 0.1:
                decision = "approved" if self.rng.random() < 0.8 else "rejected"
                self.server.decide(a["approval_id"], decision, None, "sim-approver")
                self.stats["approvals"] += 1

    # ------------------------------------------------------------ invariants

    def _check(self, final: bool = False) -> None:
        for p in check_log_invariants(self.server):
            self.violations.append(f"log: {p}")
        by_order: dict[str, int] = {}
        for r in self.mem.refunds.values():
            by_order[r["order_id"]] = by_order.get(r["order_id"], 0) + 1
        for o, n in by_order.items():
            if n > 1:
                self.violations.append(f"order {o} refunded {n} times")
        emails: dict[str, int] = {}
        for e in self.mem.emails:
            emails[e["keel_key"].rsplit(":", 2)[0]] = emails.get(e["keel_key"].rsplit(":", 2)[0], 0) + 1
        for run, n in emails.items():
            if n > 1:
                self.violations.append(f"run {run} sent {n} emails")
        cancelled = [(t, i) for _, t, i in SAGA_LEDGER["cancelled"]]
        if len(cancelled) != len(set(cancelled)):
            self.violations.append(f"a compensation ran twice: {cancelled}")
        if not final:
            return
        for rid, meta in self.runs.items():
            st = self.server.runs[rid]
            if st.status not in ("completed", "failed", "cancelled"):
                self.violations.append(f"liveness: {meta['kind']} run {rid[:8]} still {st.status} ({st.wait_reason})")
                continue
            if meta["kind"] == "refund":
                if st.status != "completed":
                    self.violations.append(f"refund run {rid[:8]} {st.status}: {st.error}")
                if emails.get(rid, 0) != 1:
                    self.violations.append(f"refund run {rid[:8]} sent {emails.get(rid, 0)} emails")
                exp = meta["expect"]["refund"]
                got = sum(r["amount_inr"] for r in self.mem.refunds.values() if r["idempotency_key"].startswith(rid))
                if exp == 0 and got:
                    self.violations.append(f"unauthorized refund of {got} on {meta['kind']}/{meta['order']['order_id']}")
            if meta["kind"] == "saga":
                trip = meta["trip"]
                booked = [i for _, t, i in SAGA_LEDGER["booked"] if t == trip]
                undone = [i for _, t, i in SAGA_LEDGER["cancelled"] if t == trip]
                if meta["fails"] and (st.status != "failed" or undone != list(reversed(booked))):
                    self.violations.append(f"saga {trip}: status={st.status} booked={booked} cancelled={undone}")
                if not meta["fails"] and (st.status != "completed" or undone):
                    self.violations.append(f"saga {trip} should complete: {st.status} {undone}")
            if meta["kind"] == "signals" and st.status == "completed" and len(st.result or []) != meta["n"]:
                self.violations.append(f"signals run got {st.result}")
            if meta["kind"] == "parent" and st.status == "completed" and st.result != sum(i * i for i in range(meta["n"])):
                self.violations.append(f"parent result {st.result}")

    def fingerprint(self) -> str:
        """Hash of every event, minus wall-clock measurements (step durations)."""
        h = hashlib.sha256()
        for rid in sorted(self.server.events):
            for e in self.server.events[rid]:
                p = {k: v for k, v in e.payload.items() if k not in ("duration_ms", "latency_ms")}
                h.update(json.dumps([rid, e.seq, e.type, e.step_id, e.epoch, p], sort_keys=True, default=str).encode())
        return h.hexdigest()[:16]

    # ------------------------------------------------------------ driver

    async def run(self, max_virtual_s: float = 10 * 86400) -> SimResult:
        start = self.clock.now_ms()
        for _ in range(200_000):
            self._inject()
            self._outside_world()
            for w in list(self.workers):
                if w.alive and self.frozen_until.get(w.worker_id, 0) <= self.clock.now_ms():
                    try:
                        await w.poll_once()
                    except TransientBackendError:
                        pass
            await quiesce()
            for w in self.workers:
                w._reap()
            self.workers = [w for w in self.workers if w.alive or w.running]
            while sum(1 for w in self.workers if w.alive) < self.n_workers:
                self._add_worker()
            self._check()
            if self.violations:
                break
            if all(self.server.runs[r].status in ("completed", "failed", "cancelled") for r in self.runs) and not self.faults:
                break
            cands = [t for t in (self.clock.next_wakeup_ms(), self.server.next_event_ms()) if t is not None]
            nxt = min(cands) if cands else self.clock.now_ms() + 1000
            # Small steps keep the world (signals, approvals, faults) moving between events.
            nxt = min(nxt, self.clock.now_ms() + self.rng.randint(200, 5000))
            if nxt <= self.clock.now_ms():  # e.g. an expired lease the down server cannot sweep yet
                nxt = self.clock.now_ms() + self.rng.randint(50, 500)
            if nxt - start > max_virtual_s * 1000:
                break
            self.clock.advance_to(max(nxt, self.clock.now_ms()))
            if not self.server.down:
                self.server.tick()
            await quiesce()
        self._check(final=not self.violations)
        for w in self.workers:
            w.kill()
        await quiesce()
        final = sum(1 for r in self.runs if self.server.runs[r].status in ("completed", "failed", "cancelled"))
        return SimResult(self.seed, not self.violations, self.violations[:10], self.fingerprint(), len(self.runs),
                         final, dict(self.stats, **self.server.stats), (self.clock.now_ms() - start) / 1000)


def simulate(seed: int, **kw: Any) -> SimResult:
    return asyncio.run(Simulation(seed, **kw).run())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("-v", action="store_true")
    ap.add_argument("--fault-rate", type=float, default=1.0)
    a = ap.parse_args()
    r = simulate(a.seed, verbose=a.v, fault_rate=a.fault_rate)
    print(json.dumps(r.__dict__, indent=2, default=str))
    return 0 if r.ok else 1


if __name__ == "__main__":
    sys.exit(main())
