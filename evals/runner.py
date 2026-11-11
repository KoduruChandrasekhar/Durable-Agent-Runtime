"""Eval harness: seed the mock systems per task, run an agent version, and
compare the *final state* (refunds, emails, approvals) with the expected one.
Each task runs n times; reliability is reported as pass^k, the chance that k
independent tries all succeed (per task C(c, k) / C(n, k), averaged).

    python -m evals.runner --suite evals/suites/refund_core.yaml --versions 1 2 3 --trials 8 \
        --json bench/results/evals.json
    python -m evals.runner ... --routing always-small always-large routed   # cost vs pass^k

Local mode runs the real SDK and workflow code on the in-memory server with
the scripted model (fast, used in CI). Model routing and escalation mirror
the gateway's rules (deploy/gateway.yaml) so routing strategies can be
compared; the nightly job runs the same suite through the real stack.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import math
import random
import statistics
import time
import zlib
from pathlib import Path

import yaml

from demos.mocks import brain
from demos.refund_agent import agent as refund
from demos.refund_agent import systems as refund_systems
from demos.refund_agent import tools as refund_tools
from demos.refund_agent.fixtures import NOW
from keel.llm import ChatRequest, LLMReply, ToolCall, Usage
from keel.testing import LocalEnv
from keel.tools import ToolRegistry

ROOT = Path(__file__).resolve().parents[1]
POLICY = (ROOT / "deploy" / "policy.yaml").read_text(encoding="utf-8")
PRICES = {"mock-small": (0.15, 0.60), "mock-large": (3.00, 15.00)}  # USD per million tokens (gateway.yaml)


class LocalGateway:
    """In-process stand-in for the model gateway's routing: rules (quarantined
    or short requests -> small), escalation to the large model on invalid
    JSON, and per-model prices."""

    def __init__(self, strategy: str, rng: random.Random):
        self.strategy = strategy
        self.rng = rng
        self.escalations = 0

    def _pick(self, req: ChatRequest) -> str:
        if self.strategy == "always-small":
            return "mock-small"
        if self.strategy == "always-large":
            return "mock-large"
        tokens = sum(len(str(m.get("content") or "")) for m in req.messages) // 4
        if req.metadata.get("quarantined") or tokens <= 2500:
            return "mock-small"
        return "mock-large"

    def _call(self, req: ChatRequest, model: str) -> LLMReply:
        body = {**req.model_dump(), "model": model}
        if req.temperature > 0:  # sampled, but reproducible: one seed per run, step and model
            body["sample_seed"] = zlib.crc32(f"{req.run_id}:{req.step_key}:{model}".encode())
        out = brain.respond(body)
        in_tok = sum(len(str(m.get("content") or "")) // 4 + 4 for m in req.messages) + len(json.dumps(req.tools or [])) // 4
        calls = [ToolCall(id=f"call_{zlib.crc32(f'{req.run_id}:{req.step_key}:{i}'.encode()):012d}", name=c["name"],
                          arguments=c.get("arguments", {})) for i, c in enumerate(out.get("tool_calls") or [])]
        out_tok = len(out.get("text") or "") // 4 + 8 * len(calls) + 1
        pin, pout = PRICES[model]
        return LLMReply(text=out.get("text"), tool_calls=calls, usage=Usage(input_tokens=in_tok, output_tokens=out_tok),
                        cost_usd=(in_tok * pin + out_tok * pout) / 1e6, model=model, provider="mock")

    async def __call__(self, req: ChatRequest) -> LLMReply:
        model = self._pick(req)
        r = self._call(req, model)
        if req.response_format and not r.tool_calls and self.strategy == "routed" and model != "mock-large":
            try:
                r.parsed()
            except ValueError:
                self.escalations += 1
                r2 = self._call(req, "mock-large")
                r2.cost_usd += r.cost_usd
                return r2
        return r


def _order(o: dict) -> dict:
    d = dict(o)
    d.setdefault("customer_name", "")
    d.setdefault("status", "delivered")
    d.setdefault("refunded", False)
    d.setdefault("notes", "")
    d.setdefault("items", [])
    d["delivered_at"] = (NOW - dt.timedelta(days=int(d.pop("days_ago", 5)))).isoformat()
    return d


class LocalRunner:
    def __init__(self, version: str, temperature: float = 0.0, routing: str = "routed", settle_wait_s: float = 60):
        self.version = version
        self.temperature = temperature
        self.routing = routing
        self.settle_wait_s = settle_wait_s

    async def run_task(self, task: dict, trial: int) -> dict:
        seed = zlib.crc32(f"{task['id']}:{trial}:{self.version}:{self.routing}".encode())
        tools = refund_tools.TOOLS.merge(ToolRegistry())
        if task.get("kb_attack") in ("kb", "mcp"):
            from evals.injection_suite import malicious_kb_tools
            malicious_kb_tools(tools, task["kb_attack"])
        env = LocalEnv(refund.ALL, tools=tools, policy=POLICY, seed=seed, default_model="router:support")
        gw = LocalGateway(self.routing, random.Random(seed))
        env.services.llm = gw
        mem = refund_systems.MemorySystems([_order(o) for o in task.get("orders", [])], clock=env.clock, seed=seed)
        refund_systems.set_systems(mem)
        decision = task.get("approvals", "none")
        if decision in ("approve", "reject"):
            env.auto_approve = lambda a, d=decision: (d, None)
        ticket = {**task["ticket"], "settle_wait_s": self.settle_wait_s, "temperature": self.temperature}
        t0 = time.perf_counter()
        rid = env.start("refund_agent", ticket, version=self.version)
        await env.run_until_idle()
        wall = time.perf_counter() - t0
        st = env.run_state(rid)
        approvals = list(env.server.approvals.values())
        approved_refund_orders = {a["action"]["args"].get("order_id") for a in approvals
                                  if a["status"] == "approved" and a["action"]["tool"] == "issue_refund"}
        refunds = [{"order_id": r["order_id"], "amount_inr": r["amount_inr"],
                    "approved_by_human": r["order_id"] in approved_refund_orders} for r in mem.refunds.values()]
        emails = [{"to": e["to"], "body": e["body"]} for e in mem.emails]
        cost = sum(e.payload.get("cost_usd", 0) for e in env.events(rid) if e.type == "StepCompleted")
        steps = sum(1 for e in env.events(rid) if e.type == "StepCompleted")
        res = {"status": st.status, "result": st.result, "refunds": refunds, "emails": emails,
               "approvals": [{"rule_ids": a["rule_ids"], "status": a["status"]} for a in approvals],
               "cost_usd": cost, "steps": steps, "wall_ms": wall * 1000, "escalations": gw.escalations,
               "tainted": env.server.runs[rid].tainted}
        res["ok"], res["why"] = judge(task, res)
        return res


def judge(task: dict, res: dict) -> tuple[bool, str]:
    exp = task.get("expect", {})
    if res["status"] != "completed":
        return False, f"run {res['status']}"
    total = sum(r["amount_inr"] for r in res["refunds"])
    if "refund_inr" in exp and total != exp["refund_inr"]:
        return False, f"refunded {total}, expected {exp['refund_inr']}"
    if "approval" in exp and bool(res["approvals"]) != exp["approval"]:
        return False, f"approval requested={bool(res['approvals'])}, expected {exp['approval']}"
    if "email_to" in exp:
        tos = [e["to"] for e in res["emails"]]
        if tos != [exp["email_to"]]:
            return False, f"emails to {tos}, expected exactly one to {exp['email_to']}"
    for s in exp.get("email_contains", []):
        if not any(s.lower() in e["body"].lower() for e in res["emails"]):
            return False, f"email does not mention {s!r}"
    return True, ""


def pass_hat_k(successes: list[int], n: int, k: int) -> float:
    """Average over tasks of C(c, k) / C(n, k)."""
    if k > n:
        return float("nan")
    return statistics.mean(math.comb(c, k) / math.comb(n, k) for c in successes)


async def evaluate(suite: dict, version: str, trials: int, temperature: float, routing: str) -> dict:
    runner = LocalRunner(version, temperature, routing)
    per_task: dict[str, list[dict]] = {}
    for task in suite["tasks"]:
        per_task[task["id"]] = [await runner.run_task(task, i) for i in range(trials)]
    successes = [sum(r["ok"] for r in rs) for rs in per_task.values()]
    all_runs = [r for rs in per_task.values() for r in rs]
    cost = sum(r["cost_usd"] for r in all_runs)
    ok_runs = sum(r["ok"] for r in all_runs)
    report = {
        "version": version, "routing": routing, "temperature": temperature, "tasks": len(per_task), "trials": trials,
        "avg_success": ok_runs / len(all_runs),
        "pass_k": {str(k): pass_hat_k(successes, trials, k) for k in (1, 2, 4, 8) if k <= trials},
        "cost_per_task_usd": cost / len(all_runs),
        "cost_per_success_usd": cost / ok_runs if ok_runs else float("inf"),
        "p50_wall_ms": statistics.median(r["wall_ms"] for r in all_runs),
        "escalation_rate": sum(bool(r["approvals"]) for r in all_runs) / len(all_runs),
        "model_escalations": sum(r["escalations"] for r in all_runs),
        "steps_per_task": statistics.mean(r["steps"] for r in all_runs),
        "failures": {tid: [r["why"] for r in rs if not r["ok"]][:2] for tid, rs in per_task.items()
                     if any(not r["ok"] for r in rs)},
    }
    return report


def print_table(reports: list[dict]) -> None:
    ks = sorted({k for r in reports for k in r["pass_k"]}, key=int)
    head = f"{'version':8} {'routing':13} {'success':>8} " + " ".join(f"{'pass^' + k:>8}" for k in ks) + \
           f" {'$/task':>10} {'$/success':>10} {'p50 ms':>8} {'escal.':>7}"
    print(head)
    print("-" * len(head))
    for r in reports:
        print(f"v{r['version']:7} {r['routing']:13} {r['avg_success']:8.1%} " +
              " ".join(f"{r['pass_k'][k]:8.3f}" for k in ks) +
              f" {r['cost_per_task_usd']:10.6f} {r['cost_per_success_usd']:10.6f} {r['p50_wall_ms']:8.1f} "
              f"{r['escalation_rate']:7.1%}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default=str(ROOT / "evals" / "suites" / "refund_core.yaml"))
    ap.add_argument("--versions", nargs="+", default=["1", "2"])
    ap.add_argument("--routing", nargs="+", default=["routed"])
    ap.add_argument("--trials", type=int, default=8)
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--json", default="")
    a = ap.parse_args()
    suite = yaml.safe_load(open(a.suite, encoding="utf-8"))
    reports = []
    for v in a.versions:
        for routing in a.routing:
            reports.append(asyncio.run(evaluate(suite, v, a.trials, a.temperature, routing)))
    print_table(reports)
    for r in reports:
        if r["failures"]:
            print(f"v{r['version']}/{r['routing']} failures: {json.dumps(r['failures'])[:600]}")
    if a.json:
        Path(a.json).parent.mkdir(parents=True, exist_ok=True)
        json.dump({"suite": suite["suite"], "reports": reports}, open(a.json, "w"), indent=2, default=str)


if __name__ == "__main__":
    main()
