"""CI gate: fail the build if a candidate agent version's pass^k drops or its
cost per task rises beyond the thresholds, relative to the baseline version.

    python -m evals.gate --report bench/results/evals.json --candidate 3
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def check(reports: list[dict], candidate: str, cfg: dict) -> list[str]:
    by_v = {r["version"]: r for r in reports if r.get("routing", "routed") == cfg.get("routing", "routed")}
    base, cand = by_v[str(cfg["baseline_version"])], by_v[str(candidate)]
    k = str(cfg["k"])
    problems = []
    if cand["pass_k"][k] < cfg["min_pass_k"]:
        problems.append(f"pass^{k} {cand['pass_k'][k]:.3f} below the floor {cfg['min_pass_k']}")
    drop = base["pass_k"][k] - cand["pass_k"][k]
    if drop > cfg["max_pass_k_drop"]:
        problems.append(f"pass^{k} dropped by {drop:.3f} vs v{cfg['baseline_version']} (max {cfg['max_pass_k_drop']})")
    rise = (cand["cost_per_task_usd"] - base["cost_per_task_usd"]) / base["cost_per_task_usd"] * 100
    if rise > cfg["max_cost_increase_pct"]:
        problems.append(f"cost per task up {rise:.1f}% vs v{cfg['baseline_version']} (max {cfg['max_cost_increase_pct']}%)")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--thresholds", default=str(ROOT / "evals" / "thresholds.yaml"))
    a = ap.parse_args()
    cfg = yaml.safe_load(open(a.thresholds, encoding="utf-8"))
    reports = json.load(open(a.report, encoding="utf-8"))["reports"]
    problems = check(reports, a.candidate, cfg)
    if problems:
        print(f"EVAL GATE FAILED for v{a.candidate}:")
        for p in problems:
            print("  -", p)
        return 1
    print(f"eval gate passed for v{a.candidate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
