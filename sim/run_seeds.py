"""Run many simulation seeds in parallel and report failures by seed.

    python -m sim.run_seeds --seeds 1000 --start 0 --jobs 8 --json bench/results/sim.json

CI runs 1,000 seeds per pull request (small workloads) and 100,000 nightly.
Any failure prints its seed; ``python -m sim.harness --seed N -v`` replays it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed


def _one(args: tuple[int, dict]) -> dict:
    seed, kw = args
    from sim.harness import simulate
    t = time.perf_counter()
    try:
        r = simulate(seed, **kw)
        return {"seed": seed, "ok": r.ok, "violations": r.violations, "fingerprint": r.fingerprint,
                "stats": r.stats, "seconds": time.perf_counter() - t}
    except Exception as e:  # a crash of the harness itself is a failure too
        return {"seed": seed, "ok": False, "violations": [f"harness exception: {e!r}"], "stats": {},
                "seconds": time.perf_counter() - t}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=100)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--jobs", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--small", action="store_true", help="smaller workload per seed (CI)")
    ap.add_argument("--json", default="")
    a = ap.parse_args()
    kw = dict(n_refund=3, n_saga=2, n_signal=1, n_parent=1, workers=2, fault_window_s=1800) if a.small else {}
    t0 = time.perf_counter()
    results: list[dict] = []
    totals: dict[str, int] = {}
    with ProcessPoolExecutor(max_workers=a.jobs) as pool:
        futs = [pool.submit(_one, (s, kw)) for s in range(a.start, a.start + a.seeds)]
        for i, f in enumerate(as_completed(futs), 1):
            r = f.result()
            results.append(r)
            for k, v in r["stats"].items():
                if isinstance(v, int):
                    totals[k] = totals.get(k, 0) + v
            if not r["ok"]:
                print(f"FAIL seed={r['seed']}: {r['violations'][:3]}", flush=True)
            if i % 50 == 0:
                print(f"{i}/{a.seeds} seeds, {sum(1 for x in results if not x['ok'])} failing, "
                      f"{time.perf_counter() - t0:.0f}s", flush=True)
    failing = sorted(r["seed"] for r in results if not r["ok"])
    summary = {"seeds": a.seeds, "start": a.start, "small": a.small, "failing_seeds": failing,
               "passed": a.seeds - len(failing), "wall_s": round(time.perf_counter() - t0, 1),
               "mean_seed_s": round(sum(r["seconds"] for r in results) / max(1, len(results)), 2),
               "fault_totals": totals}
    print(json.dumps(summary, indent=2))
    if a.json:
        os.makedirs(os.path.dirname(a.json) or ".", exist_ok=True)
        json.dump({"summary": summary, "failures": [r for r in results if not r["ok"]]}, open(a.json, "w"), indent=2)
    return 1 if failing else 0


if __name__ == "__main__":
    sys.exit(main())
