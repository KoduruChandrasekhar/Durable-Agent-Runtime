"""Produces every chart from the raw JSON in bench/results/ (one script, so
the README's numbers and figures always come from the same files).

    python -m bench.plot
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

R = Path("bench/results")
C = Path("bench/charts")
INK, DIM, GRID = "#1d1f23", "#6b7077", "#e6e6e2"
BLUE, RED, GREEN, PURPLE, AMBER = "#2f5bd3", "#c0392b", "#1f8a4c", "#8e5bd3", "#b7791f"


def load(name: str) -> dict | None:
    p = R / name
    return json.loads(p.read_text()) if p.exists() else None


def style(ax, title: str, xlabel: str = "", ylabel: str = "") -> None:
    ax.set_title(title, loc="left", fontsize=12, color=INK, pad=10)
    ax.set_xlabel(xlabel, color=DIM)
    ax.set_ylabel(ylabel, color=DIM)
    ax.grid(axis="y", color=GRID)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(colors=DIM)


def save(fig, name: str) -> None:
    C.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(C / name, dpi=150)
    plt.close(fig)
    print("wrote", C / name)


def chaos_comparison() -> None:
    k, n = load("chaos_report.json"), load("naive_report.json")
    if not (k and n):
        return
    metrics = [("lost runs", "lost_runs"), ("duplicate refunds", "duplicate_refunds"),
               ("duplicate emails", "duplicate_emails"), ("unauthorized\nhigh-risk actions", "unauthorized_high_risk_actions")]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), gridspec_kw={"width_ratios": [3, 1]})
    ax = axes[0]
    xs = range(len(metrics))
    nv = [n[m] for _, m in metrics]
    kv = [k[m] for _, m in metrics]
    b1 = ax.bar([x - 0.2 for x in xs], nv, 0.4, color=RED, label="naive loop")
    b2 = ax.bar([x + 0.2 for x in xs], kv, 0.4, color=BLUE, label="keel")
    ax.bar_label(b1, color=INK, fontsize=9)
    ax.bar_label(b2, color=INK, fontsize=9)
    ax.set_xticks(list(xs), [m for m, _ in metrics])
    style(ax, f"{k['runs']} refund agents under the same chaos")
    ax.legend(frameon=False)
    ax = axes[1]
    b = ax.bar(["naive", "keel"], [n["wasted_llm_tokens"], k["wasted_llm_tokens"]], color=[RED, BLUE])
    ax.bar_label(b, color=INK, fontsize=9)
    style(ax, "wasted LLM tokens")
    save(fig, "1_naive_vs_keel.png")


def scaling() -> None:
    d = load("bench_scaling.json")
    if not d:
        return
    pts = d["points"]
    w = [p["workers"] for p in pts]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(w, [p["steps_per_s"] for p in pts], marker="o", color=BLUE, label="no-op steps, 200 runs x 50 steps")
    for p in pts:
        ax.annotate(f"{p['steps_per_s']:.0f}", (p["workers"], p["steps_per_s"]), textcoords="offset points",
                    xytext=(0, 7), ha="center", fontsize=9, color=INK)
    ax.set_xticks(w)
    ax.legend(frameon=False, fontsize=8)
    style(ax, "Steps per second vs workers (one PostgreSQL)", "worker processes", "steps / s")
    save(fig, "2_steps_per_second_vs_workers.png")
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot([p["steps_per_s"] for p in pts], [p["pg_commits_per_s"] for p in pts], marker="o", color=PURPLE,
            label="commits / s")
    ax2 = ax.twinx()
    ax2.plot([p["steps_per_s"] for p in pts], [p["pg_wal_mb_per_s"] for p in pts], marker="s", color=AMBER,
             label="WAL MB / s")
    ax2.set_ylabel("WAL MB / s", color=DIM)
    style(ax, "PostgreSQL load vs steps per second", "steps / s", "commits / s")
    fig.legend(loc="upper left", bbox_to_anchor=(0.12, 0.88), frameon=False)
    save(fig, "8_postgres_load_vs_steps.png")


def overhead() -> None:
    a, b = load("bench_noop.json"), load("bench_noop_nobatch.json")
    if not (a and b):
        return
    fig, ax = plt.subplots(figsize=(6, 4))
    labels = ["p50", "p99"]
    xs = range(2)
    b1 = ax.bar([x - 0.2 for x in xs], [b["overhead_ms_p50"], b["overhead_ms_p99"]], 0.4, color=DIM, label="one append per event")
    b2 = ax.bar([x + 0.2 for x in xs], [a["overhead_ms_p50"], a["overhead_ms_p99"]], 0.4, color=BLUE, label="batched appends")
    ax.bar_label(b1, fmt="%.1f", fontsize=9)
    ax.bar_label(b2, fmt="%.1f", fontsize=9)
    ax.set_xticks(list(xs), labels)
    style(ax, "Engine overhead per step", "", "ms")
    ax.legend(frameon=False)
    save(fig, "3_overhead_per_step.png")


def failover() -> None:
    d = load("bench_failover.json")
    if not d or not d.get("samples"):
        return
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(d["samples"], bins=20, color=BLUE)
    ax.axvline(d["failover_s_p50"], color=INK, ls="--", lw=1)
    style(ax, f"Failover after kill -9 (p50 {d['failover_s_p50']}s, p99 {d['failover_s_p99']}s; lease 30s)",
          "seconds from kill to RunResumed elsewhere", "runs")
    save(fig, "4_failover_distribution.png")


def replay() -> None:
    d = load("bench_replay.json")
    if not d:
        return
    pts = d["points"]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.loglog([p["events"] for p in pts], [p["replay_ms_p50"] for p in pts], marker="o", color=BLUE)
    for p in pts:
        ax.annotate(f"{p['replay_ms_p50']:.1f} ms", (p["events"], p["replay_ms_p50"]), textcoords="offset points",
                    xytext=(0, 8), ha="center", fontsize=9)
    style(ax, "Replay time vs history length", "events in history", "ms (p50)")
    save(fig, "5_replay_time.png")


def burst() -> None:
    d = load("bench_burst.json")
    if not d:
        return
    s = d["samples"]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot([x / 1000 for x in s], [i / len(s) for i in range(len(s))], color=BLUE)
    style(ax, f"Queue wait during a {d['runs']}-run burst (p50 {d['queue_wait_ms_p50'] / 1000:.1f}s, "
              f"p99 {d['queue_wait_ms_p99'] / 1000:.1f}s)", "seconds from start to first claim", "fraction of runs")
    save(fig, "6_queue_wait_burst.png")


def routing() -> None:
    d = load("routing.json")
    if not d:
        return
    reps = d["reports"]
    fig, ax = plt.subplots(figsize=(6, 4))
    colors = {"always-small": GREEN, "always-large": RED, "routed": BLUE}
    for r in reps:
        ax.scatter(r["cost_per_success_usd"] * 1000, r["pass_k"]["8"], s=80, color=colors.get(r["routing"], DIM))
        ax.annotate(r["routing"], (r["cost_per_success_usd"] * 1000, r["pass_k"]["8"]), textcoords="offset points",
                    xytext=(6, 4), fontsize=9)
    ax.set_xscale("log")
    style(ax, "Cost per successful task vs reliability", "USD per 1,000 successful tasks (log)", "pass^8")
    save(fig, "7_routing_cost_vs_passk.png")


def extras() -> None:
    d = load("cache_curve.json")
    if d:
        fig, ax = plt.subplots(figsize=(6, 4))
        t = [p["threshold"] for p in d["curve"]]
        ax.plot(t, [p["hit_rate"] for p in d["curve"]], color=BLUE, label="paraphrase hit rate")
        ax.plot(t, [p["wrong_hit_rate"] for p in d["curve"]], color=RED, label="wrong-hit rate")
        ax.axvline(0.80, color=INK, ls="--", lw=1)
        style(ax, "Semantic cache: hit rate vs wrong hits", "similarity threshold", "fraction of pairs")
        ax.legend(frameon=False)
        save(fig, "9_cache_threshold_curve.png")
    e = load("evals.json")
    if e:
        fig, ax = plt.subplots(figsize=(6, 4))
        for r, col in zip(e["reports"], (DIM, BLUE, RED), strict=False):
            ks = sorted(r["pass_k"], key=int)
            ax.plot([int(k) for k in ks], [r["pass_k"][k] for k in ks], marker="o", color=col, label=f"v{r['version']}")
        style(ax, "pass^k by agent version (refund_core, 8 trials)", "k", "pass^k")
        ax.set_ylim(0, 1.02)
        ax.legend(frameon=False)
        save(fig, "10_evals_pass_k.png")
    inj = load("injection.json")
    if inj:
        fig, ax = plt.subplots(figsize=(6, 4))
        vs = list(inj)
        xs = range(len(vs))
        b1 = ax.bar([x - 0.2 for x in xs], [inj[v]["attack_success_rate"] * 100 for v in vs], 0.4, color=RED, label="attack success")
        b2 = ax.bar([x + 0.2 for x in xs], [inj[v]["false_escalation_rate"] * 100 for v in vs], 0.4, color=AMBER, label="false escalation")
        ax.bar_label(b1, fmt="%.1f%%", fontsize=9)
        ax.bar_label(b2, fmt="%.1f%%", fontsize=9)
        ax.set_xticks(list(xs), [f"v{v}" for v in vs])
        style(ax, f"Prompt-injection suite ({inj[vs[0]]['attacks']} attacks, {inj[vs[0]]['legit']} legit)", "", "%")
        ax.legend(frameon=False)
        save(fig, "11_injection_suite.png")


def fixes() -> None:
    # Same workload and same measurement before and after the fixes.
    pairs = [("steps / s (4 workers)", "bench_noop", "bench_noop_after_oldmethod", "steps_per_s"),
             ("overhead p99 ms", "bench_noop", "bench_noop_after_oldmethod", "overhead_ms_p99"),
             ("burst queue wait p50 s", "bench_burst", "bench_burst", "queue_wait_ms_p50"),
             ("timer lateness p50 s", "bench_sleepers", "bench_sleepers", "timer_lateness_ms_p50")]
    rows = []
    for label, before, after, k in pairs:
        b, a = load(before + "_before.json"), load(after + ".json")
        if b and a:
            div = 1000 if label.endswith(" s") else 1
            rows.append((label, b[k] / div, a[k] / div))
    if not rows:
        return
    fig, axes = plt.subplots(1, len(rows), figsize=(3 * len(rows), 3.6))
    for ax, (label, b, a) in zip(axes, rows, strict=True):
        bars = ax.bar(["before", "after"], [b, a], color=[DIM, BLUE])
        ax.bar_label(bars, fmt="%.1f", fontsize=9)
        style(ax, label)
    fig.suptitle("What the scaling fixes bought", x=0.02, ha="left", color=INK)
    save(fig, "12_scaling_fixes.png")


if __name__ == "__main__":
    for f in (chaos_comparison, scaling, overhead, failover, replay, burst, routing, extras, fixes):
        f()
