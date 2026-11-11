#!/usr/bin/env bash
# Runs every experiment after the chaos demo, in order, against the compose
# stack. Each step logs to bench/results/<name>.log and continues on failure.
set -uo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=. MSYS_NO_PATHCONV=1
export KEEL_URL=${KEEL_URL:-http://127.0.0.1:18080} KEEL_API_PORT=${KEEL_API_PORT:-18080}
COMPOSE="docker compose -f deploy/docker-compose.yml"
step() { local name=$1; shift; echo "=== $name"; "$@" > "bench/results/$name.log" 2>&1; echo "exit=$?" >> "bench/results/$name.log"; tail -3 "bench/results/$name.log"; }

step zombie python -m chaos.zombie --runs 60 --pause 60
step leader_failover python -m chaos.leader_failover --runs 100
step db_restart python -m chaos.db_restart --runs 150 --steps 40

$COMPOSE --profile naive up -d naive-worker
step naive_baseline python -m chaos.naive_baseline run --runs 1000
$COMPOSE --profile naive stop naive-worker

# Engine benchmarks: the mock LLM answers instantly so model latency cannot
# hide the engine's own overhead.
curl -s -X POST http://127.0.0.1:8000/admin/chaos -H 'Content-Type: application/json' \
  -d '{"target": "both", "latency_ms_min": 0, "latency_ms_max": 0, "error_rate": 0}' > /dev/null
step bench_noop python -m bench.run_bench noop --runs 300 --steps 10
step bench_noop_nobatch python -m bench.run_bench noop --runs 300 --steps 10 --no-batch
step bench_burst python -m bench.run_bench burst --runs 5000
step bench_sleepers python -m bench.run_bench sleepers --runs 2000
step bench_failover python -m bench.run_bench failover --rounds 6 --runs 20
step bench_sync_commit python -m bench.run_bench sync_commit --runs 300 --steps 10
step bench_scaling python -m bench.run_bench scaling --workers 1 2 4 8 --runs 300 --steps 10 --restore-workers 4
step plot python -m bench.plot
echo "=== done"
