#!/usr/bin/env bash
# Produces every raw result file in bench/results/ (the compose stack must be up
# with KEEL_MOCK_LLM_LATENCY_MS=0,0 for engine benchmarks), then the charts.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=.
python -m bench.replay_bench
python -m bench.run_bench noop --runs 300 --steps 10
python -m bench.run_bench noop --runs 300 --steps 10 --no-batch
python -m bench.run_bench scaling --workers 1 2 4 8 --runs 300 --steps 10
python -m bench.run_bench sync_commit --runs 300 --steps 10
python -m bench.run_bench burst --runs 5000
python -m bench.run_bench sleepers --runs 2000
python -m bench.run_bench failover --rounds 10 --runs 20
python bench/cache_pairs.py && go run ./gateway/cmd/keel-cachecurve bench/cache_pairs.json > bench/results/cache_curve.json
python -m evals.runner --versions 2 --routing always-small always-large routed --trials 8 --json bench/results/routing.json
python sandbox/tests/escape_test.py --json bench/results/sandbox.json
python -m bench.plot
