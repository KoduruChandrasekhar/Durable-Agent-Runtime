#!/usr/bin/env bash
# Every chaos experiment against the compose stack; each ends with the
# invariant checker. Results land in bench/results/.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=.
python -m chaos.demo --runs "${RUNS:-1000}"                       # worker kills + LLM errors + fail-after-commit
python -m chaos.zombie                                             # SIGSTOP (docker pause) past the lease
python -m chaos.leader_failover                                    # kill the replica holding the advisory lock
python -m chaos.db_restart                                         # restart PostgreSQL under load
docker compose -f deploy/docker-compose.yml --profile naive up -d naive-worker
python -m chaos.naive_baseline run --runs "${RUNS:-1000}"          # the baseline under the same chaos
docker compose -f deploy/docker-compose.yml --profile naive stop naive-worker
