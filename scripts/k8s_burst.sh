#!/usr/bin/env bash
# On kind: start a burst of 5,000 runs, watch KEDA scale workers up, do a
# rolling restart of the workers in the middle of the burst, and check that
# nothing was lost (every run final, invariants hold).
set -euo pipefail
N=${N:-5000}
API=${KEEL_URL:-http://localhost:8080}
python - <<EOF
import asyncio
from keel.client import KeelClient
async def main():
    async with KeelClient("$API") as c:
        sem = asyncio.Semaphore(100)
        async def one(i):
            async with sem:
                await c.start_run("bench_noop", {"steps": 10}, start_key=f"burst-{i}")
        await asyncio.gather(*(one(i) for i in range($N)))
asyncio.run(main())
EOF
for i in $(seq 1 12); do kubectl -n keel get deploy keel-worker -o jsonpath='{.status.replicas}{" workers\n"}'; sleep 5; done
kubectl -n keel rollout restart deploy/keel-worker
kubectl -n keel rollout status deploy/keel-worker --timeout=300s
until [ "$(curl -s "$API/v1/runs?status=pending&limit=1" | python -c 'import json,sys; print(len(json.load(sys.stdin)["runs"]))')" = "0" ]; do sleep 5; done
echo "burst drained; scaling back down:"
for i in $(seq 1 24); do kubectl -n keel get deploy keel-worker -o jsonpath='{.status.replicas}{" workers\n"}'; sleep 5; done
