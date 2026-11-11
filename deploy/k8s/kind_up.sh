#!/usr/bin/env bash
# Creates a kind cluster, installs KEDA, loads the locally built images and
# deploys keel. Then: a burst of 5,000 runs scales workers up, and a rolling
# restart in the middle of it loses nothing (scripts/k8s_burst.sh).
set -euo pipefail
cd "$(dirname "$0")"
kind create cluster --config kind-config.yaml || true
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.15.1/keda-2.15.1.yaml
kubectl -n keda rollout status deploy/keda-operator --timeout=180s
for img in keel/server:dev keel/gateway:dev keel/python:dev keel/ui:dev; do
  kind load docker-image "$img" --name keel
done
cp ../policy.yaml policy.yaml
cp ../gateway.yaml gateway.yaml
kubectl apply -k .
kubectl -n keel rollout status statefulset/postgres --timeout=180s
kubectl -n keel rollout status deploy/keel-server --timeout=180s
kubectl apply -f keda/scaledobject.yaml
kubectl -n keel get pods
echo "API on http://localhost:8080, UI on http://localhost:3000"
