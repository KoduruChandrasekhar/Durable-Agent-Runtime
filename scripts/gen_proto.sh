#!/usr/bin/env bash
# Regenerates Go and Python code from proto/keel/v1/worker.proto.
# Needs: pip install grpcio-tools; go install protoc-gen-go and protoc-gen-go-grpc.
set -euo pipefail
cd "$(dirname "$0")/.."
GOBIN="$(go env GOPATH)/bin"
EXT=""; case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) EXT=".exe";; esac
tmp="$(mktemp -d)"
python -m grpc_tools.protoc -I proto \
  --plugin=protoc-gen-go="$GOBIN/protoc-gen-go$EXT" \
  --plugin=protoc-gen-go-grpc="$GOBIN/protoc-gen-go-grpc$EXT" \
  --go_out="$tmp" --go_opt=paths=source_relative \
  --go-grpc_out="$tmp" --go-grpc_opt=paths=source_relative \
  proto/keel/v1/worker.proto
mkdir -p gen/keelv1
cp "$tmp"/keel/v1/*.go gen/keelv1/
rm -rf "$tmp"
python -m grpc_tools.protoc -I proto --python_out=sdk/python --grpc_python_out=sdk/python \
  --pyi_out=sdk/python proto/keel/v1/worker.proto
touch sdk/python/keel/v1/__init__.py
echo "generated gen/keelv1 and sdk/python/keel/v1"
