#!/usr/bin/env bash
# infer-router 全量测试入口：vet + build + 单测/集成（含 -race）
# 用法：bash scripts/test_all.sh
set -euo pipefail

GO_BIN="${GO_BIN:-go}"
echo "== go: $GO_BIN"
"$GO_BIN" version

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"

echo "== go vet ./..."
"$GO_BIN" vet ./...

echo "== go build ./..."
"$GO_BIN" build ./...

echo "== go test -race ./..."
"$GO_BIN" test -race ./...

echo "== ALL GREEN"
