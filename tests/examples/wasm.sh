#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="${LEGION_TEST_BINARY:-$ROOT/target/debug/legion}"
DATA="$(mktemp -d)"; LOG="$(mktemp)"; PORT="${LEGION_TEST_PORT:-18088}"; PID=""
cleanup(){ [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true; rm -rf "$DATA" "$LOG"; }
trap cleanup EXIT
[[ -x "$BINARY" ]] || { echo "ERROR: prebuilt legion binary missing: $BINARY" >&2; exit 1; }
LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA" RUST_LOG=error "$BINARY" serve >"$LOG" 2>&1 & PID=$!
ready=0
for _ in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:$PORT/health" >/dev/null; then ready=1; break; fi
  kill -0 "$PID" 2>/dev/null || { cat "$LOG" >&2; exit 1; }
  sleep .25
done
(( ready )) || { cat "$LOG" >&2; exit 1; }
output=$(LEGION_URL="http://127.0.0.1:$PORT" bun "$ROOT/examples/hello-wasm/run.ts" Example) || { cat "$LOG" >&2; exit 1; }
grep -q 'Hello, Example!' <<<"$output"
grep -q '"runtime": "wasm"' <<<"$output"
echo "Hello WASM example passed"
