#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="${LEGION_TEST_BINARY:-$ROOT/target/debug/legion}"
DATA="$(mktemp -d)"; LOG="$(mktemp)"; PORT="${LEGION_TEST_PORT:-18084}"; PID=""
cleanup(){ [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true; rm -rf "$DATA" "$LOG"; }
trap cleanup EXIT
[[ -x "$BINARY" ]] || { echo "ERROR: prebuilt legion binary missing: $BINARY" >&2; exit 1; }
LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA" RUST_LOG=error "$BINARY" serve >"$LOG" 2>&1 & PID=$!
for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break; kill -0 "$PID" 2>/dev/null || { cat "$LOG" >&2; exit 1; }; sleep .25; done
curl -sf -X POST "http://127.0.0.1:$PORT/sessions" -H 'content-type: application/json' -d '{"model":"faux/dashboard"}' >/dev/null
PLAYWRIGHT_BROWSERS_PATH=/workspace/bin/pw-browsers LEGION_TEST_URL="http://127.0.0.1:$PORT" \
  bun "$ROOT/tests/integration/dashboard_smoke.ts" || { cat "$LOG" >&2; exit 1; }
