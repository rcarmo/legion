#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="${LEGION_TEST_BINARY:-$ROOT/target/debug/legion}"
DATA="$(mktemp -d)"; LOG="$(mktemp)"; PORT="${LEGION_TEST_PORT:-18085}"; PID=""
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
export LEGION_URL="http://127.0.0.1:$PORT"

hello=$(bun "$ROOT/examples/hello-bun/run.ts" Example)
grep -q 'Hello, Example!' <<<"$hello"
inspect=$(bun "$ROOT/examples/cluster-inspector/index.ts")
grep -q '"ok": true' <<<"$inspect"
approval=$(bun "$ROOT/examples/approval-workflow/index.ts" CI)
grep -q 'awaiting_approval' <<<"$approval"
grep -q 'resuming' <<<"$approval"
approval_log=$(curl -sf "$LEGION_URL/sessions/$(jq -r .session <<<"$approval")/log")
grep -q 'SessionParked' <<<"$approval_log"
grep -q 'SessionResumed' <<<"$approval_log"
canary=$(bun "$ROOT/examples/canary-deployment/index.ts")
grep -q '"weight": 2500' <<<"$canary"
[[ "$(jq -r .before.output.version <<<"$canary")" == "stable" ]]
[[ "$(jq -r .after.output.version <<<"$canary")" == "canary" ]]

echo "Deterministic examples passed: hello-bun, cluster-inspector, approval-workflow, canary-deployment"
