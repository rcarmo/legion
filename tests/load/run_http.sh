#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
BINARY="${LEGION_LOAD_BINARY:-$TARGET_DIR/release/legion}"
PORT="${LEGION_TEST_PORT:-18080}"
DATA="$(mktemp -d)"
LOG="$(mktemp)"
PID=""
cleanup() { [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true; rm -rf "$DATA" "$LOG"; }
trap cleanup EXIT

LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA" \
LEGION_INVOKE_MAX_CONCURRENT_PER_FUNCTION="${LEGION_LOAD_SERVER_CONCURRENCY:-8}" \
LEGION_INVOKE_MAX_REQUESTS_PER_WINDOW=1000000 RUST_LOG=error "$BINARY" serve >"$LOG" 2>&1 &
PID=$!
for _ in $(seq 1 120); do
  curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break
  kill -0 "$PID" 2>/dev/null || { cat "$LOG" >&2; exit 1; }
  sleep .25
done
curl -sf -X POST "http://127.0.0.1:$PORT/functions" -H 'content-type: application/json' -d '{
  "name":"load","runtime":"bun","code":"const value=JSON.parse(await Bun.stdin.text()); process.stdout.write(JSON.stringify({ok:true,index:value.index}))"
}' >/dev/null
LEGION_LOAD_URL="http://127.0.0.1:$PORT" bun "$ROOT/tests/load/http_load.ts"

# The capacity run should remain error-free, while a deliberate burst above the
# configured ceiling must shed load with HTTP 429 rather than queue unboundedly.
statuses=$(seq 1 32 | xargs -P32 -I{} curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST "http://127.0.0.1:$PORT/functions/load/invoke" \
  -H 'content-type: application/json' -d '{"index":{}}')
grep -q '^200$' <<<"$statuses"
grep -q '^429$' <<<"$statuses"
echo "HTTP overload gate passed: successful work plus bounded 429 shedding"
