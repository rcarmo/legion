#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="${LEGION_LOAD_BINARY:-$ROOT/bin/legion}"
BASE=$((34000 + $$ % 500))
PORT="${LEGION_TEST_PORT:-$BASE}"
RAFT_PORT="${LEGION_TEST_RAFT_PORT:-$((BASE + 500))}"
DATA="$(mktemp -d)"
LOG="$(mktemp)"
PID=""
KEY="${LEGION_LOAD_API_KEY:-go-load-test-key}"
cleanup() { [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true; rm -rf "$DATA" "$LOG"; }
trap cleanup EXIT

[[ -x "$BINARY" ]] || { echo "ERROR: missing Go Legion binary: $BINARY" >&2; exit 1; }
LEGION_API_KEY="$KEY" LEGION_INVOKE_MAX_CONCURRENT_PER_FUNCTION="${LEGION_LOAD_SERVER_CONCURRENCY:-8}" \
LEGION_INVOKE_MAX_REQUESTS_PER_WINDOW=1000000 "$BINARY" --data-dir "$DATA" \
  --iroh-addr 127.0.0.1:0 --raft-addr "127.0.0.1:$RAFT_PORT" --api-addr "127.0.0.1:$PORT" \
  --9p-addr= --mdns=false --relay=false --discovery-window=10ms >"$LOG" 2>&1 &
PID=$!
for _ in $(seq 1 120); do
  curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break
  kill -0 "$PID" 2>/dev/null || { cat "$LOG" >&2; exit 1; }
  sleep .25
done
printf '%s\n' 'export async function run(value) { await new Promise(resolve => setTimeout(resolve, 10)); return {ok:true,index:value.index}; }' >"$DATA/load.ts"
LEGION_URL="http://127.0.0.1:$PORT" LEGION_API_KEY="$KEY" "$BINARY" deploy push load bun "$DATA/load.ts" >/dev/null
LEGION_LOAD_URL="http://127.0.0.1:$PORT" LEGION_LOAD_API_KEY="$KEY" bun "$ROOT/tests/load/http_load.ts"

# The capacity run should remain error-free, while a deliberate burst above the
# configured ceiling must shed load with HTTP 429 rather than queue unboundedly.
statuses=$(seq 1 32 | xargs -P32 -I{} curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST "http://127.0.0.1:$PORT/functions/load/invoke" \
  -H "Authorization: Bearer $KEY" -H 'content-type: application/json' -d '{"index":{}}')
grep -q '^200$' <<<"$statuses"
grep -q '^429$' <<<"$statuses"
echo "HTTP overload gate passed: successful work plus bounded 429 shedding"
