#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
LEGION=${LEGION_TEST_BINARY:-$ROOT/bin/legion}
BUN_BIN=${LEGION_BUN_BIN:-$(command -v bun)}
TMP=$(mktemp -d)
BASE=$((31500 + $$ % 400))
RAFT="127.0.0.1:$BASE"
API="127.0.0.1:$((BASE + 500))"
NINEP="127.0.0.1:$((BASE + 1000))"
KEY=go-m4-rest-key
CAP=go-m4-independent-ninep-capability
PID=
cleanup() {
  if [[ -n "${PID:-}" ]]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi
  if [[ -s "$TMP/server.log" ]]; then cat "$TMP/server.log" >&2; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

[[ -x "$LEGION" ]] || { echo "ERROR: missing Go Legion binary: $LEGION" >&2; exit 1; }
cat >"$TMP/load.ts" <<'BUN'
export async function run(input) {
  await new Promise(resolve => setTimeout(resolve, 250));
  return {ok: true, index: input.index};
}
BUN

LEGION_API_KEY="$KEY" LEGION_NAMESPACE_CAPABILITY="$CAP" LEGION_BUN_BIN="$BUN_BIN" \
LEGION_INVOKE_MAX_CONCURRENT_PER_FUNCTION=1 LEGION_INVOKE_MAX_REQUESTS_PER_WINDOW=1000 \
"$LEGION" --data-dir "$TMP/data" --iroh-addr 127.0.0.1:0 --raft-addr "$RAFT" \
  --api-addr "$API" --9p-addr "$NINEP" --mdns=false --relay=false \
  --discovery-window=10ms >"$TMP/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do
  curl -sf "http://$API/health" >/dev/null && break
  kill -0 "$PID" 2>/dev/null || { cat "$TMP/server.log" >&2; exit 1; }
  sleep .1
done

[[ $(curl -s -o /dev/null -w '%{http_code}' "http://$API/health") == 200 ]]
[[ $(curl -s -o /dev/null -w '%{http_code}' "http://$API/cluster/health") == 401 ]]
curl -sf -H "X-Legion-Key: $KEY" "http://$API/cluster/health" >/dev/null
LEGION_URL="http://$API" LEGION_API_KEY="$KEY" "$LEGION" cluster health | jq -e '.healthy == true' >/dev/null

LEGION_URL="http://$API" LEGION_API_KEY="$KEY" "$LEGION" deploy push process-load bun "$TMP/load.ts" >/dev/null
headers="$TMP/headers"
statuses=$(seq 1 8 | xargs -P8 -I{} sh -c 'curl -s -D "$1/h-{}" -o /dev/null -w "%{http_code}\n" -X POST "$2/functions/process-load/invoke" -H "Authorization: Bearer $3" -H "content-type: application/json" -d "{\"index\":{}}"' _ "$TMP" "http://$API" "$KEY")
grep -q '^200$' <<<"$statuses"
grep -q '^429$' <<<"$statuses"
cat "$TMP"/h-* >"$headers"
grep -qi '^Retry-After:' "$headers" || { echo "ERROR: 429 response lacks Retry-After" >&2; cat "$headers" >&2; exit 1; }

for _ in $(seq 1 50); do
  if curl -sf -H "Authorization: Bearer $KEY" "http://$API/metrics" >"$TMP/metrics"; then break; fi
  sleep .1
done
curl -sf -H "Authorization: Bearer $KEY" "http://$API/metrics" >"$TMP/metrics"
grep -q '^legion_function_invocations_total{function="process-load"' "$TMP/metrics"
grep -Eq '^legion_function_rate_limit_rejections_total [1-9][0-9]*$' "$TMP/metrics"

# REST credentials do not grant 9P access; attach authorization is independent.
if LEGION_9P_TEST_ADDR="$NINEP" LEGION_9P_TEST_CAPABILITY="$KEY" "$ROOT/bin/ninep-smoke" >/dev/null 2>&1; then
  echo "ERROR: REST API key unexpectedly authenticated to 9P" >&2
  exit 1
fi
LEGION_9P_TEST_ADDR="$NINEP" LEGION_9P_TEST_CAPABILITY="$CAP" "$ROOT/bin/ninep-smoke" >/dev/null

echo "Go Milestone 4 authenticated process smoke passed: REST/CLI, public health, metrics, 429 shedding, independent 9P capability"
