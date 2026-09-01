#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
LEGION="$ROOT/bin/legion"
JOKER="$ROOT/bin/joker"
BUN_BIN=${LEGION_BUN_BIN:-$(command -v bun)}
TMP=$(mktemp -d)
BASE=$((29000 + $$ % 500))
RAFT="127.0.0.1:$BASE"
API="127.0.0.1:$((BASE + 500))"
NINEP="127.0.0.1:$((BASE + 1000))"
PID=
cleanup() {
  if [[ -n "${PID:-}" ]]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi
  if [[ -s "$TMP/server.log" ]]; then cat "$TMP/server.log" >&2; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

cat >"$TMP/hello.joke" <<'JOKER'
(defn run [args] {"greeting" (str "Hello, " (get args "name") "!")})
JOKER
cat >"$TMP/hello.ts" <<'BUN'
const input = JSON.parse(await Bun.stdin.text());
console.log(JSON.stringify({ greeting: `Hello, ${input.name}!` }));
BUN
cp "$ROOT/target/wasm32-wasip1/release/wasm_hello.wasm" "$TMP/hello.wasm"

LEGION_JOKER_BIN="$JOKER" LEGION_BUN_BIN="$BUN_BIN" "$LEGION" \
  --data-dir "$TMP/data" --iroh-addr 127.0.0.1:0 --raft-addr "$RAFT" \
  --api-addr "$API" --9p-addr "$NINEP" --mdns=false --relay=false \
  --discovery-window=10ms >"$TMP/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do
  curl -sf "http://$API/cluster/health" >/dev/null && break
  sleep .1
done
curl -sf "http://$API/cluster/health" >/dev/null

for runtime in joker bun wasm; do
  case "$runtime" in joker) artifact="$TMP/hello.joke";; bun) artifact="$TMP/hello.ts";; wasm) artifact="$TMP/hello.wasm";; esac
  LEGION_URL="http://$API" "$LEGION" deploy push "$runtime-hello" "$runtime" "$artifact" >"$TMP/$runtime.deploy.json"
  LEGION_URL="http://$API" "$LEGION" call "$runtime-hello" '{"name":"Rui"}' >"$TMP/$runtime.call.json"
  jq -e '.status == "success" and .artifact_cid != null' "$TMP/$runtime.deploy.json" >/dev/null
  jq -e '.output.greeting == "Hello, Rui!"' "$TMP/$runtime.call.json" >/dev/null
done

printf 'Go Milestone 3 CLI/REST smoke passed: WASM, Bun, Joker\n'
