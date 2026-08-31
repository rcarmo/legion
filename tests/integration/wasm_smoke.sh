#!/usr/bin/env bash
# WASM runtime smoke test.
# Deploys a pre-built WASM module to a running Legion server and invokes it.
# Usage: bash tests/integration/wasm_smoke.sh [PORT]
set -euo pipefail

PORT="${1:-${LEGION_TEST_PORT:-18080}}"
WS="$(cd "$(dirname "$0")/../.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$WS/target}"
WASM="$TARGET_DIR/wasm32-wasip1/release/wasm_hello.wasm"

if [[ ! -f "$WASM" ]]; then
  echo "ERROR: wasm module not built at $WASM — run 'make wasm-fixture'" >&2
  exit 1
fi

if ! curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1; then
  echo "SKIP: no Legion server on :$PORT"
  exit 0
fi

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✓ $1"; }
fail() { FAIL=$((FAIL+1)); echo "  ✗ $1: $2"; }

# Deploy the WASM module (base64-encoded bytes). Stream the JSON body from a
# file to avoid exceeding the OS command argument-size limit.
echo "--- Deploying wasm-hello"
BODY=$(mktemp)
B64=$(mktemp)
trap 'rm -f "$BODY" "$B64"' EXIT
base64 -w0 "$WASM" > "$B64"
jq -n --rawfile wasm "$B64" '{
  name: "wasm-hello",
  runtime: "wasm",
  wasm_b64: $wasm
}' > "$BODY"
R=$(curl -sf -X POST "http://localhost:$PORT/functions" \
  -H "Content-Type: application/json" \
  --data-binary "@$BODY" || echo '{"error":"request failed"}')
echo "  response: $(echo "$R" | head -c 200)"
echo "$R" | grep -q '"name":"wasm-hello"' && ok "deploy wasm-hello" || fail "deploy wasm-hello" "$R"

# Invoke with args
echo "--- Invoking wasm-hello"
R=$(curl -sf -X POST "http://localhost:$PORT/functions/wasm-hello/invoke" \
  -H "Content-Type: application/json" \
  -d '{"name":"WASM"}' || echo '{"error":"request failed"}')
echo "  response: $R"
echo "$R" | grep -q 'Hello, WASM!' && ok "invoke wasm-hello" || fail "invoke wasm-hello" "$R"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
