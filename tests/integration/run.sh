#!/usr/bin/env bash
# Legion integration test: deploy a Bun function, invoke it, verify output.
# Requires: legion binary built, bun installed, curl.
set -euo pipefail

PORT="${LEGION_TEST_PORT:-18080}"
DATA_DIR="$(mktemp -d)"
BINARY="$(cargo locate-project --workspace --message-format plain 2>/dev/null | xargs dirname)/target/debug/legion"

if [[ ! -f "$BINARY" ]]; then
  echo "SKIP: legion binary not found at $BINARY (run 'make server' first)"
  exit 0
fi

echo "==> Starting legion server on :$PORT (data: $DATA_DIR)"
LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA_DIR" RUST_LOG=error "$BINARY" &
SERVER_PID=$!

# Wait for server
for i in $(seq 1 20); do
  sleep 0.3
  if curl -sf "http://localhost:$PORT/health" > /dev/null 2>&1; then
    break
  fi
  if [[ $i -eq 20 ]]; then
    echo "FAIL: server did not start"
    kill "$SERVER_PID" 2>/dev/null || true
    rm -rf "$DATA_DIR"
    exit 1
  fi
done

echo "==> Server is up"

cleanup() {
  echo "==> Stopping server (pid $SERVER_PID)"
  kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# ── Test 1: health ────────────────────────────────────────────────────────────
echo "--- Test 1: GET /health"
HEALTH=$(curl -sf "http://localhost:$PORT/health")
echo "    $HEALTH"
echo "$HEALTH" | grep -q '"ok":true' || { echo "FAIL: health"; exit 1; }
echo "    OK"

# ── Test 2: deploy a Bun function ─────────────────────────────────────────────
echo "--- Test 2: POST /functions (deploy hello)"
DEPLOY=$(curl -sf -X POST "http://localhost:$PORT/functions" \
  -H "Content-Type: application/json" \
  -d '{
    "name":        "hello",
    "runtime":     "bun",
    "description": "Returns a greeting",
    "code":        "const args = JSON.parse(await Bun.stdin.text()); process.stdout.write(JSON.stringify({ greeting: \"Hello, \" + (args.name ?? \"world\") + \"!\" }))"
  }')
echo "    $DEPLOY"
echo "$DEPLOY" | grep -q '"name":"hello"' || { echo "FAIL: deploy"; exit 1; }
echo "    OK"

# ── Test 3: list functions ────────────────────────────────────────────────────
echo "--- Test 3: GET /functions"
LIST=$(curl -sf "http://localhost:$PORT/functions")
echo "    $LIST"
echo "$LIST" | grep -q '"hello"' || { echo "FAIL: list functions"; exit 1; }
echo "    OK"

# ── Test 4: invoke via REST ───────────────────────────────────────────────────
echo "--- Test 4: POST /functions/hello/invoke"
RESULT=$(curl -sf -X POST "http://localhost:$PORT/functions/hello/invoke" \
  -H "Content-Type: application/json" \
  -d '{"name": "Legion"}')
echo "    $RESULT"
echo "$RESULT" | grep -q '"Hello, Legion!"' || { echo "FAIL: invoke"; exit 1; }
echo "    OK"

# ── Test 5: create a session ──────────────────────────────────────────────────
echo "--- Test 5: POST /sessions"
SESSION=$(curl -sf -X POST "http://localhost:$PORT/sessions" \
  -H "Content-Type: application/json" \
  -d '{"model":"faux/test"}')
echo "    $SESSION"
SESSION_ID=$(echo "$SESSION" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
[[ -n "$SESSION_ID" ]] || { echo "FAIL: no session id"; exit 1; }
echo "    OK (id: $SESSION_ID)"

# ── Test 6: get session ───────────────────────────────────────────────────────
echo "--- Test 6: GET /sessions/$SESSION_ID"
STATUS=$(curl -sf "http://localhost:$PORT/sessions/$SESSION_ID")
echo "    $STATUS"
echo "$STATUS" | grep -q '"id"' || { echo "FAIL: get session"; exit 1; }
echo "    OK"

echo ""
echo "==> All integration tests passed"
