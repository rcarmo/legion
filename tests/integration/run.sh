#!/usr/bin/env bash
# Legion integration test: deploy a Bun function, invoke it, verify output.
# The session/stream tests are skipped unless ANTHROPIC_API_KEY is set.
#
# Usage:
#   bash tests/integration/run.sh          # basic (no LLM needed)
#   ANTHROPIC_API_KEY=sk-... bash tests/integration/run.sh  # full
set -euo pipefail

PORT="${LEGION_TEST_PORT:-18080}"
DATA_DIR="$(mktemp -d)"
WS="$(cargo locate-project --workspace --message-format plain 2>/dev/null | xargs dirname)"
BINARY="$WS/target/debug/legion"

if [[ ! -f "$BINARY" ]]; then
  echo "SKIP: legion binary not built at $BINARY (run 'make server' first)"
  exit 0
fi

# ── Start server ──────────────────────────────────────────────────────────────
echo "==> Starting legion on :$PORT  data=$DATA_DIR"
LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA_DIR" RUST_LOG=error "$BINARY" &
SERVER_PID=$!

cleanup() {
  kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

for i in $(seq 1 30); do
  sleep 0.3
  if curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1; then
    echo "    server up after ${i} attempts"
    break
  fi
  if [[ $i -eq 30 ]]; then
    echo "FAIL: server did not start"
    exit 1
  fi
done

PASS=0; FAIL=0

ok()   { PASS=$((PASS+1)); echo "    ✓ $1"; }
fail() { FAIL=$((FAIL+1)); echo "    ✗ $1: $2"; }

# ── Test 1: health ─────────────────────────────────────────────────────────────
echo "--- Test 1: GET /health"
R=$(curl -sf "http://localhost:$PORT/health")
echo "$R" | grep -q '"ok":true' && ok "health" || fail "health" "$R"

# ── Test 2: deploy a Bun function ─────────────────────────────────────────────
echo "--- Test 2: POST /functions  (deploy 'hello')"
R=$(curl -sf -X POST "http://localhost:$PORT/functions" \
  -H "Content-Type: application/json" \
  -d '{
    "name":        "hello",
    "runtime":     "bun",
    "description": "Returns a greeting",
    "code":        "const args = JSON.parse(await Bun.stdin.text()); process.stdout.write(JSON.stringify({ greeting: \"Hello, \" + (args.name ?? \"world\") + \"!\" }))"
  }')
echo "$R" | grep -q '"name":"hello"' && ok "deploy hello" || fail "deploy hello" "$R"

# ── Test 3: list functions ─────────────────────────────────────────────────────
echo "--- Test 3: GET /functions"
R=$(curl -sf "http://localhost:$PORT/functions")
echo "$R" | grep -q '"hello"' && ok "list functions" || fail "list functions" "$R"

# ── Test 4: invoke via REST ────────────────────────────────────────────────────
echo "--- Test 4: POST /functions/hello/invoke"
R=$(curl -sf -X POST "http://localhost:$PORT/functions/hello/invoke" \
  -H "Content-Type: application/json" \
  -d '{"name":"Legion"}')
echo "$R" | grep -q 'Hello, Legion!' && ok "invoke hello" || fail "invoke hello" "$R"

# ── Test 5: invoke with default args ──────────────────────────────────────────
echo "--- Test 5: POST /functions/hello/invoke  (no name arg)"
R=$(curl -sf -X POST "http://localhost:$PORT/functions/hello/invoke" \
  -H "Content-Type: application/json" \
  -d '{}')
echo "$R" | grep -q 'Hello, world!' && ok "invoke default" || fail "invoke default" "$R"

# ── Test 6: 404 on unknown function ───────────────────────────────────────────
echo "--- Test 6: POST /functions/nonexistent/invoke  (expect 4xx)"
HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:$PORT/functions/nonexistent/invoke" \
  -H "Content-Type: application/json" -d '{}')
[[ "$HTTP" -ge 400 ]] && ok "404 on missing fn" || fail "404 on missing fn" "got $HTTP"

# ── Test 7: deploy a second function ──────────────────────────────────────────
echo "--- Test 7: deploy 'add' function"
R=$(curl -sf -X POST "http://localhost:$PORT/functions" \
  -H "Content-Type: application/json" \
  -d '{
    "name":    "add",
    "runtime": "bun",
    "code":    "const {a=0,b=0} = JSON.parse(await Bun.stdin.text()); process.stdout.write(JSON.stringify({sum: a+b}))"
  }')
echo "$R" | grep -q '"name":"add"' && ok "deploy add" || fail "deploy add" "$R"

R=$(curl -sf -X POST "http://localhost:$PORT/functions/add/invoke" \
  -H "Content-Type: application/json" \
  -d '{"a":3,"b":4}')
echo "$R" | grep -q '"sum":7' && ok "invoke add" || fail "invoke add" "$R"

# ── Test 8: create session (model irrelevant — just checks persistence) ────────
echo "--- Test 8: POST /sessions"
MODEL="${LEGION_TEST_MODEL:-anthropic/claude-haiku-3-5}"
R=$(curl -sf -X POST "http://localhost:$PORT/sessions" \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"$MODEL\"}")
SESSION_ID=$(echo "$R" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
if [[ -n "$SESSION_ID" ]]; then
  ok "create session  id=$SESSION_ID"
else
  fail "create session" "$R"
  SESSION_ID=""
fi

# ── Test 9: GET /sessions/:id ──────────────────────────────────────────────────
if [[ -n "$SESSION_ID" ]]; then
  echo "--- Test 9: GET /sessions/$SESSION_ID"
  R=$(curl -sf "http://localhost:$PORT/sessions/$SESSION_ID")
  echo "$R" | grep -q '"id"' && ok "get session" || fail "get session" "$R"
fi

# ── Test 10: GET /sessions/:id/log ────────────────────────────────────────────
if [[ -n "$SESSION_ID" ]]; then
  echo "--- Test 10: GET /sessions/$SESSION_ID/log"
  R=$(curl -sf "http://localhost:$PORT/sessions/$SESSION_ID/log")
  echo "$R" | grep -q '\[' && ok "get log" || fail "get log" "$R"
fi

# ── LLM-dependent tests (skip unless API key set) ─────────────────────────────
if [[ -z "${ANTHROPIC_API_KEY:-}" ]] && [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo ""
  echo "--- Skipping stream test (no API key set)"
else
  echo "--- Test 11: POST /sessions/:id/messages + GET /sessions/:id/stream"
  if [[ -n "$SESSION_ID" ]]; then
    # Send user message
    curl -sf -X POST "http://localhost:$PORT/sessions/$SESSION_ID/messages" \
      -H "Content-Type: application/json" \
      -d '{"content":"Say HELLO in all caps."}' >/dev/null

    # Read SSE stream (first 3 lines with 5s timeout)
    SSE=$(timeout 10 curl -sf -N "http://localhost:$PORT/sessions/$SESSION_ID/stream" \
      --max-time 10 2>/dev/null | head -3 || true)
    [[ -n "$SSE" ]] && ok "SSE stream non-empty" || fail "SSE stream" "empty"
  fi
fi

# ── Results ────────────────────────────────────────────────────────────────────
echo ""
echo "==> Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && echo "✓ All tests passed" && exit 0 || exit 1
