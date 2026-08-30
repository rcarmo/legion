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
LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA_DIR" \
LEGION_INVOKE_TIMEOUT_MS=100 LEGION_INVOKE_MAX_INPUT_BYTES=1024 \
LEGION_INVOKE_MAX_REQUESTS_PER_WINDOW=4 LEGION_INVOKE_RATE_WINDOW_MS=60000 \
LEGION_SESSION_MAX_REQUESTS_PER_WINDOW=1 LEGION_SESSION_RATE_WINDOW_MS=60000 \
RUST_LOG=error "$BINARY" &
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

# ── Test 8: invocation limits and metrics ─────────────────────────────────────
echo "--- Test 8: invocation limits and metrics"
R=$(curl -sf -X POST "http://localhost:$PORT/functions" \
  -H "Content-Type: application/json" \
  -d '{
    "name":    "slow",
    "runtime": "bun",
    "code":    "await Bun.sleep(500); process.stdout.write(JSON.stringify({ok:true}))"
  }')
echo "$R" | grep -q '"name":"slow"' || fail "deploy slow" "$R"

HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://localhost:$PORT/functions/slow/invoke" \
  -H "Content-Type: application/json" -d '{}')
[[ "$HTTP" = 504 ]] && ok "invoke timeout returns 504" || fail "invoke timeout" "got $HTTP"

LARGE=$(printf '%1100s' x)
HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://localhost:$PORT/functions/add/invoke" \
  -H "Content-Type: application/json" -d "{\"value\":\"$LARGE\"}")
[[ "$HTTP" = 413 ]] && ok "oversized invoke returns 413" || fail "invoke input limit" "got $HTTP"

R=$(curl -sf "http://localhost:$PORT/metrics")
echo "$R" | grep -q 'legion_function_invocations_total{function="hello",runtime="bun",outcome="success"} 2' \
  && ok "function metrics" || fail "function metrics" "$R"
echo "$R" | grep -q 'legion_function_invocations_total{function="slow",runtime="bun",outcome="timeout"} 1' \
  && ok "timeout metrics" || fail "timeout metrics" "$R"

for _ in 1 2 3; do
  curl -sf -X POST "http://localhost:$PORT/functions/add/invoke" \
    -H "Content-Type: application/json" -d '{"a":1,"b":1}' >/dev/null
 done
HEADERS=$(mktemp)
HTTP=$(curl -s -D "$HEADERS" -o /dev/null -w "%{http_code}" -X POST \
  "http://localhost:$PORT/functions/add/invoke" \
  -H "Content-Type: application/json" -d '{"a":1,"b":1}')
if [[ "$HTTP" = 429 ]] && grep -qi '^retry-after: ' "$HEADERS"; then
  ok "function rate limit returns 429 with Retry-After"
else
  fail "function rate limit" "got $HTTP"
fi
rm -f "$HEADERS"

# ── Test 9: create session (model irrelevant — just checks persistence) ────────
echo "--- Test 9: POST /sessions"
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

# ── Test 10: GET /sessions/:id ──────────────────────────────────────────────────
if [[ -n "$SESSION_ID" ]]; then
  echo "--- Test 10: GET /sessions/$SESSION_ID"
  R=$(curl -sf "http://localhost:$PORT/sessions/$SESSION_ID")
  echo "$R" | grep -q '"id"' && ok "get session" || fail "get session" "$R"

  R=$(curl -sf -X POST "http://localhost:$PORT/sessions/$SESSION_ID/events" \
    -H "Content-Type: application/json" -d '{"trigger":"first"}')
  echo "$R" | grep -q '"status":"resuming"' && ok "first session event allowed" || fail "session event" "$R"
  HEADERS=$(mktemp)
  HTTP=$(curl -s -D "$HEADERS" -o /dev/null -w "%{http_code}" -X POST \
    "http://localhost:$PORT/sessions/$SESSION_ID/events" \
    -H "Content-Type: application/json" -d '{"trigger":"second"}')
  if [[ "$HTTP" = 429 ]] && grep -qi '^retry-after: ' "$HEADERS"; then
    ok "session rate limit returns 429 with Retry-After"
  else
    fail "session rate limit" "got $HTTP"
  fi
  rm -f "$HEADERS"
fi

# ── Test 11: GET /sessions/:id/log ────────────────────────────────────────────
if [[ -n "$SESSION_ID" ]]; then
  echo "--- Test 11: GET /sessions/$SESSION_ID/log"
  R=$(curl -sf "http://localhost:$PORT/sessions/$SESSION_ID/log")
  echo "$R" | grep -q '\[' && ok "get log" || fail "get log" "$R"
fi

R=$(curl -sf "http://localhost:$PORT/metrics")
echo "$R" | grep -q 'legion_function_invocations_total{function="add",runtime="bun",outcome="rate_limited"} 1' \
  && ok "function rate limit metrics" || fail "function rate metrics" "$R"
echo "$R" | grep -q 'legion_session_rate_limit_rejections_total 1' \
  && ok "session rate limit metrics" || fail "session rate metrics" "$R"

# ── LLM-dependent tests (skip unless API key set) ─────────────────────────────
if [[ -z "${ANTHROPIC_API_KEY:-}" ]] && [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo ""
  echo "--- Skipping stream test (no API key set)"
else
  echo "--- Test 12: POST /sessions/:id/messages + GET /sessions/:id/stream"
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
