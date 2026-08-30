#!/usr/bin/env bash
# End-to-end smoke test for the Legion CLI against a local server.
set -euo pipefail

PORT="${LEGION_CLI_TEST_PORT:-18089}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="${BINARY:-$ROOT/target/debug/legion}"
DATA_DIR="$(mktemp -d /workspace/tmp/legion-cli-data.XXXXXX)"
LOG="$(mktemp /workspace/tmp/legion-cli-log.XXXXXX)"
SOURCE="$(mktemp /workspace/tmp/legion-cli-fn.XXXXXX.ts)"
URL="http://127.0.0.1:$PORT"

cleanup() {
  kill "${SERVER_PID:-}" 2>/dev/null || true
  wait "${SERVER_PID:-}" 2>/dev/null || true
  rm -rf "$DATA_DIR" "$LOG" "$SOURCE"
}
trap cleanup EXIT

cat > "$SOURCE" <<'EOF'
const input = JSON.parse(await Bun.stdin.text());
process.stdout.write(JSON.stringify({ value: input.value * 2 }));
EOF

LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA_DIR" RUST_LOG=error "$BINARY" serve >"$LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 50); do
  if "$BINARY" --url "$URL" health >/dev/null 2>&1; then break; fi
  sleep 0.2
done
"$BINARY" --url "$URL" health | grep -q '"ok": true'
"$BINARY" --url "$URL" cluster peers | grep -q '"self"'

"$BINARY" --url "$URL" deploy push cli-double "$SOURCE" --runtime bun >/dev/null
"$BINARY" --url "$URL" deploy list | grep -q 'cli-double'
echo '{"value":21}' | "$BINARY" --url "$URL" call cli-double | grep -q '"value": 42'

SESSION_JSON=$("$BINARY" --url "$URL" session new --model anthropic/claude-haiku-3-5)
SESSION_ID=$(printf '%s' "$SESSION_JSON" | jq -r .id)
[[ "$SESSION_ID" =~ ^[0-9a-f-]{36}$ ]]
"$BINARY" --url "$URL" session status "$SESSION_ID" | grep -q "$SESSION_ID"
"$BINARY" --url "$URL" session list --status idle | grep -q "$SESSION_ID"
"$BINARY" --url "$URL" session history "$SESSION_ID" | grep -q 'SessionStarted'

"$BINARY" --url "$URL" deploy delete cli-double | grep -q '"deleted": true'
if "$BINARY" --url "$URL" deploy list | grep -q 'cli-double'; then
  echo "function still listed after delete" >&2
  exit 1
fi

echo "CLI integration: PASS"
