#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="${LEGION_TEST_BINARY:-$ROOT/target/debug/legion}"
DATA="$(mktemp -d)"
LOG="$(mktemp)"
PORT="${LEGION_TEST_PORT:-18083}"
NINEP_PORT="${LEGION_TEST_NINEP_PORT:-15640}"
CAPABILITY="${LEGION_TEST_NINEP_CAPABILITY:-test-capability}"
PID=""
cleanup() { [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true; rm -rf "$DATA" "$LOG"; }
trap cleanup EXIT
[[ -x "$BINARY" ]] || { echo "ERROR: prebuilt legion binary missing: $BINARY" >&2; exit 1; }

cat >"$DATA/legion.toml" <<EOF
namespace_capability = "$CAPABILITY"
ninep_tcp_addr = "127.0.0.1:$NINEP_PORT"
[cluster]
data_dir = "$DATA/state"
bind_addr = "127.0.0.1:0"
api_port = $PORT
mdns = false
EOF
LEGION_CONFIG="$DATA/legion.toml" RUST_LOG=legion=info "$BINARY" serve >"$LOG" 2>&1 & PID=$!
for _ in $(seq 1 120); do
  curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break
  kill -0 "$PID" 2>/dev/null || { cat "$LOG" >&2; exit 1; }
  sleep .25
done
LEGION_TEST_NINEP_PORT="$NINEP_PORT" LEGION_TEST_NINEP_CAPABILITY="$CAPABILITY" \
  bun "$ROOT/tests/integration/bun_ninep_smoke.ts" || { cat "$LOG" >&2; exit 1; }
