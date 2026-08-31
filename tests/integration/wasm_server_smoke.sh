#!/usr/bin/env bash
# Start one already-built Legion server and run the WASM smoke test against it.
# Cargo is intentionally not called here; use `make wasm-integration-test`.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
BINARY="$TARGET_DIR/debug/legion"
PORT="${LEGION_TEST_PORT:-18090}"
DATA="$(mktemp -d)"
LOG="$(mktemp)"
PID=""

cleanup() {
  [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true
  rm -rf "$DATA" "$LOG"
}
trap cleanup EXIT

if [[ ! -x "$BINARY" ]]; then
  echo "ERROR: server binary missing at $BINARY; run 'make server'" >&2
  exit 1
fi

LEGION_API_PORT="$PORT" LEGION_DATA_DIR="$DATA" RUST_LOG=error "$BINARY" serve >"$LOG" 2>&1 &
PID=$!
for _ in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:$PORT/health" >/dev/null; then
    "$ROOT/tests/integration/wasm_smoke.sh" "$PORT"
    exit $?
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "ERROR: server exited before becoming ready" >&2
    cat "$LOG" >&2
    exit 1
  fi
  sleep .5
done

echo "ERROR: server did not become ready within 60 seconds" >&2
cat "$LOG" >&2
exit 1
