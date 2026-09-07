#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DATA="$(mktemp -d)"; PID=""
PORT="${LEGION_TEST_PORT:-18094}"; NINEP_PORT="${LEGION_TEST_NINEP_PORT:-15644}"
cleanup(){ if [[ -n "$PID" ]]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi; rm -rf "$DATA"; }
trap cleanup EXIT
"${LEGION_TEST_BINARY:-$ROOT/bin/legion}" --data-dir "$DATA/state" --api-addr "127.0.0.1:$PORT" --raft-addr "127.0.0.1:${LEGION_TEST_RAFT_PORT:-17094}" --iroh-addr 127.0.0.1:0 --mdns=false --relay=false --discovery-window 20ms --9p-addr "127.0.0.1:$NINEP_PORT" --9p-capability test-capability >"$DATA/server.log" 2>&1 & PID=$!
for _ in $(seq 1 120); do
 if curl -sf "http://127.0.0.1:$PORT/cluster/health" | jq -e '.raft == "Leader"' >/dev/null; then break; fi
 kill -0 "$PID" 2>/dev/null || { cat "$DATA/server.log"; exit 1; }; sleep .25
done
export LEGION_TEST_URL="http://127.0.0.1:$PORT"
export LEGION_TEST_NINEP_PORT="$NINEP_PORT" LEGION_TEST_NINEP_CAPABILITY=test-capability
bun "$ROOT/tests/integration/bun_ninep_smoke.ts" || { cat "$DATA/server.log"; exit 1; }
bun "$ROOT/tests/integration/go_client_smoke.ts" || { cat "$DATA/server.log"; exit 1; }
PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/workspace/bin/pw-browsers}" bun "$ROOT/tests/integration/dashboard_smoke.ts" || { cat "$DATA/server.log"; exit 1; }
