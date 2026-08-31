#!/usr/bin/env bash
# Verify that Legion emits OTLP/HTTP traces and token metrics to a collector endpoint.
# Uses a tiny local HTTP capture server rather than requiring a collector image.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
PROBE="$TARGET_DIR/debug/otel-probe"
PORT="${LEGION_OTEL_TEST_PORT:-14318}"
CAPTURE="$(mktemp -d)"
SERVER="$CAPTURE/server.ts"
PID=""

cleanup() {
  [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true
  rm -rf "$CAPTURE"
}
trap cleanup EXIT

cat >"$SERVER" <<'TS'
const dir = Bun.env.CAPTURE!;
const port = Number(Bun.env.PORT);
Bun.serve({
  port,
  async fetch(req) {
    const path = new URL(req.url).pathname.replaceAll('/', '_');
    await Bun.write(`${dir}/${path}-${Date.now()}.bin`, await req.arrayBuffer());
    return new Response(null, { status: 200 });
  },
});
TS
CAPTURE="$CAPTURE" PORT="$PORT" bun "$SERVER" &
PID=$!
sleep .3

OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:$PORT" "$PROBE"
for _ in $(seq 1 50); do
  traces=$(find "$CAPTURE" -name '_v1_traces-*.bin' -size +0c | wc -l)
  metrics=$(find "$CAPTURE" -name '_v1_metrics-*.bin' -size +0c | wc -l)
  if (( traces > 0 && metrics > 0 )); then
    echo "OTLP smoke passed: $traces trace request(s), $metrics metric request(s)"
    exit 0
  fi
  sleep .2
done

echo "ERROR: OTLP trace and metric payloads were not both received" >&2
find "$CAPTURE" -maxdepth 1 -type f -printf '%f %s bytes\n' >&2
exit 1
