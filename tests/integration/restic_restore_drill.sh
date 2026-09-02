#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LEGION=${LEGION_TEST_BINARY:-$ROOT/bin/legion}
TMP="$(mktemp -d)"
BASE=$((33000 + $$ % 300))
SOURCE_API="127.0.0.1:$((BASE + 500))"
SOURCE_RAFT="127.0.0.1:$BASE"
RESTORED_API="127.0.0.1:$((BASE + 501))"
RESTORED_RAFT="127.0.0.1:$((BASE + 1))"
KEY=restic-drill-key
PID=
cleanup() {
  if [[ -n "${PID:-}" ]]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT
[[ -x "$LEGION" ]] || { echo "ERROR: missing Go Legion binary: $LEGION" >&2; exit 1; }

export RESTIC_REPOSITORY="$TMP/repository"
export RESTIC_PASSWORD="integration-test-only"
export LEGION_STATE_DIR="$TMP/source"
export LEGION_BACKUP_QUIESCE=0
restic init >/dev/null

start_node() {
  local data=$1 raft=$2 api=$3 log=$4
  LEGION_API_KEY="$KEY" "$LEGION" --data-dir "$data" --iroh-addr 127.0.0.1:0 \
    --raft-addr "$raft" --api-addr "$api" --9p-addr= --mdns=false --relay=false \
    --discovery-window=10ms >"$log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    curl -sf "http://$api/health" >/dev/null && return
    kill -0 "$PID" 2>/dev/null || { cat "$log" >&2; exit 1; }
    sleep .1
  done
  echo "ERROR: node did not become healthy" >&2
  cat "$log" >&2
  exit 1
}

start_node "$LEGION_STATE_DIR" "$SOURCE_RAFT" "$SOURCE_API" "$TMP/source.log"
# Create genuine Go Raft/SQLite state and a genuine deployed CAS artifact.
session=
for _ in $(seq 1 100); do
  response=$(curl -sf -H "Authorization: Bearer $KEY" -H 'content-type: application/json' \
    -d '{"model":"faux/backup","budget":{},"tools":[]}' "http://$SOURCE_API/sessions" 2>/dev/null || true)
  session=$(jq -r '.run_id // empty' <<<"$response")
  [[ -n "$session" ]] && break
  sleep .1
done
[[ "$session" != null && -n "$session" ]]
printf 'export function run(value) { return {restored:value.marker}; }\n' >"$TMP/function.ts"
LEGION_URL="http://$SOURCE_API" LEGION_API_KEY="$KEY" "$LEGION" deploy push restored-bun bun "$TMP/function.ts" >"$TMP/deploy.json"
cid=$(jq -r .artifact_cid "$TMP/deploy.json")
[[ "$cid" != null && -n "$cid" ]]
# Stop before capture: QUIESCE=0 is valid only because the source is now offline.
kill "$PID"; wait "$PID" || true; PID=
find "$LEGION_STATE_DIR" -type f -size +0c | grep -q .
"$ROOT/contrib/backup/legion-backup-restic.sh" >/dev/null

export LEGION_STATE_DIR="$TMP/restored"
"$ROOT/contrib/backup/legion-restore-restic.sh" >/dev/null
diff -r "$TMP/source" "$TMP/restored"
# Delete the derived query view to prove a clean node reconstructs it from
# restored authoritative Raft state rather than merely opening copied SQLite.
rm -f "$LEGION_STATE_DIR/raft/state.db" "$LEGION_STATE_DIR/raft/state.db-wal" "$LEGION_STATE_DIR/raft/state.db-shm"
start_node "$LEGION_STATE_DIR" "$RESTORED_RAFT" "$RESTORED_API" "$TMP/restored.log"
for _ in $(seq 1 100); do
  status=$(curl -sf -H "Authorization: Bearer $KEY" "http://$RESTORED_API/sessions/$session" 2>/dev/null || true)
  if jq -e '.status == "idle"' <<<"$status" >/dev/null 2>&1; then break; fi
  sleep .1
done
curl -sf -H "Authorization: Bearer $KEY" "http://$RESTORED_API/sessions/$session" | jq -e '.status == "idle"' >/dev/null
LEGION_URL="http://$RESTORED_API" LEGION_API_KEY="$KEY" "$LEGION" call restored-bun '{"marker":"real-go-state"}' | jq -e '.output.restored == "real-go-state"' >/dev/null
[[ -s "$LEGION_STATE_DIR/raft/state.db" ]]
echo "Restic clean-node Go restore drill passed: Raft session rebuilt, CAS function invoked, checksums verified"
