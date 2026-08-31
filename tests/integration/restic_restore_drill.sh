#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
export RESTIC_REPOSITORY="$TMP/repository"
export RESTIC_PASSWORD="integration-test-only"
export LEGION_STATE_DIR="$TMP/source"
export LEGION_BACKUP_QUIESCE=0
mkdir -p "$LEGION_STATE_DIR/raft" "$LEGION_STATE_DIR/blobs"
printf 'durable session state\n' >"$LEGION_STATE_DIR/sessions.db"
printf 'raft state\n' >"$LEGION_STATE_DIR/raft/log"
printf 'blob payload\n' >"$LEGION_STATE_DIR/blobs/cid"
restic init >/dev/null
"$ROOT/contrib/backup/legion-backup-restic.sh" >/dev/null
export LEGION_STATE_DIR="$TMP/restored"
"$ROOT/contrib/backup/legion-restore-restic.sh" >/dev/null
diff -r "$TMP/source" "$TMP/restored"
echo "Restic clean-node restore drill passed"
