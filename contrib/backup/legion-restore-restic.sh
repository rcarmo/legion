#!/bin/sh
# Restore a Legion snapshot into an empty state directory and verify integrity.
set -eu

STATE_DIR=${LEGION_STATE_DIR:-/var/lib/legion}
SNAPSHOT=${LEGION_BACKUP_SNAPSHOT:-latest}
TAG=${LEGION_BACKUP_TAG:-legion}
RESTORE=$(mktemp -d)
trap 'rm -rf "$RESTORE"' EXIT INT TERM

: "${RESTIC_REPOSITORY:?RESTIC_REPOSITORY is required}"
if [ -z "${RESTIC_PASSWORD:-}" ] && [ -z "${RESTIC_PASSWORD_FILE:-}" ] && [ -z "${RESTIC_PASSWORD_COMMAND:-}" ]; then
    echo "RESTIC_PASSWORD, RESTIC_PASSWORD_FILE, or RESTIC_PASSWORD_COMMAND is required" >&2
    exit 2
fi
if [ -e "$STATE_DIR" ] && [ "$(find "$STATE_DIR" -mindepth 1 -print -quit)" ]; then
    echo "refusing to restore over non-empty state directory: $STATE_DIR" >&2
    exit 2
fi

restic check
restic restore "$SNAPSHOT" --tag "$TAG" --target "$RESTORE"
ROOT=$(find "$RESTORE" -type f -name SHA256SUMS -printf '%h\n' | head -1)
[ -f "$ROOT/SHA256SUMS" ] || { echo "snapshot lacks SHA256SUMS" >&2; exit 1; }
(
    cd "$ROOT/state"
    sha256sum -c "$ROOT/SHA256SUMS"
)
mkdir -p "$STATE_DIR"
cp -a "$ROOT/state/." "$STATE_DIR/"
echo "Legion restore completed and verified: $STATE_DIR"
