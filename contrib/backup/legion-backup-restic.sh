#!/bin/sh
# Create an encrypted, consistent restic snapshot of Legion state.
set -eu

STATE_DIR=${LEGION_STATE_DIR:-/var/lib/legion}
SERVICE=${LEGION_SERVICE:-legion.service}
QUIESCE=${LEGION_BACKUP_QUIESCE:-1}
TAG=${LEGION_BACKUP_TAG:-legion}
STAGE=$(mktemp -d)
STOPPED=0

cleanup() {
    status=$?
    rm -rf "$STAGE"
    if [ "$STOPPED" = 1 ]; then
        systemctl start "$SERVICE"
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

: "${RESTIC_REPOSITORY:?RESTIC_REPOSITORY is required}"
if [ -z "${RESTIC_PASSWORD:-}" ] && [ -z "${RESTIC_PASSWORD_FILE:-}" ] && [ -z "${RESTIC_PASSWORD_COMMAND:-}" ]; then
    echo "RESTIC_PASSWORD, RESTIC_PASSWORD_FILE, or RESTIC_PASSWORD_COMMAND is required" >&2
    exit 2
fi
[ -d "$STATE_DIR" ] || { echo "state directory not found: $STATE_DIR" >&2; exit 2; }

if [ "$QUIESCE" = 1 ]; then
    systemctl stop "$SERVICE"
    STOPPED=1
elif [ "$QUIESCE" != 0 ]; then
    echo "LEGION_BACKUP_QUIESCE must be 0 or 1" >&2
    exit 2
fi

mkdir -p "$STAGE/state"
# Preserve links, modes, and sparse files while the service is stopped.
cp -a "$STATE_DIR/." "$STAGE/state/"
(
    cd "$STAGE/state"
    find . -type f -print0 | sort -z | xargs -0 sha256sum
) >"$STAGE/SHA256SUMS"
printf 'created_utc=%s\nsource=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$STATE_DIR" >"$STAGE/BACKUP-METADATA"

restic backup "$STAGE" --tag "$TAG" --host "${LEGION_BACKUP_HOST:-$(hostname)}"
echo "Legion restic backup completed"
