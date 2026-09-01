#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
command -v restic >/dev/null || { echo "ERROR: restic is required" >&2; exit 1; }
if [[ "${1:-}" == "--isolated-drill" ]]; then
  exec "$ROOT/tests/integration/restic_restore_drill.sh"
fi
: "${RESTIC_REPOSITORY:?set RESTIC_REPOSITORY to an off-cluster repository}"
if [[ -z "${RESTIC_PASSWORD:-}" && -z "${RESTIC_PASSWORD_FILE:-}" ]]; then
  echo "ERROR: set RESTIC_PASSWORD or RESTIC_PASSWORD_FILE" >&2
  exit 1
fi
exec sudo --preserve-env=RESTIC_REPOSITORY,RESTIC_PASSWORD,RESTIC_PASSWORD_FILE \
  "$ROOT/contrib/backup/legion-backup-restic.sh"
