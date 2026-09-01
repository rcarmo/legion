# Backup drill

Uses Legion's production restic workflow; it never copies live SQLite, WAL, fjall, or Raft files blindly.

Run the safe isolated roundtrip used by CI:

```sh
examples/backup-drill/run.sh --isolated-drill
```

Capture the installed node to an off-cluster repository:

```sh
export RESTIC_REPOSITORY=sftp:backup-host:/srv/restic/legion
export RESTIC_PASSWORD_FILE=/run/secrets/legion-restic
examples/backup-drill/run.sh
```

The production path requires root because it quiesces `legion.service`. See `docs/12-backup-restore.md` before restoring.
