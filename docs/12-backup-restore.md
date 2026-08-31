# Backup and restore

Legion supports backend-neutral off-cluster backups. The first production backend is restic, which can target local/removable disks, SFTP, a rest-server, and S3-compatible object storage.

## Consistency model

Do not copy live SQLite, WAL, fjall, or Raft files. `contrib/backup/legion-backup-restic.sh` stops `legion.service`, stages the complete state directory, writes a SHA-256 manifest, invokes restic, and restarts the service even if backup fails. This brief quiesce is conservative and works for both single-node SQLite and distributed state. Schedule nodes separately so a Raft quorum remains available.

Set `LEGION_BACKUP_QUIESCE=0` only when the input is already a database-consistent offline snapshot, as in the automated drill.

## Backup

Configure credentials outside the repository, preferably with systemd credentials or a root-only environment file:

```sh
export RESTIC_REPOSITORY='sftp:backup@host:/srv/restic/legion'
export RESTIC_PASSWORD_FILE='/run/credentials/legion-backup/restic-password'
sudo -E contrib/backup/legion-backup-restic.sh
```

Use restic retention and integrity checks from the scheduler, for example:

```sh
restic forget --tag legion --keep-daily 7 --keep-weekly 5 --keep-monthly 12 --prune
restic check
```

The complete Legion state directory is included, including function/blob data materialized beneath it. If deployments use an external blob store, back it up independently and test it during restore.

## Clean-node restore

1. Install the same or a schema-compatible Legion version, but do not start it.
2. Configure the restic repository and password.
3. Ensure the destination state directory is empty.
4. Restore and verify:

```sh
export LEGION_STATE_DIR=/var/lib/legion
sudo -E contrib/backup/legion-restore-restic.sh
sudo chown -R legion:legion /var/lib/legion
sudo systemctl start legion
```

The restore script runs `restic check`, restores the selected snapshot/tag, validates every file against `SHA256SUMS`, and refuses to overwrite a non-empty destination. Set `LEGION_BACKUP_SNAPSHOT` to a snapshot ID instead of `latest` for point-in-time recovery.

Run `make backup-restore-drill` after changing these scripts. The drill creates an encrypted local repository, backs up representative database/Raft/blob files, restores into a clean directory, verifies checksums, and compares both trees.
