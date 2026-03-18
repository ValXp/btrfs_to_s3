# btrfs_to_s3

Backup tooling for Btrfs and ZFS snapshot/send streams to AWS S3. The project
name is historical; the current config and orchestration code support both
backends.

## Usage

`btrfs_to_s3` reads a TOML config file and supports backup, restore, and
S3-backed discovery commands:

```sh
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --source data --target /srv/restore/data
python3 -m btrfs_to_s3 list-sources --config /etc/btrfs_to_s3/config.toml
python3 -m btrfs_to_s3 list-manifests --config /etc/btrfs_to_s3/config.toml --source data
```

`--source` is the backend-neutral flag. `--subvolume` is still accepted as a
compatibility alias.

Source identifiers are backend-specific:
- Btrfs uses the basename of each `subvolumes.paths` entry, such as `data`.
- ZFS uses the dataset identifier recorded in manifests and current pointers,
  such as `tank/data`.

The internal planner and orchestrator use backend-neutral "source" naming.
Persisted state files, manifest JSON, and S3 object keys still keep the legacy
`subvolumes`, `subvolume`, and `subvol/` names so existing backups remain
compatible.

### Manual runs

Manual runs are useful when you want to:
- Run an out-of-band backup (e.g., right before a risky upgrade).
- Force a backup even if it is not due per schedule.
- Restrict a backup to specific configured sources.
- Skip uploads to S3 to validate snapshot creation locally.

Examples:

```sh
# Plan only (skip S3 upload entirely).
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --dry-run

# Force a run regardless of schedule.
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --once

# Back up only specific Btrfs sources (repeatable).
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --source data --source root

# Back up only a specific ZFS dataset.
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --source tank/data

# Validate snapshot creation without uploading to S3.
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --no-s3
```

Restores can override manifest selection and verification:

```sh
# Restore the current manifest chain for a Btrfs source.
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --source data --target /srv/restore/data

# Restore a ZFS dataset into a target under restore.target_base_dir.
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --source tank/data --target /tank/restore/data

# Restore from a specific manifest key and skip verification.
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --source data --target /srv/restore/data \
  --manifest-key subvol/data/full/manifest-20260101T000000Z.json --verify none

# Wait at most six hours for archive restores before failing.
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --source data --target /srv/restore/data \
  --restore-timeout 6h
```

`--restore-timeout` accepts raw integer seconds for backward compatibility and
duration strings such as `30m`, `6h`, or `2d`.

Discovery commands inspect S3 directly and print JSON to stdout:

```sh
# List restorable sources discovered from current.json pointers under s3.prefix.
python3 -m btrfs_to_s3 list-sources --config /etc/btrfs_to_s3/config.toml

# List available manifests for a chosen source, newest first.
python3 -m btrfs_to_s3 list-manifests --config /etc/btrfs_to_s3/config.toml --source tank/data
```

When restore verification is enabled, the application always validates the
restored target's backend metadata first. Content verification then compares
the restored tree against a local source snapshot view. For Btrfs that usually
comes from `snapshot.path`. For ZFS the application first tries
`.zfs/snapshot/<name>` from the manifest's `snapshot.identity`; if that view is
unavailable, it creates a temporary clone of the source snapshot under
`zfs.receive_parent_dataset`, verifies against that clone, and removes it
afterward.

AWS credentials are resolved via boto3's standard provider chain, including env vars, shared credentials files, profiles, IAM roles, web identity, and ECS/EC2 metadata.

## Configuration

`btrfs_to_s3` expects an absolute path to `config.toml`. All configured paths
must be absolute after `~` expansion.

Backend selection:
- Recommended: set `[filesystem].backend = "btrfs"` or `"zfs"`.
- Backward compatibility: if `[filesystem]` is omitted, the loader treats the
  config as legacy Btrfs and requires `snapshots.base_dir` plus
  `subvolumes.paths`.

Required fields by backend:
- Shared: `s3.bucket`, `s3.region`, `s3.prefix`
- Btrfs: `snapshots.base_dir`, `subvolumes.paths`
- ZFS: `[zfs] pool_name`, `mount_root`, `source_datasets`,
  `receive_parent_dataset`, `snapshot_prefix`

Example Btrfs `config.toml`:

```toml
[global]
log_level = "info" # debug|info|warning|error|critical
state_path = "/var/lib/btrfs_to_s3/state.json"
lock_path = "/var/lock/btrfs_to_s3.lock"
spool_dir = "/mnt/ssd/btrfs_to_s3_spool"
spool_size_bytes = 214748364800

[schedule]
full_every_days = 180
incremental_every_days = 7
run_at = "02:00" # host-local time; the bundled timer also defaults to 02:00

[snapshots]
base_dir = "/srv/snapshots"
retain = 2

[subvolumes]
paths = ["/srv/data", "/srv/home"]

[s3]
bucket = "my-backup-bucket"
region = "us-east-1"
prefix = "btrfs/host-01"
chunk_size_bytes = 214748364800
storage_class_chunks = "DEEP_ARCHIVE"
storage_class_manifest = "STANDARD"
concurrency = 4
spool_enabled = false
sse = "AES256"

[restore]
target_base_dir = "/srv/restore"
verify_mode = "full" # full|sample|none
sample_max_files = 1000
wait_for_restore = true
restore_timeout_seconds = 259200
restore_tier = "Standard"
```

Equivalent ZFS shape:

```toml
[global]
log_level = "info"
state_path = "/var/lib/btrfs_to_s3/state.json"
lock_path = "/var/lock/btrfs_to_s3.lock"
spool_dir = "/mnt/ssd/btrfs_to_s3_spool"
spool_size_bytes = 214748364800

[schedule]
full_every_days = 180
incremental_every_days = 7
run_at = "02:00" # host-local time; the bundled timer also defaults to 02:00

[filesystem]
backend = "zfs"

[snapshots]
retain = 2

[zfs]
pool_name = "tank"
mount_root = "/tank"
source_datasets = ["tank/data", "tank/home"]
receive_parent_dataset = "tank/restore"
snapshot_prefix = "btrfs-to-s3"

[s3]
bucket = "my-backup-bucket"
region = "us-east-1"
prefix = "zfs/host-01"
chunk_size_bytes = 214748364800
storage_class_chunks = "DEEP_ARCHIVE"
storage_class_manifest = "STANDARD"
concurrency = 4
spool_enabled = false
sse = "AES256"

[restore]
target_base_dir = "/tank/restore"
verify_mode = "full"
sample_max_files = 1000
wait_for_restore = true
restore_timeout_seconds = 259200
restore_tier = "Standard"
```

`zfs.receive_parent_dataset` can be omitted for backup-only ZFS configs. Keep
it set when you plan to restore on that host or want clone-based ZFS content
verification fallback.

You can copy `config.example.toml` as a starting point.

### Configuration reference

`filesystem`:
- `filesystem.backend`: optional; `btrfs` or `zfs`; defaults to legacy `btrfs`
  behavior when omitted.

`global`:
- `global.log_level`: default `info`; one of `debug|info|warning|error|critical`; can be overridden by `--log-level`.
- `global.state_path`: default `~/.local/state/btrfs_to_s3/state.json`; absolute path to local state tracking last runs and manifests.
- `global.lock_path`: default `/var/lock/btrfs_to_s3.lock`; absolute path for the backup lock file to prevent concurrent runs.
- `global.spool_dir`: default `/mnt/ssd/btrfs_to_s3_spool`; absolute path for multipart spooling when `s3.spool_enabled` is true.
- `global.spool_size_bytes`: default `214748364800` (200 GiB); must be > 0; caps total on-disk spool usage and limits effective multipart concurrency when spooling (`min(s3.concurrency, spool_size_bytes / part_size)`); must be >= 5 MiB to avoid multipart spool errors.

`schedule`:
- `schedule.full_every_days`: default `180`; must be > 0; number of days between full backups.
- `schedule.incremental_every_days`: default `7`; must be > 0; number of days between incremental backups.
- `schedule.run_at`: default `02:00`; 24-hour `HH:MM` host-local time used to decide if a run is due. The bundled systemd timer also defaults to `02:00`; if you customize `run_at`, customize the timer to match.

`snapshots`:
- `snapshots.base_dir`: default `/srv/snapshots`; required for Btrfs; absolute directory where local readonly snapshots are created.
- `snapshots.retain`: default `2`; must be >= 1; number of snapshots kept per source (parent snapshots are preserved while needed by incrementals).

`subvolumes`:
- `subvolumes.paths`: required for Btrfs; list of absolute source subvolume paths to back up.

`zfs`:
- `zfs.pool_name`: required when `filesystem.backend = "zfs"`; pool name used to qualify datasets.
- `zfs.mount_root`: required for ZFS; absolute mount root for source datasets and restore target mapping.
- `zfs.source_datasets`: required for ZFS; list of dataset names to back up. Use fully qualified names such as `tank/data` to keep CLI `--source` values obvious.
- `zfs.receive_parent_dataset`: optional for backup-only ZFS configs;
  dataset under which restores are received and temporary verification clones
  are created. Restore flows that need it fail clearly when it is not
  configured.
- `zfs.snapshot_prefix`: required for ZFS; prefix applied to generated snapshot names.

`s3`:
- `s3.bucket`: required; S3 bucket name for manifests and chunks.
- `s3.region`: required; AWS region for the S3 client.
- `s3.prefix`: required; prefix inside the bucket used as the root for backup objects.
- `s3.chunk_size_bytes`: default `214748364800` (200 GiB); must be > 0; logical chunk size for send streams. Multipart uploads use 128 MiB parts by default, independent of chunk size.
- `s3.storage_class_chunks`: default `DEEP_ARCHIVE`; storage class for chunk objects (archive classes may require restores).
- `s3.storage_class_manifest`: default `STANDARD`; storage class for manifest/current objects.
- `s3.concurrency`: default `4`; must be >= 1; number of multipart part uploads in flight (further capped by spooling limits).
- `s3.spool_enabled`: default `false`; must be a TOML boolean (`true` or `false`). When true, multipart parts are spooled to disk under `global.spool_dir` instead of kept in memory.
- `s3.sse`: default `AES256`; server-side encryption setting sent to S3.

`restore`:
- `restore.target_base_dir`: default `/srv/restore`; absolute base directory for restore targets when not overridden by tooling.
- `restore.verify_mode`: default `full`; one of `full|sample|none`; controls post-restore verification. Metadata checks always run unless set to `none`. ZFS content verification may create and destroy a temporary clone when `.zfs/snapshot/<name>` is unavailable on the restore host.
- `restore.sample_max_files`: default `1000`; must be > 0; maximum files hashed in `sample` mode.
- `restore.wait_for_restore`: default `true`; must be a TOML boolean (`true` or `false`). Wait for archive-class restores to become available before downloading.
- `restore.restore_timeout_seconds`: default `259200` (72 hours); must be > 0; timeout while waiting for archive restores.
- `restore.restore_tier`: default `Standard`; restore tier used for archival storage classes.

### Restore semantics

- Btrfs restores receive into `target.parent` and then rename the received
  subvolume into the exact `--target` path. Metadata verification uses
  `btrfs subvolume show`.
- ZFS restores must target a path under `restore.target_base_dir`. The relative
  path beneath that base is mapped onto child datasets of
  `zfs.receive_parent_dataset`. The mapped target dataset must not already
  exist, even if it is currently unmounted or mounted somewhere else, because
  restores will not overwrite an existing dataset. After receive, the restored
  dataset is made writable and verified with `zfs get`.
  `zfs.receive_parent_dataset` is therefore required for ZFS restores even
  though backup-only configs may omit it.

### Verification semantics

- Restore always validates downloaded chunk hashes against the manifest before
  feeding bytes into the backend receive path.
- Restore always validates backend metadata after receive unless
  `restore.verify_mode = "none"`.
- Restore validates file content only when it can resolve a source snapshot
  path locally.
- For ZFS, the application first tries `.zfs/snapshot/<snapshot-name>` as that
  source path. If the path is not available, it creates a temporary clone under
  `zfs.receive_parent_dataset`, verifies against the clone, then destroys it.
  That clone fallback is unavailable when `zfs.receive_parent_dataset` is not
  configured.
- The integration harness's
  [verify_restore.py](/root/btrfs_to_s3/integration_tests/scripts/verify_restore.py)
  uses the same ZFS strategy during harness verification.

### Manifest and state compatibility

- New manifests are version `2` and record `filesystem` plus the authoritative
  `snapshot.identity`. Btrfs manifests also keep `snapshot.path`.
- Restore still accepts legacy manifests that omit `filesystem` and treats them
  as Btrfs. If `snapshot.identity` is missing, restore falls back to
  `snapshot.path`.
- Local state now stores the backend snapshot identity in `last_snapshot` and
  may also keep `last_snapshot_name` and `last_snapshot_path` for planner and
  compatibility purposes.

## Development

- Package code lives in `btrfs_to_s3/`.
- Unit tests live in `tests/`.

Run tests:

```sh
python3 -m pytest
```

Run the package entrypoint stub:

```sh
python3 -m btrfs_to_s3
```

## Systemd setup

1. Install unit files:

```sh
sudo cp systemd/btrfs_to_s3.service /etc/systemd/system/btrfs_to_s3.service
sudo cp systemd/btrfs_to_s3.timer /etc/systemd/system/btrfs_to_s3.timer
```

2. Ensure `/etc/btrfs_to_s3/config.toml` exists and matches your backend and
   host paths.
3. Enable the timer:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs_to_s3.timer
```

Logs live in the systemd journal:

```sh
journalctl -u btrfs_to_s3.service
```

Manual systemd run:

```sh
sudo systemctl start btrfs_to_s3.service
```

If you need a one-off run with different flags or a different config, run the
CLI directly instead of systemd (see "Manual runs" above).
