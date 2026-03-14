# Design: btrfs_to_s3

## Overview
`btrfs_to_s3` is a single-host backup tool for Btrfs and ZFS. It creates
backend-specific readonly snapshots, streams the native send output into
logical chunks, uploads those chunks to S3, and publishes a manifest plus a
`current.json` pointer per source. The project name is historical; the current
config and orchestrators are backend-aware.

## Goals
- Crash-consistent backups for local filesystem sources.
- Full backups (cadence configurable, target about 6 months) and chained incrementals.
- Large object uploads (default ~200 GiB chunks), integrity via per-chunk hash.
- Resilient to interruption; re-runs should succeed without manual cleanup.
- Clear logs/exit codes suitable for systemd/cron; no overlapping runs.
- Systemd timer/service for scheduled runs.
- Backward compatibility for existing Btrfs config, manifest, and state files.

## Non-goals
- Multi-host support.
- Application-consistent snapshots.

## Assumptions
- AWS S3 is the only object store.
- The selected backend toolchain is available on the host:
  - Btrfs: `btrfs send/receive`, `btrfs subvolume`, `btrfs property`
  - ZFS: `zfs send/receive`, `zfs snapshot`, `zfs get`
- Restore may need to wait for archive-class S3 objects to become readable.

## Architecture
1. **CLI/Config**
   - Parse TOML config with `[filesystem].backend = "btrfs"` or `"zfs"`.
   - Keep legacy Btrfs configs valid when `[filesystem]` is omitted.
   - Primary commands: `backup`, `restore`.
   - `--source` is the backend-neutral selector; `--subvolume` remains a compatibility alias.
   - Internal planning/orchestration code uses `source` naming; serialized
     state, manifest fields, and S3 object keys keep the legacy
     `subvolumes`, `subvolume`, and `subvol/` names for backward compatibility.
2. **Lock**
   - File lock to prevent concurrent runs.
3. **Filesystem backend**
   - Enumerate configured backup sources.
   - Create/list/prune snapshots.
   - Open full or incremental send streams.
   - Receive, finalize, and verify restores.
4. **Planner**
   - Decide full vs incremental per source based on cadence and local state.
5. **Streamer/Chunker**
   - Run the selected backend's send command and split the stream into fixed-size chunks.
   - Compute per-chunk SHA-256 and track bytes.
6. **Uploader**
   - Upload chunks to S3 with SSE-S3 and configurable storage class.
   - Record ETag/size/hash.
7. **Manifest/Pointer**
   - Write manifest JSON describing the filesystem, snapshot identity, and chunk list.
   - Publish manifest, then update `current.json` per source atomically.
8. **State**
   - Local state file for last successful snapshot identity/manifest per source.
9. **Metrics**
   - Emit total bytes, throughput, and elapsed time.
10. **Restore**
   - Resolve manifest (current pointer or explicit manifest key).
   - Ensure objects are restored and ready for download (Glacier/Deep Archive).
   - Download chunks, validate hashes, reassemble stream.
   - Run the selected backend's receive/apply flow into the target path.
   - Validate restored data (backend metadata + file content hashes).

## Data flow per run
1. Acquire lock.
2. Load config and state.
3. Build the configured filesystem backend and enumerate sources.
4. For each source: create a readonly snapshot.
5. Plan full vs incremental per source.
6. For each source:
   - Stream the backend send output, using an incremental parent when available.
   - Chunk stream and upload chunks.
   - Build and upload manifest.
   - Update `current.json` for the source.
7. Update local state after success.
8. Retain/prune snapshots.

## Restore flow
1. Acquire lock.
2. Resolve manifest:
   - If `--manifest-key` provided, use it.
   - Else resolve `current.json` for the selected source.
3. For incremental chains, resolve parent manifests back to the most recent full.
4. For each manifest in order:
   - Ensure S3 objects are restored and ready (if archival).
   - Download chunks and validate SHA-256.
   - Reassemble stream and feed it into the backend restore operations.
5. Validate restored data:
   - Backend metadata checks.
   - File content checks (hash/size for a deterministic sample or full tree, configurable).

## Source and snapshot identity

Source identifiers:
- Btrfs sources are keyed by the basename of each path in `subvolumes.paths`.
- ZFS sources are keyed by dataset name. In practice, manifests and current
  pointers use fully qualified dataset names such as `tank/data`.

Snapshot naming:
- Shared name pattern: `<source-token>__<timestamp>__<kind>`
- `timestamp` is always UTC in `YYYYMMDDTHHMMSSZ`.
- `kind` is `full` or `inc`.
- Btrfs uses the snapshot path itself as the backend identity.
- ZFS uses `dataset@<snapshot_prefix>-<source-token>__<timestamp>__<kind>` as
  the authoritative backend identity.

## S3 layout
- Base prefix: `<prefix>/` (configurable).
- Source path: `subvol/<identifier>/`.
- Full run manifests: `subvol/<identifier>/full/manifest-<timestamp>.json`.
- Incremental run manifests: `subvol/<identifier>/incremental/manifest-<timestamp>.json`.
- Chunks: `subvol/<identifier>/<kind>/chunk-<timestamp>-<index>.bin`.
- Pointer: `subvol/<identifier>/current.json`.

## Manifest schema (JSON)
```
{
  "version": 2,
  "filesystem": "zfs",
  "subvolume": "tank/data",
  "kind": "full",
  "created_at": "2025-01-01T00:00:00Z",
  "snapshot": {
    "name": "tank_x2f_data__20250101T000000Z__full",
    "path": null,
    "identity": "tank/data@btrfs-to-s3-tank_x2f_data__20250101T000000Z__full",
    "uuid": "optional",
    "parent_uuid": "optional"
  },
  "parent_manifest": null,
  "chunks": [
    {
      "key": "subvol/tank/data/full/chunk-20250101T000000Z-0.bin",
      "size": 214748364800,
      "sha256": "...",
      "etag": "..."
    }
  ],
  "total_bytes": 214748364800,
  "chunk_size": 214748364800,
  "s3": {
    "storage_class": "DEEP_ARCHIVE"
  }
}
```

Compatibility notes:
- Legacy manifests that omit `filesystem` still restore as Btrfs.
- `snapshot.identity` is authoritative for new manifests.
- For legacy Btrfs manifests, restore falls back to `snapshot.path`.

## Current pointer schema (JSON)
```
{
  "manifest_key": "subvol/data/full/manifest-20250101T000000Z.json",
  "kind": "full",
  "created_at": "2025-01-01T00:00:00Z"
}
```

## Local state
- Path: configurable, default under user home (e.g. `~/.local/state/btrfs_to_s3/state.json`).
- Structure:
  - `subvolumes.<name>.last_snapshot`
  - `subvolumes.<name>.last_snapshot_name`
  - `subvolumes.<name>.last_snapshot_path`
  - `subvolumes.<name>.last_manifest`
  - `subvolumes.<name>.last_full_at`
  - `last_run_at`

`last_snapshot` stores the backend-specific identity. For Btrfs that is usually
an absolute snapshot path; for ZFS it is the fully qualified `dataset@snapshot`
identity. `last_snapshot_name` and `last_snapshot_path` remain available for
planner compatibility and legacy state upgrades.

## Reliability and failure handling
- **No partial publish:** only write `current.json` after all chunks + manifest
  uploads succeed.
- **Retry:** retry transient S3 failures; restart upload from scratch on
  persistent errors.
- **Idempotency:** if a run fails mid-stream, re-run creates a new timestamped
  path and does not overwrite previous artifacts.
- **Locking:** lock file prevents overlaps; includes PID for debug.
- **Snapshots:** incremental uses the last successful snapshot identity; if the
  parent snapshot or manifest is missing, the plan falls back to full for that
  source.
- **Restore cleanup:** backend-specific restore code is responsible for
  deleting partial targets on failed receive operations.

## Performance
- Chunk size configurable (default 200 GiB).
- Upload concurrency configurable (thread or async worker pool).
- Use multipart uploads for large chunks to maximize throughput on fast uplinks.
- Use a local SSD spool (default 200 GiB) to buffer multipart parts and maximize
  parallel uploads.

## Multipart upload policy (default)
- Part size: 128 MiB.
- Max in-flight parts: `s3.concurrency` (defaults to 4).
- Retry: 5 attempts per part with exponential backoff (base 1s, cap 30s) and
  jitter.
- Failure: abort multipart upload on exhausted retries; rerun restarts the
  backup stream from scratch.

## Security
- Use SSE-S3 (AES256) for all uploaded objects.
- Avoid storing credentials in config; prefer env or AWS standard mechanisms.

## CLI
- `btrfs_to_s3 backup --config /path/to/config.toml`
- `btrfs_to_s3 restore --config /path/to/config.toml --source data --target /restore/data`
- Flags: `--log-level`, `--dry-run`, `--source` (optional filter for backup),
  `--subvolume` (compatibility alias),
  `--once` (ignore schedule), `--no-s3` (local only for diagnostics).
- Restore flags:
  - `--target` (required): restore target path.
  - `--manifest-key` (optional): explicit manifest to restore.
  - `--current` (default): restore from `current.json`.
  - `--wait-restore` (default on): request/await S3 restore for archival storage.
  - `--restore-timeout` (e.g., `6h`): max time to wait for S3 restore readiness.
  - `--verify` (default on): run metadata + content verification.

## Config shapes

Btrfs:
```
[global]
log_level = "info"
state_path = "~/.local/state/btrfs_to_s3/state.json"
lock_path = "/var/lock/btrfs_to_s3.lock"
spool_dir = "/mnt/ssd/btrfs_to_s3_spool"
spool_size_bytes = 214748364800

[schedule]
full_every_days = 180
incremental_every_days = 7
run_at = "02:00"

[snapshots]
base_dir = "/srv/snapshots"
retain = 2

[subvolumes]
paths = ["/srv/data/data", "/srv/data/root", "/srv/data/home"]

[s3]
bucket = "bucket"
region = "us-east-1"
prefix = "backup/data"
chunk_size_bytes = 214748364800
storage_class_chunks = "DEEP_ARCHIVE"
storage_class_manifest = "STANDARD"
concurrency = 4
sse = "AES256"

[restore]
target_base_dir = "/srv/restore"
verify_mode = "full" # full | sample | none
sample_max_files = 1000
wait_for_restore = true
restore_timeout_seconds = 259200
restore_tier = "Standard"
```

ZFS:
```
[global]
log_level = "info"
state_path = "~/.local/state/btrfs_to_s3/state.json"
lock_path = "/var/lock/btrfs_to_s3.lock"
spool_dir = "/mnt/ssd/btrfs_to_s3_spool"
spool_size_bytes = 214748364800

[schedule]
full_every_days = 180
incremental_every_days = 7
run_at = "02:00"

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
bucket = "bucket"
region = "us-east-1"
prefix = "backup/zfs"
chunk_size_bytes = 214748364800
storage_class_chunks = "DEEP_ARCHIVE"
storage_class_manifest = "STANDARD"
concurrency = 4
sse = "AES256"

[restore]
target_base_dir = "/tank/restore"
verify_mode = "full"
sample_max_files = 1000
wait_for_restore = true
restore_timeout_seconds = 259200
restore_tier = "Standard"
```

Validation rules:
- Paths must be absolute after `~` expansion.
- `chunk_size_bytes`, `spool_size_bytes`, and cadence days must be > 0.
- `run_at` uses 24-hour `HH:MM` format.
- `snapshots.base_dir` and `subvolumes.paths` are required for Btrfs.
- `[zfs]` is required for ZFS and must include `pool_name`, `mount_root`,
  `source_datasets`, `receive_parent_dataset`, and `snapshot_prefix`.
- `s3.bucket`, `s3.region`, and `s3.prefix` are always required.

## Backend-specific restore semantics

Btrfs:
- `restore.target_base_dir` is a default only.
- `btrfs receive` writes into `target.parent`, then the restored subvolume is
  renamed into the exact `--target` path.

ZFS:
- `restore.target_base_dir` is part of the mapping contract.
- The requested `--target` path must live under `restore.target_base_dir`.
- The relative path under that base becomes child datasets under
  `zfs.receive_parent_dataset`.
- Example: `receive_parent_dataset = "tank/restore"` and
  `target_base_dir = "/tank/restore"` means `--target /tank/restore/data/app`
  restores into dataset `tank/restore/data/app`.

## Systemd
- Service: `btrfs_to_s3.service` running `backup`.
- Timer: `btrfs_to_s3.timer` scheduled at 2am local time.
None.

## Test storage default
- Use `STANDARD` for test runs to avoid minimum storage duration and retrieval
  fees.
