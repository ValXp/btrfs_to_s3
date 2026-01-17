# Design: btrfs_to_s3

## Overview
`btrfs_to_s3` is a single-host backup tool that creates crash-consistent Btrfs
snapshots, streams `btrfs send` output into fixed-size chunks, uploads those
chunks to S3, and publishes a manifest plus a "current" pointer per subvolume.
The tool supports full backups (infrequent) and chained incrementals (weekly).

## Goals
- Crash-consistent backups for `data`, `root`, `home` subvolumes.
- Full backups (cadence configurable, target ~6 months) and chained incrementals.
- Large object uploads (default ~200 GiB chunks), integrity via per-chunk hash.
- Resilient to interruption; re-runs should succeed without manual cleanup.
- Clear logs/exit codes suitable for systemd/cron; no overlapping runs.
- Systemd timer/service for scheduled runs.

## Non-goals (initial phase)
- Restore implementation (schema and layout only).
- Multi-host support.
- Application-consistent snapshots.

## Assumptions
- All subvolumes live on the same Btrfs filesystem.
- AWS S3 is the only object store.
- Btrfs tooling is available (`btrfs send/receive`, `btrfs subvolume`).

## Architecture
1. **CLI/Config**
   - Parse TOML config; allow env overrides.
   - Primary command: `backup`.
2. **Lock**
   - File lock to prevent concurrent runs.
3. **Snapshot manager**
   - Create read-only snapshots for each subvolume.
   - Retain last successful snapshot for incremental parent.
4. **Planner**
   - Decide full vs incremental per subvolume based on cadence and state.
5. **Streamer/Chunker**
   - Run `btrfs send` and split the stream into fixed-size chunks.
   - Compute per-chunk SHA-256 and track bytes.
6. **Uploader**
   - Upload chunks to S3 with SSE-S3 and configurable storage class.
   - Record ETag/size/hash.
7. **Manifest/Pointer**
   - Write manifest JSON describing stream and chunk list.
   - Publish manifest, then update `current.json` per subvolume atomically.
8. **State**
   - Local state file for last successful snapshot/manifest per subvolume.
9. **Metrics**
   - Emit total bytes, throughput, and elapsed time.

## Data flow per run
1. Acquire lock.
2. Load config and state.
3. For each subvolume: create snapshot.
4. Plan full vs incremental per subvolume.
5. For each subvolume:
   - Stream `btrfs send` (with `-p` for incrementals).
   - Chunk stream and upload chunks.
   - Build and upload manifest.
   - Update `current.json` for subvolume.
6. Update local state after success.
7. Retain/prune snapshots.

## Snapshot naming
- Base directory: configurable (default under the same filesystem).
- Name pattern: `<subvol>__<timestamp>__<kind>`
  - `timestamp` in UTC: `YYYYMMDDTHHMMSSZ`.
  - `kind`: `full` or `inc` for clarity.

## S3 layout
- Base prefix: `<prefix>/` (configurable).
- Subvolume path: `subvol/<name>/`.
- Full run: `subvol/<name>/full/<timestamp>/`.
- Incremental run: `subvol/<name>/inc/<timestamp>/`.
- Chunks: `.../chunks/part-00000.bin` etc.
- Manifest: `.../manifest.json`.
- Pointer: `subvol/<name>/current.json`.

## Manifest schema (JSON)
```
{
  "version": 1,
  "subvolume": "data",
  "kind": "full",
  "created_at": "2025-01-01T00:00:00Z",
  "snapshot": {
    "name": "data__20250101T000000Z__full",
    "path": "/srv/snapshots/data__20250101T000000Z__full",
    "uuid": "optional",
    "parent_uuid": "optional"
  },
  "parent_manifest": null,
  "chunks": [
    {
      "key": "subvol/data/full/20250101T000000Z/chunks/part-00000.bin",
      "size": 214748364800,
      "sha256": "...",
      "etag": "..."
    }
  ],
  "total_bytes": 214748364800,
  "chunk_size": 214748364800,
  "s3": {
    "bucket": "bucket-name",
    "region": "us-east-1",
    "storage_class": "DEEP_ARCHIVE"
  }
}
```

## Current pointer schema (JSON)
```
{
  "manifest_key": "subvol/data/full/20250101T000000Z/manifest.json",
  "kind": "full",
  "created_at": "2025-01-01T00:00:00Z"
}
```

## Local state
- Path: configurable, default under user home (e.g. `~/.local/state/btrfs_to_s3/state.json`).
- Structure:
  - `subvolumes.<name>.last_snapshot`
  - `subvolumes.<name>.last_manifest`
  - `subvolumes.<name>.last_full_at`
  - `last_run_at`

## Reliability and failure handling
- **No partial publish:** only write `current.json` after all chunks + manifest
  uploads succeed.
- **Retry:** retry transient S3 failures; restart upload from scratch on
  persistent errors.
- **Idempotency:** if a run fails mid-stream, re-run creates a new timestamped
  path and does not overwrite previous artifacts.
- **Locking:** lock file prevents overlaps; includes PID for debug.
- **Snapshots:** incremental uses last successful snapshot; if missing, fall back
  to full for that subvolume.

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
- Flags: `--log-level`, `--dry-run`, `--subvolume` (optional filter),
  `--once` (ignore schedule), `--no-s3` (local only for diagnostics).

## Config (TOML, proposed)
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
```

Validation rules:
- Paths must be absolute (after `~` expansion).
- `chunk_size_bytes`, `spool_size_bytes`, and cadence days must be > 0.
- `run_at` uses 24-hour `HH:MM` format.
- `s3.bucket`, `s3.region`, `s3.prefix` are required.

## Systemd
- Service: `btrfs_to_s3.service` running `backup`.
- Timer: `btrfs_to_s3.timer` scheduled at 2am local time.

## Decisions (confirmed)
- **S3 atomic publish:** no bucket versioning; publish `manifest.json` then do a
  final `PutObject` overwrite for `current.json` (S3 object puts are atomic).
- **Chunk upload method:** multipart uploads for large chunks to maximize speed.
- **State path:** default to user home under `~/.local/state/btrfs_to_s3/`.
- **Spooling:** use a local SSD spool (default 200 GiB) to enable high parallel
  multipart throughput.

## Pending decisions (need user input)
None.

## Test storage default
- Use `STANDARD` for test runs to avoid minimum storage duration and retrieval
  fees.
