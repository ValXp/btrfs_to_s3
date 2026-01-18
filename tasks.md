# Tasks: btrfs_to_s3 follow-up fixes

## Todo
### Global requirement (applies to all tasks)
- A task is only complete if `python3 -m pytest` passes. If new tests are added,
  they must pass locally.

### Task 1: Streamed chunking + multipart limits
- Scope: `btrfs_to_s3/chunker.py`, `btrfs_to_s3/uploader.py`, `btrfs_to_s3/cli.py`.
- Replace in-memory chunk buffering with streaming chunk hashing and upload.
- Enforce S3 multipart part-size limits (<= 5 GiB per part) while still honoring
  large chunk targets (e.g., 200 GiB) by streaming parts.
- Acceptance criteria:
  - Chunking does not read an entire 200 GiB chunk into RAM.
  - Multipart upload uses part sizes within AWS limits and succeeds for large
    chunk sizes.
  - Unit tests cover streaming chunk hashing and multipart part boundaries.

### Task 2: Restore stream without full-buffer reads
- Scope: `btrfs_to_s3/restore.py`.
- Stream chunk downloads directly into `btrfs receive` without loading full
  chunks into memory.
- Preserve SHA-256 verification while streaming.
- Acceptance criteria:
  - Restores do not buffer entire chunks in memory.
  - Hash verification still detects corruption.
  - Unit tests cover streaming verification behavior.

### Task 3: Planner + schedule integration
- Scope: `btrfs_to_s3/cli.py`, `btrfs_to_s3/planner.py`, `btrfs_to_s3/config.py`.
- Use the planner to choose full vs incremental based on schedule config instead
  of relying on `BTRFS_TO_S3_BACKUP_TYPE`.
- Only run backups when they are due unless `--once` is set.
- Acceptance criteria:
  - Weekly incrementals and 6-month full cadence are respected via config.
  - `--once` forces a run even if not due.
  - Unit tests cover schedule decisions in CLI flow.

### Task 4: Enforce lock during backups
- Scope: `btrfs_to_s3/cli.py`, `btrfs_to_s3/lock.py`.
- Acquire the lock path from config at the start of `backup` and release on exit.
- Ensure lock errors are surfaced with a non-zero exit.
- Acceptance criteria:
  - Concurrent runs are prevented with a clear error.
  - Lock is always released on success/failure.
  - Unit tests cover lock acquisition and contention in CLI flow.

### Task 5: Incremental fallback when parent snapshot missing
- Scope: `btrfs_to_s3/cli.py`, `btrfs_to_s3/planner.py`.
- If the last snapshot path is missing on disk, automatically fall back to a
  full backup for that subvolume.
- Acceptance criteria:
  - Missing parent snapshot triggers full backup without `btrfs send -p` failure.
  - Unit tests cover parent-missing fallback.

### Task 6: Restore verification without local snapshot dependency
- Scope: `btrfs_to_s3/restore.py`, `btrfs_to_s3/manifest.py`.
- Make verification optional or use manifest-only checks when the source
  snapshot path is unavailable on the restore host.
- Acceptance criteria:
  - Restores can complete on a different host without local snapshot paths.
  - Verification mode handles missing snapshot paths gracefully.
  - Unit tests cover verification behavior with missing snapshot paths.

### Task 7: Upload concurrency and spool usage
- Scope: `btrfs_to_s3/uploader.py`, `btrfs_to_s3/cli.py`, `btrfs_to_s3/config.py`.
- Use `s3.concurrency` to upload multiple parts/chunks concurrently.
- Implement optional spool usage (write stream to disk when enabled) while
  preserving streaming as the default path.
- Acceptance criteria:
  - Concurrency setting is honored and improves throughput.
  - Spool path and size limits are respected when enabled.
  - Unit tests cover concurrency configuration and spool behavior.

### Task 8: Lock stale PID handling for crash recovery
- Scope: `btrfs_to_s3/lock.py`, `btrfs_to_s3/cli.py`.
- Detect stale lock files and recover safely after crashes.
- Acceptance criteria:
  - Lock acquisition checks whether recorded PID is still running.
  - Stale lock files are removed automatically so reruns succeed.
  - Unit tests cover stale lock detection and recovery.

### Task 9: Incremental manifest parent validation
- Scope: `btrfs_to_s3/planner.py`, `btrfs_to_s3/cli.py`.
- Ensure incrementals are only planned/executed when a valid parent manifest is available.
- Acceptance criteria:
  - Missing `last_manifest` forces a full backup (not an incremental).
  - Manifest chains always resolve to a full backup.
  - Unit tests cover missing parent manifest fallback.
