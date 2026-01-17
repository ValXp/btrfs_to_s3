# Tasks: btrfs_to_s3 main tool

## Todo
### Global requirement (applies to all tasks)
- A task is only complete if the project builds and unit tests run and pass
  (use `python3`). No exceptions.

### Task 0: Repository scaffolding
- Define project package layout (e.g., `btrfs_to_s3/` package, `tests/`).
- Add `pyproject.toml` or `setup.cfg` with dependencies and test config.
- Document how to run tests and where to add new modules/tests.
- Acceptance criteria:
  - Running tests via `python3 -m pytest` succeeds (even if only placeholder tests).
  - The package can be imported with `python3 -m btrfs_to_s3` (stub ok).
  - Docs include the expected module/test locations.

### Task 1: Config + CLI + logging
- Define config schema and CLI flags in `DESIGN.md`.
- Implement config loader and validation.
- Implement CLI entrypoint and logging setup.
- Tests: unit tests for config validation and CLI arg parsing.
- Acceptance criteria:
  - Config validation rejects invalid paths, chunk sizes, and cadence.
  - CLI `backup` command parses flags and config correctly.
  - Logging level and structured messages are wired.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 2: Locking + state storage
- Implement lock file with PID.
- Implement state file read/write under `~/.local/state/btrfs_to_s3/`.
- Tests: unit tests for lock contention and state serialization.
- Acceptance criteria:
  - Lock prevents concurrent runs and reports PID.
  - State file persists last snapshot/manifest per subvolume.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 3: Snapshot manager
- Implement snapshot create and retention.
- Ensure last successful parent snapshot is retained.
- Tests: unit tests with mocked `btrfs` calls.
- Acceptance criteria:
  - Snapshot naming matches design and is deterministic.
  - Retention keeps last successful parent and prunes extras.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 4: Planner
- Implement full vs incremental planner per subvolume.
- Implement fallback to full if parent missing.
- Tests: unit tests for cadence logic and fallback.
- Acceptance criteria:
  - Full cadence honored; incrementals chained per subvolume.
  - Missing parent triggers full fallback.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 5: Streamer + chunker
- Implement `btrfs send` streaming and chunk split.
- Compute SHA-256 per chunk and track sizes.
- Tests: unit tests for chunk boundaries, hashing, and totals.
- Acceptance criteria:
  - Chunk sizes respect target size; final chunk allowed smaller.
  - SHA-256 hashes match input stream.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 6: S3 uploader
- Implement multipart uploads with SSE-S3 and storage class controls.
- Retry policy: 5 attempts per part, exponential backoff with jitter.
- Tests: unit tests with stubbed S3 client for retries and error handling.
- Acceptance criteria:
  - Uses multipart upload for large chunks and SSE-S3 on all objects.
  - Retries transient failures per policy; aborts on exhaustion.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 7: Manifest + pointer publish
- Implement manifest JSON and `current.json` publish.
- Ensure atomic publish semantics via final `PutObject`.
- Tests: unit tests for schema integrity and publish ordering.
- Acceptance criteria:
  - Manifest schema matches `DESIGN.md` and includes chunk list.
  - `current.json` only updated after successful manifest upload.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 8: Metrics output
- Emit total bytes, elapsed time, throughput.
- Tests: unit tests for metrics calculations.
- Acceptance criteria:
  - Metrics report is correct for known inputs.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 9: Systemd + docs
- Add systemd service/timer units.
- Document setup and scheduling.
- Acceptance criteria:
  - Units reference config path and run `backup` at 2am local time.
  - Docs include installation, enablement, and log locations.

### Task 10: Restore core
- Implement restore command and manifest resolution.
- Download chunks, verify hashes, and reassemble stream.
- `btrfs receive` into a new subvolume target path.
- Handle storage class restore requests and waiting.
- Tests: unit tests for manifest chain resolution and restore readiness.
- Acceptance criteria:
  - CLI `restore` accepts `--subvolume` + `--target` (required) and optional
    `--manifest-key` (overrides `current.json` lookup).
  - Manifest resolution for incrementals walks parents back to the most recent
    full and restores in order; failures list the missing manifest key.
  - For archival classes, the tool issues `RestoreObject` and waits until
    `Restore` header indicates readiness (poll with backoff).
  - `--restore-timeout` enforces a hard upper bound and exits non-zero with a
    clear error if readiness is not reached.
  - Chunk downloads verify SHA-256 against manifest and fail fast on mismatch.
  - `btrfs receive` writes to a brand-new subvolume path and fails if target
    already exists.
  - Unit tests cover:
    - manifest chain resolution ordering
    - timeout handling
    - restore readiness parsing for multiple storage classes
    - hash mismatch failure
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 11: Restore verification
- Implement metadata and content validation after restore.
- Tests: unit tests for verification logic and failure reporting.
- Acceptance criteria:
  - Metadata checks verify restored subvolume exists, is writable as expected,
    and reports a valid UUID from `btrfs subvolume show`.
  - Content verification mode `full` walks the entire tree and validates file
    sizes + SHA-256 for every regular file.
  - Content verification mode `sample` selects a deterministic sample set
    (stable ordering, capped by config) and validates sizes + SHA-256.
  - Verification detects mismatched hashes, missing files, or extra files and
    reports the first discrepancy with the offending path.
  - Verification can be disabled via config/flag and reports that it was skipped.
  - Unit test coverage >= 90% where reasonable.
  - All tests pass (including existing tests).

### Task 12: Integration harness
- Run existing test harness against AWS test bucket/prefix.
- Ensure end-to-end backup + restore run reports success with STANDARD storage class.
- Acceptance criteria:
  - Harness run completes without errors.
  - Artifacts uploaded to test prefix with STANDARD storage class.
  - Restore validation passes for full and incremental runs.
  - Use `testing/config/test.toml` for bucket/prefix and `testing/config/test.env`
    for credentials.
  - Execute via `python3 testing/scripts/run_all.py --config testing/config/test.toml`.
