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

### Task 10: Integration harness
- Run existing test harness against AWS test bucket/prefix.
- Ensure end-to-end run reports success with STANDARD storage class.
- Acceptance criteria:
  - Harness run completes without errors.
  - Artifacts uploaded to test prefix with STANDARD storage class.
  - Any required environment variables are documented.

## In progress

## Done
