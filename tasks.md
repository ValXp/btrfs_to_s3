# Tasks: btrfs_to_s3 main tool

## Todo
### Task 1: Config + CLI + logging
- Define config schema and CLI flags in `DESIGN.md`.
- Implement config loader and validation.
- Implement CLI entrypoint and logging setup.
- Tests: unit tests for config validation and CLI arg parsing (>=90% where reasonable).

### Task 2: Locking + state storage
- Implement lock file with PID.
- Implement state file read/write under `~/.local/state/btrfs_to_s3/`.
- Tests: unit tests for lock contention and state serialization.

### Task 3: Snapshot manager
- Implement snapshot create and retention.
- Ensure last successful parent snapshot is retained.
- Tests: unit tests with mocked `btrfs` calls.

### Task 4: Planner
- Implement full vs incremental planner per subvolume.
- Implement fallback to full if parent missing.
- Tests: unit tests for cadence logic and fallback.

### Task 5: Streamer + chunker
- Implement `btrfs send` streaming and chunk split.
- Compute SHA-256 per chunk and track sizes.
- Tests: unit tests for chunk boundaries, hashing, and totals.

### Task 6: S3 uploader
- Implement multipart uploads with SSE-S3 and storage class controls.
- Retry policy: 5 attempts per part, exponential backoff with jitter.
- Tests: unit tests with stubbed S3 client for retries and error handling.

### Task 7: Manifest + pointer publish
- Implement manifest JSON and `current.json` publish.
- Ensure atomic publish semantics via final `PutObject`.
- Tests: unit tests for schema integrity and publish ordering.

### Task 8: Metrics output
- Emit total bytes, elapsed time, throughput.
- Tests: unit tests for metrics calculations.

### Task 9: Systemd + docs
- Add systemd service/timer units.
- Document setup and scheduling.

### Task 10: Integration harness
- Run existing test harness against AWS test bucket/prefix.
- Ensure end-to-end run reports success with STANDARD storage class.

## In progress

## Done
