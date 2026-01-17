# Tasks: btrfs_to_s3 main tool

## Todo
- Finalize remaining design decisions (spooling, test storage class defaults).
- Define config schema + CLI flags (document in README or DESIGN.md).
- Implement config loader + validation; add unit tests (>=90% coverage where reasonable).
- Implement logging + exit codes; unit tests for error mapping.
- Implement lock file + state storage; unit tests for lock/state behaviors.
- Implement snapshot manager (create/retain); unit tests with mocked btrfs calls.
- Implement planner (full vs incremental); unit tests for cadence and fallback logic.
- Implement streamer/chunker + hashing; unit tests for chunk boundaries and hashes.
- Implement S3 uploader (SSE-S3, storage class, retries); unit tests with stubbed S3.
- Implement manifest + pointer publish; unit tests for schema integrity and atomic publish.
- Implement metrics output; unit tests for throughput calculations.
- Add systemd service/timer units and documentation.
- Integrate test harness end-to-end (requires AWS); verify run reports.

## In progress

## Done
