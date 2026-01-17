# Implementation Plan: btrfs_to_s3 main tool

## Scope
Build the main backup tool that manages Btrfs snapshots, streams `btrfs send`
output into chunked uploads on S3, and records manifests to enable future
restore. This plan excludes the test harness (already implemented).

## Inputs
- Requirements: `requirements.md`
- Design: `DESIGN.md`

## Milestones
1. Configuration + CLI skeleton + logging.
2. Locking + state storage.
3. Snapshot manager + retention.
4. Planner (full vs incremental per subvolume).
5. Streamer/chunker + hashing.
6. S3 uploader with storage class + SSE-S3.
7. Manifest + pointer publishing (atomic).
8. Systemd units + docs.
9. Metrics/benchmarking output.
10. Tests: unit coverage >= 90% where reasonable + harness integration.

## Detailed steps
1. Define config schema (TOML) and CLI surface.
   - Add validation for paths, chunk size, cadence.
   - Add env overrides and defaults.
2. Implement logging + exit codes suitable for systemd/cron.
3. Implement lock and state storage.
   - Lock file with PID; state file with last successful snapshot/manifest.
4. Implement snapshot manager.
   - Create read-only snapshots with deterministic naming.
   - Retain last N snapshots and keep last successful parent.
5. Implement planner.
   - Full due vs incremental; per-subvolume fallback to full if parent missing.
6. Implement streamer/chunker.
   - Run `btrfs send`; split into chunks.
   - Hash each chunk; collect size and stats.
7. Implement S3 uploader.
   - Upload chunks with SSE-S3 and storage class controls.
   - Multipart policy: 128 MiB parts, 5 retries, exponential backoff with jitter.
8. Implement manifest builder and publisher.
   - Upload manifest after chunk success.
   - Update `current.json` via chosen atomic strategy.
9. Implement metrics output.
   - Total bytes, time, throughput.
10. Add systemd service/timer and documentation.
11. Add tests.
   - Unit tests for planner, state, manifest, config, chunking.
   - Harness integration for end-to-end S3 flow.

## Risks / open decisions
## Risks / open decisions
- Multipart retry behavior under sustained network failures (timeouts, aborts).
