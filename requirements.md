# Requirements: btrfs_to_s3

## Overview

Build an automated backup system for a Proxmox host using Btrfs snapshots (`btrfs send`) and AWS S3 (primarily Glacier Deep Archive) to protect against catastrophic local data loss.

The system must back up three Btrfs subvolumes on the same filesystem:

- `data` (primary, ~5 TiB logical)
- `root`
- `home`

## Goals

- Create **crash-consistent** backups of the Btrfs subvolumes using snapshots + `btrfs send`.
- Support **full** backups (target cadence: ~every 6 months).
- Support **incremental** backups (target cadence: weekly).
- Store backup data in AWS S3 in **large objects** (default ~200 GiB chunks; configurable).
- Run automatically on an always-on host and be schedulable for **2am local time**.
- Be resilient to interruption (host crash, network drop): reruns should succeed without manual cleanup.
- Enable restoring the data from S3 (restore implementation is explicitly deferred until after backup/upload is working and validated).

## Non-goals (initially)

- Multi-host support (single host only).
- Point-in-time restores for arbitrary weekly snapshots as a primary feature (old restore points are a side-effect, not a priority).
- Application-consistent snapshots (no service quiescing) unless later required.

## Observed Data Growth (informational)

A 180-day scan using `ctime` as a proxy for “new/changed since” (append-only assumption) produced:

- `/srv/data/data`: 4950.94 GiB total; 218.66 GiB in last 180 days
- `/srv/data/root`: 39.37 GiB total; 37.07 GiB in last 180 days
- `/srv/data/home`: 10.99 GiB total; 6.32 GiB in last 180 days

Implication for incremental strategy (additive-only model):

- Weekly **chained** incrementals (“this week vs last week”) are ~10× smaller than “cumulative since full”.
- The system should default to **chained** incrementals to minimize upload volume and S3 storage.

The scan helper lives at `scripts/fs_growth_analysis.sh`.

## Backup Model

### Snapshot management (local)

- The system must manage local snapshots (no external snapshot manager assumed).
- For each backup run, create read-only snapshots for each configured subvolume.
- Local snapshot retention must be configurable and must support a minimal mode that keeps only what’s needed to generate the next incremental (typically the most recent successfully-uploaded snapshot per subvolume).
- Snapshot location and naming scheme do not need to be user-customizable, but must be deterministic and not collide.

### Full backups

- A full backup is produced via `btrfs send` of a read-only snapshot.
- Full backups should run on a ~6 month cadence (exact policy to be configurable, e.g., “every N days”).
- Only **one** full-backup “generation” must be required for normal operation; when a new full backup is successfully uploaded and recorded as current, the old generation can be deleted.

### Incremental backups

- Incremental backups should run weekly at ~2am local time.
- Incrementals must be **chained** per subvolume: each incremental is based on the prior successfully-uploaded snapshot for that subvolume.
- If the required parent snapshot is missing/unusable, the system must fall back to (or request) a new full backup for that subvolume.

## S3 Storage Model

### AWS compatibility

- AWS S3 only (no requirement for MinIO or other S3-compatible implementations).
- Bucket name and region must be configurable.
- Base key prefix must be configurable; target layout begins under `backup/data/` (single host).

### Object layout

- Backups must be stored as a sequence of large objects (“chunks”) representing a `btrfs send` stream.
- Default chunk size target: **200 GiB** (configurable).
- A small manifest/metadata object must describe each backup stream (full/incremental) and list its chunks in order.

### Storage classes

- Storage class must be configurable, at minimum separately for:
  - bulk data chunks (full + incrementals)
  - small metadata/manifests
- The system is intended to primarily use Glacier Deep Archive for bulk data, but must not hardcode it.

### Encryption

- S3 server-side encryption with S3-managed keys (SSE-S3 / AES256) must be used for uploaded objects.

### Integrity

- The system must compute and store integrity information sufficient to detect corruption during restore (e.g., per-chunk cryptographic hash recorded in the manifest).

## Reliability & Failure Handling

- Backups can be interrupted at any time; a later rerun must succeed without manual S3 cleanup.
- It is acceptable to restart an in-progress backup from scratch; partial uploads can be abandoned.
- The system must avoid producing a “current” manifest/pointer in S3 until all required chunks are successfully uploaded (atomic publish).
- The system must prevent concurrent overlapping runs (local lock).

## Performance

- The system must support high-throughput uploads on unreliable consumer internet (2 Gbps symmetrical available).
- Uploading should support configurable concurrency (multiple streams) to better saturate available bandwidth.
- Use of a scratch SSD (~500 GiB available) should be supported if needed for buffering/spooling, but streaming operation should be possible.

## Configuration & UX

- Provide a configuration file and/or CLI flags for:
  - subvolume mount paths (data/root/home)
  - schedule (weekly at 2am; full cadence)
  - S3 bucket, region, prefix
  - chunk size target
  - upload concurrency tuning
  - retention policy (at least: keep latest generation; prune local snapshots)
- Provide clear logs and exit codes suitable for systemd/cron.

## Automation

- Provide a systemd service/timer (or equivalent) to run automatically at 2am.
- Full backups and weekly incrementals must be orchestrated automatically according to configured cadence.

## Testing & Benchmarking

- Must be testable against real AWS S3 while keeping costs low:
  - ability to use a dedicated test prefix/bucket
  - ability to override storage class to avoid long minimum-storage penalties during tests
- Provide a way to benchmark throughput and record basic run stats (bytes sent, time taken).

## Restore (deferred)

- Restoration must be supported as a later milestone:
  - download required chunks
  - validate integrity
  - reassemble the `btrfs send` stream
  - `btrfs receive` into a target filesystem/subvolume

## Test Harness Notes

- The local Btrfs fixture setup requires root; it can chown test directories to `SUDO_USER` so non-root scripts can run afterward.
- Teardown still requires root to unmount and detach the loop device.
