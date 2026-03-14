# Implementation Plan: ZFS-first migration for `btrfs_to_s3`

## Objective
Add ZFS support to this project by building the integration test harness first,
using that harness to validate ZFS snapshot/send/receive behavior, and then
refactoring the application around a filesystem backend abstraction.

The main point of the sequencing is to avoid designing a backend API from
assumptions. ZFS does support snapshots and send/receive, but its object model
is dataset-based rather than path-based, and the current code is built around
Btrfs subvolume paths.

## Current constraints in this repo
- The application is Btrfs-specific in:
  - `btrfs_to_s3/snapshots.py`
  - `btrfs_to_s3/streamer.py`
  - `btrfs_to_s3/restore.py`
  - `btrfs_to_s3/orchestrator.py`
  - `btrfs_to_s3/config.py`
  - `btrfs_to_s3/cli.py`
- The integration harness is also Btrfs-specific in:
  - `integration_tests/harness/btrfs.py`
  - `integration_tests/scripts/setup_btrfs.py`
  - `integration_tests/scripts/teardown_btrfs.py`
  - `integration_tests/scripts/run_all.py`
  - `integration_tests/harness/runner.py`
  - `integration_tests/harness/config.py`
  - `integration_tests/config/*.toml`

## Assumptions
- Initial goal is ZFS support on Linux with OpenZFS tooling available.
- The package/repo name can remain `btrfs_to_s3` during the migration.
- Existing Btrfs support should not be broken while ZFS work is in progress.
- Backward-compatible restore for existing Btrfs manifests is desirable.

## Non-goals for the first ZFS phase
- Renaming the package or CLI.
- Supporting every ZFS feature flag or replication mode.
- Solving cross-filesystem restore semantics beyond the project’s current
  single-host model.

## Phase 1: Build a standalone ZFS integration harness

### Goal
Create a harness that validates ZFS pool, dataset, snapshot, send, receive, and
retention behavior independently of the main backup tool.

### Why this comes first
The current tool cannot drive ZFS correctly because its manifest, restore
workflow, and snapshot model all assume Btrfs paths. A ZFS harness-first phase
lets us prove the real operational semantics before changing the application.

### Deliverables
- New helper module:
  - `integration_tests/harness/zfs.py`
- New setup/teardown scripts:
  - `integration_tests/scripts/setup_zfs.py`
  - `integration_tests/scripts/teardown_zfs.py`
- New probe scripts for direct ZFS validation:
  - `integration_tests/scripts/run_zfs_snapshot_send_receive.py`
  - `integration_tests/scripts/run_zfs_incremental.py`
  - `integration_tests/scripts/run_zfs_retention.py`
- New config file(s):
  - `integration_tests/config/test_zfs.toml`
  - optional variants mirroring existing scenarios if useful later
- Harness documentation update:
  - `integration_tests/README.md`

### Proposed ZFS helper responsibilities
`integration_tests/harness/zfs.py` should provide:
- pool creation and destruction
- sparse backing file creation
- direct file-vdev setup if supported, with loop-device fallback only if needed
- dataset creation and listing
- snapshot creation and listing
- `zfs send` open helpers for full and incremental sends
- `zfs receive` helpers
- dataset destroy / snapshot destroy helpers
- mountpoint helpers and property inspection

### Proposed ZFS harness config shape
Add a new `[zfs]` section instead of overloading `[btrfs]`.

Example fields:
- `pool_name`
- `pool_file`
- `pool_size_gib`
- `datasets`
- `mount_root`
- `snapshot_prefix`
- `receive_parent_dataset`
- optional `zpool_create_args`
- optional `zfs_create_args`

Retain shared sections that still make sense:
- `[tool]`
- `[paths]`
- `[aws]`
- `[backup]`

### Probe scenarios to implement first
1. Pool and dataset lifecycle
   - create a disposable pool
   - create datasets
   - confirm mountpoints and writeability
2. Full snapshot send/receive round trip
   - create dataset
   - write seed data
   - snapshot
   - `zfs send` to `zfs receive`
   - verify received dataset contents and identity
3. Incremental send/receive
   - mutate source dataset
   - create second snapshot
   - send incremental stream from first to second
   - receive onto target side
   - verify target matches source
4. Retention behavior
   - destroy old snapshots not needed by the next incremental base
   - confirm dependent incrementals fail if parent snapshot is removed
5. Failure behavior
   - interrupted `zfs send`
   - receive into conflicting dataset name
   - cleanup after partial receive

### Acceptance criteria for Phase 1
- Harness can create and destroy a disposable ZFS fixture repeatedly.
- Full send/receive passes end to end.
- Incremental send/receive passes end to end.
- Logs are captured under `integration_tests/run/...` like the current harness.
- The probe scripts document the real ZFS semantics the application must target.

## Phase 2: Generalize the integration harness shape

### Goal
Make the harness filesystem-aware instead of hard-coded to Btrfs names and
assumptions.

### Deliverables
- Refactor `integration_tests/harness/config.py` to support either `[btrfs]` or
  `[zfs]` plus a top-level filesystem selector.
- Refactor `integration_tests/harness/runner.py` so it can render a tool config
  for different backends instead of always emitting `[subvolumes]` from Btrfs
  mount paths.
- Introduce neutral terminology in harness code where possible:
  - source list instead of subvolumes list
  - filesystem/backend instead of implicit Btrfs

### Specific changes
- Add a filesystem/backend selector in harness config.
- Rename generated-path helpers so they do not assume a Btrfs image or Btrfs
  mount names.
- Split setup/teardown orchestration in `run_all.py` so it dispatches to
  backend-specific scripts.
- Keep the current Btrfs path operational while adding the ZFS path.

### Acceptance criteria for Phase 2
- Harness config validation can load both Btrfs and ZFS configs.
- `run_all.py` can choose the right setup/teardown scripts from config.
- Existing Btrfs harness still runs unchanged or with a thin compatibility shim.

## Phase 3: Use the ZFS harness output to define the backend interface

### Goal
Introduce an internal filesystem backend contract based on observed behavior,
not guessed symmetry with Btrfs.

### New application module(s)
- `btrfs_to_s3/filesystems/base.py`
- `btrfs_to_s3/filesystems/btrfs.py`
- `btrfs_to_s3/filesystems/zfs.py`

### Backend interface responsibilities
- enumerate configured sources
- create snapshot
- list snapshots
- prune snapshots
- open full send stream
- open incremental send stream
- clean up a failed send
- receive a stream into a restore target
- clean up failed/partial restore targets
- finalize restored target for use
- verify backend metadata after restore

### Design constraints from ZFS
The backend contract must not assume:
- snapshots are addressable by filesystem path
- restore targets are arbitrary directories
- send parents are represented by local paths
- metadata verification can be done with one universal command

### Acceptance criteria for Phase 3
- A Btrfs backend exists that preserves current behavior.
- The orchestrator can depend on a backend object rather than direct Btrfs
  commands.
- No Btrfs command invocation remains outside the Btrfs backend implementation.

## Phase 4: Refactor the main application to the backend interface

### Goal
Move current Btrfs-specific logic behind the backend abstraction without yet
changing external behavior.

### Primary application changes
- `btrfs_to_s3/snapshots.py`
  - either remove it in favor of backend modules or reduce it to shared naming
    logic
- `btrfs_to_s3/streamer.py`
  - move Btrfs send helpers into the Btrfs backend
- `btrfs_to_s3/restore.py`
  - split generic S3/download/manifest-chain logic from backend-specific replay
    and verification
- `btrfs_to_s3/orchestrator.py`
  - instantiate the selected backend from config
- `btrfs_to_s3/config.py`
  - add backend selector and backend-specific config objects
- `btrfs_to_s3/cli.py`
  - keep current UX where possible, but stop baking in Btrfs-only names

### Data model changes needed
- Generalize “subvolume” toward “source” or “dataset” internally.
- Preserve the CLI `--subvolume` flag initially as a compatibility alias if that
  reduces churn.
- Stop assuming `Path.name` is the stable source identifier.

### Acceptance criteria for Phase 4
- All existing unit tests still pass after the refactor or are updated with no
  behavioral regression for Btrfs.
- End-to-end Btrfs harness still passes.

## Phase 5: Implement the ZFS backend in the application

### Goal
Add a real ZFS backend for backup and restore using the interface from the prior
phases.

### ZFS backend capabilities
- create snapshots using dataset@snapshot identifiers
- select the correct incremental parent snapshot
- stream with `zfs send`
- restore with `zfs receive`
- destroy snapshots/datasets for retention and cleanup
- inspect dataset properties for verification

### Key design decisions to make during implementation
- source identifiers in config:
  - raw dataset names
  - dataset plus display alias
- restore target representation:
  - target dataset name
  - parent dataset plus child name
- whether manifests store:
  - dataset name
  - snapshot name
  - GUID
  - all of the above

### Acceptance criteria for Phase 5
- Full ZFS backup flow works end to end through the application.
- Incremental ZFS backup flow works end to end through the application.
- ZFS restore works end to end through the application.
- ZFS retention does not remove incremental parents still required by state.

## Phase 6: Update manifest and state schema

### Goal
Make persisted metadata backend-aware without breaking restore of old Btrfs
artifacts.

### Changes
- Add a manifest version bump.
- Add backend/filesystem type to the manifest.
- Replace or extend `snapshot.path` with backend-neutral snapshot identity.
- Extend local state entries to carry backend-specific source identity.

### Backward compatibility target
- Existing manifests should still restore through the Btrfs backend.
- New ZFS manifests should be explicit enough that restore does not depend on
  inferring a path from a snapshot name.

### Acceptance criteria for Phase 6
- Manifest parsing supports both legacy Btrfs manifests and new backend-aware
  manifests.
- State migration is automatic or clearly versioned.

## Phase 7: Expand automated test coverage

### Unit tests
Add or update tests for:
- backend selection in config
- Btrfs backend parity
- ZFS backend snapshot naming and parent selection
- manifest parsing for multiple backend versions
- restore target validation and backend-specific verification

### Integration tests
Keep the existing Btrfs scenarios and add ZFS counterparts for:
- full backup
- incremental backup
- interrupted backup
- restore current manifest
- restore explicit incremental chain
- retention enforcement

### Acceptance criteria for Phase 7
- Unit coverage remains strong in backend-independent modules.
- ZFS integration tests are repeatable and clean up after themselves.

## Phase 8: Documentation and cleanup

### Files to update
- `README.md`
- `DESIGN.md`
- `config.example.toml`
- `integration_tests/README.md`
- systemd examples if config schema changes materially

### Documentation changes needed
- explain backend selection
- explain ZFS prerequisites
- document differences in restore target semantics for Btrfs vs ZFS
- document manifest compatibility story

## Suggested execution order
1. Build `integration_tests/harness/zfs.py`.
2. Add `setup_zfs.py` and `teardown_zfs.py`.
3. Add standalone ZFS probe scripts and `test_zfs.toml`.
4. Run and document observed ZFS full/incremental/receive semantics.
5. Refactor harness config/runner to be backend-aware.
6. Introduce application backend interface and migrate Btrfs code behind it.
7. Implement ZFS backend.
8. Add manifest/state versioning changes.
9. Expand unit and integration coverage.
10. Update docs.

## Risks and open questions
- `zfs receive` targets datasets rather than arbitrary directories, so restore
  UX needs deliberate design.
- Pool creation may differ across Linux environments; direct file-vdev support
  should be validated early, with loop-device fallback if needed.
- Manifest schema changes are unavoidable if ZFS snapshot identity must be
  stored explicitly.
- The current CLI and config use Btrfs terminology heavily; compatibility
  aliases may be needed during migration.
