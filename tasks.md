# Tasks: ZFS migration follow-up

## Todo
### Global requirement (applies to all tasks)
- Tasks 1-23 are already complete and are superseded by the Task 24+ backlog
  below. Do not renumber these new tasks.
- A task is only complete if `python3 -m pytest` passes.
- Unless a task explicitly changes compatibility, existing Btrfs behavior,
  existing Btrfs config files, and existing restore behavior must keep working.
- If a task adds or changes config fields, manifest fields, or CLI behavior, the
  same task must update tests that cover the new surface area.
- Until Task 37, ZFS-related code must be testable without requiring a live ZFS
  host. Use unit tests and mocks to verify command construction, sequencing, and
  error handling.

### Task 24: Harness config schema + ZFS docs groundwork
- Scope: `integration_tests/harness/config.py`, `integration_tests/README.md`,
  `integration_tests/config/test_zfs.toml`, tests for harness config loading.
- Context: the integration harness currently hard-requires a `[btrfs]` section
  and Btrfs-specific path keys such as `paths.btrfs_image`. There is no way to
  express a disposable ZFS pool, datasets, or backend selection in harness
  config. The first task is to let the harness describe either backend while
  keeping the current Btrfs configs valid.
- Add an explicit backend/filesystem selector to harness config, or another
  equally clear mechanism that lets the harness distinguish Btrfs from ZFS.
- Extend validation so existing Btrfs configs still load, and new ZFS configs
  can define pool, dataset, and mount-related settings without reusing Btrfs
  names.
- Add a `integration_tests/config/test_zfs.toml` template that describes a
  disposable file-backed ZFS fixture under `integration_tests/run/`.
- Update `integration_tests/README.md` with ZFS prerequisites, the new config
  structure, and how the ZFS harness differs from the Btrfs harness.
- Acceptance criteria:
  - `integration_tests/harness/config.py` validates both the current Btrfs
    config format and the new ZFS config format.
  - `integration_tests/config/test_zfs.toml` exists and contains all required
    fields for a disposable ZFS pool plus at least one source dataset.
  - `integration_tests/README.md` documents the ZFS config section and any host
    prerequisites.
  - Unit tests cover valid ZFS config loading, invalid ZFS config rejection, and
    backward compatibility for the current Btrfs config shape.

### Task 25: ZFS harness helper module
- Scope: new `integration_tests/harness/zfs.py` plus tests.
- Context: the repo already has `integration_tests/harness/btrfs.py`, which
  wraps loopback image setup, mount/umount, subvolume creation, and snapshot
  listing. The ZFS migration needs an equivalent helper module for pool,
  dataset, snapshot, send, and receive operations, but it must be unit-testable
  without a live ZFS host.
- Create `integration_tests/harness/zfs.py` with subprocess-backed helpers for:
  pool creation/destruction, backing file creation, dataset creation/listing,
  snapshot creation/listing/destruction, opening full and incremental send
  streams, receiving streams, and reading dataset properties.
- Use subprocess wrappers that capture output and surface stderr in failures,
  matching the style already used by `integration_tests/harness/btrfs.py`.
- Ensure helper inputs are constrained to paths under `integration_tests/run/`
  where applicable.
- Acceptance criteria:
  - `integration_tests/harness/zfs.py` exists and exposes a coherent helper API
    parallel to the current Btrfs helper module where that makes sense.
  - Unit tests verify the exact `zpool` and `zfs` command arguments constructed
    for full send, incremental send, receive, snapshot create/destroy, and pool
    lifecycle operations.
  - Unit tests cover error propagation so stderr from failed subprocesses is not
    lost.
  - No test in this task requires `zfs` or `zpool` to be installed.

### Task 26: ZFS setup/teardown fixture scripts
- Scope: new `integration_tests/scripts/setup_zfs.py`,
  `integration_tests/scripts/teardown_zfs.py`, new run-state files under
  `integration_tests/run/`, and tests for the script logic.
- Context: the existing harness has `setup_btrfs.py` and `teardown_btrfs.py`
  that create a loopback image, mount it, create subvolumes, and store the loop
  device in `integration_tests/run/`. The ZFS harness needs equivalent scripts
  that create and destroy a disposable pool and source datasets.
- Implement `setup_zfs.py` to read `test_zfs.toml`, create the backing file,
  create or import the disposable pool, create source datasets, create any
  receive-parent dataset, and write whatever state `teardown_zfs.py` needs under
  `integration_tests/run/`.
- Implement `teardown_zfs.py` to destroy the disposable pool and remove run
  state even after partial setup failures.
- Match the current script conventions: `argparse`, `--config`, logs under
  `integration_tests/run/logs/`, exit code 0 on success and non-zero on failure.
- Preserve the existing `SUDO_USER` chown behavior when running as root so later
  non-privileged scripts can write into `integration_tests/run/`.
- Acceptance criteria:
  - `setup_zfs.py` and `teardown_zfs.py` exist and follow the same CLI/logging
    pattern as the Btrfs fixture scripts.
  - Script unit tests verify the sequencing of helper calls, state-file writes,
    and cleanup behavior on failure.
  - Root-mode chown behavior is covered by tests or a narrowly scoped helper
    test.
  - No test in this task requires a live ZFS host.

### Task 27: Standalone ZFS probe scripts
- Scope: new `integration_tests/scripts/run_zfs_snapshot_send_receive.py`,
  `integration_tests/scripts/run_zfs_incremental.py`,
  `integration_tests/scripts/run_zfs_retention.py`, plus tests.
- Context: before changing the application, the repo needs standalone scripts
  that exercise raw ZFS semantics directly through the harness helpers. These
  scripts must not call `python -m btrfs_to_s3`; they exist to prove what the
  future backend must support.
- Add a full send/receive probe that creates a snapshot of a source dataset,
  streams it to a receive target, and verifies the expected dataset/snapshot
  results.
- Add an incremental send/receive probe that creates a base snapshot, mutates
  data, creates a second snapshot, and validates the incremental replay flow.
- Add a retention probe that destroys snapshots according to a policy and
  demonstrates what happens when an incremental parent is removed too early.
- All scripts must log under `integration_tests/run/logs/` and fail loudly with
  helpful messages if a step fails.
- Acceptance criteria:
  - All three scripts exist and are wired to the harness config and log helpers.
  - Unit tests verify that the scripts call the expected ZFS helper operations in
    the correct order and write logs for success/failure paths.
  - The scripts do not depend on the main application package yet.
  - No test in this task requires a live ZFS host.

### Task 28: Backend-aware harness runner and orchestration
- Scope: `integration_tests/harness/runner.py`,
  `integration_tests/scripts/run_all.py`, related harness tests, and any needed
  updates to `integration_tests/README.md`.
- Context: `integration_tests/harness/runner.py` currently renders a Btrfs-only
  tool config with `[snapshots]` and `[subvolumes]` based on mount paths, and
  `run_all.py` hard-codes `setup_btrfs.py` and `teardown_btrfs.py`. The harness
  must become backend-aware before the main tool can be exercised against ZFS.
- Refactor the runner so it can render either a Btrfs-oriented or a ZFS-oriented
  tool config from harness config, while keeping the current Btrfs path working.
- Refactor `run_all.py` so setup/teardown dispatch is backend-specific instead of
  always calling the Btrfs scripts.
- Keep current Btrfs harness behavior intact for the existing configs.
- Acceptance criteria:
  - `integration_tests/harness/runner.py` can emit a tool config for both
    Btrfs-backed and ZFS-backed harness configs.
  - `integration_tests/scripts/run_all.py` selects the correct setup/teardown
    scripts from config instead of hard-coding Btrfs.
  - Unit tests cover the rendered TOML for both backend types and the backend
    dispatch behavior in `run_all.py`.
  - Existing Btrfs harness tests continue to pass.

### Task 29: Application config groundwork for filesystem backends
- Scope: `btrfs_to_s3/config.py`, `config.example.toml`, `tests/test_config.py`,
  and any small supporting model changes needed outside config.
- Context: the application config currently assumes path-based Btrfs sources via
  `[subvolumes].paths` and path-based snapshot storage via `[snapshots].base_dir`.
  ZFS support needs a backend selector plus backend-specific source definitions,
  but existing Btrfs config files must remain valid.
- Extend the config model to represent the selected filesystem backend and the
  backend-specific settings needed for Btrfs and ZFS.
- Keep the current Btrfs config form working, either natively or through a clear
  compatibility layer in `Config.from_dict`.
- Update `config.example.toml` so it documents the intended configuration shape
  for the new backend-aware model.
- Acceptance criteria:
  - `Config.from_dict` and `load_config` accept both the current Btrfs config
    form and a new backend-aware form that can describe ZFS sources.
  - Validation errors for malformed backend config are explicit about which
    section/key is wrong.
  - `config.example.toml` documents the backend selector and the ZFS-specific
    fields.
  - `tests/test_config.py` covers both legacy Btrfs config loading and the new
    ZFS-oriented config loading.

### Task 30: Filesystem backend interface + Btrfs snapshot/send extraction
- Scope: new modules under `btrfs_to_s3/filesystems/`, existing
  `btrfs_to_s3/snapshots.py`, `btrfs_to_s3/streamer.py`, and related tests.
- Context: the application currently shells out to Btrfs directly from
  `snapshots.py` and `streamer.py`. ZFS support requires a filesystem backend
  contract so the orchestrator no longer knows how snapshots and send streams are
  implemented.
- Add a backend base module that defines the operations needed for snapshot
  creation, snapshot listing/pruning, full send, incremental send, and send
  cleanup.
- Move the existing Btrfs snapshot and send logic into a Btrfs backend module
  under `btrfs_to_s3/filesystems/`.
- Leave thin compatibility wrappers only if they materially reduce churn; avoid
  duplicated command logic.
- Acceptance criteria:
  - A new filesystem backend interface exists and is used by the Btrfs
    implementation.
  - Raw `btrfs subvolume snapshot`, `btrfs subvolume delete`, and `btrfs send`
    command construction no longer lives in the old generic modules.
  - Existing snapshot/send behavior is preserved for Btrfs.
  - Unit tests cover the Btrfs backend command construction and any compatibility
    wrapper behavior.

### Task 31: Move restore/finalize/verify Btrfs logic behind the backend
- Scope: `btrfs_to_s3/restore.py`, new or updated modules under
  `btrfs_to_s3/filesystems/`, and `tests/test_restore.py`.
- Context: `restore.py` currently mixes generic manifest/S3 replay logic with
  Btrfs-specific receive, subvolume deletion, writable-finalization, and metadata
  verification. ZFS support requires the generic restore pipeline to be separate
  from backend-specific apply/finalize/verify behavior.
- Split `restore.py` so the generic pieces stay in the shared restore flow:
  manifest resolution, Glacier restore polling, chunk download, hash
  verification, and content verification.
- Move Btrfs-specific receive, subvolume deletion, writable-finalization, and
  metadata verification into the Btrfs backend implementation.
- Preserve current restore semantics for Btrfs while making the generic restore
  code backend-agnostic.
- Acceptance criteria:
  - `restore.py` no longer shells out to `btrfs receive`, `btrfs subvolume
    delete`, `btrfs property set`, or `btrfs subvolume show` directly.
  - Generic restore logic is still able to stream chunks, verify hashes, and
    perform content verification independent of backend type.
  - Btrfs restore behavior remains unchanged from the caller’s point of view.
  - `tests/test_restore.py` is updated to cover the new backend seam and still
    passes.

### Task 32: Orchestrator and restore flow backend selection
- Scope: `btrfs_to_s3/orchestrator.py`, `btrfs_to_s3/cli.py`,
  `btrfs_to_s3/planner.py` if needed, backend factory code, and tests for CLI
  and orchestration.
- Context: even after backend modules exist, the application still needs to
  instantiate the selected backend from config and route backup/restore work
  through it. The current orchestrator assumes path-based Btrfs sources and
  hard-codes `SnapshotManager`, `open_btrfs_send`, and Btrfs restore helpers.
- Refactor the backup and restore orchestration flows so they obtain a backend
  from config and use it for snapshot lifecycle, send streams, receive/apply,
  and backend metadata verification.
- Preserve current CLI behavior where possible. If the `--subvolume` flag is
  kept as a compatibility alias, resolve it cleanly to backend-neutral source
  identifiers internally.
- Acceptance criteria:
  - `BackupOrchestrator` and `RestoreOrchestrator` no longer import Btrfs-only
    helpers directly.
  - Backend selection is driven by config rather than hard-coded behavior.
  - Existing CLI tests still pass, with new tests added for backend selection and
    compatibility alias handling.
  - `python3 -m pytest` passes after the refactor.

### Task 33: ZFS application backend implementation
- Scope: new `btrfs_to_s3/filesystems/zfs.py`, related tests, and any small
  shared helpers needed for ZFS snapshot identity handling.
- Context: after the backend seam exists, the application needs a real ZFS
  backend that can create snapshots, choose incremental parents, open `zfs send`
  streams, replay `zfs receive`, clean up failed restore targets, and perform
  backend metadata verification. This task is application-facing, not harness-only.
- Implement the ZFS backend for the operations required by the new backend
  interface.
- Represent snapshots using dataset/snapshot identity, not local filesystem
  paths. If both a display path and a backend identity are useful, keep the
  backend identity authoritative.
- Keep the code unit-testable without requiring a live ZFS host.
- Acceptance criteria:
  - `btrfs_to_s3/filesystems/zfs.py` implements the same interface used by the
    Btrfs backend.
  - Unit tests verify command construction for ZFS snapshot create/list/destroy,
    full send, incremental send, receive/apply, cleanup, and metadata queries.
  - Error handling preserves stderr from failed ZFS subprocesses.
  - No test in this task requires `zfs` or `zpool` to be installed.

### Task 34: Backend-aware manifest and state schema
- Scope: `btrfs_to_s3/manifest.py`, `btrfs_to_s3/state.py`,
  `btrfs_to_s3/restore.py`, `tests/test_manifest.py`, `tests/test_state.py`,
  `tests/test_restore.py`.
- Context: the current manifest stores `snapshot.path` and assumes the restore
  target can infer identity from a path-like snapshot record. ZFS snapshots are
  not naturally represented that way, so the manifest and local state need to
  become backend-aware while keeping old Btrfs manifests restorable.
- Add backend/filesystem type to the manifest schema and version the manifest if
  needed.
- Extend snapshot metadata so it can represent ZFS dataset/snapshot identity
  without overloading a filesystem path.
- Update local state so incremental-parent tracking can store backend-specific
  snapshot identity, not just a path string.
- Keep parsing/restoring legacy Btrfs manifests working.
- Acceptance criteria:
  - New manifests explicitly record backend/filesystem type and backend-aware
    snapshot identity.
  - Legacy Btrfs manifests still parse and restore successfully through the
    shared restore code plus the Btrfs backend.
  - State serialization/deserialization supports both existing Btrfs state and
    the new backend-aware form.
  - Tests cover new manifest/state fields and backward compatibility.

### Task 35: ZFS-aware unit and harness test coverage
- Scope: tests under `tests/`, harness tests under `integration_tests/` if you
  add them, and any fixture/test helpers needed for the new backend model.
- Context: Tasks 24-34 introduce new config shapes, backend seams, and a ZFS
  implementation. This task is to consolidate coverage so future refactors are
  safe and the work loop can measure success without needing a live ZFS host.
- Add or extend tests for:
  - harness config validation for Btrfs and ZFS
  - ZFS helper command construction
  - backend selection from application config
  - Btrfs backend parity through the new interface
  - ZFS backend command construction and error paths
  - manifest/state backward compatibility
- Acceptance criteria:
  - The repo has automated tests covering the new backend-aware paths end to end
    at the unit level.
  - Btrfs compatibility is covered by tests, not only assumed.
  - `python3 -m pytest` passes with the new tests included.

### Task 36: Documentation and example config updates
- Scope: `README.md`, `DESIGN.md`, `config.example.toml`,
  `integration_tests/README.md`, and any systemd/docs examples affected by the
  backend-aware config model.
- Context: after the backend-aware application and harness exist, the docs must
  stop describing the project as Btrfs-only. The migration also changes config
  shape, restore semantics, and integration-test prerequisites.
- Update the top-level README and design docs so they explain:
  - supported backends
  - backend-specific source configuration
  - differences in restore target semantics between Btrfs and ZFS
  - manifest/state compatibility expectations
- Ensure `config.example.toml` and harness config examples are aligned with the
  implemented schema.
- Acceptance criteria:
  - The top-level docs describe both Btrfs and ZFS accurately.
  - All documented config keys match the current parser and examples.
  - The integration test docs explain how to run both harness backends.
  - `python3 -m pytest` still passes after any example/config/doc-aligned test
    updates.

### Task 37: Live ZFS harness and application validation
- Scope: `integration_tests/scripts/`, harness configs under
  `integration_tests/config/`, `learnings.md`, and any narrowly scoped fixes
  required to make the live flow work.
- Context: Tasks 24-36 are structured so they can be completed with unit tests
  and mocks. This final task is the live validation step on a host that actually
  has `zpool` and `zfs` available. It should exercise the standalone ZFS probes
  first and then the application-backed ZFS flow.
- Run the standalone ZFS probe scripts created earlier and capture any semantic
  differences from assumptions in `learnings.md`.
- Run the ZFS-backed application flow through the harness far enough to prove:
  setup, seed, backup, incremental backup, restore, and teardown.
- Fix any issues discovered during live validation, but do not broaden scope
  beyond what is required to make the ZFS path actually work.
- Acceptance criteria:
  - The standalone ZFS probe scripts succeed on a live ZFS host.
  - The application-backed ZFS harness succeeds through setup, full backup,
    incremental backup, restore, verification, and teardown.
  - Any live-host gotchas discovered during validation are recorded in
    `learnings.md`.
  - `python3 -m pytest` still passes after the live-validation fixes.
