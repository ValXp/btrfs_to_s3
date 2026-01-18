Tasks for Python-First Test Harness

Goal
Build a Python 3.14 test harness for the `btrfs_to_s3` project. All harness code, configs, and docs must live under `testing/`. The harness must support end-to-end runs against a loopback Btrfs filesystem and AWS S3 using boto3, with clear verification steps.

Global constraints
- All new files must live under `testing/`.
- Use Python 3.14 features; use `tomllib` from the stdlib for TOML.
- Use boto3 for AWS S3 interactions (no AWS CLI dependency).
- Keep content ASCII-only.
- Prefer subprocess wrappers that capture output and fail fast (`check=True`).
- The harness must be runnable without editing project code, using a CLI command defined in `testing/config/test.toml`.

CLI contract (harness -> project)
- The harness invokes the project with a command array plus a config flag.
- Default in `testing/config/test.toml`:
  ```toml
  [tool]
  cmd = ["python", "-m", "btrfs_to_s3"]
  config_flag = "--config"
  ```
- The runner must allow override via env var `BTRFS_TO_S3_CMD`, which contains a JSON array or a shell-like string (define your choice and document it).
- The runner must append the config file path and additional args like `backup --mode full` or `backup --mode incremental`.

Common script conventions
- All scripts are Python entry points under `testing/scripts/`.
- Use `argparse` with a `--config` override for `testing/config/test.toml`.
- Use consistent exit codes: 0 success, non-zero on failure.
- Write logs under `testing/run/logs/`.

Task 1: Scaffolding + Docs (Agent A)
Summary
Create the directory structure, base docs, config templates, and dependency files.

Required changes
- Create `testing/README.md` with prerequisites, AWS test bucket/prefix guidance, and quickstart.
- Add `testing/.gitignore` excluding `testing/run/`, `testing/config/test.env`, and any local-only files.
- Add `testing/pyproject.toml` (or `testing/requirements.txt` if you decide) with boto3 dependency.
- Add `testing/config/test.toml` and `testing/config/test.env` templates with placeholder values.

Acceptance criteria
- A fresh agent can read `testing/README.md` and run the harness after filling in config values.
- All paths referenced in the README exist or are clearly marked as generated.

Task 2: Core Harness Modules (Agent B)
Summary
Implement core utilities that other scripts import.

Required files
- `testing/harness/config.py`: load and validate `test.toml` using `tomllib`.
- `testing/harness/env.py`: load `test.env` into `os.environ` with safe parsing.
- `testing/harness/assertions.py`: helper functions like `assert_true`, `assert_eq`, `fail`.
- `testing/harness/logs.py`: open log file, write timestamped entries, parse basic stats.

Acceptance criteria
- Modules are importable from any script under `testing/scripts/`.
- `config.py` provides a structured dict with required sections.

Task 3: CLI Runner Wiring (Agent C)
Summary
Implement the harness runner that calls `btrfs_to_s3`.

Required files
- `testing/harness/runner.py`: build the command, set env, and execute.

Behavior
- Read `tool.cmd` and `tool.config_flag` from config.
- Accept override with `BTRFS_TO_S3_CMD` (document the format and parsing).
- Add the config file path to the command.
- Add additional args passed by the calling script.
- Optionally set `PYTHONPATH` to repo root if not installed; document this choice.

Acceptance criteria
- Runner prints the command when invoked with `--dry-run`.
- Runner fails fast on non-zero exit.

Task 4: Btrfs Fixture API (Agent D)
Summary
Create a Python wrapper around Btrfs and loop device operations.

Required files
- `testing/harness/btrfs.py`: functions for loop device setup, mkfs, mount/umount, subvolume creation, snapshot listing.

Behavior
- Provide a clean teardown function that unmounts and detaches loop device.
- Use subprocess with `check=True`; capture stderr for errors.
- All operations target paths under `testing/run/`.

Acceptance criteria
- A standalone script can call the API to create a mounted fixture and clean it up.

Task 5: S3 + Manifest Verification (Agent E)
Summary
Implement boto3-backed helpers and manifest validation.

Required files
- `testing/harness/aws.py`: create boto3 client, `head_object`, `list_objects_v2`, `get_object`, `delete_objects`.
- `testing/harness/manifest.py`: load manifest JSON, validate required fields and hash list.
- `testing/expected/manifest_schema.json`: minimal schema used for validation.

Acceptance criteria
- Verification functions can report missing keys or invalid chunk order.
- S3 helpers can check storage class and SSE headers.

Task 6: Btrfs Fixture Scripts (Agent F)
Summary
Create scripts that use the Btrfs API to build and mutate test data.

Required files
- `testing/scripts/setup_btrfs.py`: creates loopback image, mounts, creates subvolumes.
- `testing/scripts/teardown_btrfs.py`: unmounts and detaches loop device.
- `testing/scripts/seed_data.py`: writes deterministic files to `data/root/home`.
- `testing/scripts/mutate_data.py`: applies a known set of changes for incrementals.

Acceptance criteria
- Running setup -> seed -> mutate -> teardown works without touching any path outside `testing/run/`.
- When run as root, setup should chown `testing/run/` to `SUDO_USER` so later scripts can run unprivileged.

Task 7: E2E Run Scripts (Agent G)
Summary
Implement scripts that run full and incremental backups using the runner.

Required files
- `testing/scripts/run_full.py`: runs a full backup using test config.
- `testing/scripts/run_incremental.py`: mutates data, then runs incremental.
- `testing/scripts/run_interrupt.py`: starts backup, kills process mid-stream, then reruns.

Acceptance criteria
- Each script uses `runner.py` and logs to `testing/run/logs/`.

Task 8: Verification + Benchmark Scripts (Agent H)
Summary
Implement verification scripts and a benchmark summary.

Required files
- `testing/scripts/verify_manifest.py`: parse and validate manifest.
- `testing/scripts/verify_s3.py`: check S3 object layout, storage class, SSE-S3.
- `testing/scripts/verify_retention.py`: confirm local snapshot retention.
- `testing/scripts/benchmark.py`: produce `testing/run/logs/benchmark.json`.

Acceptance criteria
- Each script exits non-zero on failure and writes helpful log output.

Task 9: Orchestration + Cleanup (Agent I)
Summary
Create a top-level orchestrator and cleanup tool.

Required files
- `testing/scripts/run_all.py`: run setup -> seed -> full -> mutate -> incremental -> interrupt -> verify -> teardown.
- `testing/scripts/cleanup_s3_prefix.py`: delete all objects under the test prefix.

Acceptance criteria
- `run_all.py` performs teardown even if a step fails (best-effort cleanup).
- `cleanup_s3_prefix.py` requires explicit `--yes` confirmation.

Task 10: Restore Scripts + Verification (Agent J)
Summary
Implement restore execution and full end-to-end verification.

Required files
- `testing/scripts/run_restore.py`: restore into a new subvolume target.
- `testing/scripts/verify_restore.py`: compare restored data to the source snapshot.

Behavior
- Restore uses `btrfs_to_s3 restore` and a configurable target base path.
- Restore supports `--manifest-key` override or defaults to `current.json`.
- Restore waits for archival object readiness, honoring timeout settings.
- Verification checks both Btrfs metadata and file content hashes.
- Verification supports `full` and `sample` modes (deterministic sampling).

Acceptance criteria
- Restore completes and creates a new subvolume under the target path.
- Restore fails if the target path already exists.
- Verification fails on any mismatch (missing/extra file, hash mismatch) and reports the first discrepancy.
- Scripts log to `testing/run/logs/` and exit non-zero on failure.

Task 11: Multi-chunk Scenario (Agent K)
Summary
Add a large-dataset scenario to force multi-chunk uploads and reassembly.

Required files
- `testing/config/test_large.toml`: smaller chunk size and larger dataset defaults.
- `testing/scripts/run_large.py`: run full + incremental with the large dataset config.
- Update `testing/scripts/seed_data.py` and `testing/scripts/mutate_data.py` to accept
  a dataset size option.

Behavior
- `test_large.toml` sets a small `chunk_size_bytes` and a larger dataset size to
  guarantee multiple chunks per subvolume.
- `run_large.py` verifies that at least one subvolume produced multiple chunks
  by checking the manifest or S3 listing.

Acceptance criteria
- `seed_data.py` and `mutate_data.py` accept a dataset size option and generate
  deterministic data of that size.
- `run_large.py` uses `testing/config/test_large.toml` and fails if a multi-chunk
  upload is not observed.
- Logs are written under `testing/run/logs/`.

Task 12: Orchestration Extensions (Agent I2)
Summary
Extend orchestration to include restore and the multi-chunk scenario.

Required changes
- Update `testing/scripts/run_all.py` to include restore + verify in sequence.
- Add an optional flag to include the multi-chunk scenario.
- Update `testing/README.md` to document the large scenario entrypoint.

Acceptance criteria
- `run_all.py` includes restore + verify by default.
- `run_all.py --include-large` (or equivalent) runs the multi-chunk scenario.
- Documentation references `testing/config/test_large.toml`.

Task 13: Force Incremental Runs (Agent L)
Summary
Ensure the harness actually triggers incrementals instead of skipping due to schedule.

Required changes
- Update `testing/scripts/run_incremental.py` to pass `--once` to the CLI to force a run even if the planner says “not due”.
- Remove reliance on `BTRFS_TO_S3_BACKUP_TYPE` (currently ignored by the CLI).
- Optionally add a log line stating the incremental run is forced.

Acceptance criteria
- Running `python testing/scripts/run_incremental.py --config testing/config/test.toml` produces an incremental manifest (not skipped).
- Logs show the CLI was invoked with `backup --once`.

Task 14: Cover All Subvolumes (Agent M)
Summary
Make the harness test backups for all configured subvolumes, not just the first.

Required changes
- Adjust harness behavior so the CLI runs all subvolumes even when `BTRFS_TO_S3_HARNESS_RUN_DIR` is set.
- Update `testing/scripts/run_full.py` and `testing/scripts/run_incremental.py` to run per-subvolume if needed (e.g., call `backup --subvolume <name>` per subvolume).

Acceptance criteria
- The harness produces manifests and S3 objects for each subvolume listed in `testing/config/test.toml`.
- `testing/scripts/verify_s3.py` or a new check confirms all subvolumes were backed up.

Task 15: Validate Real Manifest and Current Pointer (Agent N)
Summary
Validate the actual S3 manifest schema and the `current.json` pointer contents, not only the simplified local manifest.

Required changes
- Extend `testing/harness/manifest.py` to validate the real manifest schema in `btrfs_to_s3/manifest.py` (fields like `version`, `subvolume`, `kind`, `snapshot`, `chunks`, `total_bytes`, `chunk_size`, `s3`).
- Add a new schema file under `testing/expected/manifest_schema_full.json` and use it for S3 manifest validation.
- Add validation for `current.json` fields (`manifest_key`, `kind`, `created_at`).
- Update `testing/scripts/verify_manifest.py` to download the S3 manifest and `current.json`, validate both, and keep the existing local manifest checks.

Acceptance criteria
- `verify_manifest.py` fails if any required manifest or current pointer field is missing or malformed.
- Validation checks the real S3 manifest contents, not just the local `testing/run/manifest.json`.

Task 16: S3 Layout + Metadata Checks (Agent O)
Summary
Verify object layout, per-chunk metadata, and manifest vs chunk storage classes.

Required changes
- Extend `testing/scripts/verify_s3.py` to:
  - Confirm object keys follow the expected layout (`subvol/<name>/(full|incremental)/...`).
  - Validate that chunk objects use `s3.storage_class_chunks` and manifests/current pointers use `s3.storage_class_manifest`.
  - Verify chunk objects referenced in the manifest exist and match size.
- Add helper functions in `testing/harness/aws.py` if needed (e.g., list keys by suffix or prefix).

Acceptance criteria
- `verify_s3.py` fails on missing chunk objects, bad key layout, or storage class mismatch.
- Manifests and current pointers are checked separately from chunks for storage class.

Task 17: Incremental Chain Restore Coverage (Agent P)
Summary
Exercise restore flows using explicit manifest keys and chained incrementals.

Required changes
- Extend `testing/scripts/run_restore.py` to accept `--manifest-key` and add a new mode to restore a full+incremental chain (using an incremental manifest key).
- Update `testing/scripts/run_all.py` to run at least one restore using `--manifest-key`.
- Add a check to ensure the restore uses the full chain when given an incremental manifest.

Acceptance criteria
- At least one restore run uses `--manifest-key` and completes successfully.
- Logs show that the restore resolved parent manifests (chain) rather than only a full.

Task 18: Archive Restore Behavior (Agent Q)
Summary
Test restore wait/timeout handling for archival storage classes.

Required changes
- Add a harness config variant (e.g., `testing/config/test_archive.toml`) that uses an archival class (e.g., `GLACIER` or `DEEP_ARCHIVE`) and sets `restore.wait_for_restore`/timeout settings.
- Add a script `testing/scripts/run_restore_archive.py` to run a restore with both `--wait-restore` and `--no-wait-restore` options.
- Document any required AWS permissions or delays in `testing/README.md`.

Acceptance criteria
- The script logs both paths (wait vs no-wait) and exits non-zero on timeout or improper handling.
- The harness can be run in non-archive mode without changes.

Task 19: CLI Flag Coverage (Agent R)
Summary
Exercise CLI flags and config branches currently untested.

Required changes
- Add `testing/scripts/run_cli_flags.py` that calls the CLI with:
  - `backup --dry-run`
  - `backup --no-s3`
  - `backup --subvolume <name>`
  - `backup --once`
  - `restore --verify none|sample|full` (choose at least one non-default)
- Ensure each invocation is logged and validated for expected behavior.

Acceptance criteria
- Script exits non-zero on any unexpected CLI failure.
- Logs show each flag path was exercised.

Task 20: Lock Contention Test (Agent S)
Summary
Verify the lock prevents overlapping runs.

Required changes
- Add `testing/scripts/run_lock_contention.py` that starts one `backup` process, then quickly starts a second and verifies the second fails with a lock error.
- Add a harness helper to parse stderr/stdout for the lock failure signal if needed.

Acceptance criteria
- The second run fails with a lock error, and the script exits 0 only if contention is handled as expected.

Task 21: Spool Configuration Coverage (Agent T)
Summary
Test `s3.spool_enabled` and spool size constraints.

Required changes
- Add `testing/config/test_spool.toml` with `s3.spool_enabled = true` and a small `global.spool_size_bytes`.
- Add `testing/scripts/run_spool.py` to run a backup using this config and verify it completes (or fails deterministically if the spool size is too small).

Acceptance criteria
- The script clearly documents expected behavior (pass or fail) and enforces it.
- Logs show spool settings were in effect.

Task 22: Multi-Subvolume Restore Verification (Agent U)
Summary
Verify restore correctness across all subvolumes, not only the first.

Required changes
- Update `testing/scripts/verify_restore.py` to accept `--subvolume all` and iterate over all configured subvolumes.
- Update `testing/scripts/run_restore.py` to optionally restore all subvolumes into separate targets under a base dir.

Acceptance criteria
- A single command can restore and verify all subvolumes.
- Failure in any subvolume causes non-zero exit with a clear error.
