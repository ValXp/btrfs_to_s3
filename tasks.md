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
