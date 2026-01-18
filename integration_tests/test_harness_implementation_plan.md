Here’s a concise implementation plan for a Python-first test harness under `integration_tests/`, using Python 3.14 and boto3, and explicitly wiring how to call the `btrfs_to_s3` CLI.

Implementation Plan

- Define the harness layout under `integration_tests/` only.
  ```text
  integration_tests/
    README.md
    .gitignore
    pyproject.toml
    config/
      test.toml
      test.env
      test_large.toml
    harness/
      __init__.py
      config.py
      env.py
      assertions.py
      runner.py
      btrfs.py
      aws.py
      manifest.py
      logs.py
    scripts/
      setup_btrfs.py
      teardown_btrfs.py
      seed_data.py
      mutate_data.py
      run_full.py
      run_incremental.py
      run_interrupt.py
      run_restore.py
      run_large.py
      verify_manifest.py
      verify_s3.py
      verify_retention.py
      verify_restore.py
      benchmark.py
      cleanup_s3_prefix.py
      run_all.py
    expected/
      manifest_schema.json
    run/
      (generated runtime artifacts)
  ```

- Implement config and CLI wiring.
  - `integration_tests/config/test.toml` includes:
    - S3 bucket/region/prefix, storage class, chunk size, concurrency.
    - Local paths (`run/` dirs, scratch, lock, snapshots).
    - CLI invocation contract:
      ```toml
      [tool]
      cmd = ["python", "-m", "btrfs_to_s3"]
      config_flag = "--config"
      ```
  - `integration_tests/harness/runner.py` reads `tool.cmd` or `BTRFS_TO_S3_CMD` env override and runs the CLI with the test config path.
  - Optionally set `PYTHONPATH` to repo root inside `runner.py` if not installed.
  - Add `integration_tests/config/test_large.toml` with a small `chunk_size_bytes` and
    larger dataset defaults to force multi-chunk uploads.

- Build Python harness modules (minimal deps).
  - `config.py` loads `test.toml` (use stdlib `tomllib` in 3.14).
  - `env.py` loads `test.env` and normalizes paths.
  - `assertions.py` provides simple fail-fast helpers.
  - `btrfs.py` wraps `mkfs.btrfs`, `mount`, `subvolume`, `snapshot` via `subprocess.run`.
  - `aws.py` uses `boto3` for `head_object`, `list_objects_v2`, `delete_objects`.
  - `manifest.py` parses/validates manifest content against `expected/manifest_schema.json`.
  - `logs.py` computes throughput stats.

- Create Btrfs fixture tools (Python scripts).
  - `setup_btrfs.py` creates a loopback image in `integration_tests/run/`, sets up loop device, formats Btrfs, mounts into `integration_tests/run/mnt`, and creates `data/root/home`.
  - `setup_btrfs.py` should chown `integration_tests/run/` to `SUDO_USER` so non-root scripts can run after setup.
  - `seed_data.py` writes deterministic files (fixed sizes) to each subvolume.
  - `mutate_data.py` makes known changes for incremental runs.
  - Both scripts accept a dataset size option to generate multi-chunk fixtures.
  - `teardown_btrfs.py` unmounts and detaches loop device.

- Implement E2E run scripts (Python entry points).
  - `run_full.py` runs a full backup via the CLI contract.
  - `run_incremental.py` mutates then runs incremental.
  - `run_interrupt.py` starts a backup, kills it mid-stream, then reruns and verifies proper completion.
  - `run_restore.py` restores into a new subvolume target.
  - `run_large.py` runs full + incremental with `test_large.toml` to force
    multi-chunk uploads.
  - Restore runner should tolerate storage class restore delays (wait/poll).

- Verification scripts (Python + boto3).
  - `verify_manifest.py` checks schema, chunk ordering, hash fields, backup type.
  - `verify_s3.py` checks object layout, storage class, and SSE-S3 via `head_object`.
  - `verify_retention.py` checks snapshot retention rules on the local fixture.
  - `verify_restore.py` compares restored data to the source snapshot (metadata + file hashes).

- Benchmarking.
  - `benchmark.py` parses logs to produce `integration_tests/run/logs/benchmark.json` with bytes, elapsed time, and throughput.

- Orchestration.
  - `run_all.py` sequences: setup -> seed -> full -> mutate -> incremental -> interrupt -> restore -> verify -> teardown.
  - `run_all.py` optionally runs the multi-chunk scenario (full + incremental).
  - `cleanup_s3_prefix.py` removes test objects under the configured prefix.

- Documentation and hygiene.
  - `integration_tests/README.md` includes prerequisites (btrfs-progs, Python 3.14, boto3), AWS setup, quickstart, and lifecycle rule guidance.
  - `integration_tests/.gitignore` excludes `integration_tests/run/` and local env files.

If you want a different CLI shape than `python -m btrfs_to_s3`, tell me the exact command and flags and I’ll encode it in the harness plan and config.
