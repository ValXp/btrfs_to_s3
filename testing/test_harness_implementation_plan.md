Here’s a concise implementation plan for a Python-first test harness under `testing/`, using Python 3.14 and boto3, and explicitly wiring how to call the `btrfs_to_s3` CLI.

Implementation Plan

- Define the harness layout under `testing/` only.
  ```text
  testing/
    README.md
    .gitignore
    pyproject.toml
    config/
      test.toml
      test.env
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
  - `testing/config/test.toml` includes:
    - S3 bucket/region/prefix, storage class, chunk size, concurrency.
    - Local paths (`run/` dirs, scratch, lock, snapshots).
    - CLI invocation contract:
      ```toml
      [tool]
      cmd = ["python", "-m", "btrfs_to_s3"]
      config_flag = "--config"
      ```
  - `testing/harness/runner.py` reads `tool.cmd` or `BTRFS_TO_S3_CMD` env override and runs the CLI with the test config path.
  - Optionally set `PYTHONPATH` to repo root inside `runner.py` if not installed.

- Build Python harness modules (minimal deps).
  - `config.py` loads `test.toml` (use stdlib `tomllib` in 3.14).
  - `env.py` loads `test.env` and normalizes paths.
  - `assertions.py` provides simple fail-fast helpers.
  - `btrfs.py` wraps `mkfs.btrfs`, `mount`, `subvolume`, `snapshot` via `subprocess.run`.
  - `aws.py` uses `boto3` for `head_object`, `list_objects_v2`, `delete_objects`.
  - `manifest.py` parses/validates manifest content against `expected/manifest_schema.json`.
  - `logs.py` computes throughput stats.

- Create Btrfs fixture tools (Python scripts).
  - `setup_btrfs.py` creates a loopback image in `testing/run/`, sets up loop device, formats Btrfs, mounts into `testing/run/mnt`, and creates `data/root/home`.
  - `setup_btrfs.py` should chown `testing/run/` to `SUDO_USER` so non-root scripts can run after setup.
  - `seed_data.py` writes deterministic files (fixed sizes) to each subvolume.
  - `mutate_data.py` makes known changes for incremental runs.
  - `teardown_btrfs.py` unmounts and detaches loop device.

- Implement E2E run scripts (Python entry points).
  - `run_full.py` runs a full backup via the CLI contract.
  - `run_incremental.py` mutates then runs incremental.
  - `run_interrupt.py` starts a backup, kills it mid-stream, then reruns and verifies proper completion.
  - `run_restore.py` restores into a new subvolume target.
  - Restore runner should tolerate storage class restore delays (wait/poll).

- Verification scripts (Python + boto3).
  - `verify_manifest.py` checks schema, chunk ordering, hash fields, backup type.
  - `verify_s3.py` checks object layout, storage class, and SSE-S3 via `head_object`.
  - `verify_retention.py` checks snapshot retention rules on the local fixture.
  - `verify_restore.py` compares restored data to the source snapshot (metadata + file hashes).

- Benchmarking.
  - `benchmark.py` parses logs to produce `testing/run/logs/benchmark.json` with bytes, elapsed time, and throughput.

- Orchestration.
  - `run_all.py` sequences: setup -> seed -> full -> mutate -> incremental -> interrupt -> restore -> verify -> teardown.
  - `cleanup_s3_prefix.py` removes test objects under the configured prefix.

- Documentation and hygiene.
  - `testing/README.md` includes prerequisites (btrfs-progs, Python 3.14, boto3), AWS setup, quickstart, and lifecycle rule guidance.
  - `testing/.gitignore` excludes `testing/run/` and local env files.

If you want a different CLI shape than `python -m btrfs_to_s3`, tell me the exact command and flags and I’ll encode it in the harness plan and config.
