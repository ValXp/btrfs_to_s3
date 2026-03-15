# btrfs_to_s3 test harness

This directory contains a Python-first harness for disposable Btrfs and ZFS
fixtures plus AWS S3 test runs. The Btrfs path and the default ZFS
`run_all.py` path both exercise the main `python -m btrfs_to_s3` flow.
Standalone ZFS send/receive/retention probes remain available for low-level
diagnostics against the disposable file-backed pool.
All runtime artifacts are created under `integration_tests/run/` (generated).

Prerequisites
- Python 3.14
- btrfs-progs (mkfs.btrfs, btrfs)
- util-linux (losetup, mount, umount)
- For ZFS configs: OpenZFS userland and kernel support (`zpool`, `zfs`)
- AWS credentials with access to the test bucket/prefix
  - Note: `losetup` and `mkfs.btrfs` often live in `/usr/sbin`. If your PATH
    doesn't include `/usr/sbin`, run via `sudo -E` so the harness helpers
    can find them.
  - For ZFS-backed runs, you also need privileges to create and destroy a
    disposable file-backed pool under `integration_tests/run/`.

AWS test bucket/prefix guidance
- Use a dedicated bucket or a dedicated prefix within a shared bucket.
- Example prefix: `btrfs-to-s3-test/`
- Consider a lifecycle rule to expire test objects and control costs.
- For tests, use a non-Glacier storage class unless you accept restore delays.
- Archive restore checks require `s3:RestoreObject` and can take hours depending
  on the storage class/tier; expect additional retrieval costs.

Configuration
- `integration_tests/config/test.toml` controls harness settings and S3 parameters.
- `integration_tests/config/test_zfs.toml` is the backend-aware template for a
  disposable file-backed ZFS pool rooted under `integration_tests/run/zfs/`.
- `integration_tests/config/test_large.toml` forces multi-chunk uploads with a smaller
  chunk size and larger dataset defaults.
- `integration_tests/config/test_archive.toml` uses an archival storage class and overrides
  restore wait/timeout settings.
- `integration_tests/config/test.env` holds AWS credentials and optional overrides.
- Set all `CHANGE_ME` values before running tests.

Config structure
- Shared sections for all backends: `[tool]`, `[paths]`, `[aws]`, `[backup]`.
- Backend selection: `[filesystem] backend = "btrfs"` or `"zfs"`.
- Backward compatibility: if `[filesystem]` is omitted, the loader treats the
  config as the legacy Btrfs shape used by `integration_tests/config/test.toml`.
- Btrfs-specific config:
  - `paths.btrfs_image`, `paths.mount_dir`, `paths.data_dir`, `paths.snapshots_dir`
  - `[btrfs]` with `loopback_size_gib`, `mount_options`, and `subvolumes`
- ZFS-specific config:
  - `[zfs]` with `pool_name`, `pool_file`, `pool_size_gib`, `mount_root`,
    `source_datasets`, `receive_parent_dataset`, and `snapshot_prefix`
  - optional `zpool_create_args` and `zfs_create_args` for pool or dataset defaults

How the ZFS harness differs
- The Btrfs harness creates a loopback image, mounts one filesystem, and works
  with subvolume paths under that mount.
- The ZFS harness is designed around a disposable file-backed pool plus dataset
  names, not Btrfs path keys. Source definitions live in `[zfs]`, and mounts are
  derived from `mount_root` instead of `paths.mount_dir`.
- `integration_tests/scripts/run_all.py` dispatches setup/teardown from
  `[filesystem].backend`. For ZFS configs it now runs the application-backed
  sequence: `seed_data.py`, `run_full.py`, `mutate_data.py`,
  `run_incremental.py --skip-mutate`, `run_interrupt.py`,
  `verify_manifest.py`, `verify_s3.py`, `verify_retention.py`,
  `run_restore.py --source all`, and `verify_restore.py --source all`.
- When `setup_zfs.py` creates a fresh disposable pool, it clears stale local
  harness metadata in `paths.run_dir` such as `state.json`, `manifest.json`,
  and restore target metadata so reruns do not inherit application state from a
  previous pool.
- The standalone ZFS probe scripts (`run_zfs_snapshot_send_receive.py`,
  `run_zfs_incremental.py`, `run_zfs_retention.py`) are still available, but
  they are now manual diagnostics instead of the only ZFS path in `run_all.py`.
- `verify_retention.py` now supports ZFS as well, so the default ZFS
  `run_all.py` path covers retention checks in addition to backup/restore.
- `verify_restore.py` mirrors the application's ZFS verification strategy. When
  `.zfs/snapshot/<name>` is unavailable on the host, the script creates a
  temporary clone of the source snapshot, compares the restored tree against
  that clone, then unmounts and destroys the clone.

BTRFS_TO_S3_CMD override
- Optional: set `BTRFS_TO_S3_CMD` in `integration_tests/config/test.env` as a JSON array.
- The runner expects a JSON array only; shell-style strings are rejected.
- Example: `["python", "-m", "btrfs_to_s3"]`

Quickstart
1. Create a virtualenv and install dependencies:
   - `python3.14 -m venv integration_tests/.venv`
   - `integration_tests/.venv/bin/pip install -r integration_tests/requirements.txt`
2. Edit the config you plan to run:
   - Btrfs: `integration_tests/config/test.toml`
   - ZFS application flow: `integration_tests/config/test_zfs.toml`
   - Shared credentials/env: `integration_tests/config/test.env`
3. Load AWS credentials (so sudo preserves them):
   - `set -a; . integration_tests/config/test.env; set +a`
4. Run the full harness (from repo root so paths match config):
   - `sudo -E python3 integration_tests/scripts/run_all.py --config integration_tests/config/test.toml`
   - Optional: add `--skip-s3` to run local Btrfs setup/seed/mutate without S3.
   - Optional: add `--include-large` to run the Btrfs multi-chunk scenario.
5. Run the ZFS application-backed harness:
   - `sudo -E python3 integration_tests/scripts/run_all.py --config integration_tests/config/test_zfs.toml`
   - This runs setup, seed, full backup, mutate, incremental backup,
     interrupt/retry, manifest verification, S3 verification, retention
     verification, restore, verify_restore, and teardown.
   - `--skip-s3` reduces the ZFS run to setup, seed, mutate, and teardown when
     you only want to validate the disposable fixture without AWS.
6. Run individual ZFS application-backed steps manually (use `sudo -E` for the
   full ZFS flow so snapshot, receive, and temporary verify clones all have the
   required privileges):
   - `sudo -E python3 integration_tests/scripts/setup_zfs.py --config integration_tests/config/test_zfs.toml`
   - `sudo -E python3 integration_tests/scripts/seed_data.py --config integration_tests/config/test_zfs.toml`
   - `sudo -E python3 integration_tests/scripts/run_full.py --config integration_tests/config/test_zfs.toml`
   - `sudo -E python3 integration_tests/scripts/run_incremental.py --config integration_tests/config/test_zfs.toml`
   - `sudo -E python3 integration_tests/scripts/run_interrupt.py --config integration_tests/config/test_zfs.toml --source tank/data`
   - `sudo -E python3 integration_tests/scripts/verify_manifest.py --config integration_tests/config/test_zfs.toml`
   - `sudo -E python3 integration_tests/scripts/verify_s3.py --config integration_tests/config/test_zfs.toml`
   - `sudo -E python3 integration_tests/scripts/verify_retention.py --config integration_tests/config/test_zfs.toml`
   - `sudo -E python3 integration_tests/scripts/run_restore.py --config integration_tests/config/test_zfs.toml --source tank/data`
   - `sudo -E python3 integration_tests/scripts/verify_restore.py --config integration_tests/config/test_zfs.toml --source tank/data`
   - `sudo -E python3 integration_tests/scripts/teardown_zfs.py --config integration_tests/config/test_zfs.toml`
7. Run the standalone ZFS probe scripts manually when you need raw send/receive
   or retention diagnostics outside the application flow:
   - `python3 integration_tests/scripts/run_zfs_snapshot_send_receive.py --config integration_tests/config/test_zfs.toml`
   - `python3 integration_tests/scripts/run_zfs_incremental.py --config integration_tests/config/test_zfs.toml`
   - `python3 integration_tests/scripts/run_zfs_retention.py --config integration_tests/config/test_zfs.toml`
8. Run the multi-chunk scenario:
   - `sudo -E python3 integration_tests/scripts/run_large.py --config integration_tests/config/test_large.toml`
   - `--include-large` is only defined for the Btrfs harness right now.
9. Run the archive restore checks (optional):
   - `sudo -E python3 integration_tests/scripts/run_restore_archive.py --config integration_tests/config/test_archive.toml`

Clearing logs between runs
- Log paths are driven by `paths.logs_dir` in the config. To avoid stale logs:
  - `find integration_tests/run/logs -type f -delete`
  - `find integration_tests/run/large/logs -type f -delete`
  - `find integration_tests/run/small/logs -type f -delete`

Small vs large dataset scenarios (Btrfs)
- Small dataset / large chunk (single chunk expected):
  - Config: `integration_tests/config/test_small.toml` (1 MiB dataset, 10 MiB chunks).
  - Run: `sudo -E python3 integration_tests/scripts/run_full.py --config integration_tests/config/test_small.toml`
  - Verify: `sudo -E python3 integration_tests/scripts/verify_manifest.py --config integration_tests/config/test_small.toml`
- Large dataset / smaller chunk (multi-chunk expected):
  - Config: `integration_tests/config/test_large.toml` (size and chunk tunable).
  - Run: `sudo -E python3 integration_tests/scripts/run_large.py --config integration_tests/config/test_large.toml`

Sudo + environment notes
- `setup_btrfs.py` and `teardown_btrfs.py` require sudo.
- Use `sudo -E` after `set -a; . integration_tests/config/test.env; set +a` so the
  boto3 client sees `AWS_*` credentials.
- If you don't want to use sudo for non-privileged steps, you can run only
  setup/teardown with sudo and the rest unprivileged, but keep the same env.

Privilege model
- Run `integration_tests/scripts/setup_btrfs.py` with sudo. It will chown `integration_tests/run/` to
  `SUDO_USER` so seed/mutate/verify scripts can run without sudo.
- Run `integration_tests/scripts/teardown_btrfs.py` with sudo to unmount and detach the loop
  device.
- Run `integration_tests/scripts/setup_zfs.py` and `teardown_zfs.py` with sudo
  or equivalent privileges so the harness can create/destroy the disposable pool
  and its datasets.
- For ZFS, keep the same privilege level for `run_full.py`, `run_incremental.py`,
  `run_interrupt.py`, `run_restore.py`, `verify_retention.py`, and
  `verify_restore.py` because they invoke `zfs snapshot`, `zfs receive`, or
  temporary clone/unmount/destroy operations.

Notes
- Btrfs and ZFS setup/teardown steps require elevated privileges.
- Logs are written under the configured `paths.logs_dir`.
- The runner adds the repo root to `PYTHONPATH` if it is not already set.
