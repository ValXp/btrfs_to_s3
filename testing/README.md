# btrfs_to_s3 test harness

This directory contains a Python-first test harness for running end-to-end backups
against a loopback Btrfs filesystem and AWS S3. All runtime artifacts are
created under `testing/run/` (generated).

Prerequisites
- Python 3.14
- btrfs-progs (mkfs.btrfs, btrfs)
- util-linux (losetup, mount, umount)
- AWS credentials with access to the test bucket/prefix
  - Note: `losetup` and `mkfs.btrfs` often live in `/usr/sbin`. If your PATH
    doesn't include `/usr/sbin`, run via `sudo -E` so the harness helpers
    can find them.

AWS test bucket/prefix guidance
- Use a dedicated bucket or a dedicated prefix within a shared bucket.
- Example prefix: `btrfs-to-s3-test/`
- Consider a lifecycle rule to expire test objects and control costs.
- For tests, use a non-Glacier storage class unless you accept restore delays.
- Archive restore checks require `s3:RestoreObject` and can take hours depending
  on the storage class/tier; expect additional retrieval costs.

Configuration
- `testing/config/test.toml` controls harness settings and S3 parameters.
- `testing/config/test_large.toml` forces multi-chunk uploads with a smaller
  chunk size and larger dataset defaults.
- `testing/config/test_archive.toml` uses an archival storage class and overrides
  restore wait/timeout settings.
- `testing/config/test.env` holds AWS credentials and optional overrides.
- Set all `CHANGE_ME` values before running tests.

BTRFS_TO_S3_CMD override
- Optional: set `BTRFS_TO_S3_CMD` in `testing/config/test.env` as a JSON array.
- The runner expects a JSON array only; shell-style strings are rejected.
- Example: `["python", "-m", "btrfs_to_s3"]`

Quickstart
1. Create a virtualenv and install dependencies:
   - `python3.14 -m venv testing/.venv`
   - `testing/.venv/bin/pip install -r testing/requirements.txt`
2. Edit `testing/config/test.toml` and `testing/config/test.env`.
3. Load AWS credentials (so sudo preserves them):
   - `set -a; . testing/config/test.env; set +a`
4. Run the full harness (from repo root so paths match config):
   - `sudo -E python3 testing/scripts/run_all.py --config testing/config/test.toml`
   - Optional: add `--skip-s3` to run local setup/seed/mutate without S3.
   - Optional: add `--include-large` to run the multi-chunk scenario.
5. Run the multi-chunk scenario:
   - `sudo -E python3 testing/scripts/run_large.py --config testing/config/test_large.toml`
6. Run the archive restore checks (optional):
   - `sudo -E python3 testing/scripts/run_restore_archive.py --config testing/config/test_archive.toml`

Clearing logs between runs
- Log paths are driven by `paths.logs_dir` in the config. To avoid stale logs:
  - `find testing/run/logs -type f -delete`
  - `find testing/run/large/logs -type f -delete`
  - `find testing/run/small/logs -type f -delete`

Small vs large dataset scenarios
- Small dataset / large chunk (single chunk expected):
  - Config: `testing/config/test_small.toml` (1 MiB dataset, 10 MiB chunks).
  - Run: `sudo -E python3 testing/scripts/run_full.py --config testing/config/test_small.toml`
  - Verify: `sudo -E python3 testing/scripts/verify_manifest.py --config testing/config/test_small.toml`
- Large dataset / smaller chunk (multi-chunk expected):
  - Config: `testing/config/test_large.toml` (size and chunk tunable).
  - Run: `sudo -E python3 testing/scripts/run_large.py --config testing/config/test_large.toml`

Sudo + environment notes
- `setup_btrfs.py` and `teardown_btrfs.py` require sudo.
- Use `sudo -E` after `set -a; . testing/config/test.env; set +a` so the
  boto3 client sees `AWS_*` credentials.
- If you don't want to use sudo for non-privileged steps, you can run only
  setup/teardown with sudo and the rest unprivileged, but keep the same env.

Privilege model
- Run `testing/scripts/setup_btrfs.py` with sudo. It will chown `testing/run/` to
  `SUDO_USER` so seed/mutate/verify scripts can run without sudo.
- Run `testing/scripts/teardown_btrfs.py` with sudo to unmount and detach the loop
  device.

Notes
- Btrfs setup/teardown steps require root privileges.
- Logs are written under `testing/run/logs/`.
- The runner adds the repo root to `PYTHONPATH` if it is not already set.
