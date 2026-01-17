# btrfs_to_s3 test harness

This directory contains a Python-first test harness for running end-to-end backups
against a loopback Btrfs filesystem and AWS S3. All runtime artifacts are
created under `testing/run/` (generated).

Prerequisites
- Python 3.14
- btrfs-progs (mkfs.btrfs, btrfs)
- util-linux (losetup, mount, umount)
- AWS credentials with access to the test bucket/prefix

AWS test bucket/prefix guidance
- Use a dedicated bucket or a dedicated prefix within a shared bucket.
- Example prefix: `btrfs-to-s3-test/`
- Consider a lifecycle rule to expire test objects and control costs.
- For tests, use a non-Glacier storage class unless you accept restore delays.

Configuration
- `testing/config/test.toml` controls harness settings and S3 parameters.
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
3. Run the full harness:
   - `python testing/scripts/run_all.py --config testing/config/test.toml`

Privilege model
- Run `testing/scripts/setup_btrfs.py` with sudo. It will chown `testing/run/` to
  `SUDO_USER` so seed/mutate/verify scripts can run without sudo.
- Run `testing/scripts/teardown_btrfs.py` with sudo to unmount and detach the loop
  device.

Notes
- Btrfs setup/teardown steps require root privileges.
- Logs are written under `testing/run/logs/`.
- The runner adds the repo root to `PYTHONPATH` if it is not already set.
