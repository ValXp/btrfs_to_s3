# btrfs_to_s3

Backup tooling for Btrfs snapshots to AWS S3.

## Usage

`btrfs_to_s3` reads a TOML config file and supports two commands:

```sh
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --subvolume data --target /srv/restore/data
```

### Manual runs

Manual runs are useful when you want to:
- Run an out-of-band backup (e.g., right before a risky upgrade).
- Force a backup even if it is not due per schedule.
- Restrict a backup to specific subvolumes.
- Skip uploads to S3 to validate snapshot creation locally.

Examples:

```sh
# Plan only (skip S3 upload entirely).
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --dry-run

# Force a run regardless of schedule.
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --once

# Back up only specific subvolumes (repeatable).
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --subvolume data --subvolume root

# Validate snapshot creation without uploading to S3.
python3 -m btrfs_to_s3 backup --config /etc/btrfs_to_s3/config.toml --no-s3
```

Restores can override manifest selection and verification:

```sh
# Restore the current manifest chain for a subvolume.
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --subvolume data --target /srv/restore/data

# Restore from a specific manifest key and skip verification.
python3 -m btrfs_to_s3 restore --config /etc/btrfs_to_s3/config.toml --subvolume data --target /srv/restore/data \
  --manifest-key subvol/data/full/manifest.json --verify none
```

AWS credentials are detected via `AWS_PROFILE` or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.

## Configuration

`btrfs_to_s3` expects an absolute path to `config.toml`. All paths in the config must be absolute.
Required fields: `subvolumes.paths`, `s3.bucket`, `s3.region`, `s3.prefix`.

Example `config.toml`:

```toml
[global]
log_level = "info" # debug|info|warning|error|critical
state_path = "/var/lib/btrfs_to_s3/state.json"
lock_path = "/var/lock/btrfs_to_s3.lock"
spool_dir = "/mnt/ssd/btrfs_to_s3_spool"
spool_size_bytes = 214748364800

[schedule]
full_every_days = 180
incremental_every_days = 7
run_at = "02:00"

[snapshots]
base_dir = "/srv/snapshots"
retain = 2

[subvolumes]
paths = ["/srv/data", "/srv/home"]

[s3]
bucket = "my-backup-bucket"
region = "us-east-1"
prefix = "btrfs/host-01"
chunk_size_bytes = 214748364800
storage_class_chunks = "DEEP_ARCHIVE"
storage_class_manifest = "STANDARD"
concurrency = 4
spool_enabled = false
sse = "AES256"

[restore]
target_base_dir = "/srv/restore"
verify_mode = "full" # full|sample|none
sample_max_files = 1000
wait_for_restore = true
restore_timeout_seconds = 259200
restore_tier = "Standard"
```

You can copy `config.example.toml` as a starting point.

Notes:
- `schedule` controls when backups are due; `backup --once` ignores the schedule.
- `snapshots.base_dir` is where snapshots are created and retained locally.
- `subvolumes.paths` must include at least one subvolume path.
- `s3.prefix` is the root path inside the bucket for all backup data.
- `s3.spool_enabled` enables spooling multipart uploads to `global.spool_dir`.
- `restore.verify_mode` controls post-restore verification.

## Development

- Package code lives in `btrfs_to_s3/`.
- Unit tests live in `tests/`.

Run tests:

```sh
python3 -m pytest
```

Run the package entrypoint stub:

```sh
python3 -m btrfs_to_s3
```

## Systemd setup

1. Install unit files:

```sh
sudo cp systemd/btrfs_to_s3.service /etc/systemd/system/btrfs_to_s3.service
sudo cp systemd/btrfs_to_s3.timer /etc/systemd/system/btrfs_to_s3.timer
```

2. Ensure `/etc/btrfs_to_s3/config.toml` exists and matches your host paths.
3. Enable the timer:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs_to_s3.timer
```

Logs live in the systemd journal:

```sh
journalctl -u btrfs_to_s3.service
```

Manual systemd run:

```sh
sudo systemctl start btrfs_to_s3.service
```

If you need a one-off run with different flags or a different config, run the CLI directly instead of systemd (see "Manual runs" above).
