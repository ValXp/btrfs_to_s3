# btrfs_to_s3

Backup tooling for Btrfs snapshots to AWS S3.

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
