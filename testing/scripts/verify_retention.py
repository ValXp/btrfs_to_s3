"""Verify snapshot retention on the local fixture."""

from __future__ import annotations

import argparse
import os
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness import btrfs
from harness.config import load_config
from harness.logs import open_log


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify snapshot retention.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    backup_cfg = config["backup"]

    log_path = os.path.join(paths["logs_dir"], "verify_retention.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        snapshots_dir = paths["snapshots_dir"]
        if not os.path.isdir(snapshots_dir):
            log.write(f"missing snapshots dir {snapshots_dir}", level="ERROR")
            return 1

        snapshots = btrfs.list_snapshots(paths["mount_dir"], snapshots_dir)
        retention = backup_cfg["retention_snapshots"]
        log.write(f"found {len(snapshots)} snapshots, retention {retention}")

        if len(snapshots) > retention:
            log.write("snapshot retention exceeded", level="ERROR")
            for snapshot in snapshots:
                log.write(f"snapshot: {snapshot}", level="ERROR")
            return 1

        log.write("snapshot retention within limits")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
