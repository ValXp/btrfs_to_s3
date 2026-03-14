"""Verify snapshot retention on the local fixture."""

from __future__ import annotations

import argparse
import os
import re
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness import btrfs
from harness import zfs
from harness.config import load_config
from harness.filesystem import backend_name, source_identifiers
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
        retention = backup_cfg["retention_snapshots"]
        backend = backend_name(config)
        if backend == "zfs":
            if not _verify_zfs_retention(config, retention, log):
                return 1
        else:
            snapshots_dir = paths["snapshots_dir"]
            if not os.path.isdir(snapshots_dir):
                log.write(f"missing snapshots dir {snapshots_dir}", level="ERROR")
                return 1
            snapshots = btrfs.list_snapshots(paths["mount_dir"], snapshots_dir)
            if not _verify_btrfs_retention(snapshots, retention, log):
                return 1

        log.write("snapshot retention within limits")
    return 0


def _parse_snapshot_subvolume(name: str) -> str | None:
    match = re.match(r"^(?P<subvol>.+)__\d{8}T\d{6}Z__(full|inc)$", name)
    if not match:
        return None
    return match.group("subvol")


def _verify_btrfs_retention(snapshots: list[str], retention: int, log) -> bool:
    log.write(f"found {len(snapshots)} snapshots, retention {retention}")

    per_subvolume: dict[str, list[str]] = {}
    for snapshot in snapshots:
        name = os.path.basename(snapshot)
        subvolume = _parse_snapshot_subvolume(name)
        if not subvolume:
            log.write(f"skipping non-matching snapshot {snapshot}", level="WARN")
            continue
        per_subvolume.setdefault(subvolume, []).append(snapshot)

    return _log_retention_counts(per_subvolume, retention, log)


def _verify_zfs_retention(config: dict, retention: int, log) -> bool:
    snapshot_prefix = config["zfs"]["snapshot_prefix"]
    per_source: dict[str, list[str]] = {}
    for source in source_identifiers(config):
        snapshots = zfs.list_snapshots(source)
        matching = [
            snapshot
            for snapshot in snapshots
            if snapshot.startswith(f"{source}@{snapshot_prefix}-")
        ]
        per_source[source] = matching

    total = sum(len(items) for items in per_source.values())
    log.write(f"found {total} snapshots, retention {retention}")
    return _log_retention_counts(per_source, retention, log)


def _log_retention_counts(
    per_source: dict[str, list[str]],
    retention: int,
    log,
) -> bool:
    over_limit = False
    for source, items in sorted(per_source.items()):
        count = len(items)
        log.write(f"subvolume {source} snapshots={count}")
        if count > retention:
            over_limit = True
            log.write(
                f"snapshot retention exceeded for {source}", level="ERROR"
            )
            for snapshot in sorted(items):
                log.write(f"snapshot: {snapshot}", level="ERROR")
    return not over_limit


if __name__ == "__main__":
    raise SystemExit(main())
