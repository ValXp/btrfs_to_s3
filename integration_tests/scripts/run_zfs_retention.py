"""Run a standalone ZFS retention probe."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness import zfs
from harness.logs import open_log
from scripts import zfs_probe_support as support


LOG_NAME = "run_zfs_retention.log"
PROBE_FILE = "probe/retention.txt"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a standalone ZFS retention probe.")
    parser.add_argument("--config", default=support.DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = support.load_zfs_config(config_path)
    paths = config["paths"]
    zfs_cfg = config["zfs"]
    backup_cfg = config["backup"]

    log_path = os.path.join(paths["logs_dir"], LOG_NAME)
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            _run_probe(zfs_cfg, backup_cfg["retention_snapshots"], log)
        except Exception as exc:
            log.write(f"retention probe failed: {_describe_exception(exc)}", level="ERROR")
            return 1

    return 0


def _run_probe(zfs_cfg: dict[str, object], retention: int, log) -> None:
    if retention < 1:
        raise RuntimeError("backup.retention_snapshots must be >= 1 for the retention probe")

    source_dataset = support.source_dataset(zfs_cfg)
    prefix = zfs_cfg["snapshot_prefix"]
    snapshots: list[str] = []
    total_snapshots = retention + 1

    support.write_dataset_text(source_dataset, PROBE_FILE, "generation-1\n")
    for index in range(total_snapshots):
        if index > 0:
            support.append_dataset_text(
                source_dataset,
                PROBE_FILE,
                f"generation-{index + 1}\n",
            )
        snapshot = support.snapshot_name(source_dataset, prefix, f"retention-{index + 1}")
        zfs.create_snapshot(snapshot)
        snapshots.append(snapshot)
        log.write(f"created snapshot {snapshot}")

    destroyed = snapshots[:-retention]
    latest_snapshot = snapshots[-1]
    missing_parent = destroyed[-1]

    for snapshot in destroyed:
        zfs.destroy_snapshot(snapshot)
        log.write(f"destroyed snapshot {snapshot}")

    remaining = zfs.list_snapshots(source_dataset)
    if missing_parent in remaining:
        raise RuntimeError(f"retention did not prune expected parent snapshot: {missing_parent}")
    if latest_snapshot not in remaining:
        raise RuntimeError(f"latest snapshot missing after retention: {latest_snapshot}")

    try:
        support.drain_send_stream(latest_snapshot, parent_snapshot=missing_parent)
    except RuntimeError as exc:
        log.write(f"incremental send failed as expected after pruning {missing_parent}: {exc}")
        log.write("retention ZFS probe completed")
        return

    raise RuntimeError(
        "incremental send unexpectedly succeeded after pruning the incremental parent"
    )


def _describe_exception(exc: Exception) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = (exc.stderr or "").strip()
        if stderr:
            return stderr
    return str(exc)


if __name__ == "__main__":
    raise SystemExit(main())
