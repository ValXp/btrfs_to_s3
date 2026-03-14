"""Run a standalone ZFS full send/receive probe."""

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


LOG_NAME = "run_zfs_snapshot_send_receive.log"
PROBE_FILE = "probe/full_send_receive.txt"
PROBE_CONTENT = "standalone full send/receive probe\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a standalone ZFS full send/receive probe.")
    parser.add_argument("--config", default=support.DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = support.load_zfs_config(config_path)
    paths = config["paths"]
    zfs_cfg = config["zfs"]

    log_path = os.path.join(paths["logs_dir"], LOG_NAME)
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            _run_probe(zfs_cfg, log)
        except Exception as exc:
            log.write(f"full send/receive probe failed: {_describe_exception(exc)}", level="ERROR")
            return 1

    return 0


def _run_probe(zfs_cfg: dict[str, object], log) -> None:
    source_dataset = support.source_dataset(zfs_cfg)
    receive_parent = support.receive_parent_dataset(zfs_cfg)
    receive_dataset = support.receive_dataset(zfs_cfg, source_dataset)
    prefix = zfs_cfg["snapshot_prefix"]
    source_snapshot = support.snapshot_name(source_dataset, prefix, "full")
    received_snapshot = support.snapshot_name(receive_dataset, prefix, "full")

    log.write(f"writing probe data to {source_dataset}")
    support.write_dataset_text(source_dataset, PROBE_FILE, PROBE_CONTENT)

    zfs.create_snapshot(source_snapshot)
    log.write(f"created snapshot {source_snapshot}")

    transferred = support.stream_send_receive(source_snapshot, receive_dataset)
    log.write(f"streamed {transferred} bytes into {receive_dataset}")

    datasets = zfs.list_datasets(receive_parent)
    if receive_dataset not in datasets:
        raise RuntimeError(f"received dataset missing from zfs list output: {receive_dataset}")

    snapshots = zfs.list_snapshots(receive_dataset)
    if received_snapshot not in snapshots:
        raise RuntimeError(f"received snapshot missing from zfs list output: {received_snapshot}")

    received_content = support.read_dataset_text(receive_dataset, PROBE_FILE)
    if received_content != PROBE_CONTENT:
        raise RuntimeError("received probe data did not match source content")

    log.write("full ZFS send/receive probe completed")


def _describe_exception(exc: Exception) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = (exc.stderr or "").strip()
        if stderr:
            return stderr
    return str(exc)


if __name__ == "__main__":
    raise SystemExit(main())
