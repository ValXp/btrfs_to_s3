"""Run a standalone ZFS incremental send/receive probe."""

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


LOG_NAME = "run_zfs_incremental.log"
PROBE_FILE = "probe/incremental.txt"
BASE_CONTENT = "base generation\n"
INCREMENTAL_CONTENT = "incremental generation\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a standalone ZFS incremental probe.")
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
            log.write(f"incremental probe failed: {_describe_exception(exc)}", level="ERROR")
            return 1

    return 0


def _run_probe(zfs_cfg: dict[str, object], log) -> None:
    source_dataset = support.source_dataset(zfs_cfg)
    purpose = "the standalone incremental send/receive probe"
    receive_dataset = (
        f"{support.receive_dataset(zfs_cfg, source_dataset, purpose=purpose)}"
        "-incremental"
    )
    prefix = zfs_cfg["snapshot_prefix"]

    base_snapshot = support.snapshot_name(source_dataset, prefix, "base")
    incremental_snapshot = support.snapshot_name(source_dataset, prefix, "incremental")
    received_base_snapshot = support.snapshot_name(receive_dataset, prefix, "base")
    received_incremental_snapshot = support.snapshot_name(
        receive_dataset,
        prefix,
        "incremental",
    )

    log.write(f"writing base probe data to {source_dataset}")
    support.write_dataset_text(source_dataset, PROBE_FILE, BASE_CONTENT)
    zfs.create_snapshot(base_snapshot)
    log.write(f"created base snapshot {base_snapshot}")

    transferred = support.stream_send_receive(base_snapshot, receive_dataset)
    log.write(f"streamed full base snapshot ({transferred} bytes)")

    support.append_dataset_text(source_dataset, PROBE_FILE, INCREMENTAL_CONTENT)
    zfs.create_snapshot(incremental_snapshot)
    log.write(f"created incremental snapshot {incremental_snapshot}")

    transferred = support.stream_send_receive(
        incremental_snapshot,
        receive_dataset,
        parent_snapshot=base_snapshot,
        force=True,
    )
    log.write(f"streamed incremental snapshot ({transferred} bytes)")

    snapshots = zfs.list_snapshots(receive_dataset)
    for snapshot in (received_base_snapshot, received_incremental_snapshot):
        if snapshot not in snapshots:
            raise RuntimeError(f"received snapshot missing from zfs list output: {snapshot}")

    expected_content = BASE_CONTENT + INCREMENTAL_CONTENT
    received_content = support.read_dataset_text(receive_dataset, PROBE_FILE)
    if received_content != expected_content:
        raise RuntimeError("received incremental data did not match source content")

    log.write("incremental ZFS probe completed")


def _describe_exception(exc: Exception) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = (exc.stderr or "").strip()
        if stderr:
            return stderr
    return str(exc)


if __name__ == "__main__":
    raise SystemExit(main())
