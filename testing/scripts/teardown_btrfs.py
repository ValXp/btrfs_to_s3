"""Tear down the loopback Btrfs fixture."""

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
LOOP_DEVICE_FILE = "loop_device"


def main() -> int:
    parser = argparse.ArgumentParser(description="Tear down loopback Btrfs fixture.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    logs_dir = paths["logs_dir"]
    log_path = os.path.join(logs_dir, "teardown_btrfs.log")
    os.makedirs(logs_dir, exist_ok=True)

    loop_device_path = os.path.join(paths["run_dir"], LOOP_DEVICE_FILE)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        loop_device = None
        if os.path.exists(loop_device_path):
            with open(loop_device_path, "r", encoding="utf-8") as handle:
                loop_device = handle.read().strip() or None
            log.write(f"read loop device {loop_device!r}")
        else:
            log.write(f"missing loop device file {loop_device_path}", level="ERROR")
            return 1

        try:
            btrfs.teardown(
                paths["mount_dir"],
                loop_device,
                run_dir=paths["run_dir"],
            )
            log.write("teardown completed")
        except Exception as exc:
            log.write(f"teardown failed: {exc}", level="ERROR")
            return 1

        try:
            os.remove(loop_device_path)
        except FileNotFoundError:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
