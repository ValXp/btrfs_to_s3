"""Set up a loopback Btrfs fixture for tests."""

from __future__ import annotations

import argparse
import os
import sys
import pwd

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
    parser = argparse.ArgumentParser(description="Set up loopback Btrfs fixture.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    btrfs_cfg = config["btrfs"]

    logs_dir = paths["logs_dir"]
    log_path = os.path.join(logs_dir, "setup_btrfs.log")
    os.makedirs(logs_dir, exist_ok=True)

    loop_device_path = os.path.join(paths["run_dir"], LOOP_DEVICE_FILE)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        for dir_path in (
            paths["run_dir"],
            paths["scratch_dir"],
            paths["lock_dir"],
            paths["snapshots_dir"],
        ):
            os.makedirs(dir_path, exist_ok=True)

        try:
            image_path = btrfs.create_loopback_image(
                paths["btrfs_image"],
                btrfs_cfg["loopback_size_gib"],
                run_dir=paths["run_dir"],
            )
            log.write(f"created loopback image {image_path}")

            loop_device = btrfs.setup_loop_device(image_path)
            log.write(f"attached loop device {loop_device}")

            btrfs.format_btrfs(loop_device)
            log.write("formatted loop device with btrfs")

            mount_dir = btrfs.mount_btrfs(
                loop_device,
                paths["mount_dir"],
                btrfs_cfg["mount_options"],
                run_dir=paths["run_dir"],
            )
            log.write(f"mounted btrfs at {mount_dir}")

            created = btrfs.create_subvolumes(
                mount_dir,
                btrfs_cfg["subvolumes"],
                run_dir=paths["run_dir"],
            )
            log.write(f"created subvolumes: {created}")

            _chown_for_user(
                [
                    paths["run_dir"],
                    paths["mount_dir"],
                    paths["snapshots_dir"],
                    paths["scratch_dir"],
                    paths["lock_dir"],
                ],
                log,
            )

            with open(loop_device_path, "w", encoding="utf-8") as handle:
                handle.write(loop_device + "\n")
            log.write(f"stored loop device in {loop_device_path}")
        except Exception as exc:
            log.write(f"setup failed: {exc}", level="ERROR")
            return 1

    return 0


def _chown_for_user(paths: list[str], log) -> None:
    if os.geteuid() != 0:
        log.write("skipping chown; not running as root")
        return
    sudo_user = os.environ.get("SUDO_USER")
    if not sudo_user:
        log.write("skipping chown; SUDO_USER not set")
        return
    try:
        user_info = pwd.getpwnam(sudo_user)
    except KeyError:
        log.write(f"skipping chown; unknown user {sudo_user}", level="ERROR")
        return
    uid = user_info.pw_uid
    gid = user_info.pw_gid
    for path in paths:
        if not os.path.exists(path):
            continue
        os.chown(path, uid, gid)
        for root, dirs, files in os.walk(path):
            for name in dirs + files:
                os.chown(os.path.join(root, name), uid, gid)
    log.write(f"chowned paths to {sudo_user}")


if __name__ == "__main__":
    raise SystemExit(main())
