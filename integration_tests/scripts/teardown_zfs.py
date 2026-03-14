"""Tear down the disposable ZFS fixture."""

from __future__ import annotations

from typing import Any
import argparse
import json
import os
import subprocess
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness import zfs
from harness.config import load_config
from harness.logs import open_log


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test_zfs.toml")
)
POOL_STATE_FILE = "zfs_pool_state.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Tear down disposable ZFS fixtures.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    zfs_cfg = config["zfs"]

    run_dir = os.path.abspath(paths["run_dir"])
    logs_dir = os.path.abspath(paths["logs_dir"])
    state_path = os.path.join(run_dir, POOL_STATE_FILE)
    log_path = os.path.join(logs_dir, "teardown_zfs.log")
    os.makedirs(logs_dir, exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        state = _load_pool_state(state_path, log)
        pool_name = _state_value(state, "pool_name", zfs_cfg["pool_name"])
        mount_root = os.path.abspath(
            _state_value(state, "mount_root", zfs_cfg["mount_root"])
        )

        success = True
        try:
            success = _destroy_pool(
                pool_name=pool_name,
                mount_root=mount_root,
                run_dir=run_dir,
                log=log,
            )
        finally:
            _remove_state_file(state_path, log)

    return 0 if success else 1


def _destroy_pool(
    *,
    pool_name: str,
    mount_root: str,
    run_dir: str,
    log,
) -> bool:
    try:
        zfs.destroy_pool(pool_name)
        log.write(f"destroyed pool {pool_name}")
        return True
    except subprocess.CalledProcessError as exc:
        if not _is_missing_pool_error(exc):
            log.write(_format_error("zpool destroy", exc), level="ERROR")
            return False
        log.write(
            f"pool {pool_name} was not active; attempting import before destroy",
            level="WARN",
        )

    try:
        zfs.import_pool(pool_name, mount_root, run_dir=run_dir)
        log.write(f"imported pool {pool_name}")
    except subprocess.CalledProcessError as exc:
        if _is_missing_pool_error(exc):
            log.write(f"pool {pool_name} is already absent", level="WARN")
            return True
        log.write(_format_error("zpool import", exc), level="ERROR")
        return False

    try:
        zfs.destroy_pool(pool_name)
        log.write(f"destroyed pool {pool_name}")
        return True
    except subprocess.CalledProcessError as exc:
        if _is_missing_pool_error(exc):
            log.write(f"pool {pool_name} is already absent", level="WARN")
            return True
        log.write(_format_error("zpool destroy", exc), level="ERROR")
        return False


def _load_pool_state(path: str, log) -> dict[str, Any] | None:
    if not os.path.exists(path):
        log.write(f"missing pool state file {path}", level="WARN")
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            state = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        log.write(f"failed to read pool state {path}: {exc}", level="WARN")
        return None
    if not isinstance(state, dict):
        log.write(f"invalid pool state {path}: expected object", level="WARN")
        return None
    log.write(f"loaded pool state from {path}")
    return state


def _state_value(state: dict[str, Any] | None, key: str, default: str) -> str:
    if state is None:
        return default
    value = state.get(key)
    if not isinstance(value, str) or not value:
        return default
    return value


def _remove_state_file(path: str, log) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        log.write(f"pool state already absent {path}", level="WARN")
    else:
        log.write(f"removed pool state {path}")


def _format_error(label: str, exc: subprocess.CalledProcessError) -> str:
    stderr = (exc.stderr or "").strip()
    if stderr:
        return f"{label}: {stderr}"
    return f"{label}: {exc}"


def _is_missing_pool_error(exc: subprocess.CalledProcessError) -> bool:
    stderr = (exc.stderr or "").lower()
    return "no such pool" in stderr or "no pools available" in stderr


if __name__ == "__main__":
    raise SystemExit(main())
