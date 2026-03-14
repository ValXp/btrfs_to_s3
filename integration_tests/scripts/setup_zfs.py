"""Set up a disposable ZFS fixture for tests."""

from __future__ import annotations

import argparse
import json
import os
import pwd
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
    parser = argparse.ArgumentParser(description="Set up disposable ZFS fixtures.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    zfs_cfg = config["zfs"]

    run_dir = os.path.abspath(paths["run_dir"])
    logs_dir = os.path.abspath(paths["logs_dir"])
    scratch_dir = os.path.abspath(paths["scratch_dir"])
    lock_dir = os.path.abspath(paths["lock_dir"])
    pool_file = os.path.abspath(zfs_cfg["pool_file"])
    mount_root = os.path.abspath(zfs_cfg["mount_root"])

    log_path = os.path.join(logs_dir, "setup_zfs.log")
    state_path = os.path.join(run_dir, POOL_STATE_FILE)
    os.makedirs(logs_dir, exist_ok=True)

    source_datasets = [
        _dataset_name(zfs_cfg["pool_name"], name)
        for name in zfs_cfg["source_datasets"]
    ]
    receive_parent_dataset = _dataset_name(
        zfs_cfg["pool_name"],
        zfs_cfg["receive_parent_dataset"],
    )
    state = {
        "pool_name": zfs_cfg["pool_name"],
        "pool_file": pool_file,
        "mount_root": mount_root,
        "source_datasets": source_datasets,
        "receive_parent_dataset": receive_parent_dataset,
    }

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        for dir_path in (run_dir, logs_dir, scratch_dir, lock_dir):
            os.makedirs(dir_path, exist_ok=True)

        try:
            image_path = zfs.create_backing_file(
                pool_file,
                zfs_cfg["pool_size_gib"],
                run_dir=run_dir,
            )
            log.write(f"created backing file {image_path}")

            state["pool_status"] = _create_or_import_pool(
                pool_name=zfs_cfg["pool_name"],
                pool_file=image_path,
                mount_root=mount_root,
                run_dir=run_dir,
                create_args=zfs_cfg.get("zpool_create_args", ()),
                log=log,
            )
            _write_pool_state(state_path, state)
            log.write(f"stored pool state in {state_path}")

            datasets = list(
                _iter_unique(source_datasets + [receive_parent_dataset])
            )
            for dataset in datasets:
                _ensure_dataset(
                    dataset,
                    create_args=zfs_cfg.get("zfs_create_args", ()),
                    log=log,
                )
            state["datasets"] = datasets
            _write_pool_state(state_path, state)
            log.write(f"updated pool state in {state_path}")

            _chown_for_user(
                [
                    run_dir,
                    mount_root,
                    scratch_dir,
                    lock_dir,
                ],
                log,
            )
        except subprocess.CalledProcessError as exc:
            log.write(f"setup failed: {_format_error(exc)}", level="ERROR")
            return 1
        except Exception as exc:
            log.write(f"setup failed: {exc}", level="ERROR")
            return 1

    return 0


def _create_or_import_pool(
    *,
    pool_name: str,
    pool_file: str,
    mount_root: str,
    run_dir: str,
    create_args: list[str] | tuple[str, ...],
    log,
) -> str:
    try:
        zfs.create_pool(
            pool_name,
            pool_file,
            mount_root,
            create_args=create_args,
            run_dir=run_dir,
        )
        log.write(f"created pool {pool_name}")
        return "created"
    except subprocess.CalledProcessError as exc:
        if not _is_existing_pool_error(exc):
            raise
        log.write(
            f"pool {pool_name} already exists; attempting import",
            level="WARN",
        )
        zfs.import_pool(pool_name, mount_root, run_dir=run_dir)
        log.write(f"imported pool {pool_name}")
        return "imported"


def _ensure_dataset(
    dataset: str,
    *,
    create_args: list[str] | tuple[str, ...],
    log,
) -> None:
    try:
        zfs.create_dataset(dataset, create_args=create_args)
        log.write(f"created dataset {dataset}")
    except subprocess.CalledProcessError as exc:
        if not _is_existing_dataset_error(exc):
            raise
        log.write(f"dataset already exists {dataset}", level="WARN")


def _write_pool_state(path: str, state: dict[str, object]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _dataset_name(pool_name: str, dataset_name: str) -> str:
    if dataset_name == pool_name or dataset_name.startswith(pool_name + "/"):
        return dataset_name
    return f"{pool_name}/{dataset_name}"


def _iter_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _is_existing_pool_error(exc: subprocess.CalledProcessError) -> bool:
    return _stderr_contains(exc, "already exists", "is imported")


def _is_existing_dataset_error(exc: subprocess.CalledProcessError) -> bool:
    return _stderr_contains(exc, "dataset already exists", "already exists")


def _stderr_contains(
    exc: subprocess.CalledProcessError,
    *markers: str,
) -> bool:
    stderr = (exc.stderr or "").lower()
    return any(marker in stderr for marker in markers)


def _format_error(exc: subprocess.CalledProcessError) -> str:
    stderr = (exc.stderr or "").strip()
    if stderr:
        return stderr
    return str(exc)


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
