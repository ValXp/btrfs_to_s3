"""Mutate seeded data to create incremental changes."""

from __future__ import annotations

import argparse
import os
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.config import load_config
from harness.logs import open_log


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)
MUTATION_PATCH_SIZE = 4096


def main() -> int:
    parser = argparse.ArgumentParser(description="Mutate seeded Btrfs data.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    subvolumes = config["btrfs"]["subvolumes"]
    mount_dir = paths["mount_dir"]

    logs_dir = paths["logs_dir"]
    log_path = os.path.join(logs_dir, "mutate_data.log")
    os.makedirs(logs_dir, exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        for name in subvolumes:
            subvol_path = os.path.join(mount_dir, name)
            try:
                subvol_path = _ensure_under_root(paths["run_dir"], subvol_path)
            except ValueError as exc:
                log.write(str(exc), level="ERROR")
                return 1
            if not os.path.isdir(subvol_path):
                log.write(f"missing subvolume {subvol_path}", level="ERROR")
                return 1
            changes = _mutate_subvolume(subvol_path, name)
            log.write(f"mutated {name}: {changes}")

    return 0


def _mutate_subvolume(path: str, name: str) -> list[str]:
    changes: list[str] = []
    seed_path = os.path.join(path, "seed.txt")
    _append_text(seed_path, f"{name} mutation\n")
    changes.append(f"appended {seed_path}")

    blob_path = os.path.join(path, "nested", "blob.bin")
    _patch_binary(blob_path, name)
    changes.append(f"patched {blob_path}")

    new_path = os.path.join(path, "new.txt")
    _write_text(new_path, f"{name} new file\n")
    changes.append(f"created {new_path}")

    info_path = os.path.join(path, "nested", "info.txt")
    if os.path.exists(info_path):
        os.remove(info_path)
        changes.append(f"removed {info_path}")

    return changes


def _append_text(path: str, content: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(content)


def _patch_binary(path: str, name: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    byte_value = (sum(name.encode("ascii")) + 1) % 256
    patch = bytes([byte_value]) * MUTATION_PATCH_SIZE
    with open(path, "r+b") as handle:
        handle.seek(0)
        handle.write(patch)


def _write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def _ensure_under_root(root: str, path: str) -> str:
    root = os.path.abspath(root)
    path = os.path.abspath(path)
    if os.path.commonpath([root, path]) != root:
        raise ValueError(f"{path} is not under {root}")
    return path


if __name__ == "__main__":
    raise SystemExit(main())
