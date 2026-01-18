"""Seed deterministic data into the Btrfs fixture."""

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
SEED_BINARY_SIZE = 128 * 1024


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed deterministic Btrfs data.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument(
        "--dataset-size-mib",
        type=int,
        default=None,
        help="Optional dataset size override in MiB.",
    )
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    subvolumes = config["btrfs"]["subvolumes"]
    mount_dir = paths["mount_dir"]
    dataset_size_mib = _resolve_dataset_size_mib(args.dataset_size_mib, config)
    dataset_size_bytes = dataset_size_mib * 1024 * 1024

    logs_dir = paths["logs_dir"]
    log_path = os.path.join(logs_dir, "seed_data.log")
    os.makedirs(logs_dir, exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        if dataset_size_mib:
            log.write(f"dataset size override: {dataset_size_mib} MiB")
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
            created = _seed_subvolume(subvol_path, name, dataset_size_bytes)
            log.write(f"seeded {name}: {created}")

    return 0


def _seed_subvolume(path: str, name: str, dataset_size_bytes: int) -> list[str]:
    created: list[str] = []
    text_path = os.path.join(path, "seed.txt")
    lines = [f"{name} seed line {i:03d}\n" for i in range(1, 21)]
    _write_text(text_path, "".join(lines))
    created.append(text_path)

    nested_dir = os.path.join(path, "nested")
    info_path = os.path.join(nested_dir, "info.txt")
    _write_text(info_path, f"{name} nested info\n")
    created.append(info_path)

    blob_path = os.path.join(nested_dir, "blob.bin")
    byte_value = sum(name.encode("ascii")) % 256
    _write_binary(blob_path, SEED_BINARY_SIZE, byte_value)
    created.append(blob_path)

    if dataset_size_bytes > 0:
        dataset_path = os.path.join(path, "dataset.bin")
        dataset_value = (byte_value + 17) % 256
        _write_binary(dataset_path, dataset_size_bytes, dataset_value)
        created.append(dataset_path)

    return created


def _write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def _write_binary(path: str, size: int, byte_value: int) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    chunk = bytes([byte_value]) * 4096
    remaining = size
    with open(path, "wb") as handle:
        while remaining > 0:
            write_size = min(remaining, len(chunk))
            handle.write(chunk[:write_size])
            remaining -= write_size


def _ensure_under_root(root: str, path: str) -> str:
    root = os.path.abspath(root)
    path = os.path.abspath(path)
    if os.path.commonpath([root, path]) != root:
        raise ValueError(f"{path} is not under {root}")
    return path


def _resolve_dataset_size_mib(
    override: int | None,
    config: dict[str, object],
) -> int:
    if override is not None:
        if override < 0:
            raise ValueError("dataset size must be >= 0")
        return override
    dataset_cfg = config.get("dataset")
    if isinstance(dataset_cfg, dict):
        size = dataset_cfg.get("size_mib", 0)
        if isinstance(size, int):
            if size < 0:
                raise ValueError("dataset.size_mib must be >= 0")
            return size
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
