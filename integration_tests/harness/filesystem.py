"""Backend-aware filesystem helpers for harness scripts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import os


@dataclass(frozen=True)
class SourceSpec:
    identifier: str
    path: str


def backend_name(config: dict[str, Any]) -> str:
    filesystem = config.get("filesystem")
    if isinstance(filesystem, dict):
        backend = filesystem.get("backend")
        if isinstance(backend, str) and backend:
            return backend
    return "btrfs"


def source_specs(config: dict[str, Any]) -> list[SourceSpec]:
    backend = backend_name(config)
    if backend == "btrfs":
        return _btrfs_source_specs(config)
    if backend == "zfs":
        return _zfs_source_specs(config)
    raise ValueError(f"unsupported backend {backend!r}")


def source_identifiers(config: dict[str, Any]) -> list[str]:
    return [source.identifier for source in source_specs(config)]


def normalize_s3_prefix(prefix: str) -> str:
    if prefix and not prefix.endswith("/"):
        return f"{prefix}/"
    return prefix


def source_object_prefixes(
    config: dict[str, Any],
    s3_prefix: str,
) -> dict[str, str]:
    normalized = normalize_s3_prefix(s3_prefix)
    return {
        source: f"{normalized}subvol/{source}/"
        for source in source_identifiers(config)
    }


def source_path(config: dict[str, Any], identifier: str) -> str:
    for source in source_specs(config):
        if source.identifier == identifier:
            return source.path
    raise ValueError(f"unknown source {identifier!r}")


def restore_base_dir(config: dict[str, Any]) -> str:
    restore_cfg = config.get("restore")
    if isinstance(restore_cfg, dict):
        target_base_dir = restore_cfg.get("target_base_dir")
        if isinstance(target_base_dir, str) and target_base_dir:
            return os.path.abspath(target_base_dir)

    backend = backend_name(config)
    if backend == "btrfs":
        return os.path.abspath(os.path.join(config["paths"]["mount_dir"], "restore"))

    zfs_cfg = config["zfs"]
    mount_root = os.path.abspath(zfs_cfg["mount_root"])
    dataset_path = _zfs_dataset_mount_path(
        zfs_cfg["pool_name"],
        zfs_cfg["receive_parent_dataset"],
    )
    return os.path.join(mount_root, dataset_path)


def restore_target_dataset(config: dict[str, Any], target_path: str) -> str:
    if backend_name(config) != "zfs":
        raise ValueError("restore target datasets are only defined for ZFS configs")

    restore_base = os.path.abspath(restore_base_dir(config))
    target_path = os.path.abspath(target_path)
    relative = os.path.relpath(target_path, restore_base)
    if relative == os.pardir or relative.startswith(f"{os.pardir}{os.sep}"):
        raise ValueError(f"{target_path} is not under {restore_base}")

    zfs_cfg = config["zfs"]
    dataset = _qualify_zfs_dataset(
        zfs_cfg["pool_name"],
        zfs_cfg["receive_parent_dataset"],
    )
    if relative == ".":
        return dataset

    for part in Path(relative).parts:
        dataset = f"{dataset}/{part}"
    return dataset


def zfs_snapshot_mount_path(
    config: dict[str, Any],
    source_identifier: str,
    snapshot: str,
) -> str:
    source_mount = source_path(config, source_identifier)
    snapshot_name = snapshot.split("@", 1)[1] if "@" in snapshot else snapshot
    return os.path.join(source_mount, ".zfs", "snapshot", snapshot_name)


def zfs_dataset_mount_path(
    config: dict[str, Any],
    dataset_identifier: str,
) -> str:
    if backend_name(config) != "zfs":
        raise ValueError("dataset mount paths are only defined for ZFS configs")
    zfs_cfg = config["zfs"]
    mount_root = os.path.abspath(zfs_cfg["mount_root"])
    return os.path.join(
        mount_root,
        _zfs_dataset_mount_path(zfs_cfg["pool_name"], dataset_identifier),
    )


def target_token(source_identifier: str) -> str:
    token = source_identifier.replace("\\", "/").strip("/")
    token = token.replace("/", "__")
    return token or "source"


def _btrfs_source_specs(config: dict[str, Any]) -> list[SourceSpec]:
    mount_dir = os.path.abspath(config["paths"]["mount_dir"])
    return [
        SourceSpec(identifier=name, path=os.path.join(mount_dir, name))
        for name in config["btrfs"]["subvolumes"]
    ]


def _zfs_source_specs(config: dict[str, Any]) -> list[SourceSpec]:
    zfs_cfg = config["zfs"]
    pool_name = zfs_cfg["pool_name"]
    mount_root = os.path.abspath(zfs_cfg["mount_root"])
    specs: list[SourceSpec] = []
    for dataset in zfs_cfg["source_datasets"]:
        identifier = _qualify_zfs_dataset(pool_name, dataset)
        specs.append(
            SourceSpec(
                identifier=identifier,
                path=os.path.join(
                    mount_root,
                    _zfs_dataset_mount_path(pool_name, dataset),
                ),
            )
        )
    return specs


def _qualify_zfs_dataset(pool_name: str, dataset_name: str) -> str:
    if dataset_name == pool_name or dataset_name.startswith(pool_name + "/"):
        return dataset_name
    return f"{pool_name}/{dataset_name}"


def _zfs_dataset_mount_path(pool_name: str, dataset_name: str) -> str:
    qualified = _qualify_zfs_dataset(pool_name, dataset_name)
    if qualified == pool_name:
        return ""
    return qualified[len(pool_name) + 1 :]
