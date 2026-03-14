"""Load and validate test harness configuration."""

from __future__ import annotations

from typing import Any
import tomllib


_SHARED_PATH_KEYS = (
    "run_dir",
    "logs_dir",
    "scratch_dir",
    "lock_dir",
)

_BTRFS_PATH_KEYS = (
    "btrfs_image",
    "mount_dir",
    "data_dir",
    "snapshots_dir",
)

_VALID_BACKENDS = frozenset({"btrfs", "zfs"})


def load_config(path: str) -> dict[str, Any]:
    """Load a TOML config file and validate required sections."""
    with open(path, "rb") as handle:
        data = tomllib.load(handle)
    validate_config(data, path)
    data.setdefault("filesystem", {"backend": _resolve_backend(data, path)})
    return data


def validate_config(data: dict[str, Any], path: str = "<config>") -> None:
    """Validate config structure and required keys."""
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected top-level table")

    tool = _require_table(data, "tool", path)
    _require_list_str(tool, "cmd", path, "tool")
    _require_str(tool, "config_flag", path, "tool")

    paths = _require_table(data, "paths", path)
    for key in _SHARED_PATH_KEYS:
        _require_str(paths, key, path, "paths")

    backend = _resolve_backend(data, path)
    if backend == "btrfs":
        _validate_btrfs_config(data, paths, path)
    else:
        _validate_zfs_config(data, path)

    aws = _require_table(data, "aws", path)
    _require_str(aws, "region", path, "aws")
    _require_str(aws, "bucket", path, "aws")
    _require_str(aws, "prefix", path, "aws")
    _require_str(aws, "storage_class", path, "aws")
    _require_str(aws, "sse", path, "aws")

    backup = _require_table(data, "backup", path)
    _require_int(backup, "chunk_size_mib", path, "backup", min_value=1)
    _require_int(backup, "concurrency", path, "backup", min_value=1)
    _require_int(backup, "retention_snapshots", path, "backup", min_value=0)


def _resolve_backend(data: dict[str, Any], path: str) -> str:
    filesystem = data.get("filesystem")
    if filesystem is None:
        if "zfs" in data:
            raise ValueError(
                f'{path}: ZFS configs must set [filesystem].backend = "zfs"'
            )
        return "btrfs"
    if not isinstance(filesystem, dict):
        raise ValueError(f"{path}: missing or invalid table [filesystem]")

    backend = filesystem.get("backend")
    if not isinstance(backend, str) or not backend:
        raise ValueError(f"{path}: [filesystem] missing or invalid 'backend'")
    if backend not in _VALID_BACKENDS:
        options = ", ".join(sorted(_VALID_BACKENDS))
        raise ValueError(
            f"{path}: [filesystem] 'backend' must be one of: {options}"
        )
    return backend


def _validate_btrfs_config(
    data: dict[str, Any],
    paths: dict[str, Any],
    path: str,
) -> None:
    for key in _BTRFS_PATH_KEYS:
        _require_str(paths, key, path, "paths")

    btrfs = _require_table(data, "btrfs", path)
    _require_int(btrfs, "loopback_size_gib", path, "btrfs", min_value=1)
    _require_str(btrfs, "mount_options", path, "btrfs")
    _require_list_str(btrfs, "subvolumes", path, "btrfs")


def _validate_zfs_config(data: dict[str, Any], path: str) -> None:
    zfs = _require_table(data, "zfs", path)
    _require_str(zfs, "pool_name", path, "zfs")
    _require_str(zfs, "pool_file", path, "zfs")
    _require_int(zfs, "pool_size_gib", path, "zfs", min_value=1)
    _require_str(zfs, "mount_root", path, "zfs")
    _require_str(zfs, "snapshot_prefix", path, "zfs")
    _require_str(zfs, "receive_parent_dataset", path, "zfs")
    _require_list_str(zfs, "source_datasets", path, "zfs")
    _require_optional_list_str(zfs, "zpool_create_args", path, "zfs")
    _require_optional_list_str(zfs, "zfs_create_args", path, "zfs")


def _require_table(data: dict[str, Any], key: str, path: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{path}: missing or invalid table [{key}]")
    return value


def _require_str(
    section: dict[str, Any],
    key: str,
    path: str,
    section_name: str,
) -> None:
    value = section.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}: [{section_name}] missing or invalid '{key}'")


def _require_int(
    section: dict[str, Any],
    key: str,
    path: str,
    section_name: str,
    *,
    min_value: int | None = None,
) -> None:
    value = section.get(key)
    if not isinstance(value, int):
        raise ValueError(f"{path}: [{section_name}] missing or invalid '{key}'")
    if min_value is not None and value < min_value:
        raise ValueError(f"{path}: [{section_name}] '{key}' must be >= {min_value}")


def _require_list_str(
    section: dict[str, Any],
    key: str,
    path: str,
    section_name: str,
) -> None:
    value = section.get(key)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{path}: [{section_name}] missing or invalid '{key}'")
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(
                f"{path}: [{section_name}] '{key}' must be a list of strings"
            )


def _require_optional_list_str(
    section: dict[str, Any],
    key: str,
    path: str,
    section_name: str,
) -> None:
    if key not in section:
        return
    value = section[key]
    if not isinstance(value, list):
        raise ValueError(f"{path}: [{section_name}] missing or invalid '{key}'")
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(
                f"{path}: [{section_name}] '{key}' must be a list of strings"
            )
