"""Load and validate test harness configuration."""

from __future__ import annotations

from typing import Any
import tomllib


_REQUIRED_PATH_KEYS = (
    "run_dir",
    "logs_dir",
    "scratch_dir",
    "lock_dir",
    "btrfs_image",
    "mount_dir",
    "data_dir",
    "snapshots_dir",
)


def load_config(path: str) -> dict[str, Any]:
    """Load a TOML config file and validate required sections."""
    with open(path, "rb") as handle:
        data = tomllib.load(handle)
    validate_config(data, path)
    return data


def validate_config(data: dict[str, Any], path: str = "<config>") -> None:
    """Validate config structure and required keys."""
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected top-level table")

    tool = _require_table(data, "tool", path)
    _require_list_str(tool, "cmd", path)
    _require_str(tool, "config_flag", path)

    paths = _require_table(data, "paths", path)
    for key in _REQUIRED_PATH_KEYS:
        _require_str(paths, key, path)

    btrfs = _require_table(data, "btrfs", path)
    _require_int(btrfs, "loopback_size_gib", path, min_value=1)
    _require_str(btrfs, "mount_options", path)
    _require_list_str(btrfs, "subvolumes", path)

    aws = _require_table(data, "aws", path)
    _require_str(aws, "region", path)
    _require_str(aws, "bucket", path)
    _require_str(aws, "prefix", path)
    _require_str(aws, "storage_class", path)
    _require_str(aws, "sse", path)

    backup = _require_table(data, "backup", path)
    _require_int(backup, "chunk_size_mib", path, min_value=1)
    _require_int(backup, "concurrency", path, min_value=1)
    _require_int(backup, "retention_snapshots", path, min_value=0)


def _require_table(data: dict[str, Any], key: str, path: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{path}: missing or invalid table [{key}]")
    return value


def _require_str(section: dict[str, Any], key: str, path: str) -> None:
    value = section.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}: missing or invalid '{key}'")


def _require_int(
    section: dict[str, Any],
    key: str,
    path: str,
    *,
    min_value: int | None = None,
) -> None:
    value = section.get(key)
    if not isinstance(value, int):
        raise ValueError(f"{path}: missing or invalid '{key}'")
    if min_value is not None and value < min_value:
        raise ValueError(f"{path}: '{key}' must be >= {min_value}")


def _require_list_str(section: dict[str, Any], key: str, path: str) -> None:
    value = section.get(key)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{path}: missing or invalid '{key}'")
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(f"{path}: '{key}' must be a list of strings")
