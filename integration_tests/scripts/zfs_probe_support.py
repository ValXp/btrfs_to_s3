"""Shared helpers for standalone ZFS probe scripts."""

from __future__ import annotations

import os
import sys


TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness import zfs
from harness.config import load_config


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test_zfs.toml")
)
COPY_CHUNK_SIZE = 1024 * 1024


def load_zfs_config(config_path: str) -> dict[str, object]:
    """Load a harness config and require the ZFS backend."""
    config = load_config(config_path)
    backend = config["filesystem"]["backend"]
    if backend != "zfs":
        raise ValueError(
            f'{config_path}: expected [filesystem].backend = "zfs", got {backend!r}'
        )
    return config


def source_dataset(zfs_cfg: dict[str, object], index: int = 0) -> str:
    """Return the fully-qualified source dataset name."""
    pool_name = _str_value(zfs_cfg, "pool_name")
    source_datasets = _str_list(zfs_cfg, "source_datasets")
    return dataset_name(pool_name, source_datasets[index])


def receive_parent_dataset(zfs_cfg: dict[str, object]) -> str | None:
    """Return the fully-qualified receive parent dataset name when configured."""
    pool_name = _str_value(zfs_cfg, "pool_name")
    receive_parent = zfs_cfg.get("receive_parent_dataset")
    if receive_parent is None:
        return None
    if not isinstance(receive_parent, str):
        raise TypeError("expected 'receive_parent_dataset' to be a string")
    return dataset_name(pool_name, receive_parent)


def require_receive_parent_dataset(
    zfs_cfg: dict[str, object],
    *,
    purpose: str,
) -> str:
    """Return the receive parent dataset or raise a clear probe error."""
    receive_parent = receive_parent_dataset(zfs_cfg)
    if receive_parent is None:
        raise RuntimeError(f"zfs.receive_parent_dataset is required for {purpose}")
    return receive_parent


def receive_dataset(
    zfs_cfg: dict[str, object],
    source: str,
    *,
    purpose: str,
) -> str:
    """Return the receive dataset used for the provided source dataset."""
    parent = require_receive_parent_dataset(zfs_cfg, purpose=purpose)
    leaf = source.rsplit("/", 1)[-1]
    return f"{parent}/{leaf}"


def snapshot_name(dataset: str, prefix: str, label: str) -> str:
    """Build a snapshot name for probe scripts."""
    return f"{dataset}@{prefix}-{label}"


def write_dataset_text(dataset: str, relative_path: str, contents: str) -> str:
    """Write a text file within a dataset mountpoint."""
    path = _dataset_path(dataset, relative_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(contents)
    return path


def append_dataset_text(dataset: str, relative_path: str, contents: str) -> str:
    """Append text within a dataset mountpoint."""
    path = _dataset_path(dataset, relative_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(contents)
    return path


def read_dataset_text(dataset: str, relative_path: str) -> str:
    """Read a text file from a dataset mountpoint."""
    path = _dataset_path(dataset, relative_path)
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def drain_send_stream(
    snapshot: str,
    *,
    parent_snapshot: str | None = None,
) -> int:
    """Drain a send stream without receiving it."""
    send = zfs.open_zfs_send(snapshot, parent_snapshot=parent_snapshot)
    transferred = 0
    try:
        while True:
            chunk = send.stdout.read(COPY_CHUNK_SIZE)
            if not chunk:
                break
            transferred += len(chunk)
        _wait_success(send.process, "zfs send")
        return transferred
    except Exception:
        _terminate(send.process)
        raise
    finally:
        _close_quietly(send.stdout)


def stream_send_receive(
    snapshot: str,
    receive_target: str,
    *,
    parent_snapshot: str | None = None,
    force: bool = False,
) -> int:
    """Stream a ZFS send directly into a ZFS receive."""
    send = zfs.open_zfs_send(snapshot, parent_snapshot=parent_snapshot)
    receive = None
    transferred = 0
    try:
        receive = zfs.open_zfs_receive(receive_target, force=force)
        while True:
            chunk = send.stdout.read(COPY_CHUNK_SIZE)
            if not chunk:
                break
            receive.stdin.write(chunk)
            transferred += len(chunk)
        receive.stdin.close()
        _wait_success(send.process, "zfs send")
        _wait_success(receive.process, "zfs receive")
        return transferred
    except Exception:
        _terminate(send.process)
        if receive is not None:
            _terminate(receive.process)
        raise
    finally:
        _close_quietly(send.stdout)
        if receive is not None:
            _close_quietly(receive.stdin)


def dataset_name(pool_name: str, dataset: str) -> str:
    """Normalize a dataset name to pool-qualified form."""
    if dataset == pool_name or dataset.startswith(pool_name + "/"):
        return dataset
    return f"{pool_name}/{dataset}"


def _dataset_path(dataset: str, relative_path: str) -> str:
    mountpoint = zfs.get_dataset_property(dataset, "mountpoint")
    if not mountpoint or mountpoint == "-":
        raise RuntimeError(f"dataset {dataset} has no usable mountpoint")
    root = os.path.abspath(mountpoint)
    path = os.path.abspath(os.path.join(root, relative_path))
    if os.path.commonpath([root, path]) != root:
        raise ValueError(f"{relative_path} escapes dataset mountpoint {root}")
    return path


def _wait_success(process, label: str) -> None:
    returncode = process.wait()
    if returncode != 0:
        stderr = _read_stderr(process)
        if stderr:
            raise RuntimeError(f"{label} failed: {stderr}")
        raise RuntimeError(f"{label} failed with exit code {returncode}")


def _read_stderr(process) -> str:
    stderr = getattr(process, "stderr", None)
    if stderr is None or not hasattr(stderr, "read"):
        return ""
    data = stderr.read()
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace").strip()
    if isinstance(data, str):
        return data.strip()
    return ""


def _terminate(process) -> None:
    try:
        process.kill()
    except Exception:
        return
    try:
        process.wait()
    except Exception:
        return


def _close_quietly(handle) -> None:
    try:
        handle.close()
    except Exception:
        return


def _str_value(section: dict[str, object], key: str) -> str:
    value = section[key]
    if not isinstance(value, str):
        raise TypeError(f"expected {key!r} to be a string")
    return value


def _str_list(section: dict[str, object], key: str) -> list[str]:
    value = section[key]
    if not isinstance(value, list):
        raise TypeError(f"expected {key!r} to be a list")
    if not all(isinstance(item, str) for item in value):
        raise TypeError(f"expected {key!r} to be a list of strings")
    return value
