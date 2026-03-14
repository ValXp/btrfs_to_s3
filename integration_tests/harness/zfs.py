"""ZFS helpers for the integration test harness."""

from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Sequence
import os
import subprocess


class StreamError(RuntimeError):
    """Raised when streaming pipes cannot be opened."""


@dataclass(frozen=True)
class ZFSSendProcess:
    process: subprocess.Popen[bytes]
    stdout: BinaryIO


@dataclass(frozen=True)
class ZFSReceiveProcess:
    process: subprocess.Popen[bytes]
    stdin: BinaryIO


def create_backing_file(
    path: str,
    size_gib: int,
    *,
    run_dir: str | None = None,
) -> str:
    """Create a sparse file for a disposable ZFS pool."""
    path = _ensure_under_root(run_dir, path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _run(["truncate", "-s", f"{size_gib}G", path])
    return path


def create_pool(
    pool_name: str,
    pool_file: str,
    mount_root: str,
    *,
    create_args: Sequence[str] = (),
    run_dir: str | None = None,
) -> str:
    """Create a disposable ZFS pool backed by a file."""
    pool_file = _ensure_under_root(run_dir, pool_file)
    mount_root = _ensure_under_root(run_dir, mount_root)
    os.makedirs(os.path.dirname(pool_file), exist_ok=True)
    os.makedirs(mount_root, exist_ok=True)
    command = ["zpool", "create", "-f", "-R", mount_root]
    command.extend(create_args)
    command.extend([pool_name, pool_file])
    _run(command)
    return pool_name


def import_pool(
    pool_name: str,
    mount_root: str,
    *,
    import_args: Sequence[str] = (),
    run_dir: str | None = None,
) -> str:
    """Import an existing pool under the harness mount root."""
    mount_root = _ensure_under_root(run_dir, mount_root)
    os.makedirs(mount_root, exist_ok=True)
    command = ["zpool", "import", "-f", "-R", mount_root]
    command.extend(import_args)
    command.append(pool_name)
    _run(command)
    return pool_name


def export_pool(pool_name: str) -> None:
    """Export a pool."""
    _run(["zpool", "export", pool_name])


def destroy_pool(pool_name: str) -> None:
    """Destroy a disposable pool."""
    _run(["zpool", "destroy", "-f", pool_name])


def destroy_dataset(
    dataset: str,
    *,
    recursive: bool = False,
    force: bool = False,
) -> None:
    """Destroy a dataset."""
    command = ["zfs", "destroy"]
    if recursive:
        command.append("-r")
    if force:
        command.append("-f")
    command.append(dataset)
    _run(command)


def unmount_dataset(
    dataset: str,
    *,
    force: bool = False,
) -> None:
    """Unmount a dataset."""
    command = ["zfs", "unmount"]
    if force:
        command.append("-f")
    command.append(dataset)
    _run(command)


def dataset_exists(dataset: str) -> bool:
    """Return whether a dataset currently exists."""
    result = subprocess.run(
        ["zfs", "list", "-H", "-o", "name", dataset],
        check=False,
        text=True,
        capture_output=True,
        env=_command_env(),
    )
    if result.returncode != 0:
        return False
    return any(line.strip() == dataset for line in result.stdout.splitlines())


def create_dataset(
    dataset: str,
    *,
    create_args: Sequence[str] = (),
) -> str:
    """Create a dataset and return its name."""
    command = ["zfs", "create"]
    command.extend(create_args)
    command.append(dataset)
    _run(command)
    return dataset


def list_datasets(dataset_root: str) -> list[str]:
    """List datasets below the provided dataset root."""
    result = _run(["zfs", "list", "-H", "-o", "name", "-t", "filesystem", "-r", dataset_root])
    return sorted(line.strip() for line in result.stdout.splitlines() if line.strip())


def create_snapshot(snapshot: str, *, recursive: bool = False) -> str:
    """Create a snapshot and return its full name."""
    command = ["zfs", "snapshot"]
    if recursive:
        command.append("-r")
    command.append(snapshot)
    _run(command)
    return snapshot


def list_snapshots(dataset_root: str) -> list[str]:
    """List snapshots below the provided dataset root."""
    result = _run(
        ["zfs", "list", "-H", "-o", "name", "-s", "creation", "-t", "snapshot", "-r", dataset_root]
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def destroy_snapshot(
    snapshot: str,
    *,
    recursive: bool = False,
    defer: bool = False,
) -> None:
    """Destroy a snapshot."""
    command = ["zfs", "destroy"]
    if recursive:
        command.append("-r")
    if defer:
        command.append("-d")
    command.append(snapshot)
    _run(command)


def clone_snapshot(
    snapshot: str,
    clone_dataset: str,
    *,
    clone_args: Sequence[str] = (),
) -> str:
    """Clone a snapshot to a writable dataset and return the clone name."""
    command = ["zfs", "clone"]
    command.extend(clone_args)
    command.extend([snapshot, clone_dataset])
    _run(command)
    return clone_dataset


def open_zfs_send(
    snapshot: str,
    *,
    parent_snapshot: str | None = None,
) -> ZFSSendProcess:
    """Open a full or incremental send stream."""
    command = ["zfs", "send"]
    if parent_snapshot is not None:
        command.extend(["-i", parent_snapshot])
    command.append(snapshot)
    process = _popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.stdout is None:
        process.kill()
        raise StreamError("failed to capture zfs send output")
    return ZFSSendProcess(process=process, stdout=process.stdout)


def open_zfs_receive(
    dataset: str,
    *,
    force: bool = False,
) -> ZFSReceiveProcess:
    """Open a receive process and return its stdin."""
    command = ["zfs", "receive"]
    if force:
        command.append("-F")
    command.append(dataset)
    process = _popen(command, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.stdin is None:
        process.kill()
        raise StreamError("failed to capture zfs receive input")
    return ZFSReceiveProcess(process=process, stdin=process.stdin)


def get_dataset_property(dataset: str, property_name: str) -> str:
    """Read a ZFS property value."""
    result = _run(["zfs", "get", "-H", "-o", "value", property_name, dataset])
    return result.stdout.strip()


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        check=True,
        text=True,
        capture_output=True,
        env=_command_env(),
    )


def _popen(command: Sequence[str], **kwargs: object) -> subprocess.Popen[bytes]:
    return subprocess.Popen(list(command), env=_command_env(), **kwargs)


def _command_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = _ensure_sbin_on_path(env.get("PATH", ""))
    return env


def _ensure_under_root(root: str | None, path: str) -> str:
    if root is None:
        return os.path.abspath(path)
    root = os.path.abspath(root)
    path = os.path.abspath(path)
    if os.path.commonpath([root, path]) != root:
        raise ValueError(f"{path} is not under {root}")
    return path


def _ensure_sbin_on_path(path: str) -> str:
    parts = [entry for entry in path.split(os.pathsep) if entry]
    for entry in ("/usr/sbin", "/sbin"):
        if entry not in parts:
            parts.append(entry)
    return os.pathsep.join(parts)
