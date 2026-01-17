"""Btrfs and loop device helpers for the test harness."""

from __future__ import annotations

from typing import Sequence
import os
import subprocess


def create_loopback_image(
    image_path: str,
    size_gib: int,
    *,
    run_dir: str | None = None,
) -> str:
    """Create a sparse loopback image file."""
    image_path = _ensure_under_root(run_dir, image_path)
    os.makedirs(os.path.dirname(image_path), exist_ok=True)
    _run(["truncate", "-s", f"{size_gib}G", image_path])
    return image_path


def setup_loop_device(image_path: str) -> str:
    """Attach the loopback image and return the loop device path."""
    result = _run(["losetup", "--find", "--show", image_path])
    loop_device = result.stdout.strip()
    if not loop_device:
        raise RuntimeError("losetup did not return a loop device")
    return loop_device


def format_btrfs(loop_device: str) -> None:
    """Format a loop device with Btrfs."""
    _run(["mkfs.btrfs", "-f", loop_device])


def mount_btrfs(
    loop_device: str,
    mount_dir: str,
    mount_options: str,
    *,
    run_dir: str | None = None,
) -> str:
    """Mount a Btrfs filesystem."""
    mount_dir = _ensure_under_root(run_dir, mount_dir)
    os.makedirs(mount_dir, exist_ok=True)
    command = ["mount", "-t", "btrfs"]
    if mount_options:
        command.extend(["-o", mount_options])
    command.extend([loop_device, mount_dir])
    _run(command)
    return mount_dir


def unmount(mount_dir: str, *, run_dir: str | None = None) -> None:
    """Unmount a mountpoint if it is mounted."""
    mount_dir = _ensure_under_root(run_dir, mount_dir)
    if os.path.ismount(mount_dir):
        _run(["umount", mount_dir])


def detach_loop_device(loop_device: str) -> None:
    """Detach a loop device."""
    _run(["losetup", "-d", loop_device])


def create_subvolume(
    mount_dir: str,
    name: str,
    *,
    run_dir: str | None = None,
) -> str:
    """Create a Btrfs subvolume under the mount."""
    mount_dir = _ensure_under_root(run_dir, mount_dir)
    path = os.path.join(mount_dir, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _run(["btrfs", "subvolume", "create", path])
    return path


def create_subvolumes(
    mount_dir: str,
    subvolumes: Sequence[str],
    *,
    run_dir: str | None = None,
) -> list[str]:
    """Create multiple subvolumes and return their paths."""
    created: list[str] = []
    for name in subvolumes:
        created.append(create_subvolume(mount_dir, name, run_dir=run_dir))
    return created


def list_subvolumes(mount_dir: str) -> list[str]:
    """List subvolumes under a mount using btrfs tooling."""
    result = _run(["btrfs", "subvolume", "list", "-o", mount_dir])
    paths: list[str] = []
    for line in result.stdout.splitlines():
        marker = " path "
        if marker in line:
            paths.append(line.split(marker, 1)[1].strip())
    return sorted(paths)


def list_snapshots(mount_dir: str, snapshots_dir: str) -> list[str]:
    """List snapshot paths under the snapshots directory."""
    mount_dir = os.path.abspath(mount_dir)
    snapshots_dir = os.path.abspath(snapshots_dir)
    if os.path.commonpath([mount_dir, snapshots_dir]) == mount_dir:
        relative = os.path.relpath(snapshots_dir, mount_dir)
        snapshots = [
            path
            for path in list_subvolumes(mount_dir)
            if path == relative or path.startswith(relative + os.sep)
        ]
        return sorted(snapshots)
    if not os.path.isdir(snapshots_dir):
        return []
    entries = []
    for name in os.listdir(snapshots_dir):
        full_path = os.path.join(snapshots_dir, name)
        if os.path.isdir(full_path):
            entries.append(name)
    return sorted(entries)


def teardown(
    mount_dir: str,
    loop_device: str | None,
    *,
    run_dir: str | None = None,
) -> None:
    """Unmount and detach the loop device."""
    errors: list[str] = []
    try:
        unmount(mount_dir, run_dir=run_dir)
    except subprocess.CalledProcessError as exc:
        errors.append(_format_error("umount", exc))
    if loop_device:
        try:
            detach_loop_device(loop_device)
        except subprocess.CalledProcessError as exc:
            errors.append(_format_error("losetup -d", exc))
    if errors:
        raise RuntimeError("teardown failed:\n" + "\n".join(errors))


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        check=True,
        text=True,
        capture_output=True,
    )


def _ensure_under_root(root: str | None, path: str) -> str:
    if root is None:
        return os.path.abspath(path)
    root = os.path.abspath(root)
    path = os.path.abspath(path)
    if os.path.commonpath([root, path]) != root:
        raise ValueError(f"{path} is not under {root}")
    return path


def _format_error(label: str, exc: subprocess.CalledProcessError) -> str:
    stderr = exc.stderr or ""
    stderr = stderr.strip()
    if stderr:
        return f"{label}: {stderr}"
    return f"{label}: {exc}"
