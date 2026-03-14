"""Compatibility exports for the Btrfs snapshot manager."""

from __future__ import annotations

from btrfs_to_s3.filesystems.base import (
    CommandRunner,
    Snapshot,
    SnapshotError,
    parse_snapshot_name,
    select_retention,
    snapshot_name,
)
from btrfs_to_s3.filesystems.btrfs import BtrfsSnapshotManager


class SnapshotManager(BtrfsSnapshotManager):
    """Backward-compatible snapshot manager import."""


__all__ = [
    "CommandRunner",
    "Snapshot",
    "SnapshotError",
    "SnapshotManager",
    "parse_snapshot_name",
    "select_retention",
    "snapshot_name",
]
