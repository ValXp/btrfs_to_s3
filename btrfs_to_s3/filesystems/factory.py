"""Filesystem backend construction helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from btrfs_to_s3.config import Config
from btrfs_to_s3.filesystems.base import (
    CommandRunner,
    RestoreOperations,
    SendOperations,
    SnapshotOperations,
)
from btrfs_to_s3.filesystems.btrfs import (
    BtrfsRestoreOperations,
    BtrfsSendOperations,
    BtrfsSnapshotManager,
)


class BackendSelectionError(RuntimeError):
    """Raised when the configured filesystem backend cannot be built."""


@dataclass(frozen=True)
class BackupSource:
    identifier: str
    path: Path


@dataclass(frozen=True)
class FilesystemBackend:
    name: str
    sources: tuple[BackupSource, ...]
    snapshot_operations: SnapshotOperations
    send_operations: SendOperations
    restore_operations: RestoreOperations


def create_filesystem_backend(
    config: Config,
    *,
    runner: CommandRunner,
    snapshot_operations: SnapshotOperations | None = None,
    send_operations: SendOperations | None = None,
    restore_operations: RestoreOperations | None = None,
) -> FilesystemBackend:
    backend = config.filesystem.backend
    if backend == "btrfs":
        return FilesystemBackend(
            name="btrfs",
            sources=tuple(
                BackupSource(identifier=path.name, path=path)
                for path in config.subvolumes.paths
            ),
            snapshot_operations=snapshot_operations
            or BtrfsSnapshotManager(config.snapshots.base_dir, runner),
            send_operations=send_operations or BtrfsSendOperations(),
            restore_operations=restore_operations or BtrfsRestoreOperations(),
        )
    raise BackendSelectionError(
        f'filesystem backend "{backend}" is not implemented'
    )
