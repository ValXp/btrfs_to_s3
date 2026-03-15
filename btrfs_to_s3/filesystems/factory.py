"""Filesystem backend construction helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from btrfs_to_s3.config import Config, qualify_zfs_dataset
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
from btrfs_to_s3.filesystems.zfs import (
    ZFSRestoreOperations,
    ZFSSendOperations,
    ZFSSnapshotManager,
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
        sources = tuple(
            BackupSource(identifier=path.name, path=path)
            for path in config.subvolumes.paths
        )
        _ensure_unique_source_identifiers(sources)
        return FilesystemBackend(
            name="btrfs",
            sources=sources,
            snapshot_operations=snapshot_operations
            or BtrfsSnapshotManager(config.snapshots.base_dir, runner),
            send_operations=send_operations or BtrfsSendOperations(),
            restore_operations=restore_operations or BtrfsRestoreOperations(),
        )
    if backend == "zfs":
        if config.zfs is None:
            raise BackendSelectionError("missing zfs configuration")
        pool_name = config.zfs.pool_name
        source_datasets = tuple(
            qualify_zfs_dataset(pool_name, dataset)
            for dataset in config.zfs.source_datasets
        )
        receive_parent_dataset = qualify_zfs_dataset(
            pool_name,
            config.zfs.receive_parent_dataset,
        )
        sources = tuple(
            BackupSource(
                identifier=dataset,
                path=_dataset_mount_path(
                    config.zfs.mount_root,
                    pool_name,
                    dataset,
                ),
            )
            for dataset in source_datasets
        )
        _ensure_unique_source_identifiers(sources)
        return FilesystemBackend(
            name="zfs",
            sources=sources,
            snapshot_operations=snapshot_operations
            or ZFSSnapshotManager(
                snapshot_prefix=config.zfs.snapshot_prefix,
                runner=runner,
            ),
            send_operations=send_operations or ZFSSendOperations(),
            restore_operations=restore_operations
            or ZFSRestoreOperations(
                receive_parent_dataset=receive_parent_dataset,
                restore_base_dir=config.restore.target_base_dir,
                pool_name=pool_name,
                mount_root=config.zfs.mount_root,
            ),
        )
    raise BackendSelectionError(
        f'filesystem backend "{backend}" is not implemented'
    )


def _qualify_dataset(pool_name: str, dataset: str) -> str:
    return qualify_zfs_dataset(pool_name, dataset)


def _dataset_mount_path(mount_root: Path, pool_name: str, dataset: str) -> Path:
    qualified = _qualify_dataset(pool_name, dataset)
    if qualified == pool_name:
        return mount_root
    return mount_root / qualified[len(pool_name) + 1 :]


def _ensure_unique_source_identifiers(
    sources: tuple[BackupSource, ...],
) -> None:
    seen: set[str] = set()
    duplicates: list[str] = []
    for source in sources:
        if source.identifier in seen and source.identifier not in duplicates:
            duplicates.append(source.identifier)
            continue
        seen.add(source.identifier)
    if duplicates:
        raise BackendSelectionError(
            "duplicate source identifiers are not allowed: "
            + ", ".join(duplicates)
        )
