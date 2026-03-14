"""Filesystem backend interfaces and implementations."""

from btrfs_to_s3.filesystems.base import (
    CommandRunner,
    ReceiveStream,
    RestoreBackendError,
    RestoreOperations,
    SendOperations,
    SendStream,
    Snapshot,
    SnapshotError,
    SnapshotOperations,
    StreamError,
    parse_snapshot_name,
    select_retention,
    snapshot_name,
)
from btrfs_to_s3.filesystems.factory import (
    BackendSelectionError,
    BackupSource,
    FilesystemBackend,
    create_filesystem_backend,
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

__all__ = [
    "BtrfsRestoreOperations",
    "BtrfsSendOperations",
    "BtrfsSnapshotManager",
    "BackendSelectionError",
    "BackupSource",
    "CommandRunner",
    "FilesystemBackend",
    "ReceiveStream",
    "RestoreBackendError",
    "RestoreOperations",
    "SendOperations",
    "SendStream",
    "Snapshot",
    "SnapshotError",
    "SnapshotOperations",
    "StreamError",
    "ZFSRestoreOperations",
    "ZFSSendOperations",
    "ZFSSnapshotManager",
    "create_filesystem_backend",
    "parse_snapshot_name",
    "select_retention",
    "snapshot_name",
]
