"""Compatibility exports for the Btrfs send helpers."""

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO

from btrfs_to_s3.filesystems.base import SendStream, StreamError
from btrfs_to_s3.filesystems.btrfs import BtrfsSendOperations

BtrfsSendProcess = SendStream
_SEND_OPERATIONS = BtrfsSendOperations()


def cleanup_btrfs_send(
    process,
    stdout: BinaryIO | None = None,
    timeout: float = 5.0,
) -> str:
    """Terminate a Btrfs send stream and return stderr output."""
    return _SEND_OPERATIONS.cleanup_send(
        process, stdout=stdout, timeout=timeout
    )


def open_btrfs_send(
    snapshot_path: Path, parent_snapshot: Path | None = None
) -> BtrfsSendProcess:
    """Open a Btrfs send stream."""
    return _SEND_OPERATIONS.open_send(snapshot_path, parent_snapshot)


__all__ = [
    "BtrfsSendProcess",
    "StreamError",
    "cleanup_btrfs_send",
    "open_btrfs_send",
]
