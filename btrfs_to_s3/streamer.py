"""Btrfs send stream helpers."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


class StreamError(RuntimeError):
    """Raised when streaming fails."""


@dataclass(frozen=True)
class BtrfsSendProcess:
    process: subprocess.Popen[bytes]
    stdout: BinaryIO


def open_btrfs_send(
    snapshot_path: Path, parent_snapshot: Path | None = None
) -> BtrfsSendProcess:
    args = ["btrfs", "send"]
    if parent_snapshot is not None:
        args.extend(["-p", str(parent_snapshot)])
    args.append(str(snapshot_path))
    process = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdout is None:
        process.kill()
        raise StreamError("failed to capture btrfs send output")
    return BtrfsSendProcess(process=process, stdout=process.stdout)
