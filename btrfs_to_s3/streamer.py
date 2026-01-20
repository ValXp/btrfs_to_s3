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


def cleanup_btrfs_send(
    process: subprocess.Popen[bytes],
    stdout: BinaryIO | None = None,
    timeout: float = 5.0,
) -> str:
    """Terminate btrfs send and return stderr output."""
    if stdout is not None:
        try:
            stdout.close()
        except Exception:
            pass
    try:
        if process.poll() is None:
            process.terminate()
        _stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        _stdout, stderr = process.communicate()
    return stderr.decode("utf-8", errors="replace").strip()


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
