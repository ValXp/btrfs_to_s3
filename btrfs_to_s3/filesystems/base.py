"""Shared filesystem backend types and interfaces."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable, Protocol


class SnapshotError(RuntimeError):
    """Raised on snapshot management errors."""


class StreamError(RuntimeError):
    """Raised when streaming fails."""


@dataclass(frozen=True)
class Snapshot:
    name: str
    path: Path
    kind: str
    created_at: datetime


@dataclass(frozen=True)
class SendStream:
    process: subprocess.Popen[bytes]
    stdout: BinaryIO


class CommandRunner(Protocol):
    """Command runner abstraction for testability."""

    def run(self, args: list[str]) -> None:
        """Run a filesystem backend command."""


class SnapshotOperations(Protocol):
    """Snapshot lifecycle operations exposed by a filesystem backend."""

    def create_snapshot(
        self, subvolume_path: Path, subvolume_name: str, kind: str
    ) -> Snapshot:
        """Create a new readonly snapshot."""

    def list_snapshots(self, subvolume_name: str) -> list[Snapshot]:
        """List snapshots for a source filesystem object."""

    def prune_snapshots(
        self,
        subvolume_name: str,
        retain: int,
        keep_name: str | None = None,
    ) -> list[Path]:
        """Delete snapshots that are no longer retained."""


class SendOperations(Protocol):
    """Send stream operations exposed by a filesystem backend."""

    def open_send(
        self, snapshot_path: Path, parent_snapshot: Path | None = None
    ) -> SendStream:
        """Open a full or incremental send stream."""

    def cleanup_send(
        self,
        process: subprocess.Popen[bytes],
        stdout: BinaryIO | None = None,
        timeout: float = 5.0,
    ) -> str:
        """Stop a send stream process and return stderr output."""


def snapshot_name(subvolume_name: str, created_at: datetime, kind: str) -> str:
    if created_at.tzinfo is None:
        raise SnapshotError("created_at must be timezone-aware")
    timestamp = created_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{subvolume_name}__{timestamp}__{kind}"


def parse_snapshot_name(name: str) -> tuple[str, datetime, str] | None:
    match = re.match(r"^(?P<subvol>.+)__(?P<ts>\d{8}T\d{6}Z)__(?P<kind>full|inc)$", name)
    if not match:
        return None
    ts = datetime.strptime(match.group("ts"), "%Y%m%dT%H%M%SZ").replace(
        tzinfo=timezone.utc
    )
    return match.group("subvol"), ts, match.group("kind")


def select_retention(
    snapshots: Iterable[Snapshot], retain: int, keep_name: str | None
) -> set[str]:
    ordered = sorted(snapshots, key=lambda snap: snap.created_at, reverse=True)
    keep = {snap.name for snap in ordered[:retain]}
    if keep_name:
        keep.add(keep_name)
    return keep
