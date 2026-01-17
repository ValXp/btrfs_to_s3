"""Snapshot creation and retention handling."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


class SnapshotError(RuntimeError):
    """Raised on snapshot management errors."""


@dataclass(frozen=True)
class Snapshot:
    name: str
    path: Path
    kind: str
    created_at: datetime


class CommandRunner:
    """Command runner abstraction for testability."""

    def run(self, args: list[str]) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class SnapshotManager:
    def __init__(
        self,
        base_dir: Path,
        runner: CommandRunner,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.base_dir = base_dir
        self.runner = runner
        self.now = now or (lambda: datetime.now(timezone.utc))

    def create_snapshot(
        self, subvolume_path: Path, subvolume_name: str, kind: str
    ) -> Snapshot:
        timestamp = self.now()
        name = snapshot_name(subvolume_name, timestamp, kind)
        path = self.base_dir / name
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.runner.run(
            [
                "btrfs",
                "subvolume",
                "snapshot",
                "-r",
                str(subvolume_path),
                str(path),
            ]
        )
        return Snapshot(name=name, path=path, kind=kind, created_at=timestamp)

    def list_snapshots(self, subvolume_name: str) -> list[Snapshot]:
        if not self.base_dir.exists():
            return []
        snapshots: list[Snapshot] = []
        for entry in self.base_dir.iterdir():
            parsed = parse_snapshot_name(entry.name)
            if parsed is None:
                continue
            name, created_at, kind = parsed
            if name != subvolume_name:
                continue
            snapshots.append(
                Snapshot(
                    name=entry.name,
                    path=entry,
                    kind=kind,
                    created_at=created_at,
                )
            )
        snapshots.sort(key=lambda snap: snap.created_at, reverse=True)
        return snapshots

    def prune_snapshots(
        self,
        subvolume_name: str,
        retain: int,
        keep_name: str | None = None,
    ) -> list[Path]:
        snapshots = self.list_snapshots(subvolume_name)
        to_keep = {snap.name for snap in snapshots[:retain]}
        if keep_name:
            to_keep.add(keep_name)
        deleted: list[Path] = []
        for snapshot in snapshots:
            if snapshot.name in to_keep:
                continue
            self.runner.run(["btrfs", "subvolume", "delete", str(snapshot.path)])
            deleted.append(snapshot.path)
        return deleted


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
