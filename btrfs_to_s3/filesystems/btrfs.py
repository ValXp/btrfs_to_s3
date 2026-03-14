"""Btrfs-specific filesystem backend implementations."""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Callable

from btrfs_to_s3.filesystems.base import (
    CommandRunner,
    SendOperations,
    SendStream,
    Snapshot,
    SnapshotOperations,
    StreamError,
    parse_snapshot_name,
    snapshot_name,
)


class BtrfsSnapshotManager(SnapshotOperations):
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


class BtrfsSendOperations(SendOperations):
    def cleanup_send(
        self,
        process: subprocess.Popen[bytes],
        stdout: BinaryIO | None = None,
        timeout: float = 5.0,
    ) -> str:
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

    def open_send(
        self, snapshot_path: Path, parent_snapshot: Path | None = None
    ) -> SendStream:
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
        return SendStream(process=process, stdout=process.stdout)
