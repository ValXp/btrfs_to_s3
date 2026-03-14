"""Btrfs-specific filesystem backend implementations."""

from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Callable

from btrfs_to_s3.filesystems.base import (
    CommandRunner,
    ReceiveStream,
    RestoreBackendError,
    RestoreOperations,
    SendOperations,
    SendStream,
    Snapshot,
    SnapshotOperations,
    StreamError,
    parse_snapshot_name,
    snapshot_name,
)
from btrfs_to_s3.path_utils import ensure_sbin_on_path


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


class BtrfsRestoreOperations(RestoreOperations):
    def __init__(
        self,
        *,
        popen: Callable[..., subprocess.Popen[bytes]] | None = None,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self._popen = popen or subprocess.Popen
        self._runner = runner or subprocess.run

    def open_receive(
        self,
        target: Path,
        snapshot_path: str | None,
    ) -> ReceiveStream:
        if not snapshot_path:
            raise RestoreBackendError("missing snapshot path")
        target.parent.mkdir(parents=True, exist_ok=True)
        process = self._popen(
            ["btrfs", "receive", str(target.parent)],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.stdin is None:
            process.kill()
            raise RestoreBackendError("failed to capture btrfs receive input")
        return ReceiveStream(
            process=process,
            stdin=process.stdin,
            created_path=target.parent / Path(snapshot_path).name,
        )

    def cleanup_receive(
        self,
        process: subprocess.Popen[bytes],
        timeout: float = 5.0,
    ) -> str:
        try:
            if process.poll() is None:
                process.terminate()
            _stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            _stdout, stderr = process.communicate()
        return _decode_stderr(stderr)

    def complete_receive(
        self,
        stream: ReceiveStream,
        target: Path,
    ) -> None:
        _stdout, stderr = stream.process.communicate()
        code = stream.process.returncode
        if code != 0:
            error = _decode_stderr(stderr)
            if error:
                raise RestoreBackendError(
                    f"btrfs receive failed with exit code {code}: {error}"
                )
            raise RestoreBackendError(
                f"btrfs receive failed with exit code {code}"
            )
        created_path = stream.created_path
        if created_path == target:
            return
        if not created_path.exists():
            raise RestoreBackendError(
                f"received subvolume missing: {created_path}"
            )
        if target.exists():
            self._delete_subvolume(target)
        os.rename(created_path, target)

    def finalize_restore(self, target: Path) -> None:
        self._run(
            ["btrfs", "property", "set", "-f", "-ts", str(target), "ro", "false"]
        )

    def verify_metadata(self, target: Path) -> None:
        if not target.is_dir():
            raise RestoreBackendError(
                f"restore target is not a directory: {target}"
            )
        if not os.access(target, os.W_OK):
            raise RestoreBackendError(
                f"restore target is not writable: {target}"
            )
        result = self._run(["btrfs", "subvolume", "show", str(target)])
        if _parse_uuid(result.stdout) is None:
            raise RestoreBackendError("restore target has no valid UUID")

    def resolve_verify_source(
        self,
        source_name: str,
        snapshot_path: str | None,
        snapshot_identity: str | None,
    ) -> Path | None:
        del source_name
        del snapshot_identity
        if not snapshot_path:
            return None
        return Path(snapshot_path).expanduser()

    def cleanup_verify_source(self, source: Path | None) -> None:
        del source

    def _delete_subvolume(self, path: Path) -> None:
        self._run(["btrfs", "subvolume", "delete", str(path)])

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["PATH"] = ensure_sbin_on_path(env.get("PATH", ""))
        try:
            return self._runner(
                args,
                check=True,
                text=True,
                capture_output=True,
                env=env,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            message = (
                f"{args[0]} command failed with exit code {exc.returncode}"
            )
            if stderr:
                message = f"{message}: {stderr}"
            raise RestoreBackendError(message) from exc


def _decode_stderr(stderr: bytes) -> str:
    return stderr.decode("utf-8", errors="replace").strip()


def _parse_uuid(output: str) -> str | None:
    for line in output.splitlines():
        if line.strip().lower().startswith("uuid:"):
            value = line.split(":", 1)[1].strip()
            try:
                uuid.UUID(value)
            except ValueError:
                return None
            return value
    return None
