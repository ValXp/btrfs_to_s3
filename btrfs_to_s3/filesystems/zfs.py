"""ZFS-specific filesystem backend implementations."""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from dataclasses import dataclass
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
    SnapshotError,
    SnapshotOperations,
    StreamError,
    parse_snapshot_name,
    select_retention,
    snapshot_name,
)
from btrfs_to_s3.path_utils import ensure_sbin_on_path


@dataclass(frozen=True)
class _ReceiveContext:
    dataset: str
    target: Path


VERIFY_CLONE_DESTROY_ATTEMPTS = 5
VERIFY_CLONE_DESTROY_DELAY_SECONDS = 0.2


class ZFSSnapshotManager(SnapshotOperations):
    def __init__(
        self,
        snapshot_prefix: str,
        runner: CommandRunner,
        now: Callable[[], datetime] | None = None,
        command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.snapshot_prefix = snapshot_prefix
        self.runner = runner
        self.now = now or (lambda: datetime.now(timezone.utc))
        self._command_runner = command_runner or subprocess.run

    def create_snapshot(
        self, subvolume_path: Path, subvolume_name: str, kind: str
    ) -> Snapshot:
        del subvolume_path
        timestamp = self.now()
        name = snapshot_name(_dataset_token(subvolume_name), timestamp, kind)
        identity = _snapshot_identity(
            subvolume_name,
            snapshot_prefix=self.snapshot_prefix,
            snapshot_name_value=name,
        )
        self._run_command(["zfs", "snapshot", identity])
        return Snapshot(name=name, path=Path(identity), kind=kind, created_at=timestamp)

    def list_snapshots(self, subvolume_name: str) -> list[Snapshot]:
        result = self._run_capture(
            [
                "zfs",
                "list",
                "-H",
                "-o",
                "name",
                "-s",
                "creation",
                "-t",
                "snapshot",
                "-d",
                "1",
                subvolume_name,
            ]
        )
        snapshots: list[Snapshot] = []
        for line in result.stdout.splitlines():
            identity = line.strip()
            if not identity:
                continue
            parsed = _parse_snapshot_identity(
                identity,
                snapshot_prefix=self.snapshot_prefix,
            )
            if parsed is None:
                continue
            name, created_at, kind = parsed
            snapshots.append(
                Snapshot(
                    name=name,
                    path=Path(identity),
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
        to_keep = select_retention(snapshots, retain, keep_name)
        deleted: list[Path] = []
        for snapshot in snapshots:
            if snapshot.name in to_keep:
                continue
            self._run_command(["zfs", "destroy", str(snapshot.path)])
            deleted.append(snapshot.path)
        return deleted

    def _run_command(self, args: list[str]) -> None:
        try:
            self.runner.run(args)
        except subprocess.CalledProcessError as exc:
            raise SnapshotError(_command_error(exc)) from exc

    def _run_capture(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return self._command_runner(
                args,
                check=True,
                text=True,
                capture_output=True,
                env=_command_env(),
            )
        except subprocess.CalledProcessError as exc:
            raise SnapshotError(_command_error(exc)) from exc


class ZFSSendOperations(SendOperations):
    def __init__(
        self,
        *,
        popen: Callable[..., subprocess.Popen[bytes]] | None = None,
    ) -> None:
        self._popen = popen or subprocess.Popen

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
        return _decode_stderr(stderr)

    def open_send(
        self, snapshot_path: Path, parent_snapshot: Path | None = None
    ) -> SendStream:
        args = ["zfs", "send"]
        if parent_snapshot is not None:
            args.extend(["-i", str(parent_snapshot)])
        args.append(str(snapshot_path))
        process = self._popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_command_env(),
        )
        if process.stdout is None:
            process.kill()
            raise StreamError("failed to capture zfs send output")
        return SendStream(process=process, stdout=process.stdout)


class ZFSRestoreOperations(RestoreOperations):
    def __init__(
        self,
        *,
        receive_parent_dataset: str | None,
        restore_base_dir: Path,
        pool_name: str | None = None,
        mount_root: Path | None = None,
        popen: Callable[..., subprocess.Popen[bytes]] | None = None,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.receive_parent_dataset = receive_parent_dataset
        self.restore_base_dir = restore_base_dir
        self.pool_name = pool_name
        self.mount_root = mount_root
        self._popen = popen or subprocess.Popen
        self._runner = runner or subprocess.run
        self._active_receives: dict[int, _ReceiveContext] = {}
        self._active_verify_sources: dict[str, str] = {}

    def validate_restore_target(self, target: Path) -> None:
        dataset = self._target_dataset(target)
        if self._dataset_exists(dataset):
            raise RestoreBackendError(
                f"restore target dataset already exists and cannot be overwritten: {dataset}"
            )

    def open_receive(
        self,
        target: Path,
        snapshot_path: str | None,
    ) -> ReceiveStream:
        del snapshot_path
        dataset = self._target_dataset(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        process = self._popen(
            ["zfs", "receive", "-F", dataset],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_command_env(),
        )
        if process.stdin is None:
            process.kill()
            raise RestoreBackendError("failed to capture zfs receive input")
        self._active_receives[id(process)] = _ReceiveContext(
            dataset=dataset,
            target=target,
        )
        return ReceiveStream(process=process, stdin=process.stdin, created_path=target)

    def cleanup_receive(
        self,
        process: subprocess.Popen[bytes],
        timeout: float = 5.0,
    ) -> str:
        context = self._active_receives.pop(id(process), None)
        try:
            if process.poll() is None:
                process.terminate()
            _code, stderr = _finalize_process(process, timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            _code, stderr = _finalize_process(process)
        receive_error = _decode_stderr(stderr)
        cleanup_error = self._cleanup_failed_dataset(context)
        if receive_error and cleanup_error:
            return f"{receive_error}; cleanup failed: {cleanup_error}"
        if cleanup_error:
            return f"cleanup failed: {cleanup_error}"
        return receive_error

    def complete_receive(
        self,
        stream: ReceiveStream,
        target: Path,
    ) -> None:
        context = self._active_receives.pop(id(stream.process), None)
        code, stderr = _finalize_process(stream.process)
        if code != 0:
            cleanup_error = self._cleanup_failed_dataset(
                context or _ReceiveContext(self._target_dataset(target), target)
            )
            message = f"zfs receive failed with exit code {code}"
            receive_error = _decode_stderr(stderr)
            if receive_error:
                message = f"{message}: {receive_error}"
            if cleanup_error:
                message = f"{message}; cleanup failed: {cleanup_error}"
            raise RestoreBackendError(message)

    def finalize_restore(self, target: Path) -> None:
        self._run(["zfs", "set", "readonly=off", self._target_dataset(target)])

    def verify_metadata(self, target: Path) -> None:
        if not target.is_dir():
            raise RestoreBackendError(
                f"restore target is not a directory: {target}"
            )
        if not os.access(target, os.W_OK):
            raise RestoreBackendError(
                f"restore target is not writable: {target}"
            )
        dataset = self._target_dataset(target)
        dataset_type = self._get_property(dataset, "type")
        if dataset_type != "filesystem":
            raise RestoreBackendError(
                f"restore target has unexpected dataset type: {dataset_type}"
            )
        mounted = self._get_property(dataset, "mounted")
        if mounted != "yes":
            raise RestoreBackendError(
                f"restore target dataset is not mounted: {dataset}"
            )
        mountpoint = self._get_property(dataset, "mountpoint")
        if os.path.abspath(mountpoint) != os.path.abspath(str(target)):
            raise RestoreBackendError(
                f"restore target mountpoint mismatch: {mountpoint}"
            )
        readonly = self._get_property(dataset, "readonly")
        if readonly != "off":
            raise RestoreBackendError(
                f"restore target is readonly: {target}"
            )

    def resolve_verify_source(
        self,
        source_name: str,
        snapshot_path: str | None,
        snapshot_identity: str | None,
    ) -> Path | None:
        del snapshot_path
        if self.pool_name is None or self.mount_root is None:
            return None
        dataset_snapshot = _split_snapshot_identity(snapshot_identity)
        if dataset_snapshot is None:
            return None
        dataset, snapshot_name_value = dataset_snapshot
        snapshot_path_value = self._snapshot_mount_path(dataset, snapshot_name_value)
        if snapshot_path_value is None:
            return None
        if self._path_is_dir(snapshot_path_value):
            return snapshot_path_value

        clone_dataset = self._verify_clone_dataset(source_name, dataset)
        self._run(
            [
                "zfs",
                "clone",
                "-o",
                "readonly=on",
                f"{dataset}@{snapshot_name_value}",
                clone_dataset,
            ]
        )
        clone_path = self._dataset_mount_path(clone_dataset)
        if clone_path is None:
            try:
                self._cleanup_verify_clone(clone_dataset)
            except RestoreBackendError:
                pass
            raise RestoreBackendError(
                f"verify clone dataset is outside restore base: {clone_dataset}"
            )
        if not self._path_is_dir(clone_path):
            try:
                self._cleanup_verify_clone(clone_dataset)
            except RestoreBackendError:
                pass
            raise RestoreBackendError(f"verify clone missing: {clone_path}")
        self._active_verify_sources[self._verify_source_key(clone_path)] = clone_dataset
        return clone_path

    def cleanup_verify_source(self, source: Path | None) -> None:
        if source is None:
            return
        clone_dataset = self._active_verify_sources.pop(
            self._verify_source_key(source), None
        )
        if clone_dataset is None:
            return
        self._cleanup_verify_clone(clone_dataset)

    def _cleanup_failed_dataset(self, context: _ReceiveContext | None) -> str:
        if context is None:
            return ""
        try:
            self._run(["zfs", "destroy", "-r", context.dataset])
        except RestoreBackendError as exc:
            return str(exc)
        return ""

    def _get_property(self, dataset: str, property_name: str) -> str:
        result = self._run(
            ["zfs", "get", "-H", "-o", "value", property_name, dataset]
        )
        value = result.stdout.strip()
        if not value:
            raise RestoreBackendError(
                f"zfs get returned an empty value for {property_name}"
            )
        return value

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return self._runner(
                args,
                check=True,
                text=True,
                capture_output=True,
                env=_command_env(),
            )
        except subprocess.CalledProcessError as exc:
            raise RestoreBackendError(_command_error(exc)) from exc

    def _path_is_dir(self, path: Path) -> bool:
        return path.is_dir()

    def _snapshot_mount_path(
        self,
        dataset: str,
        snapshot_name_value: str,
    ) -> Path | None:
        mount_path = _dataset_mount_path(
            self.mount_root,
            self.pool_name,
            dataset,
        )
        if mount_path is None:
            return None
        return mount_path / ".zfs" / "snapshot" / snapshot_name_value

    def _dataset_mount_path(self, dataset: str) -> Path | None:
        relative = _dataset_relative_to_parent(
            self._require_receive_parent_dataset(),
            dataset,
        )
        if relative is None:
            return None
        if relative == ".":
            return self.restore_base_dir
        return self.restore_base_dir / Path(relative)

    def _verify_clone_dataset(self, source_name: str, dataset: str) -> str:
        token_source = source_name or dataset
        token = _dataset_token(token_source)
        return (
            f"{self._require_receive_parent_dataset()}"
            f"/__verify__{token}__{uuid.uuid4().hex}"
        )

    def _verify_source_key(self, source: Path) -> str:
        return os.path.abspath(str(source))

    def _cleanup_verify_clone(self, clone_dataset: str) -> None:
        last_error: RestoreBackendError | None = None
        try:
            self._run(["zfs", "unmount", "-f", clone_dataset])
        except RestoreBackendError as exc:
            if not self._dataset_exists(clone_dataset):
                return
            last_error = exc

        for attempt in range(VERIFY_CLONE_DESTROY_ATTEMPTS):
            try:
                self._run(["zfs", "destroy", "-r", "-f", clone_dataset])
                return
            except RestoreBackendError as exc:
                if not self._dataset_exists(clone_dataset):
                    return
                last_error = exc
                if attempt == VERIFY_CLONE_DESTROY_ATTEMPTS - 1:
                    break
                time.sleep(VERIFY_CLONE_DESTROY_DELAY_SECONDS)

        if last_error is not None:
            raise last_error

    def _dataset_exists(self, dataset: str) -> bool:
        result = self._runner(
            ["zfs", "list", "-H", "-o", "name", dataset],
            check=False,
            text=True,
            capture_output=True,
            env=_command_env(),
        )
        if result.returncode != 0:
            return False
        return any(line.strip() == dataset for line in result.stdout.splitlines())

    def _target_dataset(self, target: Path) -> str:
        try:
            relative = os.path.relpath(
                os.path.abspath(target),
                os.path.abspath(self.restore_base_dir),
            )
        except ValueError as exc:
            raise RestoreBackendError(
                f"restore target must live under {self.restore_base_dir}: {target}"
            ) from exc
        if relative == os.pardir or relative.startswith(f"{os.pardir}{os.sep}"):
            raise RestoreBackendError(
                f"restore target must live under {self.restore_base_dir}: {target}"
            )
        dataset = self._require_receive_parent_dataset()
        if relative == ".":
            return dataset
        for part in Path(relative).parts:
            dataset = f"{dataset}/{part}"
        return dataset

    def _require_receive_parent_dataset(self) -> str:
        if not self.receive_parent_dataset:
            raise RestoreBackendError(
                "zfs.receive_parent_dataset is required for ZFS restore operations"
            )
        return self.receive_parent_dataset


def _command_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = ensure_sbin_on_path(env.get("PATH", ""))
    return env


def _command_error(exc: subprocess.CalledProcessError) -> str:
    program = exc.cmd[0] if exc.cmd else "command"
    message = f"{program} command failed with exit code {exc.returncode}"
    stderr = (exc.stderr or "").strip()
    if stderr:
        message = f"{message}: {stderr}"
    return message


def _dataset_token(dataset: str) -> str:
    token_parts: list[str] = []
    for char in dataset:
        if char.isalnum() or char in {"-", "."}:
            token_parts.append(char)
            continue
        token_parts.append(f"_x{ord(char):02x}_")
    return "".join(token_parts)


def _snapshot_identity(
    dataset: str,
    *,
    snapshot_prefix: str,
    snapshot_name_value: str,
) -> str:
    return f"{dataset}@{snapshot_prefix}-{snapshot_name_value}"


def _parse_snapshot_identity(
    identity: str,
    *,
    snapshot_prefix: str,
) -> tuple[str, datetime, str] | None:
    if "@" not in identity:
        return None
    _dataset, snapshot_component = identity.rsplit("@", 1)
    prefix = f"{snapshot_prefix}-"
    if not snapshot_component.startswith(prefix):
        return None
    snapshot_value = snapshot_component[len(prefix) :]
    parsed = parse_snapshot_name(snapshot_value)
    if parsed is None:
        return None
    _source_name, created_at, kind = parsed
    return snapshot_value, created_at, kind


def _split_snapshot_identity(
    identity: str | None,
) -> tuple[str, str] | None:
    if not identity or "@" not in identity:
        return None
    dataset, snapshot_name_value = identity.rsplit("@", 1)
    if not dataset or not snapshot_name_value:
        return None
    return dataset, snapshot_name_value


def _dataset_mount_path(
    mount_root: Path,
    pool_name: str,
    dataset: str,
) -> Path | None:
    if dataset == pool_name:
        return mount_root
    prefix = f"{pool_name}/"
    if not dataset.startswith(prefix):
        return None
    return mount_root / dataset[len(prefix) :]


def _dataset_relative_to_parent(
    parent_dataset: str,
    dataset: str,
) -> str | None:
    if dataset == parent_dataset:
        return "."
    prefix = f"{parent_dataset}/"
    if not dataset.startswith(prefix):
        return None
    return dataset[len(prefix) :]


def _decode_stderr(stderr: bytes) -> str:
    return stderr.decode("utf-8", errors="replace").strip()


def _finalize_process(
    process: subprocess.Popen[bytes],
    *,
    timeout: float | None = None,
) -> tuple[int | None, bytes]:
    stdin = getattr(process, "stdin", None)
    if stdin is not None and getattr(stdin, "closed", False):
        if timeout is None:
            code = process.wait()
        else:
            code = process.wait(timeout=timeout)
        stderr_pipe = getattr(process, "stderr", None)
        stderr = stderr_pipe.read() if stderr_pipe is not None else b""
        if not isinstance(stderr, bytes):
            stderr = b""
        return getattr(process, "returncode", code), stderr
    _stdout, stderr = process.communicate(timeout=timeout)
    return getattr(process, "returncode", None), stderr
