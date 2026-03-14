"""Backup and restore orchestration logic."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from btrfs_to_s3.chunker import chunk_stream
from btrfs_to_s3.config import Config
from btrfs_to_s3.filesystems import (
    BackendSelectionError,
    BackupSource,
    FilesystemBackend,
    create_filesystem_backend,
)
from btrfs_to_s3.filesystems.base import RestoreBackendError
from btrfs_to_s3.lock import LockError, LockFile
from btrfs_to_s3.manifest import (
    MANIFEST_VERSION,
    ChunkEntry,
    CurrentPointer,
    Manifest,
    SnapshotInfo,
    publish_manifest,
)
from btrfs_to_s3.metrics import calculate_metrics, format_throughput
from btrfs_to_s3.path_utils import ensure_sbin_on_path
from btrfs_to_s3.planner import PlanItem, plan_backups
from btrfs_to_s3.restore import (
    RestoreError,
    fetch_current_manifest_key,
    resolve_manifest_chain,
    restore_chain,
    verify_restore,
)
from btrfs_to_s3.state import SourceState, State, load_state, save_state
from btrfs_to_s3.uploader import MAX_PART_SIZE, S3Uploader


@dataclass(frozen=True)
class BackupRequest:
    dry_run: bool
    source_names: tuple[str, ...] | None
    once: bool
    no_s3: bool


@dataclass(frozen=True)
class RestoreRequest:
    source_name: str
    target: Path
    manifest_key: str | None
    restore_timeout: int | None
    wait_restore: bool | None
    verify: str | None


class BackupOrchestrator:
    def __init__(
        self,
        config: Config,
        logger: logging.Logger | None = None,
        backend_factory: Callable[..., FilesystemBackend] = create_filesystem_backend,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._backend_factory = backend_factory

    def run(self, request: BackupRequest) -> int:
        if request.dry_run:
            self.logger.info("event=backup_dry_run status=skipped")
            return 0

        lock = LockFile(self.config.global_cfg.lock_path)
        try:
            lock.acquire()
        except LockError as exc:
            self.logger.error("event=backup_lock_failed error=%s", exc)
            return 1

        try:
            return self._run_locked(request)
        finally:
            lock.release()

    def _run_locked(self, request: BackupRequest) -> int:
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        prefix = _build_prefix(self.config.s3.prefix)
        run_dir = os.environ.get("BTRFS_TO_S3_HARNESS_RUN_DIR")
        write_manifest = run_dir is not None

        backend = self._get_backend()
        if backend is None:
            return 1

        state = load_state(self.config.global_cfg.state_path)
        state_sources = dict(state.sources)
        selected = self._select_sources(
            backend, write_manifest, request.source_names
        )
        if not selected:
            self.logger.error("event=backup_no_sources status=failed")
            return 2

        work_items = self._plan_work(
            state, now, backend, selected, request.once
        )
        if not work_items:
            self.logger.info("event=backup_not_due status=skipped")
            return 0

        if request.no_s3 or not _has_aws_credentials():
            self.logger.info("event=backup_no_s3 status=skipped")
            return 0

        client = self._init_s3_client()
        if client is None:
            return 1
        uploader = self._make_uploader(client)

        for item in work_items:
            result = self._backup_item(
                item,
                state_sources,
                timestamp,
                prefix,
                backend,
                uploader,
                write_manifest,
                run_dir,
                selected,
            )
            if result != 0:
                return result

        save_state(
            self.config.global_cfg.state_path,
            State(sources=state_sources, last_run_at=timestamp),
        )
        return 0

    def _select_sources(
        self,
        backend: FilesystemBackend,
        write_manifest: bool,
        names: tuple[str, ...] | None,
    ) -> list[BackupSource]:
        sources = list(backend.sources)
        if names:
            name_set = set(names)
            return [
                source
                for source in sources
                if source.identifier in name_set
            ]
        if write_manifest:
            return sources[:1]
        return sources

    def _plan_work(
        self,
        state: State,
        now: datetime,
        backend: FilesystemBackend,
        selected: list[BackupSource],
        force_run: bool,
    ) -> list[tuple[BackupSource, PlanItem, str]]:
        plan_by_name = {
            item.source_name: item
            for item in _build_plan(
                self.config,
                state,
                now,
                backend.snapshot_operations,
                selected,
            )
        }
        return _filter_plan_items(
            plan_by_name, selected, force_run, self.logger
        )

    def _get_backend(self) -> FilesystemBackend | None:
        try:
            return self._backend_factory(self.config, runner=_ShellRunner())
        except BackendSelectionError as exc:
            self.logger.error("event=filesystem_backend_failed error=%s", exc)
            return None

    def _init_s3_client(self):
        try:
            return _get_s3_client(self.config.s3.region)
        except RuntimeError as exc:
            self.logger.error("event=backup_s3_client_failed error=%s", exc)
            return None

    def _make_uploader(self, client) -> S3Uploader:
        return S3Uploader(
            client,
            bucket=self.config.s3.bucket,
            storage_class=self.config.s3.storage_class_chunks,
            sse=self.config.s3.sse,
            part_size=min(self.config.s3.chunk_size_bytes, MAX_PART_SIZE),
            concurrency=self.config.s3.concurrency,
            spool_dir=self.config.global_cfg.spool_dir
            if self.config.s3.spool_enabled
            else None,
            spool_size_bytes=self.config.global_cfg.spool_size_bytes,
        )

    def _backup_item(
        self,
        item: tuple[BackupSource, PlanItem, str],
        state_sources: dict[str, SourceState],
        timestamp: str,
        prefix: str,
        backend: FilesystemBackend,
        uploader: S3Uploader,
        write_manifest: bool,
        run_dir: str | None,
        selected: list[BackupSource],
    ) -> int:
        source, plan_item, action = item
        source_name = source.identifier
        source_state = state_sources.get(source_name, SourceState())
        action, parent_snapshot, parent_manifest = self._resolve_parents(
            action,
            plan_item,
            source_name,
            source_state,
            backend.name,
        )
        parent_snapshot_name = None
        if action == "inc":
            parent_snapshot_name = source_state.snapshot_name
            if (
                parent_snapshot_name is None
                and plan_item.parent_snapshot
                and backend.name == "btrfs"
            ):
                parent_snapshot_name = Path(plan_item.parent_snapshot).name
        effective_kind = "full" if action == "full" else "incremental"
        snapshot_kind = "full" if action == "full" else "inc"

        snapshot = self._create_snapshot(
            backend,
            source.path,
            source_name,
            snapshot_kind,
        )

        send_parent = parent_snapshot if effective_kind == "incremental" else None
        start_time = time.monotonic()
        stream_result = self._upload_stream(
            backend,
            snapshot.path,
            send_parent,
            source_name,
            effective_kind,
            timestamp,
            prefix,
            uploader,
        )
        if stream_result is None:
            return 1
        total_bytes, chunks, local_chunks = stream_result

        manifest_key = self._publish_manifest(
            uploader.client,
            backend.name,
            source_name,
            effective_kind,
            timestamp,
            prefix,
            snapshot,
            parent_manifest,
            chunks,
            total_bytes,
        )
        self._log_backup_metrics(source_name, total_bytes, start_time)
        self.logger.info(
            "event=backup_uploaded source=%s manifest_key=%s chunk_count=%d",
            source_name,
            manifest_key,
            len(chunks),
        )

        if write_manifest and run_dir and source.identifier == selected[0].identifier:
            self._write_manifest(run_dir, effective_kind, local_chunks)

        state_sources[source_name] = SourceState(
            last_snapshot=str(snapshot.path),
            last_snapshot_name=snapshot.name,
            last_snapshot_path=str(snapshot.path)
            if backend.name == "btrfs"
            else None,
            last_manifest=manifest_key,
            last_full_at=timestamp
            if effective_kind == "full"
            else source_state.last_full_at,
        )
        backend.snapshot_operations.prune_snapshots(
            source_name,
            self.config.snapshots.retain,
            keep_name=parent_snapshot_name,
        )
        return 0

    def _create_snapshot(
        self,
        backend: FilesystemBackend,
        source_path: Path,
        source_name: str,
        snapshot_kind: str,
    ):
        snapshot = backend.snapshot_operations.create_snapshot(
            source_path, source_name, snapshot_kind
        )
        self.logger.info(
            "event=snapshot_created source=%s path=%s kind=%s",
            source_name,
            snapshot.path,
            snapshot_kind,
        )
        return snapshot

    def _publish_manifest(
        self,
        client,
        filesystem: str,
        source_name: str,
        effective_kind: str,
        timestamp: str,
        prefix: str,
        snapshot,
        parent_manifest: str | None,
        chunks: list[ChunkEntry],
        total_bytes: int,
    ) -> str:
        manifest_key = (
            f"{_source_object_prefix(prefix, source_name)}{effective_kind}/"
            f"manifest-{timestamp}.json"
        )
        current_key = f"{_source_object_prefix(prefix, source_name)}current.json"
        snapshot_identity = str(snapshot.path)
        snapshot_path = snapshot_identity if filesystem == "btrfs" else None
        manifest = Manifest(
            version=MANIFEST_VERSION,
            filesystem=filesystem,
            source_name=source_name,
            kind=effective_kind,
            created_at=timestamp,
            snapshot=SnapshotInfo(
                name=snapshot.name,
                path=snapshot_path,
                identity=snapshot_identity,
                uuid=None,
                parent_uuid=None,
            ),
            parent_manifest=parent_manifest
            if effective_kind == "incremental"
            else None,
            chunks=tuple(chunks),
            total_bytes=total_bytes,
            chunk_size=self.config.s3.chunk_size_bytes,
            s3={"storage_class": self.config.s3.storage_class_chunks},
        )
        pointer = CurrentPointer(
            manifest_key=manifest_key,
            kind=effective_kind,
            created_at=timestamp,
        )
        publish_manifest(
            client,
            bucket=self.config.s3.bucket,
            manifest_key=manifest_key,
            current_key=current_key,
            manifest=manifest,
            pointer=pointer,
            storage_class=self.config.s3.storage_class_manifest,
            sse=self.config.s3.sse,
        )
        return manifest_key

    def _log_backup_metrics(
        self, source_name: str, total_bytes: int, start_time: float
    ) -> None:
        elapsed = time.monotonic() - start_time
        metrics = calculate_metrics(total_bytes, elapsed)
        self.logger.info(
            "event=backup_metrics source=%s total_bytes=%d elapsed_seconds=%.3f throughput=%s",
            source_name,
            metrics.total_bytes,
            metrics.elapsed_seconds,
            format_throughput(metrics.throughput_bytes_per_sec),
        )

    def _resolve_parents(
        self,
        action: str,
        plan_item: PlanItem,
        source_name: str,
        source_state: SourceState,
        filesystem: str,
    ) -> tuple[str, Path | None, str | None]:
        parent_snapshot = None
        if action == "inc" and plan_item.parent_snapshot:
            parent_snapshot = Path(plan_item.parent_snapshot)
            parent_snapshot_path = (
                source_state.snapshot_path or plan_item.parent_snapshot
            )
            if filesystem == "btrfs" and (
                parent_snapshot_path is None
                or not Path(parent_snapshot_path).exists()
            ):
                self.logger.info(
                    "event=backup_parent_missing source=%s path=%s",
                    source_name,
                    parent_snapshot_path or parent_snapshot,
                )
                action = "full"
                parent_snapshot = None
        if action == "inc" and not source_state.last_manifest:
            self.logger.info(
                "event=backup_parent_manifest_missing source=%s",
                source_name,
            )
            action = "full"
            parent_snapshot = None
        parent_manifest = source_state.last_manifest if action == "inc" else None
        return action, parent_snapshot, parent_manifest

    def _upload_stream(
        self,
        backend: FilesystemBackend,
        snapshot_path: Path,
        send_parent: Path | None,
        source_name: str,
        effective_kind: str,
        timestamp: str,
        prefix: str,
        uploader: S3Uploader,
    ) -> tuple[int, list[ChunkEntry], list[dict[str, object]]] | None:
        stream = backend.send_operations.open_send(snapshot_path, send_parent)
        chunks: list[ChunkEntry] = []
        local_chunks: list[dict[str, object]] = []
        total_bytes = 0
        stream_error: Exception | None = None
        try:
            for chunk in chunk_stream(
                stream.stdout, self.config.s3.chunk_size_bytes
            ):
                chunk_key = (
                    f"{_source_object_prefix(prefix, source_name)}{effective_kind}/"
                    f"chunk-{timestamp}-{chunk.index}.bin"
                )
                result = uploader.upload_stream(chunk_key, chunk.reader)
                chunks.append(
                    ChunkEntry(
                        key=chunk_key,
                        size=chunk.size,
                        sha256=chunk.sha256,
                        etag=result.etag,
                    )
                )
                local_chunks.append(
                    {
                        "index": chunk.index,
                        "key": chunk_key,
                        "sha256": chunk.sha256,
                    }
                )
                total_bytes += chunk.size
        except Exception as exc:
            stream_error = exc
        finally:
            if stream_error is not None:
                error = backend.send_operations.cleanup_send(
                    stream.process, stdout=stream.stdout
                )
                self.logger.error(
                    "event=backup_stream_failed source=%s error=%s send_error=%s",
                    source_name,
                    stream_error,
                    error,
                )
                return None
            stream.stdout.close()
            _stdout, stderr = stream.process.communicate()
            if stream.process.returncode != 0:
                error = stderr.decode("utf-8", errors="replace").strip()
                self.logger.error(
                    "event=send_failed source=%s error=%s",
                    source_name,
                    error,
                )
                return None
        return total_bytes, chunks, local_chunks

    def _write_manifest(
        self,
        run_dir: str,
        backup_type: str,
        local_chunks: list[dict[str, object]],
    ) -> None:
        os.makedirs(run_dir, exist_ok=True)
        manifest_path = os.path.join(run_dir, "manifest.json")
        local_manifest = {"backup_type": backup_type, "chunks": local_chunks}
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(local_manifest, handle, indent=2, sort_keys=True)
        self.logger.info("event=manifest_written path=%s", manifest_path)


class RestoreOrchestrator:
    def __init__(
        self,
        config: Config,
        logger: logging.Logger | None = None,
        backend_factory: Callable[..., FilesystemBackend] = create_filesystem_backend,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._backend_factory = backend_factory

    def run(self, request: RestoreRequest) -> int:
        backend = self._get_backend()
        if backend is None:
            return 1

        if not _has_aws_credentials():
            self.logger.error("event=restore_no_credentials status=failed")
            return 1

        client = self._init_s3_client()
        if client is None:
            return 1

        prefix = _build_prefix(self.config.s3.prefix)
        current_key = (
            f"{_source_object_prefix(prefix, request.source_name)}current.json"
        )
        manifest_key = request.manifest_key
        if not manifest_key:
            manifest_key = self._fetch_manifest_key(client, current_key)
            if manifest_key is None:
                return 1

        manifests = self._resolve_chain(client, manifest_key)
        if manifests is None:
            return 1

        wait_restore = (
            request.wait_restore
            if request.wait_restore is not None
            else self.config.restore.wait_for_restore
        )
        restore_timeout = (
            request.restore_timeout
            if request.restore_timeout is not None
            else self.config.restore.restore_timeout_seconds
        )
        start_time = time.monotonic()
        try:
            total_bytes = restore_chain(
                client,
                self.config.s3.bucket,
                manifests,
                request.target,
                wait_for_restore=wait_restore,
                restore_tier=self.config.restore.restore_tier,
                restore_timeout_seconds=restore_timeout,
                restore_operations=backend.restore_operations,
            )
        except RestoreError as exc:
            self.logger.error("event=restore_failed error=%s", exc)
            return 1
        elapsed = time.monotonic() - start_time
        metrics = calculate_metrics(total_bytes, elapsed)
        self.logger.info(
            "event=restore_metrics source=%s total_bytes=%d elapsed_seconds=%.3f throughput=%s",
            request.source_name,
            metrics.total_bytes,
            metrics.elapsed_seconds,
            format_throughput(metrics.throughput_bytes_per_sec),
        )
        verify_mode = (
            request.verify
            if request.verify is not None
            else self.config.restore.verify_mode
        )
        if (
            self._verify_restore(
                verify_mode,
                request.source_name,
                manifests,
                request.target,
                backend.restore_operations,
            )
            != 0
        ):
            return 1
        self.logger.info("event=restore_complete status=ok")
        return 0

    def _get_backend(self) -> FilesystemBackend | None:
        try:
            return self._backend_factory(self.config, runner=_ShellRunner())
        except BackendSelectionError as exc:
            self.logger.error("event=filesystem_backend_failed error=%s", exc)
            return None

    def _init_s3_client(self):
        try:
            return _get_s3_client(self.config.s3.region)
        except RuntimeError as exc:
            self.logger.error("event=restore_s3_client_failed error=%s", exc)
            return None

    def _fetch_manifest_key(self, client, current_key: str) -> str | None:
        try:
            return fetch_current_manifest_key(
                client, self.config.s3.bucket, current_key
            )
        except RestoreError as exc:
            self.logger.error("event=restore_current_failed error=%s", exc)
            return None

    def _resolve_chain(self, client, manifest_key: str):
        try:
            return resolve_manifest_chain(
                client, self.config.s3.bucket, manifest_key
            )
        except RestoreError as exc:
            self.logger.error("event=restore_manifest_failed error=%s", exc)
            return None

    def _verify_restore(
        self,
        verify_mode: str,
        source_name: str,
        manifests,
        target_path: Path,
        restore_operations,
    ) -> int:
        if verify_mode == "none":
            self.logger.info("event=restore_verify_skipped mode=none")
            return 0
        cleanup_error: RestoreBackendError | None = None
        try:
            source_path = self._resolve_verify_source(
                source_name,
                manifests,
                restore_operations,
            )
        except RestoreBackendError as exc:
            self.logger.error("event=restore_verify_failed error=%s", exc)
            return 1
        try:
            if source_path is None:
                self.logger.info(
                    "event=restore_verify_metadata_only reason=source_unresolvable source=%s snapshot=%s",
                    source_name,
                    manifests[-1].snapshot_reference if manifests else "unknown",
                )
            elif not source_path.exists():
                self.logger.info(
                    "event=restore_verify_metadata_only reason=source_missing path=%s",
                    source_path,
                )
                source_path = None
            verify_restore(
                source_path,
                target_path,
                mode=verify_mode,
                sample_max_files=self.config.restore.sample_max_files,
                restore_operations=restore_operations,
            )
        except RestoreError as exc:
            self.logger.error("event=restore_verify_failed error=%s", exc)
            return 1
        finally:
            try:
                restore_operations.cleanup_verify_source(source_path)
            except RestoreBackendError as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            self.logger.error(
                "event=restore_verify_cleanup_failed error=%s", cleanup_error
            )
            return 1
        self.logger.info(
            "event=restore_verify_complete status=ok mode=%s", verify_mode
        )
        return 0

    def _resolve_verify_source(
        self,
        source_name: str,
        manifests,
        restore_operations,
    ) -> Path | None:
        if not manifests:
            return None
        manifest = manifests[-1]
        return restore_operations.resolve_verify_source(
            source_name,
            manifest.snapshot_path,
            manifest.snapshot_identity,
        )


class _ShellRunner:
    def run(self, args: list[str]) -> None:
        env = os.environ.copy()
        env["PATH"] = ensure_sbin_on_path(env.get("PATH", ""))
        subprocess.run(
            args,
            check=True,
            text=True,
            capture_output=True,
            env=env,
        )


def _build_prefix(prefix: str) -> str:
    normalized = prefix.rstrip("/")
    return f"{normalized}/" if normalized else ""


def _build_plan(
    config: Config,
    state: State,
    now: datetime,
    snapshot_operations,
    selected: list[BackupSource],
) -> list[PlanItem]:
    available_snapshots: set[str] = set()
    for source in selected:
        for snapshot in snapshot_operations.list_snapshots(source.identifier):
            available_snapshots.add(snapshot.name)
    return plan_backups(
        config,
        state,
        now,
        available_snapshots=available_snapshots,
        source_names=[source.identifier for source in selected],
    )


def _filter_plan_items(
    plan_by_name: dict[str, PlanItem],
    selected: list[BackupSource],
    force_run: bool,
    logger: logging.Logger,
) -> list[tuple[BackupSource, PlanItem, str]]:
    work_items: list[tuple[BackupSource, PlanItem, str]] = []
    for source in selected:
        plan = plan_by_name.get(source.identifier)
        if plan is None:
            continue
        action = plan.action
        if action == "skip" and force_run:
            action = "inc" if plan.parent_snapshot else "full"
        if action == "skip":
            logger.info(
                "event=backup_not_due source=%s reason=%s",
                plan.source_name,
                plan.reason,
            )
            continue
        work_items.append((source, plan, action))
    return work_items


def _source_object_prefix(prefix: str, source_name: str) -> str:
    # Keep the legacy `subvol/` object layout so previously uploaded backups,
    # manifests, and current pointers remain restorable.
    return f"{prefix}subvol/{source_name}/"


def _has_aws_credentials() -> bool:
    if os.environ.get("AWS_PROFILE"):
        return True
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    return bool(access_key and secret_key)


def _get_s3_client(region: str):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 operations") from exc
    return boto3.client("s3", region_name=region)
