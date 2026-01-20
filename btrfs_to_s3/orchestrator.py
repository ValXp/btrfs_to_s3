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

from btrfs_to_s3.chunker import chunk_stream
from btrfs_to_s3.config import Config
from btrfs_to_s3.lock import LockError, LockFile
from btrfs_to_s3.manifest import (
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
from btrfs_to_s3.snapshots import SnapshotManager
from btrfs_to_s3.state import State, SubvolumeState, load_state, save_state
from btrfs_to_s3.streamer import cleanup_btrfs_send, open_btrfs_send
from btrfs_to_s3.uploader import MAX_PART_SIZE, S3Uploader


@dataclass(frozen=True)
class BackupRequest:
    dry_run: bool
    subvolume_names: tuple[str, ...] | None
    once: bool
    no_s3: bool


@dataclass(frozen=True)
class RestoreRequest:
    subvolume: str
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
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

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

        state = load_state(self.config.global_cfg.state_path)
        state_subvols = dict(state.subvolumes)
        selected = self._select_subvolumes(
            write_manifest, request.subvolume_names
        )
        if not selected:
            self.logger.error("event=backup_no_subvolumes status=failed")
            return 2

        snapshot_manager = SnapshotManager(
            self.config.snapshots.base_dir,
            _ShellRunner(),
        )
        work_items = self._plan_work(
            state, now, snapshot_manager, selected, request.once
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
                state_subvols,
                timestamp,
                prefix,
                snapshot_manager,
                uploader,
                write_manifest,
                run_dir,
                selected,
            )
            if result != 0:
                return result

        save_state(
            self.config.global_cfg.state_path,
            State(subvolumes=state_subvols, last_run_at=timestamp),
        )
        return 0

    def _select_subvolumes(
        self, write_manifest: bool, names: tuple[str, ...] | None
    ) -> list[Path]:
        subvolume_paths = list(self.config.subvolumes.paths)
        if names:
            name_set = set(names)
            return [path for path in subvolume_paths if path.name in name_set]
        if write_manifest:
            return subvolume_paths[:1]
        return subvolume_paths

    def _plan_work(
        self,
        state: State,
        now: datetime,
        snapshot_manager: SnapshotManager,
        selected: list[Path],
        force_run: bool,
    ) -> list[tuple[Path, PlanItem, str]]:
        plan_by_name = {
            item.subvolume: item
            for item in _build_plan(
                self.config, state, now, snapshot_manager, selected
            )
        }
        return _filter_plan_items(
            plan_by_name, selected, force_run, self.logger
        )

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
        item: tuple[Path, PlanItem, str],
        state_subvols: dict[str, SubvolumeState],
        timestamp: str,
        prefix: str,
        snapshot_manager: SnapshotManager,
        uploader: S3Uploader,
        write_manifest: bool,
        run_dir: str | None,
        selected: list[Path],
    ) -> int:
        subvolume_path, plan_item, action = item
        subvol_name = subvolume_path.name
        subvol_state = state_subvols.get(subvol_name, SubvolumeState())
        action, parent_snapshot, parent_manifest = self._resolve_parents(
            action, plan_item, subvol_name, subvol_state
        )
        effective_kind = "full" if action == "full" else "incremental"
        snapshot_kind = "full" if action == "full" else "inc"

        snapshot = self._create_snapshot(
            snapshot_manager, subvolume_path, subvol_name, snapshot_kind
        )

        send_parent = parent_snapshot if effective_kind == "incremental" else None
        start_time = time.monotonic()
        stream_result = self._upload_stream(
            snapshot.path,
            send_parent,
            subvol_name,
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
            subvol_name,
            effective_kind,
            timestamp,
            prefix,
            snapshot,
            parent_manifest,
            chunks,
            total_bytes,
        )
        self._log_backup_metrics(subvol_name, total_bytes, start_time)
        self.logger.info(
            "event=backup_uploaded subvolume=%s manifest_key=%s chunk_count=%d",
            subvol_name,
            manifest_key,
            len(chunks),
        )

        if write_manifest and run_dir and subvolume_path == selected[0]:
            self._write_manifest(run_dir, effective_kind, local_chunks)

        state_subvols[subvol_name] = SubvolumeState(
            last_snapshot=str(snapshot.path),
            last_manifest=manifest_key,
            last_full_at=timestamp
            if effective_kind == "full"
            else subvol_state.last_full_at,
        )
        snapshot_manager.prune_snapshots(
            subvol_name,
            self.config.snapshots.retain,
            keep_name=parent_snapshot.name if parent_snapshot else None,
        )
        return 0

    def _create_snapshot(
        self,
        snapshot_manager: SnapshotManager,
        subvolume_path: Path,
        subvol_name: str,
        snapshot_kind: str,
    ):
        snapshot = snapshot_manager.create_snapshot(
            subvolume_path, subvol_name, snapshot_kind
        )
        self.logger.info(
            "event=snapshot_created subvolume=%s path=%s kind=%s",
            subvol_name,
            snapshot.path,
            snapshot_kind,
        )
        return snapshot

    def _publish_manifest(
        self,
        client,
        subvol_name: str,
        effective_kind: str,
        timestamp: str,
        prefix: str,
        snapshot,
        parent_manifest: str | None,
        chunks: list[ChunkEntry],
        total_bytes: int,
    ) -> str:
        manifest_key = (
            f"{prefix}subvol/{subvol_name}/{effective_kind}/"
            f"manifest-{timestamp}.json"
        )
        current_key = f"{prefix}subvol/{subvol_name}/current.json"
        manifest = Manifest(
            version=1,
            subvolume=subvol_name,
            kind=effective_kind,
            created_at=timestamp,
            snapshot=SnapshotInfo(
                name=snapshot.name,
                path=str(snapshot.path),
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
        self, subvol_name: str, total_bytes: int, start_time: float
    ) -> None:
        elapsed = time.monotonic() - start_time
        metrics = calculate_metrics(total_bytes, elapsed)
        self.logger.info(
            "event=backup_metrics subvolume=%s total_bytes=%d elapsed_seconds=%.3f throughput=%s",
            subvol_name,
            metrics.total_bytes,
            metrics.elapsed_seconds,
            format_throughput(metrics.throughput_bytes_per_sec),
        )

    def _resolve_parents(
        self,
        action: str,
        plan_item: PlanItem,
        subvol_name: str,
        subvol_state: SubvolumeState,
    ) -> tuple[str, Path | None, str | None]:
        parent_snapshot = None
        if action == "inc" and plan_item.parent_snapshot:
            parent_snapshot = Path(plan_item.parent_snapshot)
            if not parent_snapshot.exists():
                self.logger.info(
                    "event=backup_parent_missing subvolume=%s path=%s",
                    subvol_name,
                    parent_snapshot,
                )
                action = "full"
                parent_snapshot = None
        if action == "inc" and not subvol_state.last_manifest:
            self.logger.info(
                "event=backup_parent_manifest_missing subvolume=%s",
                subvol_name,
            )
            action = "full"
            parent_snapshot = None
        parent_manifest = subvol_state.last_manifest if action == "inc" else None
        return action, parent_snapshot, parent_manifest

    def _upload_stream(
        self,
        snapshot_path: Path,
        send_parent: Path | None,
        subvol_name: str,
        effective_kind: str,
        timestamp: str,
        prefix: str,
        uploader: S3Uploader,
    ) -> tuple[int, list[ChunkEntry], list[dict[str, object]]] | None:
        stream = open_btrfs_send(snapshot_path, send_parent)
        chunks: list[ChunkEntry] = []
        local_chunks: list[dict[str, object]] = []
        total_bytes = 0
        stream_error: Exception | None = None
        try:
            for chunk in chunk_stream(
                stream.stdout, self.config.s3.chunk_size_bytes
            ):
                chunk_key = (
                    f"{prefix}subvol/{subvol_name}/{effective_kind}/"
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
                error = cleanup_btrfs_send(
                    stream.process, stdout=stream.stdout
                )
                self.logger.error(
                    "event=backup_stream_failed subvolume=%s error=%s btrfs_send_error=%s",
                    subvol_name,
                    stream_error,
                    error,
                )
                return None
            stream.stdout.close()
            _stdout, stderr = stream.process.communicate()
            if stream.process.returncode != 0:
                error = stderr.decode("utf-8", errors="replace").strip()
                self.logger.error(
                    "event=btrfs_send_failed subvolume=%s error=%s",
                    subvol_name,
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
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    def run(self, request: RestoreRequest) -> int:
        if not _has_aws_credentials():
            self.logger.error("event=restore_no_credentials status=failed")
            return 1

        client = self._init_s3_client()
        if client is None:
            return 1

        prefix = _build_prefix(self.config.s3.prefix)
        current_key = f"{prefix}subvol/{request.subvolume}/current.json"
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
            )
        except RestoreError as exc:
            self.logger.error("event=restore_failed error=%s", exc)
            return 1
        elapsed = time.monotonic() - start_time
        metrics = calculate_metrics(total_bytes, elapsed)
        self.logger.info(
            "event=restore_metrics subvolume=%s total_bytes=%d elapsed_seconds=%.3f throughput=%s",
            request.subvolume,
            metrics.total_bytes,
            metrics.elapsed_seconds,
            format_throughput(metrics.throughput_bytes_per_sec),
        )
        verify_mode = (
            request.verify
            if request.verify is not None
            else self.config.restore.verify_mode
        )
        if self._verify_restore(verify_mode, manifests, request.target) != 0:
            return 1
        self.logger.info("event=restore_complete status=ok")
        return 0

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
        manifests,
        target_path: Path,
    ) -> int:
        if verify_mode == "none":
            self.logger.info("event=restore_verify_skipped mode=none")
            return 0
        snapshot_path = manifests[-1].snapshot_path if manifests else None
        source_path = Path(snapshot_path).expanduser() if snapshot_path else None
        if source_path is None or not source_path.exists():
            self.logger.info(
                "event=restore_verify_source_missing path=%s",
                snapshot_path or "unknown",
            )
        try:
            verify_restore(
                source_path,
                target_path,
                mode=verify_mode,
                sample_max_files=self.config.restore.sample_max_files,
            )
        except RestoreError as exc:
            self.logger.error("event=restore_verify_failed error=%s", exc)
            return 1
        self.logger.info(
            "event=restore_verify_complete status=ok mode=%s", verify_mode
        )
        return 0


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
    snapshot_manager: SnapshotManager,
    selected: list[Path],
) -> list[PlanItem]:
    available_snapshots: set[str] = set()
    for path in selected:
        for snapshot in snapshot_manager.list_snapshots(path.name):
            available_snapshots.add(snapshot.name)
    if len(selected) == len(config.subvolumes.paths):
        plan_config = config
    else:
        plan_config = Config(
            global_cfg=config.global_cfg,
            schedule=config.schedule,
            snapshots=config.snapshots,
            subvolumes=type(config.subvolumes)(paths=tuple(selected)),
            s3=config.s3,
            restore=config.restore,
        )
    return plan_backups(
        plan_config, state, now, available_snapshots=available_snapshots
    )


def _filter_plan_items(
    plan_by_name: dict[str, PlanItem],
    selected: list[Path],
    force_run: bool,
    logger: logging.Logger,
) -> list[tuple[Path, PlanItem, str]]:
    work_items: list[tuple[Path, PlanItem, str]] = []
    for path in selected:
        plan = plan_by_name.get(path.name)
        if plan is None:
            continue
        action = plan.action
        if action == "skip" and force_run:
            action = "inc" if plan.parent_snapshot else "full"
        if action == "skip":
            logger.info(
                "event=backup_not_due subvolume=%s reason=%s",
                plan.subvolume,
                plan.reason,
            )
            continue
        work_items.append((path, plan, action))
    return work_items


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
