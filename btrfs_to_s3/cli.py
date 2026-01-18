"""CLI entrypoint and logging setup."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
import subprocess

from btrfs_to_s3.config import (
    Config,
    ConfigError,
    GlobalConfig,
    load_config,
    validate_config,
)
from btrfs_to_s3.manifest import ChunkEntry, CurrentPointer, Manifest, SnapshotInfo, publish_manifest
from btrfs_to_s3.restore import (
    RestoreError,
    fetch_current_manifest_key,
    resolve_manifest_chain,
    restore_chain,
    verify_restore,
)
from btrfs_to_s3.snapshots import SnapshotManager
from btrfs_to_s3.chunker import chunk_stream
from btrfs_to_s3.lock import LockError, LockFile
from btrfs_to_s3.planner import PlanItem, plan_backups
from btrfs_to_s3.streamer import open_btrfs_send
from btrfs_to_s3.state import State, SubvolumeState, load_state, save_state
from btrfs_to_s3.uploader import MAX_PART_SIZE, S3Uploader

import boto3


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="btrfs_to_s3")
    subparsers = parser.add_subparsers(dest="command")

    backup = subparsers.add_parser("backup", help="run backup")
    backup.add_argument("--config", required=True, help="path to config.toml")
    backup.add_argument("--log-level", help="override log level")
    backup.add_argument("--dry-run", action="store_true", help="plan only")
    backup.add_argument(
        "--subvolume",
        action="append",
        help="limit backup to specific subvolume (repeatable)",
    )
    backup.add_argument("--once", action="store_true", help="ignore schedule")
    backup.add_argument(
        "--no-s3", action="store_true", help="skip S3 uploads for diagnostics"
    )

    restore = subparsers.add_parser("restore", help="restore backup")
    restore.add_argument("--config", required=True, help="path to config.toml")
    restore.add_argument("--log-level", help="override log level")
    restore.add_argument("--subvolume", required=True, help="subvolume name")
    restore.add_argument("--target", required=True, help="restore target path")
    restore.add_argument(
        "--manifest-key", help="override current pointer with manifest key"
    )
    restore.add_argument(
        "--restore-timeout",
        type=int,
        help="max seconds to wait for archive restore",
    )
    restore.add_argument(
        "--wait-restore",
        dest="wait_restore",
        action="store_true",
        help="wait for archive restore readiness",
    )
    restore.add_argument(
        "--no-wait-restore",
        dest="wait_restore",
        action="store_false",
        help="skip waiting for archive restore",
    )
    restore.add_argument(
        "--verify",
        choices=("full", "sample", "none"),
        help="override restore verification mode",
    )
    restore.set_defaults(wait_restore=None)

    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        raise SystemExit(0)
    args = parser.parse_args(list(argv))
    if args.command is None:
        parser.error("command required")
    return args


def setup_logging(level: str) -> None:
    numeric = _parse_level(level)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main(argv: Iterable[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        config = _load_and_override_config(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    setup_logging(config.global_cfg.log_level)
    logging.getLogger(__name__).info(
        "event=command_start command=%s subvolume_filter=%s",
        args.command,
        args.subvolume,
    )
    if args.command == "backup":
        return run_backup(args, config)
    if args.command == "restore":
        return run_restore(args, config)
    return 2


def run_backup(args: argparse.Namespace, config: Config) -> int:
    logger = logging.getLogger(__name__)
    if args.dry_run:
        logger.info("event=backup_dry_run status=skipped")
        return 0

    lock = LockFile(config.global_cfg.lock_path)
    try:
        lock.acquire()
    except LockError as exc:
        logger.error("event=backup_lock_failed error=%s", exc)
        return 1

    try:
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        prefix = config.s3.prefix.rstrip("/")
        if prefix:
            prefix = prefix + "/"

        run_dir = os.environ.get("BTRFS_TO_S3_HARNESS_RUN_DIR")
        write_manifest = run_dir is not None

        state = load_state(config.global_cfg.state_path)
        state_subvols = dict(state.subvolumes)

        subvolume_paths = list(config.subvolumes.paths)
        if args.subvolume:
            selected = [
                path
                for path in subvolume_paths
                if path.name in set(args.subvolume)
            ]
        else:
            selected = subvolume_paths[:1] if write_manifest else subvolume_paths

        if not selected:
            logger.error("event=backup_no_subvolumes status=failed")
            return 2

        snapshot_manager = SnapshotManager(
            config.snapshots.base_dir,
            _ShellRunner(),
        )

        plan_items = _build_plan(config, state, now, snapshot_manager, selected)
        plan_by_name = {item.subvolume: item for item in plan_items}
        work_items = _filter_plan_items(
            plan_by_name, selected, args.once, logger
        )
        if not work_items:
            logger.info("event=backup_not_due status=skipped")
            return 0

        if args.no_s3 or not _has_aws_credentials():
            logger.info("event=backup_no_s3 status=skipped")
            return 0

        client = boto3.client("s3", region_name=config.s3.region)
        chunk_uploader = S3Uploader(
            client,
            bucket=config.s3.bucket,
            storage_class=config.s3.storage_class_chunks,
            sse=config.s3.sse,
            part_size=min(config.s3.chunk_size_bytes, MAX_PART_SIZE),
            concurrency=config.s3.concurrency,
            spool_dir=config.global_cfg.spool_dir
            if config.s3.spool_enabled
            else None,
            spool_size_bytes=config.global_cfg.spool_size_bytes,
        )

        for subvolume_path, plan_item, action in work_items:
            subvol_name = subvolume_path.name
            subvol_state = state_subvols.get(subvol_name, SubvolumeState())
            parent_snapshot = None
            if action == "inc" and plan_item.parent_snapshot:
                parent_snapshot = Path(plan_item.parent_snapshot)
                if not parent_snapshot.exists():
                    logger.info(
                        "event=backup_parent_missing subvolume=%s path=%s",
                        subvol_name,
                        parent_snapshot,
                    )
                    action = "full"
                    parent_snapshot = None
            parent_manifest = (
                subvol_state.last_manifest if action == "inc" else None
            )
            effective_kind = "full" if action == "full" else "incremental"
            effective_snapshot_kind = "full" if action == "full" else "inc"

            snapshot = snapshot_manager.create_snapshot(
                subvolume_path, subvol_name, effective_snapshot_kind
            )
            logger.info(
                "event=snapshot_created subvolume=%s path=%s kind=%s",
                subvol_name,
                snapshot.path,
                effective_snapshot_kind,
            )

            send_parent = (
                parent_snapshot if effective_kind == "incremental" else None
            )
            stream = open_btrfs_send(snapshot.path, send_parent)
            chunks: list[ChunkEntry] = []
            local_chunks: list[dict[str, object]] = []
            total_bytes = 0
            try:
                for chunk in chunk_stream(
                    stream.stdout, config.s3.chunk_size_bytes
                ):
                    chunk_key = (
                        f"{prefix}subvol/{subvol_name}/{effective_kind}/"
                        f"chunk-{timestamp}-{chunk.index}.bin"
                    )
                    result = chunk_uploader.upload_stream(
                        chunk_key, chunk.reader
                    )
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
            finally:
                stream.stdout.close()
                _stdout, stderr = stream.process.communicate()
                if stream.process.returncode != 0:
                    error = stderr.decode("utf-8", errors="replace").strip()
                    logger.error(
                        "event=btrfs_send_failed subvolume=%s error=%s",
                        subvol_name,
                        error,
                    )
                    return 1

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
                chunk_size=config.s3.chunk_size_bytes,
                s3={"storage_class": config.s3.storage_class_chunks},
            )
            pointer = CurrentPointer(
                manifest_key=manifest_key,
                kind=effective_kind,
                created_at=timestamp,
            )
            publish_manifest(
                client,
                bucket=config.s3.bucket,
                manifest_key=manifest_key,
                current_key=current_key,
                manifest=manifest,
                pointer=pointer,
                storage_class=config.s3.storage_class_manifest,
                sse=config.s3.sse,
            )
            logger.info(
                "event=backup_uploaded subvolume=%s manifest_key=%s chunk_count=%d",
                subvol_name,
                manifest_key,
                len(chunks),
            )

            if write_manifest and subvolume_path == selected[0]:
                os.makedirs(run_dir, exist_ok=True)
                manifest_path = os.path.join(run_dir, "manifest.json")
                local_manifest = {
                    "backup_type": effective_kind,
                    "chunks": local_chunks,
                }
                with open(manifest_path, "w", encoding="utf-8") as handle:
                    json.dump(local_manifest, handle, indent=2, sort_keys=True)
                logger.info("event=manifest_written path=%s", manifest_path)

            state_subvols[subvol_name] = SubvolumeState(
                last_snapshot=str(snapshot.path),
                last_manifest=manifest_key,
                last_full_at=timestamp
                if effective_kind == "full"
                else subvol_state.last_full_at,
            )
            snapshot_manager.prune_snapshots(
                subvol_name,
                config.snapshots.retain,
                keep_name=parent_snapshot.name if parent_snapshot else None,
            )

        save_state(
            config.global_cfg.state_path,
            State(subvolumes=state_subvols, last_run_at=timestamp),
        )

        return 0
    finally:
        lock.release()


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


class _ShellRunner:
    def run(self, args: list[str]) -> None:
        env = os.environ.copy()
        env["PATH"] = _ensure_sbin_on_path(env.get("PATH", ""))
        subprocess.run(
            args,
            check=True,
            text=True,
            capture_output=True,
            env=env,
        )


def _has_aws_credentials() -> bool:
    if os.environ.get("AWS_PROFILE"):
        return True
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    return bool(access_key and secret_key)


def _load_and_override_config(args: argparse.Namespace) -> Config:
    config = load_config(Path(args.config).expanduser())
    if args.log_level:
        config = Config(
            global_cfg=GlobalConfig(
                log_level=args.log_level,
                state_path=config.global_cfg.state_path,
                lock_path=config.global_cfg.lock_path,
                spool_dir=config.global_cfg.spool_dir,
                spool_size_bytes=config.global_cfg.spool_size_bytes,
            ),
            schedule=config.schedule,
            snapshots=config.snapshots,
            subvolumes=config.subvolumes,
            s3=config.s3,
            restore=config.restore,
        )
        validate_config(config)
    return config


def _parse_level(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        pass
    normalized = value.lower()
    mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    if normalized not in mapping:
        raise ConfigError(f"invalid log level: {value}")
    return mapping[normalized]


def _ensure_sbin_on_path(path: str) -> str:
    parts = [entry for entry in path.split(os.pathsep) if entry]
    for entry in ("/usr/sbin", "/sbin"):
        if entry not in parts:
            parts.append(entry)
    return os.pathsep.join(parts)


def run_restore(args: argparse.Namespace, config: Config) -> int:
    logger = logging.getLogger(__name__)
    if not _has_aws_credentials():
        logger.error("event=restore_no_credentials status=failed")
        return 1

    client = boto3.client("s3", region_name=config.s3.region)
    prefix = config.s3.prefix.rstrip("/")
    if prefix:
        prefix = prefix + "/"
    current_key = f"{prefix}subvol/{args.subvolume}/current.json"
    manifest_key = args.manifest_key
    if not manifest_key:
        try:
            manifest_key = fetch_current_manifest_key(
                client, config.s3.bucket, current_key
            )
        except RestoreError as exc:
            logger.error("event=restore_current_failed error=%s", exc)
            return 1

    try:
        manifests = resolve_manifest_chain(
            client, config.s3.bucket, manifest_key
        )
    except RestoreError as exc:
        logger.error("event=restore_manifest_failed error=%s", exc)
        return 1

    wait_restore = (
        args.wait_restore
        if args.wait_restore is not None
        else config.restore.wait_for_restore
    )
    restore_timeout = (
        args.restore_timeout
        if args.restore_timeout is not None
        else config.restore.restore_timeout_seconds
    )
    try:
        restore_chain(
            client,
            config.s3.bucket,
            manifests,
            Path(args.target).expanduser(),
            wait_for_restore=wait_restore,
            restore_tier=config.restore.restore_tier,
            restore_timeout_seconds=restore_timeout,
        )
    except RestoreError as exc:
        logger.error("event=restore_failed error=%s", exc)
        return 1
    verify_mode = (
        args.verify if args.verify is not None else config.restore.verify_mode
    )
    if verify_mode == "none":
        logger.info("event=restore_verify_skipped mode=none")
    else:
        snapshot_path = manifests[-1].snapshot_path if manifests else None
        source_path = (
            Path(snapshot_path).expanduser() if snapshot_path else None
        )
        if source_path is None or not source_path.exists():
            logger.info(
                "event=restore_verify_source_missing path=%s",
                snapshot_path or "unknown",
            )
        try:
            verify_restore(
                source_path,
                Path(args.target).expanduser(),
                mode=verify_mode,
                sample_max_files=config.restore.sample_max_files,
            )
        except RestoreError as exc:
            logger.error("event=restore_verify_failed error=%s", exc)
            return 1
        logger.info("event=restore_verify_complete status=ok mode=%s", verify_mode)
    logger.info("event=restore_complete status=ok")
    return 0
