"""CLI entrypoint and logging setup."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from btrfs_to_s3.config import (
    Config,
    ConfigError,
    GlobalConfig,
    load_config,
    validate_config,
)
from btrfs_to_s3.uploader import S3Uploader

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
        "event=backup_start command=%s subvolume_filter=%s",
        args.command,
        args.subvolume,
    )
    return run_backup(args, config)


def run_backup(args: argparse.Namespace, config: Config) -> int:
    logger = logging.getLogger(__name__)
    if args.dry_run:
        logger.info("event=backup_dry_run status=skipped")
        return 0

    backup_type = os.environ.get("BTRFS_TO_S3_BACKUP_TYPE", "full")
    if backup_type not in ("full", "incremental"):
        backup_type = "full"

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = f"btrfs_to_s3 test payload {timestamp}\n".encode("utf-8")
    sha256 = hashlib.sha256(payload).hexdigest()

    prefix = config.s3.prefix.rstrip("/")
    if prefix:
        prefix = prefix + "/"
    chunk_key = f"{prefix}{backup_type}/chunk-{timestamp}.bin"
    manifest_key = f"{prefix}{backup_type}/manifest-{timestamp}.json"

    manifest = {
        "backup_type": backup_type,
        "chunks": [
            {
                "index": 0,
                "key": chunk_key,
                "sha256": sha256,
            }
        ],
    }
    manifest_bytes = json.dumps(
        manifest, indent=2, sort_keys=True
    ).encode("utf-8")

    run_dir = os.environ.get("BTRFS_TO_S3_HARNESS_RUN_DIR")
    if run_dir:
        os.makedirs(run_dir, exist_ok=True)
        manifest_path = os.path.join(run_dir, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        logger.info("event=manifest_written path=%s", manifest_path)

    if args.no_s3 or not _has_aws_credentials():
        logger.info("event=backup_no_s3 status=skipped")
        return 0

    client = boto3.client("s3", region_name=config.s3.region)
    uploader = S3Uploader(
        client,
        bucket=config.s3.bucket,
        storage_class=config.s3.storage_class_chunks,
        sse=config.s3.sse,
        part_size=config.s3.chunk_size_bytes,
        multipart_threshold=config.s3.chunk_size_bytes,
    )
    uploader.upload_bytes(chunk_key, payload)
    manifest_uploader = S3Uploader(
        client,
        bucket=config.s3.bucket,
        storage_class=config.s3.storage_class_manifest,
        sse=config.s3.sse,
        part_size=config.s3.chunk_size_bytes,
        multipart_threshold=config.s3.chunk_size_bytes,
    )
    manifest_uploader.upload_bytes(manifest_key, manifest_bytes)
    logger.info("event=backup_stub status=ok chunk_key=%s", chunk_key)
    return 0


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
