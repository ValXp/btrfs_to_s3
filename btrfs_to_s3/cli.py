"""CLI entrypoint and logging setup."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable

from btrfs_to_s3.config import (
    Config,
    ConfigError,
    GlobalConfig,
    load_config,
    validate_config,
)
from btrfs_to_s3.orchestrator import (
    BackupOrchestrator,
    BackupRequest,
    RestoreOrchestrator,
    RestoreRequest,
)


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
    request = BackupRequest(
        dry_run=args.dry_run,
        subvolume_names=tuple(args.subvolume) if args.subvolume else None,
        once=args.once,
        no_s3=args.no_s3,
    )
    orchestrator = BackupOrchestrator(
        config, logger=logging.getLogger(__name__)
    )
    return orchestrator.run(request)


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


def run_restore(args: argparse.Namespace, config: Config) -> int:
    request = RestoreRequest(
        subvolume=args.subvolume,
        target=Path(args.target).expanduser(),
        manifest_key=args.manifest_key,
        restore_timeout=args.restore_timeout,
        wait_restore=args.wait_restore,
        verify=args.verify,
    )
    orchestrator = RestoreOrchestrator(
        config, logger=logging.getLogger(__name__)
    )
    return orchestrator.run(request)
