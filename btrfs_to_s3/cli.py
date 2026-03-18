"""CLI entrypoint and logging setup."""

from __future__ import annotations

import argparse
import json
import logging
import re
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
from btrfs_to_s3.discovery import (
    DiscoveryError,
    get_s3_client,
    has_aws_credentials,
    list_available_manifests,
    list_restorable_sources,
)
from btrfs_to_s3.orchestrator import (
    BackupOrchestrator,
    BackupRequest,
    RestoreOrchestrator,
    RestoreRequest,
)

_DURATION_UNITS = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
}
_DURATION_PART_RE = re.compile(r"(?P<value>\d+)(?P<unit>[smhdSMHD])")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="btrfs_to_s3")
    subparsers = parser.add_subparsers(dest="command")

    backup = subparsers.add_parser("backup", help="run backup")
    backup.add_argument("--config", required=True, help="path to config.toml")
    backup.add_argument("--log-level", help="override log level")
    backup.add_argument("--dry-run", action="store_true", help="plan only")
    backup.add_argument(
        "--source",
        "--subvolume",
        dest="source",
        action="append",
        help="limit backup to specific source (repeatable); --subvolume is a compatibility alias",
    )
    backup.add_argument("--once", action="store_true", help="ignore schedule")
    backup.add_argument(
        "--no-s3", action="store_true", help="skip S3 uploads for diagnostics"
    )

    restore = subparsers.add_parser("restore", help="restore backup")
    restore.add_argument("--config", required=True, help="path to config.toml")
    restore.add_argument("--log-level", help="override log level")
    restore.add_argument(
        "--source",
        "--subvolume",
        dest="source",
        help="source identifier; --subvolume is a compatibility alias",
    )
    restore.add_argument("--target", required=True, help="restore target path")
    restore.add_argument(
        "--manifest-key", help="override current pointer with manifest key"
    )
    restore.add_argument(
        "--restore-timeout",
        type=_parse_restore_timeout,
        help=(
            "max time to wait for archive restore "
            "(seconds or durations like 6h or 30m)"
        ),
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

    list_sources = subparsers.add_parser(
        "list-sources", help="list restorable sources from S3"
    )
    list_sources.add_argument(
        "--config", required=True, help="path to config.toml"
    )
    list_sources.add_argument("--log-level", help="override log level")

    list_manifests = subparsers.add_parser(
        "list-manifests", help="list manifests for a source from S3"
    )
    list_manifests.add_argument(
        "--config", required=True, help="path to config.toml"
    )
    list_manifests.add_argument("--log-level", help="override log level")
    list_manifests.add_argument(
        "--source",
        "--subvolume",
        dest="source",
        required=True,
        help="source identifier; --subvolume is a compatibility alias",
    )

    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        raise SystemExit(0)
    args = parser.parse_args(list(argv))
    if args.command is None:
        parser.error("command required")
    if (
        args.command == "restore"
        and args.source is None
        and args.manifest_key is None
    ):
        parser.error(
            "restore requires --source unless --manifest-key is provided"
        )
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
        "event=command_start command=%s source_filter=%s",
        args.command,
        getattr(args, "source", None),
    )
    if args.command == "backup":
        return run_backup(args, config)
    if args.command == "restore":
        return run_restore(args, config)
    if args.command == "list-sources":
        return run_list_sources(config)
    if args.command == "list-manifests":
        return run_list_manifests(args, config)
    return 2


def run_backup(args: argparse.Namespace, config: Config) -> int:
    request = BackupRequest(
        dry_run=args.dry_run,
        source_names=tuple(args.source) if args.source else None,
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
            filesystem=config.filesystem,
            zfs=config.zfs,
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


def _parse_restore_timeout(value: str) -> int:
    error_message = (
        "restore timeout must be a positive integer number of seconds or "
        "a duration like 6h or 30m"
    )
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError(error_message)
    if normalized.isdigit():
        seconds = int(normalized)
        if seconds <= 0:
            raise argparse.ArgumentTypeError(error_message)
        return seconds

    total_seconds = 0
    offset = 0
    for match in _DURATION_PART_RE.finditer(normalized):
        if match.start() != offset:
            raise argparse.ArgumentTypeError(error_message)
        total_seconds += int(match.group("value")) * _DURATION_UNITS[
            match.group("unit").lower()
        ]
        offset = match.end()

    if offset != len(normalized) or total_seconds <= 0:
        raise argparse.ArgumentTypeError(error_message)
    return total_seconds


def run_restore(args: argparse.Namespace, config: Config) -> int:
    request = RestoreRequest(
        source_name=args.source,
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


def run_list_sources(config: Config) -> int:
    logger = logging.getLogger(__name__)
    client = _init_discovery_client(config, logger)
    if client is None:
        return 1
    try:
        sources = list_restorable_sources(
            client,
            config.s3.bucket,
            config.s3.prefix,
        )
    except DiscoveryError as exc:
        logger.error("event=list_sources_failed error=%s", exc)
        return 1
    _write_json([item.to_dict() for item in sources])
    return 0


def run_list_manifests(args: argparse.Namespace, config: Config) -> int:
    logger = logging.getLogger(__name__)
    client = _init_discovery_client(config, logger)
    if client is None:
        return 1
    try:
        manifests = list_available_manifests(
            client,
            config.s3.bucket,
            config.s3.prefix,
            args.source,
        )
    except DiscoveryError as exc:
        logger.error("event=list_manifests_failed error=%s", exc)
        return 1
    _write_json([item.to_dict() for item in manifests])
    return 0


def _init_discovery_client(config: Config, logger: logging.Logger):
    try:
        has_credentials = has_aws_credentials()
    except RuntimeError as exc:
        logger.error("event=discovery_s3_client_failed error=%s", exc)
        return None
    if not has_credentials:
        logger.error("event=discovery_no_credentials status=failed")
        return None
    try:
        return get_s3_client(config.s3.region)
    except RuntimeError as exc:
        logger.error("event=discovery_s3_client_failed error=%s", exc)
        return None


def _write_json(payload: object) -> None:
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
