"""Exercise CLI flags for backup and restore paths."""

from __future__ import annotations

import argparse
import datetime
import os
import subprocess
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.config import load_config
from harness.logs import open_log
from harness.runner import run_tool


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run CLI flag coverage.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--subvolume", default=None)
    parser.add_argument("--restore-target-base", default=None)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_cli_flags.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            subvolume = _resolve_subvolume(args.subvolume, config)
            restore_target = _build_restore_target(
                args.restore_target_base, paths["mount_dir"], subvolume
            )
        except ValueError as exc:
            log.write(str(exc), level="ERROR")
            return 1

        steps = [
            ("backup_dry_run", ["backup", "--dry-run", "--subvolume", subvolume]),
            ("backup_no_s3", ["backup", "--no-s3", "--subvolume", subvolume]),
            ("backup_subvolume", ["backup", "--subvolume", subvolume]),
            ("backup_once", ["backup", "--once", "--subvolume", subvolume]),
            (
                "restore_verify_sample",
                [
                    "restore",
                    "--subvolume",
                    subvolume,
                    "--target",
                    restore_target,
                    "--verify",
                    "sample",
                ],
            ),
        ]

        for name, extra_args in steps:
            log.write(f"running {name}: {' '.join(extra_args)}")
            try:
                result = run_tool(config_path, extra_args)
                if result:
                    _log_process(log, name, result)
            except subprocess.CalledProcessError as exc:
                _log_process_error(log, name, exc)
                return 1
            except Exception as exc:
                log.write(f"{name} failed: {exc}", level="ERROR")
                return 1

    return 0


def _resolve_subvolume(requested: str | None, config: dict) -> str:
    subvolumes = config["btrfs"]["subvolumes"]
    if requested is None:
        if not subvolumes:
            raise ValueError("config has no btrfs.subvolumes entries")
        return subvolumes[0]
    if requested not in subvolumes:
        raise ValueError(
            f"subvolume {requested} not in config list: {', '.join(subvolumes)}"
        )
    return requested


def _build_restore_target(
    target_base: str | None, mount_dir: str, subvolume: str
) -> str:
    base = target_base or os.path.join(mount_dir, "restore-cli-flags")
    base = os.path.abspath(base)
    _ensure_under_root(os.path.abspath(mount_dir), base)
    os.makedirs(base, exist_ok=True)
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(base, f"{subvolume}__cli_flags__{timestamp}")


def _ensure_under_root(root: str, path: str) -> None:
    if os.path.commonpath([root, path]) != root:
        raise ValueError(f"{path} is not under {root}")


def _log_process(log, label: str, result: subprocess.CompletedProcess[str]) -> None:
    if result.stdout:
        log.write(f"{label} stdout: {result.stdout.strip()}")
    if result.stderr:
        log.write(f"{label} stderr: {result.stderr.strip()}", level="WARN")


def _log_process_error(log, label: str, exc: subprocess.CalledProcessError) -> None:
    log.write(f"{label} failed with code {exc.returncode}", level="ERROR")
    if exc.stdout:
        log.write(f"{label} stdout: {exc.stdout.strip()}", level="ERROR")
    if exc.stderr:
        log.write(f"{label} stderr: {exc.stderr.strip()}", level="ERROR")


if __name__ == "__main__":
    raise SystemExit(main())
