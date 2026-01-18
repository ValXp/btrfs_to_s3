"""Exercise archive restore wait/no-wait behavior."""

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
from harness.env import load_env
from harness.logs import open_log
from harness.runner import run_tool


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test_archive.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run archive restores with wait/no-wait behavior."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--subvolume", default=None)
    parser.add_argument("--target-base", default=None)
    parser.add_argument("--restore-timeout", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    env_path = os.path.join(os.path.dirname(config_path), "test.env")
    if os.path.exists(env_path):
        load_env(env_path, override=False)

    log_path = os.path.join(paths["logs_dir"], "run_restore_archive.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            subvolume = _resolve_subvolume(args.subvolume, config)
            target_base = _resolve_target_base(args.target_base, paths["mount_dir"])
        except ValueError as exc:
            log.write(str(exc), level="ERROR")
            return 1

        if args.dry_run:
            log.write("dry run: printing command only")

        for label, wait_flag in (("wait", "--wait-restore"), ("no-wait", "--no-wait-restore")):
            target = _build_target_path(target_base, subvolume, label)
            log.write(f"restoring {label} to {target}")
            extra_args = [
                "restore",
                "--subvolume",
                subvolume,
                "--target",
                target,
                wait_flag,
            ]
            if args.restore_timeout is not None:
                extra_args.extend(["--restore-timeout", str(args.restore_timeout)])
            try:
                result = run_tool(
                    config_path,
                    extra_args,
                    dry_run=args.dry_run,
                )
                if result:
                    _log_process(log, f"restore[{label}]", result)
            except subprocess.CalledProcessError as exc:
                _log_process_error(log, f"restore[{label}]", exc)
                return 1
            except Exception as exc:
                log.write(f"restore {label} failed: {exc}", level="ERROR")
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


def _resolve_target_base(target_base: str | None, mount_dir: str) -> str:
    base = target_base or os.path.join(mount_dir, "restore-archive")
    base = os.path.abspath(base)
    _ensure_under_root(os.path.abspath(mount_dir), base)
    os.makedirs(base, exist_ok=True)
    return base


def _build_target_path(base: str, subvolume: str, label: str) -> str:
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(base, f"{subvolume}__archive__{label}__{timestamp}")


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
