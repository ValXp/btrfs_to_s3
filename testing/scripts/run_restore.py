"""Restore a subvolume into a new target path."""

from __future__ import annotations

import argparse
import datetime
import json
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
DEFAULT_MANIFEST_KEY = "current.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a restore into a new target.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--subvolume", default=None)
    parser.add_argument("--target-base", default=None)
    parser.add_argument("--target-name", default=None)
    parser.add_argument("--target", default=None)
    parser.add_argument("--manifest-key", default=DEFAULT_MANIFEST_KEY)
    parser.add_argument("--restore-timeout", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_restore.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            subvolume = _resolve_subvolume(args.subvolume, config)
            target_path = _resolve_target_path(args, paths, subvolume)
        except ValueError as exc:
            log.write(str(exc), level="ERROR")
            return 1

        log.write(f"restoring subvolume {subvolume} to {target_path}")
        if args.manifest_key:
            log.write(f"using manifest key {args.manifest_key}")
        if args.dry_run:
            log.write("dry run: printing command only")

        extra_args = ["restore", "--subvolume", subvolume, "--target", target_path]
        if args.manifest_key:
            extra_args.extend(["--manifest-key", args.manifest_key])
        if args.restore_timeout is not None:
            extra_args.extend(["--restore-timeout", str(args.restore_timeout)])

        try:
            result = run_tool(
                config_path,
                extra_args,
                dry_run=args.dry_run,
            )
            if result:
                _log_process(log, "restore", result)
            if not args.dry_run:
                _write_restore_metadata(paths["run_dir"], subvolume, target_path)
        except subprocess.CalledProcessError as exc:
            _log_process_error(log, "restore", exc)
            return 1
        except Exception as exc:
            log.write(f"restore failed: {exc}", level="ERROR")
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


def _resolve_target_path(args, paths: dict[str, str], subvolume: str) -> str:
    run_dir = os.path.abspath(paths["run_dir"])
    if args.target:
        target = os.path.abspath(args.target)
        _ensure_under_root(run_dir, target)
    else:
        target_base = args.target_base
        if target_base is None:
            target_base = os.path.join(run_dir, "restore")
        target_base = os.path.abspath(target_base)
        _ensure_under_root(run_dir, target_base)
        target_name = args.target_name or _default_target_name(subvolume)
        target = os.path.join(target_base, target_name)
        _ensure_under_root(run_dir, target)

    if os.path.exists(target):
        raise ValueError(f"target path already exists: {target}")

    os.makedirs(os.path.dirname(target), exist_ok=True)
    return target


def _default_target_name(subvolume: str) -> str:
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{subvolume}__restore__{timestamp}"


def _write_restore_metadata(run_dir: str, subvolume: str, target_path: str) -> None:
    payload = {
        "subvolume": subvolume,
        "target_path": target_path,
        "written_at": datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }
    metadata_path = os.path.join(run_dir, "restore_target.json")
    os.makedirs(run_dir, exist_ok=True)
    with open(metadata_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _ensure_under_root(root: str, path: str) -> None:
    root = os.path.abspath(root)
    path = os.path.abspath(path)
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
