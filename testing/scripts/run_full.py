"""Run a full backup using the harness runner."""

from __future__ import annotations

import argparse
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
    parser = argparse.ArgumentParser(description="Run a full backup.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_full.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        os.environ["BTRFS_TO_S3_BACKUP_TYPE"] = "full"
        if args.dry_run:
            log.write("dry run: printing command only")
        try:
            subvolumes = config["btrfs"]["subvolumes"]
            if not subvolumes:
                log.write("no subvolumes configured", level="ERROR")
                return 1
            for name in subvolumes:
                log.write(f"running full backup for subvolume {name}")
                result = run_tool(
                    config_path,
                    ["backup", "--subvolume", name],
                    dry_run=args.dry_run,
                )
                if result:
                    _log_process(log, f"backup[{name}]", result)
        except subprocess.CalledProcessError as exc:
            _log_process_error(log, "backup", exc)
            return 1
        except Exception as exc:
            log.write(f"run failed: {exc}", level="ERROR")
            return 1

    return 0


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
