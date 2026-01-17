"""Run an incremental backup after mutating data."""

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
    parser = argparse.ArgumentParser(description="Run an incremental backup.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-mutate", action="store_true")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_incremental.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        os.environ["BTRFS_TO_S3_BACKUP_TYPE"] = "incremental"
        if args.dry_run:
            log.write("dry run: skipping mutation and printing command only")
        elif args.skip_mutate:
            log.write("skipping mutation step")
        else:
            try:
                _run_mutation(config_path, log)
            except subprocess.CalledProcessError as exc:
                _log_process_error(log, "mutate_data", exc)
                return 1
            except Exception as exc:
                log.write(f"mutation failed: {exc}", level="ERROR")
                return 1

        try:
            result = run_tool(
                config_path,
                ["backup"],
                dry_run=args.dry_run,
            )
            if result:
                _log_process(log, "backup", result)
        except subprocess.CalledProcessError as exc:
            _log_process_error(log, "backup", exc)
            return 1
        except Exception as exc:
            log.write(f"run failed: {exc}", level="ERROR")
            return 1

    return 0


def _run_mutation(config_path: str, log) -> None:
    script_path = os.path.join(os.path.dirname(__file__), "mutate_data.py")
    command = [sys.executable, script_path, "--config", config_path]
    log.write(f"running mutation: {' '.join(command)}")
    result = subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
    )
    _log_process(log, "mutate_data", result)


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
