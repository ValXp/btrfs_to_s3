"""Run a backup with spool configuration enabled."""

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
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test_spool.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run backup with spooling enabled.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_spool.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        s3_cfg = config.get("s3", {})
        if isinstance(s3_cfg, dict):
            log.write(f"spool_enabled={s3_cfg.get('spool_enabled')!r}")
        global_cfg = config.get("global", {})
        if isinstance(global_cfg, dict):
            log.write(f"spool_size_bytes={global_cfg.get('spool_size_bytes')!r}")
        subvolumes = config["btrfs"]["subvolumes"]
        if not subvolumes:
            log.write("no subvolumes configured", level="ERROR")
            return 1
        if args.dry_run:
            log.write("dry run: printing commands only")
        try:
            for name in subvolumes:
                log.write(f"running backup with spool for subvolume {name}")
                result = run_tool(
                    config_path,
                    ["backup", "--once", "--subvolume", name],
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
