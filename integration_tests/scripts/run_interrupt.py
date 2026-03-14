"""Run a backup, interrupt it, then rerun."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.config import load_config
from harness.filesystem import backend_name, source_identifiers
from harness.logs import open_log
from harness import runner
from harness.env import load_env


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Interrupt a backup and rerun.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument(
        "--source",
        "--subvolume",
        dest="source",
        default=None,
        help=(
            "Source identifier to interrupt. Defaults to the first configured ZFS "
            "source and to all sources for legacy Btrfs configs."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=int, default=5)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_interrupt.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        os.environ["BTRFS_TO_S3_BACKUP_TYPE"] = "full"
        try:
            extra_args = _resolve_backup_args(config, args.source)
        except ValueError as exc:
            log.write(str(exc), level="ERROR")
            return 1
        if len(extra_args) > 2:
            log.write(f"interrupting source {extra_args[-1]}")
        elif backend_name(config) == "zfs":
            log.write("interrupting all configured ZFS sources")
        if args.dry_run:
            log.write("dry run: printing command only")
            runner.run_tool(config_path, extra_args, dry_run=True)
            return 0

        env_path = os.path.join(os.path.dirname(config_path), "test.env")
        if os.path.exists(env_path):
            load_env(env_path, override=False)

        command = runner.build_command(config, config_path, extra_args)
        env = runner.build_env()
        env["BTRFS_TO_S3_HARNESS_RUN_DIR"] = os.path.abspath(paths["run_dir"])
        log.write(f"starting backup: {shlex.join(command)}")

        process = subprocess.Popen(
            command,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(max(args.sleep, 0))

        if process.poll() is None:
            log.write(f"terminating backup pid={process.pid}")
            process.terminate()
            try:
                stdout, stderr = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
        else:
            stdout, stderr = process.communicate()
            log.write("process completed before interrupt")

        if stdout:
            log.write(f"interrupt stdout: {stdout.strip()}")
        if stderr:
            log.write(f"interrupt stderr: {stderr.strip()}", level="WARN")
        log.write(f"interrupt exit code {process.returncode}")

        try:
            result = runner.run_command(command, env=env)
            if result:
                _log_process(log, "rerun", result)
        except subprocess.CalledProcessError as exc:
            _log_process_error(log, "rerun", exc)
            return 1
        except Exception as exc:
            log.write(f"rerun failed: {exc}", level="ERROR")
            return 1

    return 0


def _resolve_backup_args(config: dict, requested_source: str | None) -> list[str]:
    sources = source_identifiers(config)
    if requested_source == "all":
        return ["backup"]
    if requested_source is not None:
        if requested_source not in sources:
            raise ValueError(
                f"source {requested_source} not in config list: {', '.join(sources)}"
            )
        return ["backup", "--source", requested_source]
    if backend_name(config) == "zfs":
        if not sources:
            raise ValueError("config has no sources")
        return ["backup", "--source", sources[0]]
    return ["backup"]


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
