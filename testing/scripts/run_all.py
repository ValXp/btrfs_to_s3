"""Run the full end-to-end harness sequence."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.config import load_config
from harness.logs import open_log


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full test harness.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_all.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    steps = [
        ("setup", "setup_btrfs.py", []),
        ("seed", "seed_data.py", []),
        ("full", "run_full.py", []),
        ("mutate", "mutate_data.py", []),
        ("incremental", "run_incremental.py", ["--skip-mutate"]),
        ("interrupt", "run_interrupt.py", []),
        ("verify_manifest", "verify_manifest.py", []),
        ("verify_s3", "verify_s3.py", []),
        ("verify_retention", "verify_retention.py", []),
    ]
    teardown_step = ("teardown", "teardown_btrfs.py", [])

    success = True
    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            for name, script, extra_args in steps:
                if not _run_step(name, script, config_path, extra_args, log):
                    success = False
                    break
        finally:
            if not _run_step(
                teardown_step[0],
                teardown_step[1],
                config_path,
                teardown_step[2],
                log,
                allow_failure=True,
            ):
                success = False

    return 0 if success else 1


def _run_step(
    name: str,
    script: str,
    config_path: str,
    extra_args: list[str],
    log,
    *,
    allow_failure: bool = False,
) -> bool:
    script_path = os.path.join(os.path.dirname(__file__), script)
    command = [sys.executable, script_path, "--config", config_path]
    command.extend(extra_args)
    log.write(f"running {name}: {shlex.join(command)}")
    try:
        result = subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=True,
        )
        if result.stdout:
            log.write(f"{name} stdout: {result.stdout.strip()}")
        if result.stderr:
            log.write(f"{name} stderr: {result.stderr.strip()}", level="WARN")
        return True
    except subprocess.CalledProcessError as exc:
        log.write(f"{name} failed with code {exc.returncode}", level="ERROR")
        if exc.stdout:
            log.write(f"{name} stdout: {exc.stdout.strip()}", level="ERROR")
        if exc.stderr:
            log.write(f"{name} stderr: {exc.stderr.strip()}", level="ERROR")
        return allow_failure


if __name__ == "__main__":
    raise SystemExit(main())
