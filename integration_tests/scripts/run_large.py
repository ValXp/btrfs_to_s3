"""Run a large dataset scenario to force multi-chunk uploads."""

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
from harness import manifest as manifest_lib


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test_large.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a full + incremental backup with a large dataset."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_large.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    steps = [
        ("setup", "setup_btrfs.py", []),
        ("seed", "seed_data.py", []),
        ("full", "run_full.py", []),
        ("mutate", "mutate_data.py", []),
        ("incremental", "run_incremental.py", ["--skip-mutate"]),
    ]
    teardown_step = ("teardown", "teardown_btrfs.py", [])

    success = True
    multi_chunk_observed = False
    manifest_path = os.path.join(paths["run_dir"], "manifest.json")

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            for name, script, extra_args in steps:
                if not _run_step(
                    name,
                    script,
                    config_path,
                    extra_args,
                    log,
                    dry_run=args.dry_run,
                ):
                    success = False
                    break
                if not args.dry_run and name in {"full", "incremental"}:
                    try:
                        if _check_multi_chunk(manifest_path, log, name):
                            multi_chunk_observed = True
                    except Exception as exc:
                        log.write(f"manifest check failed: {exc}", level="ERROR")
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
                dry_run=args.dry_run,
            ):
                success = False

        if not args.dry_run and success and not multi_chunk_observed:
            log.write("multi-chunk upload not observed", level="ERROR")
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
    dry_run: bool = False,
) -> bool:
    script_path = os.path.join(os.path.dirname(__file__), script)
    command = [sys.executable, script_path, "--config", config_path]
    command.extend(extra_args)
    log.write(f"running {name}: {shlex.join(command)}")
    if dry_run:
        return True
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


def _check_multi_chunk(path: str, log, label: str) -> bool:
    manifest = manifest_lib.load_manifest(path)
    chunks = manifest.get("chunks")
    if not isinstance(chunks, list):
        log.write(f"{label}: manifest missing chunks", level="ERROR")
        raise ValueError("manifest missing chunks")
    chunk_count = len(chunks)
    log.write(f"{label}: manifest chunk count {chunk_count}")
    return chunk_count > 1


if __name__ == "__main__":
    raise SystemExit(main())
