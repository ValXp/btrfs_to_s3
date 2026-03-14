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
_BACKEND_LIFECYCLE_SCRIPTS = {
    "btrfs": ("setup_btrfs.py", "teardown_btrfs.py"),
    "zfs": ("setup_zfs.py", "teardown_zfs.py"),
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full test harness.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="Skip backup and S3 verification steps.",
    )
    parser.add_argument(
        "--include-large",
        action="store_true",
        help="Run the multi-chunk scenario after the main sequence.",
    )
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    backend = config["filesystem"]["backend"]
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_all.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    steps, teardown_step = _build_steps(config, skip_s3=args.skip_s3)

    success = True
    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        if backend == "zfs" and args.skip_s3:
            log.write("--skip-s3 has no effect for the ZFS probe flow", level="WARN")
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
        if args.include_large:
            if backend != "btrfs":
                log.write(
                    "skipping large scenario because it is only defined for the Btrfs harness",
                    level="WARN",
                )
            else:
                large_config_path = os.path.abspath(
                    os.path.join(
                        os.path.dirname(__file__), os.pardir, "config", "test_large.toml"
                    )
                )
                if success:
                    if not _run_step(
                        "large",
                        "run_large.py",
                        large_config_path,
                        [],
                        log,
                    ):
                        success = False
                else:
                    log.write(
                        "skipping large scenario due to earlier failure", level="WARN"
                    )

    return 0 if success else 1


def _build_steps(
    config: dict[str, object],
    *,
    skip_s3: bool,
) -> tuple[list[tuple[str, str, list[str]]], tuple[str, str, list[str]]]:
    backend = config["filesystem"]["backend"]
    setup_script, teardown_script = _lifecycle_scripts(backend)
    if backend == "btrfs":
        steps = [("setup", setup_script, []), ("seed", "seed_data.py", [])]
        if skip_s3:
            steps.append(("mutate", "mutate_data.py", []))
        else:
            steps.extend(
                [
                    ("full", "run_full.py", []),
                    ("mutate", "mutate_data.py", []),
                    ("incremental", "run_incremental.py", ["--skip-mutate"]),
                    ("interrupt", "run_interrupt.py", []),
                    ("verify_manifest", "verify_manifest.py", []),
                    ("verify_s3", "verify_s3.py", []),
                    ("verify_retention", "verify_retention.py", []),
                    ("restore", "run_restore.py", []),
                    ("restore_chain", "run_restore.py", ["--use-incremental-manifest"]),
                    ("verify_restore", "verify_restore.py", []),
                ]
            )
        return steps, ("teardown", teardown_script, [])
    if backend == "zfs":
        return (
            [
                ("setup", setup_script, []),
                ("snapshot_send_receive", "run_zfs_snapshot_send_receive.py", []),
                ("incremental", "run_zfs_incremental.py", []),
                ("retention", "run_zfs_retention.py", []),
            ],
            ("teardown", teardown_script, []),
        )
    raise ValueError(f"unsupported backend {backend!r}")


def _lifecycle_scripts(backend: str) -> tuple[str, str]:
    try:
        return _BACKEND_LIFECYCLE_SCRIPTS[backend]
    except KeyError as exc:
        raise ValueError(f"unsupported backend {backend!r}") from exc


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
