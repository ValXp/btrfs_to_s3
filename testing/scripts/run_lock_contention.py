"""Verify lock contention behavior by overlapping backup runs."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.config import load_config
from harness.logs import open_log
from harness.runner import build_command, build_env


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run overlapping backups to verify lock contention."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--subvolume", default=None)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "run_lock_contention.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        try:
            subvolume = _resolve_subvolume(args.subvolume, config)
        except ValueError as exc:
            log.write(str(exc), level="ERROR")
            return 1

        command = build_command(
            config,
            config_path,
            ["backup", "--subvolume", subvolume, "--no-s3"],
        )
        env = build_env(set_pythonpath=True)
        env["BTRFS_TO_S3_HARNESS_RUN_DIR"] = os.path.abspath(paths["run_dir"])
        lock_path = os.path.join(paths["lock_dir"], "btrfs_to_s3.lock")

        log.write(f"starting first backup: {' '.join(command)}")
        proc1 = subprocess.Popen(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        try:
            if not _wait_for_lock(lock_path, proc1.pid, timeout=10.0):
                log.write("lock file not observed for first backup", level="ERROR")
                return 1

            log.write("pausing first backup to hold lock")
            os.kill(proc1.pid, signal.SIGSTOP)

            log.write("starting second backup to trigger contention")
            result = subprocess.run(
                command,
                text=True,
                capture_output=True,
                env=env,
            )

            output = (result.stdout or "") + (result.stderr or "")
            if result.returncode == 0:
                log.write("second backup unexpectedly succeeded", level="ERROR")
                return 1
            if not _is_lock_error(output):
                log.write(
                    "second backup failed without lock error", level="ERROR"
                )
                if output.strip():
                    log.write(output.strip(), level="ERROR")
                return 1

            log.write("lock contention verified")
            return 0
        finally:
            try:
                os.kill(proc1.pid, signal.SIGCONT)
            except OSError:
                pass
            _finalize_process(proc1, log)


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


def _wait_for_lock(lock_path: str, pid: int, *, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(lock_path):
            try:
                with open(lock_path, "r", encoding="utf-8") as handle:
                    contents = handle.read().strip()
            except OSError:
                contents = ""
            if contents == str(pid):
                return True
        time.sleep(0.1)
    return False


def _is_lock_error(output: str) -> bool:
    return "backup_lock_failed" in output or "lock already held" in output


def _finalize_process(proc: subprocess.Popen[str], log) -> None:
    try:
        stdout, stderr = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        log.write("first backup did not exit, terminating", level="WARN")
        proc.terminate()
        try:
            stdout, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            log.write("first backup did not terminate, killing", level="WARN")
            proc.kill()
            stdout, stderr = proc.communicate()
    if stdout:
        log.write(f"first backup stdout: {stdout.strip()}")
    if stderr:
        log.write(f"first backup stderr: {stderr.strip()}", level="WARN")


if __name__ == "__main__":
    raise SystemExit(main())
