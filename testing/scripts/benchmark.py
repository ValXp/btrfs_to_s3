"""Generate a benchmark summary from harness logs."""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.config import load_config
from harness.logs import open_log, parse_stats


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize harness logs.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    logs_dir = paths["logs_dir"]
    log_path = os.path.join(logs_dir, "benchmark.log")
    os.makedirs(logs_dir, exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        stats = _collect_stats(logs_dir)
        output_path = os.path.join(logs_dir, "benchmark.json")
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(stats, handle, indent=2, sort_keys=True)
            handle.write("\n")
        log.write(f"wrote benchmark summary to {output_path}")

    return 0


def _collect_stats(logs_dir: str) -> dict[str, object]:
    summary: dict[str, object] = {
        "generated_at": _timestamp(),
        "logs_dir": logs_dir,
        "log_stats": {},
        "total_errors": 0,
        "total_warnings": 0,
    }
    total_errors = 0
    total_warnings = 0
    log_stats: dict[str, object] = {}
    for name in sorted(os.listdir(logs_dir)):
        if not name.endswith(".log"):
            continue
        path = os.path.join(logs_dir, name)
        stats = parse_stats(path)
        log_stats[name] = stats
        total_errors += stats.get("errors", 0) or 0
        total_warnings += stats.get("warnings", 0) or 0
    summary["log_stats"] = log_stats
    summary["total_errors"] = total_errors
    summary["total_warnings"] = total_warnings
    return summary


def _timestamp() -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.isoformat(timespec="seconds").replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
