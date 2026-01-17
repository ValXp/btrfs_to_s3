"""Log helpers for harness scripts."""

from __future__ import annotations

from typing import Any
import datetime
import os


class LogFile:
    """Append-only log file with timestamped entries."""

    def __init__(self, path: str) -> None:
        self.path = path
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        self._handle = open(path, "a", encoding="utf-8")

    def write(self, message: str, *, level: str = "INFO") -> None:
        ts = _timestamp()
        line = f"{ts} {level.upper()} {message}\n"
        self._handle.write(line)
        self._handle.flush()

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "LogFile":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def open_log(path: str) -> LogFile:
    """Open a log file for appending."""
    return LogFile(path)


def parse_stats(path: str) -> dict[str, Any]:
    """Parse basic stats from a log file."""
    stats: dict[str, Any] = {
        "lines": 0,
        "errors": 0,
        "warnings": 0,
        "first_ts": None,
        "last_ts": None,
        "duration_seconds": None,
    }
    if not os.path.exists(path):
        return stats

    first_dt: datetime.datetime | None = None
    last_dt: datetime.datetime | None = None

    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            stats["lines"] += 1
            line = raw_line.rstrip("\n")
            if not line:
                continue
            parts = line.split(" ", 2)
            if len(parts) < 2:
                continue
            ts_str, level = parts[0], parts[1]
            if level == "ERROR":
                stats["errors"] += 1
            elif level == "WARN":
                stats["warnings"] += 1

            parsed = _parse_timestamp(ts_str)
            if parsed is None:
                continue
            if first_dt is None or parsed < first_dt:
                first_dt = parsed
            if last_dt is None or parsed > last_dt:
                last_dt = parsed

    if first_dt is not None:
        stats["first_ts"] = _format_timestamp(first_dt)
    if last_dt is not None:
        stats["last_ts"] = _format_timestamp(last_dt)
    if first_dt is not None and last_dt is not None:
        stats["duration_seconds"] = int((last_dt - first_dt).total_seconds())

    return stats


def _timestamp() -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    return _format_timestamp(now)


def _format_timestamp(value: datetime.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime.datetime | None:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
