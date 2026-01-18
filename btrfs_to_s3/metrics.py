"""Metrics calculations."""

from __future__ import annotations

from dataclasses import dataclass

_THROUGHPUT_UNITS = ("B/s", "KB/s", "MB/s", "GB/s", "TB/s", "PB/s")


@dataclass(frozen=True)
class Metrics:
    total_bytes: int
    elapsed_seconds: float

    @property
    def throughput_bytes_per_sec(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.total_bytes / self.elapsed_seconds

    def to_dict(self) -> dict[str, float]:
        return {
            "total_bytes": float(self.total_bytes),
            "elapsed_seconds": float(self.elapsed_seconds),
            "throughput_bytes_per_sec": self.throughput_bytes_per_sec,
        }


def calculate_metrics(total_bytes: int, elapsed_seconds: float) -> Metrics:
    if total_bytes < 0:
        raise ValueError("total_bytes must be >= 0")
    if elapsed_seconds < 0:
        raise ValueError("elapsed_seconds must be >= 0")
    return Metrics(total_bytes=total_bytes, elapsed_seconds=elapsed_seconds)


def format_throughput(bytes_per_sec: float) -> str:
    value = max(bytes_per_sec, 0.0)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(_THROUGHPUT_UNITS) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f}{_THROUGHPUT_UNITS[unit_index]}"
