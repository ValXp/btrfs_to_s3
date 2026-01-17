"""Metrics tests."""

from __future__ import annotations

import unittest

from btrfs_to_s3.metrics import calculate_metrics


class MetricsTests(unittest.TestCase):
    def test_metrics_throughput(self) -> None:
        metrics = calculate_metrics(total_bytes=1000, elapsed_seconds=2)
        self.assertEqual(metrics.total_bytes, 1000)
        self.assertEqual(metrics.elapsed_seconds, 2)
        self.assertEqual(metrics.throughput_bytes_per_sec, 500)

    def test_zero_elapsed(self) -> None:
        metrics = calculate_metrics(total_bytes=1000, elapsed_seconds=0)
        self.assertEqual(metrics.throughput_bytes_per_sec, 0)


if __name__ == "__main__":
    unittest.main()
