"""Config loading and validation tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from btrfs_to_s3 import config as config_module


VALID_TOML = """
[global]
log_level = "info"
state_path = "/tmp/btrfs_to_s3/state.json"
lock_path = "/tmp/btrfs_to_s3/lock"
spool_dir = "/tmp/btrfs_to_s3/spool"
spool_size_bytes = 1024

[schedule]
full_every_days = 180
incremental_every_days = 7
run_at = "02:00"

[snapshots]
base_dir = "/tmp/btrfs_to_s3/snapshots"
retain = 2

[subvolumes]
paths = ["/srv/data/data", "/srv/data/root"]

[s3]
bucket = "bucket-name"
region = "us-east-1"
prefix = "backup/data"
chunk_size_bytes = 2048
storage_class_chunks = "STANDARD"
storage_class_manifest = "STANDARD"
concurrency = 2
spool_enabled = false
sse = "AES256"
"""


class ConfigTests(unittest.TestCase):
    def test_load_valid_config(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(VALID_TOML)
            path = Path(handle.name)
        config = config_module.load_config(path)
        self.assertEqual(config.s3.bucket, "bucket-name")
        self.assertEqual(config.schedule.run_at, "02:00")
        self.assertEqual(len(config.subvolumes.paths), 2)

    def test_rejects_relative_paths(self) -> None:
        toml = VALID_TOML.replace("/tmp/btrfs_to_s3/state.json", "state.json")
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(toml)
            path = Path(handle.name)
        with self.assertRaises(config_module.ConfigError):
            config_module.load_config(path)

    def test_rejects_invalid_chunk_size(self) -> None:
        toml = VALID_TOML.replace("chunk_size_bytes = 2048", "chunk_size_bytes = 0")
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(toml)
            path = Path(handle.name)
        with self.assertRaises(config_module.ConfigError):
            config_module.load_config(path)

    def test_rejects_invalid_cadence(self) -> None:
        toml = VALID_TOML.replace("full_every_days = 180", "full_every_days = 0")
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(toml)
            path = Path(handle.name)
        with self.assertRaises(config_module.ConfigError):
            config_module.load_config(path)

    def test_rejects_invalid_run_at(self) -> None:
        toml = VALID_TOML.replace("run_at = \"02:00\"", "run_at = \"25:00\"")
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(toml)
            path = Path(handle.name)
        with self.assertRaises(config_module.ConfigError):
            config_module.load_config(path)


if __name__ == "__main__":
    unittest.main()
