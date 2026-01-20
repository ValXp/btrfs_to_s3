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
    def _valid_data(self) -> dict[str, object]:
        return {
            "global": {
                "log_level": "info",
                "state_path": "/tmp/btrfs_to_s3/state.json",
                "lock_path": "/tmp/btrfs_to_s3/lock",
                "spool_dir": "/tmp/btrfs_to_s3/spool",
                "spool_size_bytes": 1024,
            },
            "schedule": {
                "full_every_days": 180,
                "incremental_every_days": 7,
                "run_at": "02:00",
            },
            "snapshots": {
                "base_dir": "/tmp/btrfs_to_s3/snapshots",
                "retain": 2,
            },
            "subvolumes": {"paths": ["/srv/data/data", "/srv/data/root"]},
            "s3": {
                "bucket": "bucket-name",
                "region": "us-east-1",
                "prefix": "backup/data",
                "chunk_size_bytes": 2048,
                "storage_class_chunks": "STANDARD",
                "storage_class_manifest": "STANDARD",
                "concurrency": 2,
                "spool_enabled": False,
                "sse": "AES256",
            },
            "restore": {
                "target_base_dir": "/srv/restore",
                "verify_mode": "full",
                "sample_max_files": 1000,
                "wait_for_restore": True,
                "restore_timeout_seconds": 3600,
                "restore_tier": "Standard",
            },
        }

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

    def test_rejects_relative_config_path(self) -> None:
        with self.assertRaises(config_module.ConfigError):
            config_module.load_config(Path("config.toml"))

    def test_rejects_missing_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            missing = Path(temp_dir) / "missing.toml"
            with self.assertRaises(config_module.ConfigError):
                config_module.load_config(missing)

    def test_rejects_unreadable_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            with self.assertRaises(config_module.ConfigError):
                config_module.load_config(path)

    def test_from_dict_uses_defaults(self) -> None:
        data = self._valid_data()
        data["schedule"] = {}
        data["global"] = {"log_level": "INFO"}
        data["s3"] = {"bucket": "bucket-name", "region": "us-east-1", "prefix": "x"}
        config = config_module.Config.from_dict(data)
        self.assertEqual(config.schedule.run_at, config_module.DEFAULT_RUN_AT)
        self.assertEqual(
            config.s3.chunk_size_bytes, config_module.DEFAULT_CHUNK_SIZE_BYTES
        )
        self.assertEqual(
            config.restore.verify_mode, config_module.DEFAULT_RESTORE_VERIFY_MODE
        )
        self.assertTrue(config.global_cfg.state_path.is_absolute())

    def test_rejects_invalid_log_level(self) -> None:
        data = self._valid_data()
        data["global"]["log_level"] = "verbose"
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)

    def test_rejects_invalid_snapshot_retain(self) -> None:
        data = self._valid_data()
        data["snapshots"]["retain"] = 0
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)

    def test_rejects_missing_subvolumes(self) -> None:
        data = self._valid_data()
        data["subvolumes"]["paths"] = []
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)

    def test_rejects_missing_s3_bucket(self) -> None:
        data = self._valid_data()
        data["s3"]["bucket"] = ""
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)

    def test_rejects_invalid_restore_mode(self) -> None:
        data = self._valid_data()
        data["restore"]["verify_mode"] = "bad"
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)

    def test_rejects_invalid_restore_sample_size(self) -> None:
        data = self._valid_data()
        data["restore"]["sample_max_files"] = 0
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)

    def test_rejects_invalid_restore_timeout(self) -> None:
        data = self._valid_data()
        data["restore"]["restore_timeout_seconds"] = 0
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)


if __name__ == "__main__":
    unittest.main()
