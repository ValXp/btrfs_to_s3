"""Config loading and validation tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from btrfs_to_s3 import config as config_module


MIN_SPOOL_SIZE_BYTES = 5 * 1024 * 1024


VALID_TOML = """
[global]
log_level = "info"
state_path = "/tmp/btrfs_to_s3/state.json"
lock_path = "/tmp/btrfs_to_s3/lock"
spool_dir = "/tmp/btrfs_to_s3/spool"
spool_size_bytes = 5242880

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

VALID_ZFS_TOML = """
[global]
log_level = "info"
state_path = "/tmp/btrfs_to_s3/state.json"
lock_path = "/tmp/btrfs_to_s3/lock"
spool_dir = "/tmp/btrfs_to_s3/spool"
spool_size_bytes = 5242880

[schedule]
full_every_days = 180
incremental_every_days = 7
run_at = "02:00"

[filesystem]
backend = "zfs"

[snapshots]
retain = 2

[zfs]
pool_name = "tank"
mount_root = "/tank"
source_datasets = ["tank/data", "tank/home"]
receive_parent_dataset = "tank/restore"
snapshot_prefix = "btrfs-to-s3"

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
                "spool_size_bytes": MIN_SPOOL_SIZE_BYTES,
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

    def _valid_zfs_data(self) -> dict[str, object]:
        return {
            "global": {
                "log_level": "info",
                "state_path": "/tmp/btrfs_to_s3/state.json",
                "lock_path": "/tmp/btrfs_to_s3/lock",
                "spool_dir": "/tmp/btrfs_to_s3/spool",
                "spool_size_bytes": MIN_SPOOL_SIZE_BYTES,
            },
            "schedule": {
                "full_every_days": 180,
                "incremental_every_days": 7,
                "run_at": "02:00",
            },
            "filesystem": {"backend": "zfs"},
            "snapshots": {"retain": 2},
            "zfs": {
                "pool_name": "tank",
                "mount_root": "/tank",
                "source_datasets": ["tank/data", "tank/home"],
                "receive_parent_dataset": "tank/restore",
                "snapshot_prefix": "btrfs-to-s3",
            },
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

    def test_load_valid_legacy_btrfs_config(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(VALID_TOML)
            path = Path(handle.name)
        config = config_module.load_config(path)
        self.assertEqual(config.s3.bucket, "bucket-name")
        self.assertEqual(config.schedule.run_at, "02:00")
        self.assertEqual(len(config.subvolumes.paths), 2)
        self.assertEqual(
            config.filesystem.backend,
            config_module.DEFAULT_FILESYSTEM_BACKEND,
        )
        self.assertIsNone(config.zfs)

    def test_load_valid_zfs_config(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(VALID_ZFS_TOML)
            path = Path(handle.name)
        config = config_module.load_config(path)
        self.assertEqual(config.filesystem.backend, "zfs")
        self.assertIsNotNone(config.zfs)
        assert config.zfs is not None
        self.assertEqual(config.zfs.pool_name, "tank")
        self.assertEqual(
            config.zfs.source_datasets,
            ("tank/data", "tank/home"),
        )
        self.assertIsNone(config.snapshots.base_dir)
        self.assertEqual(config.subvolumes.paths, ())

    def test_rejects_relative_paths(self) -> None:
        toml = VALID_TOML.replace("/tmp/btrfs_to_s3/state.json", "state.json")
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(toml)
            path = Path(handle.name)
        with self.assertRaises(config_module.ConfigError):
            config_module.load_config(path)

    def test_rejects_duplicate_btrfs_source_identifiers(self) -> None:
        data = self._valid_data()
        data["subvolumes"] = {
            "paths": ["/srv/primary/data", "/srv/archive/data"]
        }

        with self.assertRaises(config_module.ConfigError) as context:
            config_module.Config.from_dict(data)

        self.assertIn(
            "duplicate source identifiers in subvolumes.paths",
            str(context.exception),
        )
        self.assertIn("data", str(context.exception))

    def test_rejects_duplicate_zfs_source_identifiers_after_normalization(
        self,
    ) -> None:
        data = self._valid_zfs_data()
        data["zfs"] = {
            "pool_name": "tank",
            "mount_root": "/tank",
            "source_datasets": ["data", "tank/data"],
            "receive_parent_dataset": "tank/restore",
            "snapshot_prefix": "btrfs-to-s3",
        }

        with self.assertRaises(config_module.ConfigError) as context:
            config_module.Config.from_dict(data)

        self.assertIn(
            "duplicate source identifiers in zfs.source_datasets",
            str(context.exception),
        )
        self.assertIn("tank/data", str(context.exception))

    def test_rejects_invalid_chunk_size(self) -> None:
        toml = VALID_TOML.replace("chunk_size_bytes = 2048", "chunk_size_bytes = 0")
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(toml)
            path = Path(handle.name)
        with self.assertRaises(config_module.ConfigError):
            config_module.load_config(path)

    def test_rejects_spool_size_below_minimum(self) -> None:
        data = self._valid_data()
        data["global"]["spool_size_bytes"] = MIN_SPOOL_SIZE_BYTES - 1

        with self.assertRaisesRegex(
            config_module.ConfigError,
            r"global\.spool_size_bytes must be >= 5 MiB",
        ):
            config_module.Config.from_dict(data)

    def test_accepts_spool_size_at_minimum(self) -> None:
        data = self._valid_data()
        data["global"]["spool_size_bytes"] = MIN_SPOOL_SIZE_BYTES

        config = config_module.Config.from_dict(data)

        self.assertEqual(
            config.global_cfg.spool_size_bytes,
            MIN_SPOOL_SIZE_BYTES,
        )

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

    def test_rejects_malformed_toml(self) -> None:
        toml = (
            "[global]\n"
            'log_level = "bad\\q"\n'
            "[subvolumes]\n"
            'paths = ["/srv/data/data"]\n'
            "[s3]\n"
            'bucket = "bucket-name"\n'
            'region = "us-east-1"\n'
            'prefix = "backup/data"\n'
        )
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(toml)
            path = Path(handle.name)
        with self.assertRaises(config_module.ConfigError) as context:
            config_module.load_config(path)
        self.assertIn("failed to parse config", str(context.exception))

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
        self.assertEqual(
            config.filesystem.backend,
            config_module.DEFAULT_FILESYSTEM_BACKEND,
        )

    def test_from_dict_loads_zfs_backend(self) -> None:
        config = config_module.Config.from_dict(self._valid_zfs_data())
        self.assertEqual(config.filesystem.backend, "zfs")
        self.assertIsNotNone(config.zfs)
        assert config.zfs is not None
        self.assertEqual(config.zfs.mount_root, Path("/tank"))
        self.assertEqual(config.snapshots.retain, 2)
        self.assertIsNone(config.snapshots.base_dir)

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

    def test_allows_missing_subvolumes_for_restore_only_config(self) -> None:
        data = self._valid_data()
        data["subvolumes"]["paths"] = []
        config = config_module.Config.from_dict(data)
        self.assertEqual(config.subvolumes.paths, ())

    def test_rejects_invalid_backend_name(self) -> None:
        data = self._valid_data()
        data["filesystem"] = {"backend": "xfs"}
        with self.assertRaisesRegex(
            config_module.ConfigError,
            r"filesystem\.backend",
        ):
            config_module.Config.from_dict(data)

    def test_rejects_zfs_without_section(self) -> None:
        data = self._valid_data()
        data["filesystem"] = {"backend": "zfs"}
        with self.assertRaisesRegex(
            config_module.ConfigError,
            r'zfs section is required when filesystem\.backend = "zfs"',
        ):
            config_module.Config.from_dict(data)

    def test_allows_zfs_without_source_datasets(self) -> None:
        data = self._valid_zfs_data()
        del data["zfs"]["source_datasets"]
        config = config_module.Config.from_dict(data)
        assert config.zfs is not None
        self.assertEqual(config.zfs.source_datasets, ())

    def test_allows_zfs_without_receive_parent_dataset(self) -> None:
        data = self._valid_zfs_data()
        del data["zfs"]["receive_parent_dataset"]

        config = config_module.Config.from_dict(data)

        assert config.zfs is not None
        self.assertIsNone(config.zfs.receive_parent_dataset)

    def test_rejects_relative_zfs_mount_root(self) -> None:
        data = self._valid_zfs_data()
        data["zfs"]["mount_root"] = "tank"
        with self.assertRaisesRegex(
            config_module.ConfigError,
            r"zfs\.mount_root",
        ):
            config_module.Config.from_dict(data)

    def test_rejects_missing_s3_bucket(self) -> None:
        data = self._valid_data()
        data["s3"]["bucket"] = ""
        with self.assertRaises(config_module.ConfigError):
            config_module.Config.from_dict(data)

    def test_rejects_non_boolean_s3_spool_enabled(self) -> None:
        data = self._valid_data()
        data["s3"]["spool_enabled"] = "false"
        with self.assertRaisesRegex(
            config_module.ConfigError,
            r"s3\.spool_enabled must be true or false",
        ):
            config_module.Config.from_dict(data)

    def test_rejects_non_boolean_restore_wait_for_restore(self) -> None:
        data = self._valid_data()
        data["restore"]["wait_for_restore"] = "false"
        with self.assertRaisesRegex(
            config_module.ConfigError,
            r"restore\.wait_for_restore must be true or false",
        ):
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
