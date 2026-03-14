"""Tests for integration harness config loading."""

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from integration_tests.harness import config as harness_config


REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "integration_tests" / "config"


class HarnessConfigTests(unittest.TestCase):
    def test_loads_legacy_btrfs_config_shape(self) -> None:
        config = harness_config.load_config(str(CONFIG_DIR / "test.toml"))

        self.assertEqual(config["filesystem"]["backend"], "btrfs")
        self.assertEqual(config["btrfs"]["subvolumes"], ["data", "root", "home"])

    def test_loads_valid_zfs_config(self) -> None:
        config = harness_config.load_config(str(CONFIG_DIR / "test_zfs.toml"))

        self.assertEqual(config["filesystem"]["backend"], "zfs")
        self.assertEqual(config["zfs"]["pool_name"], "btrfs_to_s3_test")
        self.assertEqual(config["zfs"]["source_datasets"], ["data", "home"])

    def test_rejects_zfs_config_without_backend_selector(self) -> None:
        invalid_toml = textwrap.dedent(
            """
            [tool]
            cmd = ["python3", "-m", "btrfs_to_s3"]
            config_flag = "--config"

            [paths]
            run_dir = "integration_tests/run/zfs"
            logs_dir = "integration_tests/run/zfs/logs"
            scratch_dir = "integration_tests/run/zfs/scratch"
            lock_dir = "integration_tests/run/zfs/lock"

            [zfs]
            pool_name = "btrfs_to_s3_test"
            pool_file = "integration_tests/run/zfs/pool.img"
            pool_size_gib = 4
            mount_root = "integration_tests/run/zfs/mnt"
            snapshot_prefix = "btrfs-to-s3"
            receive_parent_dataset = "restore"
            source_datasets = ["data"]

            [aws]
            region = "us-east-1"
            bucket = "codex-btrfs-test-bucket"
            prefix = "btrfs-to-s3-test/zfs/"
            storage_class = "STANDARD"
            sse = "AES256"

            [backup]
            chunk_size_mib = 64
            concurrency = 4
            retention_snapshots = 2
            """
        )

        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as handle:
            handle.write(invalid_toml)
            path = handle.name

        with self.assertRaisesRegex(
            ValueError,
            r'ZFS configs must set \[filesystem\]\.backend = "zfs"',
        ):
            harness_config.load_config(path)

    def test_rejects_invalid_zfs_config(self) -> None:
        invalid_toml = textwrap.dedent(
            """
            [tool]
            cmd = ["python3", "-m", "btrfs_to_s3"]
            config_flag = "--config"

            [filesystem]
            backend = "zfs"

            [paths]
            run_dir = "integration_tests/run/zfs"
            logs_dir = "integration_tests/run/zfs/logs"
            scratch_dir = "integration_tests/run/zfs/scratch"
            lock_dir = "integration_tests/run/zfs/lock"

            [zfs]
            pool_name = "btrfs_to_s3_test"
            pool_file = "integration_tests/run/zfs/pool.img"
            pool_size_gib = 4
            mount_root = "integration_tests/run/zfs/mnt"
            snapshot_prefix = "btrfs-to-s3"
            receive_parent_dataset = "restore"
            source_datasets = []

            [aws]
            region = "us-east-1"
            bucket = "codex-btrfs-test-bucket"
            prefix = "btrfs-to-s3-test/zfs/"
            storage_class = "STANDARD"
            sse = "AES256"

            [backup]
            chunk_size_mib = 64
            concurrency = 4
            retention_snapshots = 2
            """
        )

        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as handle:
            handle.write(invalid_toml)
            path = handle.name

        with self.assertRaisesRegex(ValueError, r"\[zfs\].*source_datasets"):
            harness_config.load_config(path)


if __name__ == "__main__":
    unittest.main()
