"""CLI parsing tests."""

from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from btrfs_to_s3 import cli

CONFIG_TOML = """
[subvolumes]
paths = ["/srv/data/data"]

[s3]
bucket = "bucket-name"
region = "us-east-1"
prefix = "backup/data"
"""


class CliTests(unittest.TestCase):
    def test_parse_backup_args(self) -> None:
        args = cli.parse_args(
            [
                "backup",
                "--config",
                "/tmp/config.toml",
                "--log-level",
                "debug",
                "--dry-run",
                "--subvolume",
                "data",
                "--subvolume",
                "root",
                "--once",
                "--no-s3",
            ]
        )
        self.assertEqual(args.command, "backup")
        self.assertEqual(args.config, "/tmp/config.toml")
        self.assertEqual(args.log_level, "debug")
        self.assertTrue(args.dry_run)
        self.assertEqual(args.subvolume, ["data", "root"])
        self.assertTrue(args.once)
        self.assertTrue(args.no_s3)

    def test_main_runs_with_config(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(CONFIG_TOML)
            path = Path(handle.name)
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code = cli.main(["backup", "--config", str(path)])
        self.assertEqual(exit_code, 0)

    def test_parse_restore_args(self) -> None:
        args = cli.parse_args(
            [
                "restore",
                "--config",
                "/tmp/config.toml",
                "--subvolume",
                "data",
                "--target",
                "/srv/restore/data",
                "--manifest-key",
                "subvol/data/full/manifest.json",
                "--restore-timeout",
                "120",
                "--no-wait-restore",
                "--verify",
                "sample",
            ]
        )
        self.assertEqual(args.command, "restore")
        self.assertEqual(args.config, "/tmp/config.toml")
        self.assertEqual(args.subvolume, "data")
        self.assertEqual(args.target, "/srv/restore/data")
        self.assertEqual(args.manifest_key, "subvol/data/full/manifest.json")
        self.assertEqual(args.restore_timeout, 120)
        self.assertFalse(args.wait_restore)
        self.assertEqual(args.verify, "sample")


if __name__ == "__main__":
    unittest.main()
