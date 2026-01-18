"""CLI parsing tests."""

from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from btrfs_to_s3 import cli
from btrfs_to_s3.config import (
    Config,
    GlobalConfig,
    RestoreConfig,
    S3Config,
    ScheduleConfig,
    SnapshotsConfig,
    SubvolumesConfig,
)
from btrfs_to_s3.lock import LockError
from btrfs_to_s3.planner import PlanItem
from btrfs_to_s3.snapshots import Snapshot

CONFIG_TOML = """
[subvolumes]
paths = ["/srv/data/data"]

[s3]
bucket = "bucket-name"
region = "us-east-1"
prefix = "backup/data"
"""


class CliTests(unittest.TestCase):
    def _make_config(self, temp_dir: str) -> Config:
        return Config(
            global_cfg=GlobalConfig(
                log_level="info",
                state_path=Path(temp_dir) / "state.json",
                lock_path=Path(temp_dir) / "lock",
                spool_dir=Path(temp_dir) / "spool",
                spool_size_bytes=1024,
            ),
            schedule=ScheduleConfig(
                full_every_days=180,
                incremental_every_days=7,
                run_at="02:00",
            ),
            snapshots=SnapshotsConfig(
                base_dir=Path(temp_dir) / "snapshots", retain=2
            ),
            subvolumes=SubvolumesConfig(
                paths=(Path(temp_dir) / "data",)
            ),
            s3=S3Config(
                bucket="bucket",
                region="us-east-1",
                prefix="backup/data",
                chunk_size_bytes=2048,
                storage_class_chunks="STANDARD",
                storage_class_manifest="STANDARD",
                concurrency=1,
                sse="AES256",
            ),
            restore=RestoreConfig(
                target_base_dir=Path(temp_dir) / "restore",
                verify_mode="full",
                sample_max_files=100,
                wait_for_restore=True,
                restore_timeout_seconds=3600,
                restore_tier="Standard",
            ),
        )

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

    def test_backup_skips_when_not_due(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            args = cli.parse_args(
                ["backup", "--config", str(Path(temp_dir) / "config.toml")]
            )
            args.dry_run = False
            args.no_s3 = False
            args.once = False
            args.subvolume = None
            plan = [
                PlanItem(
                    subvolume="data",
                    action="skip",
                    parent_snapshot="snap",
                    reason="incremental_not_due",
                )
            ]
            with mock.patch(
                "btrfs_to_s3.cli.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.cli._has_aws_credentials",
                side_effect=AssertionError("credentials should not be checked"),
            ):
                result = cli.run_backup(args, config)
            self.assertEqual(result, 0)

    def test_backup_once_forces_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            args = cli.parse_args(
                ["backup", "--config", str(Path(temp_dir) / "config.toml")]
            )
            args.dry_run = False
            args.no_s3 = False
            args.once = True
            args.subvolume = None
            plan = [
                PlanItem(
                    subvolume="data",
                    action="skip",
                    parent_snapshot="snap",
                    reason="incremental_not_due",
                )
            ]
            with mock.patch(
                "btrfs_to_s3.cli.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.cli._has_aws_credentials", return_value=False
            ) as creds_check:
                result = cli.run_backup(args, config)
            self.assertEqual(result, 0)
            self.assertTrue(creds_check.called)

    def test_backup_lock_contention_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            args = cli.parse_args(
                ["backup", "--config", str(Path(temp_dir) / "config.toml")]
            )
            args.dry_run = False
            args.no_s3 = False
            args.once = False
            args.subvolume = None

            class FakeLock:
                def __init__(self, path: Path) -> None:
                    self.path = path

                def acquire(self):
                    raise LockError("locked")

            with mock.patch("btrfs_to_s3.cli.LockFile", FakeLock):
                result = cli.run_backup(args, config)
            self.assertEqual(result, 1)

    def test_backup_releases_lock_on_skip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            args = cli.parse_args(
                ["backup", "--config", str(Path(temp_dir) / "config.toml")]
            )
            args.dry_run = False
            args.no_s3 = False
            args.once = False
            args.subvolume = None
            plan = [
                PlanItem(
                    subvolume="data",
                    action="skip",
                    parent_snapshot="snap",
                    reason="incremental_not_due",
                )
            ]
            lock_state: dict[str, bool] = {"released": False}

            class FakeLock:
                def __init__(self, path: Path) -> None:
                    self.path = path

                def acquire(self):
                    return self

                def release(self) -> None:
                    lock_state["released"] = True

            with mock.patch(
                "btrfs_to_s3.cli.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.cli.LockFile", FakeLock
            ):
                result = cli.run_backup(args, config)
            self.assertEqual(result, 0)
            self.assertTrue(lock_state["released"])

    def test_backup_missing_parent_falls_back_to_full(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            args = cli.parse_args(
                ["backup", "--config", str(Path(temp_dir) / "config.toml")]
            )
            args.dry_run = False
            args.no_s3 = False
            args.once = False
            args.subvolume = None
            missing_parent = str(Path(temp_dir) / "missing_snapshot")
            plan = [
                PlanItem(
                    subvolume="data",
                    action="inc",
                    parent_snapshot=missing_parent,
                    reason="incremental_due",
                )
            ]
            created: dict[str, str] = {}
            parent_holder: dict[str, object | None] = {}

            class FakeProcess:
                returncode = 0

                def communicate(self):
                    return b"", b""

            class FakeStream:
                def __init__(self) -> None:
                    self.stdout = io.BytesIO(b"")
                    self.process = FakeProcess()

            def fake_open_btrfs_send(path: Path, parent: Path | None):
                parent_holder["parent"] = parent
                return FakeStream()

            def fake_create_snapshot(self, subvolume_path, subvolume_name, kind):
                created["kind"] = kind
                return Snapshot(
                    name="data__20260101T000000Z__full",
                    path=Path(temp_dir) / "snap",
                    kind=kind,
                    created_at=datetime.now(timezone.utc),
                )

            with mock.patch(
                "btrfs_to_s3.cli.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.cli._has_aws_credentials", return_value=True
            ), mock.patch(
                "btrfs_to_s3.cli.boto3.client", return_value=object()
            ), mock.patch(
                "btrfs_to_s3.cli.chunk_stream", return_value=iter([])
            ), mock.patch(
                "btrfs_to_s3.cli.publish_manifest"
            ), mock.patch(
                "btrfs_to_s3.cli.open_btrfs_send",
                side_effect=fake_open_btrfs_send,
            ), mock.patch.object(
                cli.SnapshotManager, "create_snapshot", fake_create_snapshot
            ), mock.patch.object(
                cli.SnapshotManager, "prune_snapshots", return_value=[]
            ):
                result = cli.run_backup(args, config)
            self.assertEqual(result, 0)
            self.assertEqual(created.get("kind"), "full")
            self.assertIsNone(parent_holder.get("parent"))


if __name__ == "__main__":
    unittest.main()
