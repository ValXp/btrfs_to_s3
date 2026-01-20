"""CLI parsing tests."""

from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
import hashlib
import logging

from btrfs_to_s3 import cli
from btrfs_to_s3.orchestrator import (
    BackupOrchestrator,
    BackupRequest,
    RestoreOrchestrator,
    RestoreRequest,
)
from btrfs_to_s3.uploader import UploadResult
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
from btrfs_to_s3.snapshots import Snapshot, SnapshotManager

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
                spool_enabled=False,
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
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )
            plan = [
                PlanItem(
                    subvolume="data",
                    action="skip",
                    parent_snapshot="snap",
                    reason="incremental_not_due",
                )
            ]
            with mock.patch(
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                side_effect=AssertionError("credentials should not be checked"),
            ):
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                result = orchestrator.run(request)
            self.assertEqual(result, 0)

    def test_backup_once_forces_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=True,
                no_s3=False,
            )
            plan = [
                PlanItem(
                    subvolume="data",
                    action="skip",
                    parent_snapshot="snap",
                    reason="incremental_not_due",
                )
            ]
            with mock.patch(
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=False,
            ) as creds_check:
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                result = orchestrator.run(request)
            self.assertEqual(result, 0)
            self.assertTrue(creds_check.called)

    def test_backup_lock_contention_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )

            class FakeLock:
                def __init__(self, path: Path) -> None:
                    self.path = path

                def acquire(self):
                    raise LockError("locked")

            with mock.patch("btrfs_to_s3.orchestrator.LockFile", FakeLock):
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                result = orchestrator.run(request)
            self.assertEqual(result, 1)

    def test_backup_releases_lock_on_skip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )
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
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator.LockFile", FakeLock
            ):
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                result = orchestrator.run(request)
            self.assertEqual(result, 0)
            self.assertTrue(lock_state["released"])

    def test_backup_stale_lock_is_recovered(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            lock_path = config.global_cfg.lock_path
            lock_path.write_text("999999")
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )
            plan = [
                PlanItem(
                    subvolume="data",
                    action="skip",
                    parent_snapshot="snap",
                    reason="incremental_not_due",
                )
            ]
            with mock.patch(
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                side_effect=AssertionError("credentials should not be checked"),
            ):
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                result = orchestrator.run(request)
            self.assertEqual(result, 0)
            self.assertFalse(lock_path.exists())

    def test_backup_missing_parent_falls_back_to_full(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )
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
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch(
                "btrfs_to_s3.orchestrator._get_s3_client",
                return_value=object(),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.chunk_stream",
                return_value=iter([]),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.publish_manifest"
            ), mock.patch(
                "btrfs_to_s3.orchestrator.open_btrfs_send",
                side_effect=fake_open_btrfs_send,
            ), mock.patch.object(
                BackupOrchestrator, "_make_uploader"
            ) as make_uploader, mock.patch.object(
                BackupOrchestrator, "_write_manifest"
            ), mock.patch.object(
                SnapshotManager, "create_snapshot", fake_create_snapshot
            ), mock.patch.object(
                SnapshotManager, "prune_snapshots", return_value=[]
            ):
                make_uploader.return_value = mock.Mock(client=object())
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                result = orchestrator.run(request)
            self.assertEqual(result, 0)
            self.assertEqual(created.get("kind"), "full")
            self.assertIsNone(parent_holder.get("parent"))

    def test_backup_missing_parent_manifest_falls_back_to_full(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )

            state_path = config.global_cfg.state_path
            state_path.write_text(
                '{"subvolumes":{"data":{"last_snapshot":"%s","last_full_at":"2025-12-15T00:00:00Z"}},"last_run_at":null}\n'
                % (Path(temp_dir) / "snapshots" / "data__20260101T000000Z__inc")
            )

            parent_snapshot = Path(temp_dir) / "snapshots" / "data__20260101T000000Z__inc"
            parent_snapshot.parent.mkdir(parents=True, exist_ok=True)
            parent_snapshot.write_text("stub")

            plan = [
                PlanItem(
                    subvolume="data",
                    action="inc",
                    parent_snapshot=str(parent_snapshot),
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
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch(
                "btrfs_to_s3.orchestrator._get_s3_client",
                return_value=object(),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.chunk_stream",
                return_value=iter([]),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.publish_manifest"
            ), mock.patch(
                "btrfs_to_s3.orchestrator.open_btrfs_send",
                side_effect=fake_open_btrfs_send,
            ), mock.patch.object(
                BackupOrchestrator, "_make_uploader"
            ) as make_uploader, mock.patch.object(
                BackupOrchestrator, "_write_manifest"
            ), mock.patch.object(
                SnapshotManager, "create_snapshot", fake_create_snapshot
            ), mock.patch.object(
                SnapshotManager, "prune_snapshots", return_value=[]
            ):
                make_uploader.return_value = mock.Mock(client=object())
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                result = orchestrator.run(request)
            self.assertEqual(result, 0)
            self.assertEqual(created.get("kind"), "full")
            self.assertIsNone(parent_holder.get("parent"))

    def test_backup_logs_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )
            plan = [
                PlanItem(
                    subvolume="data",
                    action="full",
                    parent_snapshot=None,
                    reason="full_due",
                )
            ]

            class FakeProcess:
                returncode = 0

                def communicate(self):
                    return b"", b""

            class FakeStream:
                def __init__(self) -> None:
                    self.stdout = io.BytesIO(b"")
                    self.process = FakeProcess()

            class FakeChunk:
                def __init__(self, index: int, payload: bytes) -> None:
                    self.index = index
                    self.reader = io.BytesIO(payload)
                    self._size = len(payload)
                    self._sha256 = hashlib.sha256(payload).hexdigest()

                @property
                def size(self) -> int:
                    return self._size

                @property
                def sha256(self) -> str:
                    return self._sha256

            class FakeUploader:
                def __init__(self, *args, **kwargs) -> None:
                    self.client = object()

                def upload_stream(self, key: str, reader: io.BytesIO) -> UploadResult:
                    return UploadResult(key=key, size=0, etag="etag")

            def fake_open_btrfs_send(path: Path, parent: Path | None):
                return FakeStream()

            def fake_create_snapshot(self, subvolume_path, subvolume_name, kind):
                return Snapshot(
                    name="data__20260101T000000Z__full",
                    path=Path(temp_dir) / "snap",
                    kind=kind,
                    created_at=datetime.now(timezone.utc),
                )

            time_values = iter([10.0, 12.5])

            with mock.patch(
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch(
                "btrfs_to_s3.orchestrator._get_s3_client",
                return_value=object(),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.chunk_stream",
                return_value=iter([FakeChunk(0, b"data")]),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.publish_manifest"
            ), mock.patch(
                "btrfs_to_s3.orchestrator.open_btrfs_send",
                side_effect=fake_open_btrfs_send,
            ), mock.patch.object(
                SnapshotManager, "create_snapshot", fake_create_snapshot
            ), mock.patch.object(
                SnapshotManager, "prune_snapshots", return_value=[]
            ), mock.patch(
                "btrfs_to_s3.orchestrator.S3Uploader", FakeUploader
            ), mock.patch(
                "btrfs_to_s3.orchestrator.time.monotonic",
                side_effect=lambda: next(time_values),
            ):
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                with self.assertLogs("btrfs_to_s3.cli", level="INFO") as logs:
                    result = orchestrator.run(request)
            self.assertEqual(result, 0)
            metrics_logs = [
                entry
                for entry in logs.output
                if "event=backup_metrics" in entry
            ]
            self.assertTrue(metrics_logs)

    def test_backup_upload_failure_cleans_up_send(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )
            plan = [
                PlanItem(
                    subvolume="data",
                    action="full",
                    parent_snapshot=None,
                    reason="full_due",
                )
            ]

            class FakeProcess:
                pass

            class FakeStream:
                def __init__(self) -> None:
                    self.stdout = io.BytesIO(b"stream")
                    self.process = FakeProcess()

            class FakeChunk:
                def __init__(self, index: int, payload: bytes) -> None:
                    self.index = index
                    self.reader = io.BytesIO(payload)
                    self._size = len(payload)
                    self._sha256 = hashlib.sha256(payload).hexdigest()

                @property
                def size(self) -> int:
                    return self._size

                @property
                def sha256(self) -> str:
                    return self._sha256

            class FakeUploader:
                def __init__(self, *args, **kwargs) -> None:
                    self.client = object()

                def upload_stream(self, key: str, reader: io.BytesIO) -> UploadResult:
                    raise RuntimeError("upload failed")

            def fake_open_btrfs_send(path: Path, parent: Path | None):
                return FakeStream()

            def fake_create_snapshot(self, subvolume_path, subvolume_name, kind):
                return Snapshot(
                    name="data__20260101T000000Z__full",
                    path=Path(temp_dir) / "snap",
                    kind=kind,
                    created_at=datetime.now(timezone.utc),
                )

            with mock.patch(
                "btrfs_to_s3.orchestrator.plan_backups", return_value=plan
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch(
                "btrfs_to_s3.orchestrator._get_s3_client",
                return_value=object(),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.chunk_stream",
                return_value=iter([FakeChunk(0, b"data")]),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.publish_manifest"
            ) as publish_manifest, mock.patch(
                "btrfs_to_s3.orchestrator.open_btrfs_send",
                side_effect=fake_open_btrfs_send,
            ), mock.patch(
                "btrfs_to_s3.orchestrator.cleanup_btrfs_send",
                return_value="send failed",
            ) as cleanup, mock.patch.object(
                SnapshotManager, "create_snapshot", fake_create_snapshot
            ), mock.patch.object(
                SnapshotManager, "prune_snapshots", return_value=[]
            ), mock.patch(
                "btrfs_to_s3.orchestrator.S3Uploader", FakeUploader
            ):
                orchestrator = BackupOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                with self.assertLogs("btrfs_to_s3.cli", level="ERROR") as logs:
                    result = orchestrator.run(request)
            self.assertEqual(result, 1)
            publish_manifest.assert_not_called()
            self.assertTrue(cleanup.called)
            error_logs = [
                entry
                for entry in logs.output
                if "event=backup_stream_failed" in entry
            ]
            self.assertTrue(error_logs)

    def test_restore_logs_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self._make_config(temp_dir)
            target_path = Path(temp_dir) / "restore" / "data"
            request = RestoreRequest(
                subvolume="data",
                target=target_path,
                manifest_key=None,
                restore_timeout=None,
                wait_restore=None,
                verify="none",
            )

            time_values = iter([5.0, 6.5])

            with mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch(
                "btrfs_to_s3.orchestrator._get_s3_client",
                return_value=object(),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.fetch_current_manifest_key",
                return_value="manifest.json",
            ), mock.patch(
                "btrfs_to_s3.orchestrator.resolve_manifest_chain",
                return_value=["manifest"],
            ), mock.patch(
                "btrfs_to_s3.orchestrator.restore_chain", return_value=2048
            ), mock.patch(
                "btrfs_to_s3.orchestrator.time.monotonic",
                side_effect=lambda: next(time_values),
            ):
                orchestrator = RestoreOrchestrator(
                    config, logger=logging.getLogger("btrfs_to_s3.cli")
                )
                with self.assertLogs("btrfs_to_s3.cli", level="INFO") as logs:
                    result = orchestrator.run(request)
            self.assertEqual(result, 0)
            metrics_logs = [
                entry
                for entry in logs.output
                if "event=restore_metrics" in entry
            ]
            self.assertTrue(metrics_logs)


if __name__ == "__main__":
    unittest.main()
