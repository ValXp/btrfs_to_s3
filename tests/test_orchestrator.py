"""Orchestrator unit tests."""

from __future__ import annotations

import io
import logging
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from btrfs_to_s3.config import (
    Config,
    GlobalConfig,
    RestoreConfig,
    S3Config,
    ScheduleConfig,
    SnapshotsConfig,
    SubvolumesConfig,
)
from btrfs_to_s3.orchestrator import (
    BackupOrchestrator,
    BackupRequest,
    RestoreOrchestrator,
    RestoreRequest,
    _ShellRunner,
    _build_plan,
    _build_prefix,
    _filter_plan_items,
    _get_s3_client,
    _has_aws_credentials,
)
from btrfs_to_s3.planner import PlanItem
from btrfs_to_s3.restore import ManifestInfo
from btrfs_to_s3.snapshots import Snapshot, SnapshotManager
from btrfs_to_s3.uploader import UploadResult


def _make_config(temp_dir: str, subvolumes: tuple[Path, ...] | None = None) -> Config:
    if subvolumes is None:
        subvolumes = (Path(temp_dir) / "data",)
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
        subvolumes=SubvolumesConfig(paths=subvolumes),
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


class OrchestratorHelperTests(unittest.TestCase):
    def test_build_prefix_normalizes(self) -> None:
        self.assertEqual(_build_prefix(""), "")
        self.assertEqual(_build_prefix("backup"), "backup/")
        self.assertEqual(_build_prefix("backup/"), "backup/")

    def test_has_aws_credentials_looks_for_profile_or_keys(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertFalse(_has_aws_credentials())
        with mock.patch.dict("os.environ", {"AWS_PROFILE": "default"}):
            self.assertTrue(_has_aws_credentials())
        with mock.patch.dict(
            "os.environ",
            {"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"},
        ):
            self.assertTrue(_has_aws_credentials())

    def test_get_s3_client_requires_boto3(self) -> None:
        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "boto3":
                raise ImportError("missing boto3")
            return real_import(name, globals, locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(RuntimeError):
                _get_s3_client("us-east-1")

    def test_get_s3_client_uses_boto3_client(self) -> None:
        fake_boto3 = mock.Mock()
        fake_boto3.client.return_value = "client"
        with mock.patch.dict("sys.modules", {"boto3": fake_boto3}):
            self.assertEqual(_get_s3_client("us-east-1"), "client")
        fake_boto3.client.assert_called_once_with("s3", region_name="us-east-1")

    def test_select_subvolumes_honors_names_and_manifest_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = (
                Path(temp_dir) / "data",
                Path(temp_dir) / "root",
            )
            config = _make_config(temp_dir, subvolumes=paths)
            orchestrator = BackupOrchestrator(config)
            selected = orchestrator._select_subvolumes(True, None)
            self.assertEqual(selected, [paths[0]])
            selected = orchestrator._select_subvolumes(False, ("root",))
            self.assertEqual(selected, [paths[1]])

    def test_build_plan_limits_subvolumes_for_selection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = (
                Path(temp_dir) / "data",
                Path(temp_dir) / "root",
            )
            config = _make_config(temp_dir, subvolumes=paths)
            selected = [paths[1]]
            now = datetime.now(timezone.utc)
            state = mock.Mock()
            snapshot = Snapshot(
                name="root__20260101T000000Z__full",
                path=Path(temp_dir) / "snap",
                kind="full",
                created_at=now,
            )

            class FakeSnapshotManager:
                def list_snapshots(self, name: str):
                    return [snapshot] if name == "root" else []

            captured: dict[str, object] = {}

            def fake_plan_backups(plan_config, plan_state, plan_now, available_snapshots):
                captured["config"] = plan_config
                captured["snapshots"] = available_snapshots
                return []

            with mock.patch(
                "btrfs_to_s3.orchestrator.plan_backups",
                side_effect=fake_plan_backups,
            ):
                _build_plan(config, state, now, FakeSnapshotManager(), selected)

            plan_config = captured["config"]
            self.assertEqual(plan_config.subvolumes.paths, tuple(selected))
            self.assertEqual(captured["snapshots"], {snapshot.name})

    def test_filter_plan_items_respects_force_run(self) -> None:
        logger = logging.getLogger("btrfs_to_s3.orchestrator_test")
        plan_by_name = {
            "data": PlanItem(
                subvolume="data",
                action="skip",
                parent_snapshot=None,
                reason="incremental_not_due",
            )
        }
        with self.assertLogs("btrfs_to_s3.orchestrator_test", level="INFO") as logs:
            items = _filter_plan_items(
                plan_by_name,
                [Path("/srv/data")],
                False,
                logger,
            )
        self.assertEqual(items, [])
        self.assertTrue(
            any("event=backup_not_due" in entry for entry in logs.output)
        )

        forced_plan = {
            "data": PlanItem(
                subvolume="data",
                action="skip",
                parent_snapshot="snap",
                reason="incremental_not_due",
            )
        }
        items = _filter_plan_items(
            forced_plan, [Path("/srv/data")], True, logger
        )
        self.assertEqual(items[0][2], "inc")

    def test_filter_plan_items_skips_unknown_subvolume(self) -> None:
        logger = logging.getLogger("btrfs_to_s3.orchestrator_test")
        plan_by_name = {}
        items = _filter_plan_items(
            plan_by_name, [Path("/srv/unknown")], False, logger
        )
        self.assertEqual(items, [])

    def test_shell_runner_adds_sbin_to_path(self) -> None:
        runner = _ShellRunner()
        with mock.patch(
            "btrfs_to_s3.orchestrator.ensure_sbin_on_path",
            return_value="/usr/sbin:/usr/bin",
        ), mock.patch(
            "btrfs_to_s3.orchestrator.subprocess.run"
        ) as run:
            runner.run(["btrfs", "subvolume", "list", "/srv"])
        _, kwargs = run.call_args
        self.assertEqual(kwargs["env"]["PATH"], "/usr/sbin:/usr/bin")


class OrchestratorBackupTests(unittest.TestCase):
    def test_backup_dry_run_skips_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            request = BackupRequest(
                dry_run=True,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )

            class FakeLock:
                def __init__(self, path: Path) -> None:
                    raise AssertionError("lock should not be used")

            with mock.patch("btrfs_to_s3.orchestrator.LockFile", FakeLock):
                orchestrator = BackupOrchestrator(config)
                result = orchestrator.run(request)
            self.assertEqual(result, 0)

    def test_backup_no_subvolumes_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir, subvolumes=())
            orchestrator = BackupOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=True,
            )
            with self.assertLogs(
                "btrfs_to_s3.orchestrator_test", level="ERROR"
            ) as logs:
                result = orchestrator._run_locked(request)
            self.assertEqual(result, 2)
            self.assertTrue(
                any(
                    "event=backup_no_subvolumes" in entry
                    for entry in logs.output
                )
            )

    def test_backup_run_returns_error_when_s3_client_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = BackupOrchestrator(config)
            request = BackupRequest(
                dry_run=False,
                subvolume_names=None,
                once=False,
                no_s3=False,
            )
            with mock.patch.object(
                BackupOrchestrator,
                "_plan_work",
                return_value=[(Path(temp_dir), mock.Mock(), "full")],
            ), mock.patch.object(
                BackupOrchestrator,
                "_select_subvolumes",
                return_value=[Path(temp_dir)],
            ), mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch.object(
                BackupOrchestrator, "_init_s3_client", return_value=None
            ):
                self.assertEqual(orchestrator._run_locked(request), 1)

    def test_init_s3_client_failure_is_logged(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = BackupOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator._get_s3_client",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertLogs(
                    "btrfs_to_s3.orchestrator_test", level="ERROR"
                ):
                    self.assertIsNone(orchestrator._init_s3_client())

    def test_backup_write_manifest_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = BackupOrchestrator(config)
            state_subvols: dict[str, object] = {}
            snapshot = Snapshot(
                name="data__20260101T000000Z__full",
                path=Path(temp_dir) / "snap",
                kind="full",
                created_at=datetime.now(timezone.utc),
            )
            plan_item = PlanItem(
                subvolume="data",
                action="full",
                parent_snapshot=None,
                reason="full_due",
            )
            with mock.patch.object(
                BackupOrchestrator,
                "_upload_stream",
                return_value=(0, [], []),
            ), mock.patch.object(
                BackupOrchestrator,
                "_publish_manifest",
                return_value="manifest",
            ), mock.patch.object(
                BackupOrchestrator, "_log_backup_metrics"
            ), mock.patch.object(
                SnapshotManager, "create_snapshot", return_value=snapshot
            ), mock.patch.object(
                SnapshotManager, "prune_snapshots", return_value=[]
            ), mock.patch.object(
                BackupOrchestrator, "_write_manifest"
            ) as write_manifest:
                result = orchestrator._backup_item(
                    (Path(temp_dir) / "data", plan_item, "full"),
                    state_subvols,
                    "20260101T000000Z",
                    "backup/",
                    SnapshotManager(config.snapshots.base_dir, mock.Mock()),
                    mock.Mock(client=object()),
                    True,
                    temp_dir,
                    [Path(temp_dir) / "data"],
                )
            self.assertEqual(result, 0)
            write_manifest.assert_called_once()

    def test_upload_stream_errors_on_send_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = BackupOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )

            class FakeProcess:
                returncode = 1

                def communicate(self):
                    return b"", b"send failed"

            class FakeStream:
                def __init__(self) -> None:
                    self.stdout = io.BytesIO(b"")
                    self.process = FakeProcess()

            with mock.patch(
                "btrfs_to_s3.orchestrator.open_btrfs_send",
                return_value=FakeStream(),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.chunk_stream",
                return_value=iter([]),
            ):
                with self.assertLogs(
                    "btrfs_to_s3.orchestrator_test", level="ERROR"
                ) as logs:
                    result = orchestrator._upload_stream(
                        Path(temp_dir),
                        None,
                        "data",
                        "full",
                        "20260101T000000Z",
                        "backup/",
                        mock.Mock(),
                    )
            self.assertIsNone(result)
            self.assertTrue(
                any("event=btrfs_send_failed" in entry for entry in logs.output)
            )

    def test_upload_stream_success_returns_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = BackupOrchestrator(config)

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
                    self._payload = payload

                @property
                def size(self) -> int:
                    return len(self._payload)

                @property
                def sha256(self) -> str:
                    return "sha"

            uploader = mock.Mock()
            uploader.upload_stream.return_value = UploadResult(
                key="key", size=3, etag="etag"
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator.open_btrfs_send",
                return_value=FakeStream(),
            ), mock.patch(
                "btrfs_to_s3.orchestrator.chunk_stream",
                return_value=iter([FakeChunk(0, b"abc")]),
            ):
                total_bytes, chunks, local_chunks = orchestrator._upload_stream(
                    Path(temp_dir),
                    None,
                    "data",
                    "full",
                    "20260101T000000Z",
                    "backup/",
                    uploader,
                )
            self.assertEqual(total_bytes, 3)
            self.assertEqual(len(chunks), 1)
            self.assertEqual(len(local_chunks), 1)

    def test_write_manifest_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = BackupOrchestrator(config)
            orchestrator._write_manifest(temp_dir, "full", [])
            manifest_path = Path(temp_dir) / "manifest.json"
            self.assertTrue(manifest_path.exists())


class OrchestratorRestoreTests(unittest.TestCase):
    def test_restore_requires_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            request = RestoreRequest(
                subvolume="data",
                target=Path(temp_dir) / "restore",
                manifest_key=None,
                restore_timeout=None,
                wait_restore=None,
                verify="none",
            )
            orchestrator = RestoreOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=False,
            ):
                self.assertEqual(orchestrator.run(request), 1)

    def test_restore_client_failure_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            request = RestoreRequest(
                subvolume="data",
                target=Path(temp_dir) / "restore",
                manifest_key="manifest.json",
                restore_timeout=None,
                wait_restore=None,
                verify="none",
            )
            orchestrator = RestoreOrchestrator(config)
            with mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch.object(
                RestoreOrchestrator, "_init_s3_client", return_value=None
            ):
                self.assertEqual(orchestrator.run(request), 1)

    def test_restore_fetch_manifest_failure_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            request = RestoreRequest(
                subvolume="data",
                target=Path(temp_dir) / "restore",
                manifest_key=None,
                restore_timeout=None,
                wait_restore=None,
                verify="none",
            )
            orchestrator = RestoreOrchestrator(config)
            with mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch.object(
                RestoreOrchestrator,
                "_init_s3_client",
                return_value=object(),
            ), mock.patch.object(
                RestoreOrchestrator, "_fetch_manifest_key", return_value=None
            ):
                self.assertEqual(orchestrator.run(request), 1)

    def test_restore_resolve_chain_failure_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            request = RestoreRequest(
                subvolume="data",
                target=Path(temp_dir) / "restore",
                manifest_key="manifest.json",
                restore_timeout=None,
                wait_restore=None,
                verify="none",
            )
            orchestrator = RestoreOrchestrator(config)
            with mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch.object(
                RestoreOrchestrator,
                "_init_s3_client",
                return_value=object(),
            ), mock.patch.object(
                RestoreOrchestrator, "_resolve_chain", return_value=None
            ):
                self.assertEqual(orchestrator.run(request), 1)

    def test_restore_chain_failure_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            request = RestoreRequest(
                subvolume="data",
                target=Path(temp_dir) / "restore",
                manifest_key="manifest.json",
                restore_timeout=None,
                wait_restore=None,
                verify="none",
            )
            orchestrator = RestoreOrchestrator(config)
            with mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch.object(
                RestoreOrchestrator,
                "_init_s3_client",
                return_value=object(),
            ), mock.patch.object(
                RestoreOrchestrator,
                "_resolve_chain",
                return_value=[ManifestInfo("key", "full", None, (), {}, None)],
            ), mock.patch(
                "btrfs_to_s3.orchestrator.restore_chain",
                side_effect=Exception("restore failed"),
            ):
                with mock.patch(
                    "btrfs_to_s3.orchestrator.RestoreError",
                    Exception,
                ):
                    self.assertEqual(orchestrator.run(request), 1)

    def test_restore_verify_failure_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            request = RestoreRequest(
                subvolume="data",
                target=Path(temp_dir) / "restore",
                manifest_key="manifest.json",
                restore_timeout=None,
                wait_restore=None,
                verify="full",
            )
            orchestrator = RestoreOrchestrator(config)
            with mock.patch(
                "btrfs_to_s3.orchestrator._has_aws_credentials",
                return_value=True,
            ), mock.patch.object(
                RestoreOrchestrator,
                "_init_s3_client",
                return_value=object(),
            ), mock.patch.object(
                RestoreOrchestrator,
                "_resolve_chain",
                return_value=[ManifestInfo("key", "full", None, (), {}, None)],
            ), mock.patch(
                "btrfs_to_s3.orchestrator.restore_chain",
                return_value=1,
            ), mock.patch.object(
                RestoreOrchestrator, "_verify_restore", return_value=1
            ):
                self.assertEqual(orchestrator.run(request), 1)

    def test_restore_init_s3_client_logs_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = RestoreOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator._get_s3_client",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertLogs(
                    "btrfs_to_s3.orchestrator_test", level="ERROR"
                ):
                    self.assertIsNone(orchestrator._init_s3_client())

    def test_restore_fetch_manifest_logs_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = RestoreOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator.fetch_current_manifest_key",
                side_effect=Exception("boom"),
            ):
                with mock.patch(
                    "btrfs_to_s3.orchestrator.RestoreError",
                    Exception,
                ):
                    with self.assertLogs(
                        "btrfs_to_s3.orchestrator_test", level="ERROR"
                    ):
                        self.assertIsNone(
                            orchestrator._fetch_manifest_key(
                                object(), "current.json"
                            )
                        )

    def test_restore_resolve_chain_logs_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = RestoreOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator.resolve_manifest_chain",
                side_effect=Exception("boom"),
            ):
                with mock.patch(
                    "btrfs_to_s3.orchestrator.RestoreError",
                    Exception,
                ):
                    with self.assertLogs(
                        "btrfs_to_s3.orchestrator_test", level="ERROR"
                    ):
                        self.assertIsNone(
                            orchestrator._resolve_chain(object(), "manifest.json")
                        )

    def test_verify_restore_logs_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = RestoreOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            manifest = ManifestInfo(
                key="key",
                kind="full",
                parent_manifest=None,
                chunks=(),
                s3={},
                snapshot_path=None,
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator.verify_restore",
                side_effect=Exception("boom"),
            ):
                with mock.patch(
                    "btrfs_to_s3.orchestrator.RestoreError",
                    Exception,
                ):
                    with self.assertLogs(
                        "btrfs_to_s3.orchestrator_test", level="ERROR"
                    ):
                        self.assertEqual(
                            orchestrator._verify_restore(
                                "full", [manifest], Path(temp_dir)
                            ),
                            1,
                        )
    def test_verify_restore_skips_when_none(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = RestoreOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            with self.assertLogs(
                "btrfs_to_s3.orchestrator_test", level="INFO"
            ) as logs:
                result = orchestrator._verify_restore(
                    "none", [], Path(temp_dir)
                )
            self.assertEqual(result, 0)
            self.assertTrue(
                any(
                    "event=restore_verify_skipped" in entry
                    for entry in logs.output
                )
            )

    def test_verify_restore_logs_missing_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _make_config(temp_dir)
            orchestrator = RestoreOrchestrator(
                config, logger=logging.getLogger("btrfs_to_s3.orchestrator_test")
            )
            manifest = ManifestInfo(
                key="key",
                kind="full",
                parent_manifest=None,
                chunks=(),
                s3={},
                snapshot_path=str(Path(temp_dir) / "missing"),
            )
            with mock.patch(
                "btrfs_to_s3.orchestrator.verify_restore"
            ) as verify, self.assertLogs(
                "btrfs_to_s3.orchestrator_test", level="INFO"
            ) as logs:
                result = orchestrator._verify_restore(
                    "full", [manifest], Path(temp_dir)
                )
            self.assertEqual(result, 0)
            verify.assert_called_once()
            self.assertTrue(
                any(
                    "event=restore_verify_source_missing" in entry
                    for entry in logs.output
                )
            )


if __name__ == "__main__":
    unittest.main()
