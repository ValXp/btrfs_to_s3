"""ZFS filesystem backend tests."""

from __future__ import annotations

import io
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from btrfs_to_s3.config import (
    Config,
    FilesystemConfig,
    GlobalConfig,
    RestoreConfig,
    S3Config,
    ScheduleConfig,
    SnapshotsConfig,
    SubvolumesConfig,
    ZFSConfig,
)
from btrfs_to_s3.filesystems.base import (
    ReceiveStream,
    RestoreBackendError,
    SendStream,
    SnapshotError,
    StreamError,
)
from btrfs_to_s3.filesystems.factory import (
    BackendSelectionError,
    create_filesystem_backend,
)
from btrfs_to_s3.filesystems.zfs import (
    ZFSRestoreOperations,
    ZFSSendOperations,
    ZFSSnapshotManager,
)


class RecordingRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.error: subprocess.CalledProcessError | None = None

    def run(self, args: list[str]) -> None:
        if self.error is not None:
            raise self.error
        self.calls.append(args)


class ZFSSnapshotManagerTests(unittest.TestCase):
    def test_create_snapshot_records_command(self) -> None:
        runner = RecordingRunner()
        manager = ZFSSnapshotManager(
            snapshot_prefix="btrfs-to-s3",
            runner=runner,
            now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        snapshot = manager.create_snapshot(Path("/tank/data"), "tank/data", "full")

        self.assertEqual(snapshot.name, "tank_x2f_data__20260101T000000Z__full")
        self.assertEqual(
            runner.calls,
            [
                [
                    "zfs",
                    "snapshot",
                    "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full",
                ]
            ],
        )
        self.assertEqual(
            str(snapshot.path),
            "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full",
        )

    def test_create_snapshot_preserves_stderr(self) -> None:
        error = subprocess.CalledProcessError(
            1,
            ["zfs", "snapshot"],
            stderr="permission denied",
        )
        runner = RecordingRunner()
        runner.error = error
        manager = ZFSSnapshotManager(
            snapshot_prefix="btrfs-to-s3",
            runner=runner,
        )

        with self.assertRaises(SnapshotError) as context:
            manager.create_snapshot(Path("/tank/data"), "tank/data", "full")

        self.assertIn("permission denied", str(context.exception))

    def test_list_snapshots_filters_prefix_and_orders_newest_first(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["zfs", "list"],
            returncode=0,
            stdout=(
                "tank/data@manual-before\n"
                "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full\n"
                "tank/data@btrfs-to-s3-tank_x2f_data__20260108T000000Z__inc\n"
            ),
            stderr="",
        )
        manager = ZFSSnapshotManager(
            snapshot_prefix="btrfs-to-s3",
            runner=RecordingRunner(),
            command_runner=mock.Mock(return_value=completed),
        )

        snapshots = manager.list_snapshots("tank/data")

        self.assertEqual(
            [snapshot.name for snapshot in snapshots],
            [
                "tank_x2f_data__20260108T000000Z__inc",
                "tank_x2f_data__20260101T000000Z__full",
            ],
        )
        manager._command_runner.assert_called_once_with(
            [
                "zfs",
                "list",
                "-H",
                "-o",
                "name",
                "-s",
                "creation",
                "-t",
                "snapshot",
                "-d",
                "1",
                "tank/data",
            ],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )

    def test_prune_retains_parent_snapshot(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["zfs", "list"],
            returncode=0,
            stdout=(
                "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full\n"
                "tank/data@btrfs-to-s3-tank_x2f_data__20260108T000000Z__inc\n"
                "tank/data@btrfs-to-s3-tank_x2f_data__20260115T000000Z__inc\n"
            ),
            stderr="",
        )
        runner = RecordingRunner()
        manager = ZFSSnapshotManager(
            snapshot_prefix="btrfs-to-s3",
            runner=runner,
            command_runner=mock.Mock(return_value=completed),
        )

        deleted = manager.prune_snapshots(
            "tank/data",
            retain=1,
            keep_name="tank_x2f_data__20260101T000000Z__full",
        )

        self.assertEqual(
            deleted,
            [Path("tank/data@btrfs-to-s3-tank_x2f_data__20260108T000000Z__inc")],
        )
        self.assertEqual(
            runner.calls,
            [
                [
                    "zfs",
                    "destroy",
                    "tank/data@btrfs-to-s3-tank_x2f_data__20260108T000000Z__inc",
                ]
            ],
        )


class ZFSSendOperationsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.operations = ZFSSendOperations()

    def test_cleanup_send_terminates_and_returns_stderr(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self.terminated = False
                self.killed = False
                self._poll = None

            def poll(self):
                return self._poll

            def terminate(self) -> None:
                self.terminated = True
                self._poll = 0

            def kill(self) -> None:
                self.killed = True
                self._poll = 0

            def communicate(self, timeout: float | None = None):
                return b"", b"stderr output"

        process = FakeProcess()
        stdout = io.BytesIO(b"stream")

        error = self.operations.cleanup_send(process, stdout=stdout)

        self.assertTrue(stdout.closed)
        self.assertTrue(process.terminated)
        self.assertFalse(process.killed)
        self.assertEqual(error, "stderr output")

    def test_open_send_builds_incremental_args(self) -> None:
        stdout = io.BytesIO(b"stream")
        process = mock.Mock()
        process.stdout = stdout
        popen = mock.Mock(return_value=process)
        operations = ZFSSendOperations(popen=popen)

        result = operations.open_send(
            Path("tank/data@snap-2"),
            parent_snapshot=Path("tank/data@snap-1"),
        )

        popen.assert_called_once_with(
            ["zfs", "send", "-i", "tank/data@snap-1", "tank/data@snap-2"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=mock.ANY,
        )
        self.assertEqual(result, SendStream(process=process, stdout=stdout))

    def test_open_send_raises_without_stdout(self) -> None:
        process = mock.Mock()
        process.stdout = None
        operations = ZFSSendOperations(popen=mock.Mock(return_value=process))

        with self.assertRaises(StreamError):
            operations.open_send(Path("tank/data@snap-1"))

        process.kill.assert_called_once_with()


class ZFSRestoreOperationsTests(unittest.TestCase):
    def test_open_receive_builds_dataset_from_target(self) -> None:
        process = mock.Mock()
        process.stdin = io.BytesIO()
        popen = mock.Mock(return_value=process)
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            popen=popen,
        )
        target = Path("/tank/restore/data__restore__20260101")

        result = operations.open_receive(
            target,
            "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full",
        )

        popen.assert_called_once_with(
            ["zfs", "receive", "-F", "tank/restore/data__restore__20260101"],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=mock.ANY,
        )
        self.assertEqual(result.created_path, target)

    def test_open_receive_rejects_target_outside_restore_base(self) -> None:
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            popen=mock.Mock(),
        )

        with self.assertRaises(RestoreBackendError) as context:
            operations.open_receive(Path("/tmp/restore"), "tank/data@snap")

        self.assertIn("/tank/restore", str(context.exception))

    def test_cleanup_receive_destroys_partial_dataset(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None
                self.terminated = False

            def poll(self):
                return self._poll

            def terminate(self) -> None:
                self.terminated = True
                self._poll = 0

            def kill(self) -> None:
                self._poll = 0

            def communicate(self, timeout: float | None = None):
                return b"", b"receive stderr"

        runner = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=["zfs", "destroy"],
                returncode=0,
                stdout="",
                stderr="",
            )
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            runner=runner,
        )
        process = FakeProcess()
        operations._active_receives[id(process)] = mock.Mock(
            dataset="tank/restore/data__restore__20260101",
            target=Path("/tank/restore/data__restore__20260101"),
        )

        error = operations.cleanup_receive(process)

        self.assertEqual(error, "receive stderr")
        runner.assert_called_once_with(
            ["zfs", "destroy", "-r", "tank/restore/data__restore__20260101"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )

    def test_complete_receive_reports_stderr_and_cleanup_failure(self) -> None:
        process = mock.Mock()
        process.communicate.return_value = (b"", b"receive failed")
        process.returncode = 1
        runner = mock.Mock(
            side_effect=subprocess.CalledProcessError(
                1,
                ["zfs", "destroy"],
                stderr="busy dataset",
            )
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            runner=runner,
        )
        operations._active_receives[id(process)] = mock.Mock(
            dataset="tank/restore/data__restore__20260101",
            target=Path("/tank/restore/data__restore__20260101"),
        )

        with self.assertRaises(RestoreBackendError) as context:
            operations.complete_receive(
                mock.Mock(process=process, stdin=io.BytesIO(), created_path=Path("/tank/restore/data__restore__20260101")),
                Path("/tank/restore/data__restore__20260101"),
            )

        self.assertIn("receive failed", str(context.exception))
        self.assertIn("busy dataset", str(context.exception))

    def test_complete_receive_avoids_communicate_after_stdin_closed(self) -> None:
        process = mock.Mock()
        process.stdin = io.BytesIO()
        process.stdin.close()
        process.stderr = io.BytesIO(b"")
        process.wait.return_value = 0
        process.returncode = 0
        process.communicate.side_effect = AssertionError(
            "communicate should not be called"
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            runner=mock.Mock(),
        )

        operations.complete_receive(
            ReceiveStream(
                process=process,
                stdin=io.BytesIO(),
                created_path=Path("/tank/restore/data__restore__20260101"),
            ),
            Path("/tank/restore/data__restore__20260101"),
        )

        process.wait.assert_called_once_with()

    def test_finalize_restore_sets_readonly_off(self) -> None:
        runner = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=["zfs", "set"],
                returncode=0,
                stdout="",
                stderr="",
            )
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            runner=runner,
        )

        operations.finalize_restore(Path("/tank/restore/data"))

        runner.assert_called_once_with(
            ["zfs", "set", "readonly=off", "tank/restore/data"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )

    def test_verify_metadata_reads_expected_properties(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "data"
            target.mkdir()
            runner = mock.Mock(
                side_effect=[
                    subprocess.CompletedProcess(
                        args=["zfs", "get"],
                        returncode=0,
                        stdout="filesystem\n",
                        stderr="",
                    ),
                    subprocess.CompletedProcess(
                        args=["zfs", "get"],
                        returncode=0,
                        stdout="yes\n",
                        stderr="",
                    ),
                    subprocess.CompletedProcess(
                        args=["zfs", "get"],
                        returncode=0,
                        stdout=f"{target}\n",
                        stderr="",
                    ),
                    subprocess.CompletedProcess(
                        args=["zfs", "get"],
                        returncode=0,
                        stdout="off\n",
                        stderr="",
                    ),
                ]
            )
            operations = ZFSRestoreOperations(
                receive_parent_dataset="tank/restore",
                restore_base_dir=Path(temp_dir),
                runner=runner,
            )

            operations.verify_metadata(target)

        self.assertEqual(
            runner.call_args_list,
            [
                mock.call(
                    ["zfs", "get", "-H", "-o", "value", "type", "tank/restore/data"],
                    check=True,
                    text=True,
                    capture_output=True,
                    env=mock.ANY,
                ),
                mock.call(
                    ["zfs", "get", "-H", "-o", "value", "mounted", "tank/restore/data"],
                    check=True,
                    text=True,
                    capture_output=True,
                    env=mock.ANY,
                ),
                mock.call(
                    ["zfs", "get", "-H", "-o", "value", "mountpoint", "tank/restore/data"],
                    check=True,
                    text=True,
                    capture_output=True,
                    env=mock.ANY,
                ),
                mock.call(
                    ["zfs", "get", "-H", "-o", "value", "readonly", "tank/restore/data"],
                    check=True,
                    text=True,
                    capture_output=True,
                    env=mock.ANY,
                ),
            ],
        )

    def test_verify_metadata_preserves_stderr(self) -> None:
        runner = mock.Mock(
            side_effect=subprocess.CalledProcessError(
                1,
                ["zfs", "get"],
                stderr="dataset does not exist",
            )
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            runner=runner,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "data"
            target.mkdir()
            operations = ZFSRestoreOperations(
                receive_parent_dataset="tank/restore",
                restore_base_dir=Path(temp_dir),
                runner=runner,
            )
            with self.assertRaises(RestoreBackendError) as context:
                operations.verify_metadata(target)

        self.assertIn("dataset does not exist", str(context.exception))

    def test_resolve_verify_source_uses_snapshot_mount_path(self) -> None:
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            pool_name="tank",
            mount_root=Path("/tank"),
            runner=mock.Mock(),
        )

        with mock.patch.object(operations, "_path_is_dir", return_value=True):
            resolved = operations.resolve_verify_source(
                "tank/data",
                None,
                "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full",
            )

        self.assertEqual(
            resolved,
            Path(
                "/tank/data/.zfs/snapshot/"
                "btrfs-to-s3-tank_x2f_data__20260101T000000Z__full"
            ),
        )

    def test_resolve_verify_source_clones_when_snapshot_mount_missing(self) -> None:
        runner = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=["zfs", "clone"],
                returncode=0,
                stdout="",
                stderr="",
            )
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            pool_name="tank",
            mount_root=Path("/tank"),
            runner=runner,
        )

        with mock.patch.object(
            operations,
            "_path_is_dir",
            side_effect=[False, True],
        ), mock.patch(
            "btrfs_to_s3.filesystems.zfs.uuid.uuid4",
            return_value=mock.Mock(hex="abc123"),
        ):
            resolved = operations.resolve_verify_source(
                "tank/data",
                None,
                "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full",
            )

        self.assertEqual(
            resolved,
            Path("/tank/restore/__verify__tank_x2f_data__abc123"),
        )
        runner.assert_called_once_with(
            [
                "zfs",
                "clone",
                "-o",
                "readonly=on",
                "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__full",
                "tank/restore/__verify__tank_x2f_data__abc123",
            ],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )

    def test_cleanup_verify_source_destroys_temporary_clone(self) -> None:
        runner = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=["zfs", "cmd"],
                returncode=0,
                stdout="",
                stderr="",
            )
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            runner=runner,
        )
        source_path = Path("/tank/restore/__verify__tank_x2f_data__abc123")
        operations._active_verify_sources[str(source_path)] = (
            "tank/restore/__verify__tank_x2f_data__abc123"
        )

        operations.cleanup_verify_source(source_path)

        self.assertEqual(
            runner.call_args_list,
            [
                mock.call(
                    ["zfs", "unmount", "-f", "tank/restore/__verify__tank_x2f_data__abc123"],
                    check=True,
                    text=True,
                    capture_output=True,
                    env=mock.ANY,
                ),
                mock.call(
                    ["zfs", "destroy", "-r", "-f", "tank/restore/__verify__tank_x2f_data__abc123"],
                    check=True,
                    text=True,
                    capture_output=True,
                    env=mock.ANY,
                ),
            ],
        )
        self.assertEqual(operations._active_verify_sources, {})

    def test_cleanup_verify_source_retries_when_destroy_fails_but_dataset_remains(self) -> None:
        destroy_error = subprocess.CalledProcessError(
            1,
            ["zfs", "destroy"],
            stderr="dataset busy",
        )
        runner = mock.Mock(
            side_effect=[
                subprocess.CompletedProcess(["zfs", "unmount"], 0, "", ""),
                destroy_error,
                subprocess.CompletedProcess(["zfs", "destroy"], 0, "", ""),
            ]
        )
        operations = ZFSRestoreOperations(
            receive_parent_dataset="tank/restore",
            restore_base_dir=Path("/tank/restore"),
            runner=runner,
        )
        source_path = Path("/tank/restore/__verify__tank_x2f_data__abc123")
        operations._active_verify_sources[str(source_path)] = (
            "tank/restore/__verify__tank_x2f_data__abc123"
        )

        with mock.patch.object(
            operations,
            "_dataset_exists",
            return_value=True,
        ) as exists_mock, mock.patch(
            "btrfs_to_s3.filesystems.zfs.time.sleep"
        ) as sleep_mock:
            operations.cleanup_verify_source(source_path)

        exists_mock.assert_called_once_with(
            "tank/restore/__verify__tank_x2f_data__abc123"
        )
        sleep_mock.assert_called_once_with(0.2)


class CreateFilesystemBackendTests(unittest.TestCase):
    def test_create_zfs_backend_builds_sources_and_operations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = Config(
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
                snapshots=SnapshotsConfig(base_dir=None, retain=2),
                subvolumes=SubvolumesConfig(paths=()),
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
                filesystem=FilesystemConfig(backend="zfs"),
                zfs=ZFSConfig(
                    pool_name="tank",
                    mount_root=Path("/tank"),
                    source_datasets=("data", "tank/home"),
                    receive_parent_dataset="restore",
                    snapshot_prefix="btrfs-to-s3",
                ),
            )

            backend = create_filesystem_backend(config, runner=RecordingRunner())

        self.assertEqual(backend.name, "zfs")
        self.assertEqual(len(backend.sources), 2)
        self.assertEqual(backend.sources[0].identifier, "tank/data")
        self.assertEqual(backend.sources[0].path, Path("/tank/data"))
        self.assertEqual(backend.sources[1].identifier, "tank/home")
        self.assertEqual(backend.sources[1].path, Path("/tank/home"))
        self.assertIsInstance(backend.snapshot_operations, ZFSSnapshotManager)
        self.assertIsInstance(backend.send_operations, ZFSSendOperations)
        self.assertIsInstance(backend.restore_operations, ZFSRestoreOperations)

    def test_create_zfs_backend_rejects_duplicate_identifiers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = Config(
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
                snapshots=SnapshotsConfig(base_dir=None, retain=2),
                subvolumes=SubvolumesConfig(paths=()),
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
                filesystem=FilesystemConfig(backend="zfs"),
                zfs=ZFSConfig(
                    pool_name="tank",
                    mount_root=Path("/tank"),
                    source_datasets=("data", "tank/data"),
                    receive_parent_dataset="restore",
                    snapshot_prefix="btrfs-to-s3",
                ),
            )

            with self.assertRaises(BackendSelectionError) as context:
                create_filesystem_backend(config, runner=RecordingRunner())

        self.assertIn("duplicate source identifiers", str(context.exception))
        self.assertIn("tank/data", str(context.exception))

    def test_create_zfs_backend_allows_empty_source_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = Config(
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
                snapshots=SnapshotsConfig(base_dir=None, retain=2),
                subvolumes=SubvolumesConfig(paths=()),
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
                filesystem=FilesystemConfig(backend="zfs"),
                zfs=ZFSConfig(
                    pool_name="tank",
                    mount_root=Path("/tank"),
                    source_datasets=(),
                    receive_parent_dataset="restore",
                    snapshot_prefix="btrfs-to-s3",
                ),
            )

            backend = create_filesystem_backend(config, runner=RecordingRunner())

        self.assertEqual(backend.sources, ())
        self.assertIsInstance(backend.restore_operations, ZFSRestoreOperations)


if __name__ == "__main__":
    unittest.main()
