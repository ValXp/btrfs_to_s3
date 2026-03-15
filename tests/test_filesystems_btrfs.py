"""Btrfs filesystem backend tests."""

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
    GlobalConfig,
    RestoreConfig,
    S3Config,
    ScheduleConfig,
    SnapshotsConfig,
    SubvolumesConfig,
)
from btrfs_to_s3.filesystems.base import (
    ReceiveStream,
    RestoreBackendError,
    SendStream,
    StreamError,
)
from btrfs_to_s3.filesystems.btrfs import (
    BtrfsRestoreOperations,
    BtrfsSendOperations,
    BtrfsSnapshotManager,
)
from btrfs_to_s3.filesystems.factory import create_filesystem_backend


class RecordingRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def run(self, args: list[str]) -> None:
        self.calls.append(args)


class BtrfsSnapshotManagerTests(unittest.TestCase):
    def test_create_snapshot_records_command(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = RecordingRunner()
            manager = BtrfsSnapshotManager(
                base_dir=Path(temp_dir),
                runner=runner,
                now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            snapshot = manager.create_snapshot(
                Path("/srv/data/data"), "data", "full"
            )
        self.assertEqual(snapshot.name, "data__20260101T000000Z__full")
        self.assertEqual(
            runner.calls,
            [
                [
                    "btrfs",
                    "subvolume",
                    "snapshot",
                    "-r",
                    "/srv/data/data",
                    str(snapshot.path),
                ]
            ],
        )

    def test_prune_retains_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            names = [
                "data__20260101T000000Z__full",
                "data__20260108T000000Z__inc",
                "data__20260115T000000Z__inc",
            ]
            for name in names:
                (base_dir / name).mkdir()
            runner = RecordingRunner()
            manager = BtrfsSnapshotManager(base_dir=base_dir, runner=runner)
            deleted = manager.prune_snapshots(
                "data", retain=1, keep_name=names[0]
            )
            deleted_names = {path.name for path in deleted}
            self.assertEqual(deleted_names, {names[1]})
            self.assertEqual(
                runner.calls,
                [["btrfs", "subvolume", "delete", str(base_dir / names[1])]],
            )


class BtrfsSendOperationsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.operations = BtrfsSendOperations()

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

    def test_cleanup_send_kills_on_timeout(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self.terminated = False
                self.killed = False
                self._poll = None
                self._calls = 0

            def poll(self):
                return self._poll

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True
                self._poll = 0

            def communicate(self, timeout: float | None = None):
                self._calls += 1
                if self._calls == 1:
                    raise subprocess.TimeoutExpired("btrfs send", 1.0)
                return b"", b"forced stderr"

        process = FakeProcess()
        stdout = io.BytesIO(b"stream")
        error = self.operations.cleanup_send(process, stdout=stdout, timeout=0.01)
        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        self.assertEqual(error, "forced stderr")

    def test_open_send_builds_full_args(self) -> None:
        stdout = io.BytesIO(b"stream")
        process = mock.Mock()
        process.stdout = stdout
        with mock.patch("btrfs_to_s3.filesystems.btrfs.subprocess.Popen") as popen:
            popen.return_value = process
            result = self.operations.open_send(Path("/snapshots/child"))

        popen.assert_called_once_with(
            ["btrfs", "send", "/snapshots/child"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(result, SendStream(process=process, stdout=stdout))

    def test_open_send_builds_incremental_args(self) -> None:
        stdout = io.BytesIO(b"stream")
        process = mock.Mock()
        process.stdout = stdout
        with mock.patch("btrfs_to_s3.filesystems.btrfs.subprocess.Popen") as popen:
            popen.return_value = process
            result = self.operations.open_send(
                Path("/snapshots/child"),
                parent_snapshot=Path("/snapshots/parent"),
            )

        popen.assert_called_once_with(
            ["btrfs", "send", "-p", "/snapshots/parent", "/snapshots/child"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(result, SendStream(process=process, stdout=stdout))

    def test_open_send_raises_without_stdout(self) -> None:
        process = mock.Mock()
        process.stdout = None
        with mock.patch("btrfs_to_s3.filesystems.btrfs.subprocess.Popen") as popen:
            popen.return_value = process
            with self.assertRaises(StreamError):
                self.operations.open_send(Path("/snapshots/child"))

        process.kill.assert_called_once_with()


class BtrfsRestoreOperationsTests(unittest.TestCase):
    def test_open_receive_builds_args_and_created_path(self) -> None:
        process = mock.Mock()
        process.stdin = io.BytesIO()
        popen = mock.Mock(return_value=process)
        operations = BtrfsRestoreOperations(popen=popen)

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "restore" / "data"
            result = operations.open_receive(
                target,
                "/snapshots/data__20260101T000000Z__full",
            )

        popen.assert_called_once_with(
            ["btrfs", "receive", str(target.parent)],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(
            result,
            ReceiveStream(
                process=process,
                stdin=process.stdin,
                created_path=target.parent / "data__20260101T000000Z__full",
            ),
        )

    def test_open_receive_requires_snapshot_path(self) -> None:
        operations = BtrfsRestoreOperations(popen=mock.Mock())

        with self.assertRaises(RestoreBackendError) as context:
            operations.open_receive(Path("/tmp/restore/data"), None)
        self.assertIn("missing snapshot path", str(context.exception))

    def test_open_receive_raises_without_stdin(self) -> None:
        process = mock.Mock()
        process.stdin = None
        popen = mock.Mock(return_value=process)
        operations = BtrfsRestoreOperations(popen=popen)

        with self.assertRaises(RestoreBackendError):
            operations.open_receive(
                Path("/tmp/restore/data"),
                "/snapshots/data__20260101T000000Z__full",
            )

        process.kill.assert_called_once_with()

    def test_cleanup_receive_terminates_and_returns_stderr(self) -> None:
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

        operations = BtrfsRestoreOperations()
        process = FakeProcess()

        error = operations.cleanup_receive(process)

        self.assertTrue(process.terminated)
        self.assertFalse(process.killed)
        self.assertEqual(error, "stderr output")

    def test_cleanup_receive_kills_on_timeout(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self.terminated = False
                self.killed = False
                self._poll = None
                self._calls = 0

            def poll(self):
                return self._poll

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True
                self._poll = 0

            def communicate(self, timeout: float | None = None):
                self._calls += 1
                if self._calls == 1:
                    raise subprocess.TimeoutExpired("btrfs receive", 1.0)
                return b"", b"forced stderr"

        operations = BtrfsRestoreOperations()
        process = FakeProcess()

        error = operations.cleanup_receive(process, timeout=0.01)

        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        self.assertEqual(error, "forced stderr")

    def test_complete_receive_replaces_existing_target(self) -> None:
        calls: list[list[str]] = []

        def runner(*args, **kwargs):
            command = args[0]
            calls.append(command)
            if command[:3] == ["btrfs", "subvolume", "delete"]:
                Path(command[3]).rmdir()
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        operations = BtrfsRestoreOperations(runner=runner)

        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            target = base / "target"
            target.mkdir()
            created = base / "data__20260101T000000Z__full"
            created.mkdir()
            process = mock.Mock()
            process.returncode = 0
            process.communicate.return_value = (b"", b"")

            operations.complete_receive(
                ReceiveStream(
                    process=process,
                    stdin=io.BytesIO(),
                    created_path=created,
                ),
                target,
            )

            self.assertEqual(
                calls,
                [["btrfs", "subvolume", "delete", str(target)]],
            )
            self.assertTrue(target.exists())
            self.assertFalse(created.exists())

    def test_complete_receive_reports_stderr(self) -> None:
        operations = BtrfsRestoreOperations(runner=mock.Mock())
        process = mock.Mock()
        process.returncode = 2
        process.communicate.return_value = (b"", b"receive stderr")

        with self.assertRaises(RestoreBackendError) as context:
            operations.complete_receive(
                ReceiveStream(
                    process=process,
                    stdin=io.BytesIO(),
                    created_path=Path("/tmp/created"),
                ),
                Path("/tmp/target"),
            )
        self.assertIn("exit code 2", str(context.exception))
        self.assertIn("receive stderr", str(context.exception))

    def test_complete_receive_clears_closed_stdin_before_communicate(self) -> None:
        operations = BtrfsRestoreOperations(runner=mock.Mock())
        process = mock.Mock()
        process.stdin = io.BytesIO()
        process.stdin.close()
        process.returncode = 0

        def communicate():
            if process.stdin is not None:
                raise ValueError("flush of closed file")
            return b"", b""

        process.communicate.side_effect = communicate

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "target"
            created = Path(temp_dir) / "data__20260101T000000Z__full"
            created.mkdir()

            operations.complete_receive(
                ReceiveStream(
                    process=process,
                    stdin=io.BytesIO(),
                    created_path=created,
                ),
                target,
            )

        process.communicate.assert_called_once_with()
        self.assertIsNone(process.stdin)

    def test_complete_receive_requires_created_path(self) -> None:
        operations = BtrfsRestoreOperations(runner=mock.Mock())
        process = mock.Mock()
        process.returncode = 0
        process.communicate.return_value = (b"", b"")

        with self.assertRaises(RestoreBackendError) as context:
            operations.complete_receive(
                ReceiveStream(
                    process=process,
                    stdin=io.BytesIO(),
                    created_path=Path("/tmp/missing"),
                ),
                Path("/tmp/target"),
            )
        self.assertIn("received subvolume missing", str(context.exception))

    def test_finalize_restore_runs_property_set(self) -> None:
        captured: dict[str, object] = {}

        def runner(*args, **kwargs):
            captured["args"] = args[0]
            captured["path"] = kwargs["env"]["PATH"]
            return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

        operations = BtrfsRestoreOperations(runner=runner)

        operations.finalize_restore(Path("/tmp/target"))

        self.assertEqual(
            captured["args"],
            ["btrfs", "property", "set", "-f", "-ts", "/tmp/target", "ro", "false"],
        )
        assert isinstance(captured["path"], str)
        self.assertIn("/usr/sbin", captured["path"])
        self.assertIn("/sbin", captured["path"])

    def test_verify_metadata_uses_sbin_on_path(self) -> None:
        captured: dict[str, object] = {}

        def runner(*args, **kwargs):
            captured["args"] = args[0]
            captured["path"] = kwargs["env"]["PATH"]
            return subprocess.CompletedProcess(
                args[0],
                0,
                stdout="UUID: 11111111-2222-3333-4444-555555555555\n",
                stderr="",
            )

        operations = BtrfsRestoreOperations(runner=runner)

        with tempfile.TemporaryDirectory() as target_dir:
            with mock.patch.dict("os.environ", {"PATH": "/bin"}):
                operations.verify_metadata(Path(target_dir))

        self.assertEqual(
            captured["args"],
            ["btrfs", "subvolume", "show", target_dir],
        )
        assert isinstance(captured["path"], str)
        self.assertIn("/usr/sbin", captured["path"])
        self.assertIn("/sbin", captured["path"])

    def test_verify_metadata_requires_directory(self) -> None:
        operations = BtrfsRestoreOperations(runner=mock.Mock())

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "file.txt"
            target.write_text("data", encoding="utf-8")
            with self.assertRaises(RestoreBackendError) as context:
                operations.verify_metadata(target)
        self.assertIn("not a directory", str(context.exception))

    def test_verify_metadata_requires_writable(self) -> None:
        operations = BtrfsRestoreOperations(runner=mock.Mock())

        with tempfile.TemporaryDirectory() as target_dir:
            with mock.patch("os.access", return_value=False):
                with self.assertRaises(RestoreBackendError) as context:
                    operations.verify_metadata(Path(target_dir))
        self.assertIn("not writable", str(context.exception))

    def test_verify_metadata_invalid_uuid(self) -> None:
        def runner(*args, **kwargs):
            return subprocess.CompletedProcess(
                args[0],
                0,
                stdout="UUID: not-a-uuid\n",
                stderr="",
            )

        operations = BtrfsRestoreOperations(runner=runner)

        with tempfile.TemporaryDirectory() as target_dir:
            with self.assertRaises(RestoreBackendError) as context:
                operations.verify_metadata(Path(target_dir))
        self.assertIn("valid UUID", str(context.exception))

    def test_resolve_verify_source_returns_snapshot_path(self) -> None:
        operations = BtrfsRestoreOperations(runner=mock.Mock())

        resolved = operations.resolve_verify_source(
            "data",
            "~/snapshots/data__20260101T000000Z__full",
            None,
        )

        self.assertEqual(
            resolved,
            Path("~/snapshots/data__20260101T000000Z__full").expanduser(),
        )

    def test_finalize_restore_surfaces_stderr(self) -> None:
        def runner(*args, **kwargs):
            raise subprocess.CalledProcessError(
                1,
                args[0],
                stderr="property set failed",
            )

        operations = BtrfsRestoreOperations(runner=runner)

        with self.assertRaises(RestoreBackendError) as context:
            operations.finalize_restore(Path("/tmp/target"))
        self.assertIn("property set failed", str(context.exception))


class CreateFilesystemBackendTests(unittest.TestCase):
    def test_create_btrfs_backend_preserves_legacy_source_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshots_dir = Path(temp_dir) / "snapshots"
            data_path = Path(temp_dir) / "data"
            root_path = Path(temp_dir) / "root"
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
                snapshots=SnapshotsConfig(base_dir=snapshots_dir, retain=2),
                subvolumes=SubvolumesConfig(paths=(data_path, root_path)),
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

            backend = create_filesystem_backend(config, runner=RecordingRunner())

        self.assertEqual(backend.name, "btrfs")
        self.assertEqual(
            tuple(source.identifier for source in backend.sources),
            ("data", "root"),
        )
        self.assertEqual(
            tuple(source.path for source in backend.sources),
            (data_path, root_path),
        )
        self.assertIsInstance(backend.snapshot_operations, BtrfsSnapshotManager)
        self.assertEqual(backend.snapshot_operations.base_dir, snapshots_dir)
        self.assertIsInstance(backend.send_operations, BtrfsSendOperations)
        self.assertIsInstance(backend.restore_operations, BtrfsRestoreOperations)


if __name__ == "__main__":
    unittest.main()
