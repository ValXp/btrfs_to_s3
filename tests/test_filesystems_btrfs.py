"""Btrfs filesystem backend tests."""

from __future__ import annotations

import io
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from btrfs_to_s3.filesystems.base import SendStream, StreamError
from btrfs_to_s3.filesystems.btrfs import (
    BtrfsSendOperations,
    BtrfsSnapshotManager,
)


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


if __name__ == "__main__":
    unittest.main()
