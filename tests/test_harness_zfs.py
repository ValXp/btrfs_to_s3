"""Tests for the ZFS integration harness helpers."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from integration_tests.harness import zfs


class ZFSHarnessCommandTests(unittest.TestCase):
    def test_create_backing_file_builds_truncate_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "integration_tests" / "run"
            pool_file = run_dir / "zfs" / "pool.img"
            run_dir.mkdir(parents=True)
            with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
                run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
                result = zfs.create_backing_file(str(pool_file), 4, run_dir=str(run_dir))

        run.assert_called_once_with(
            ["truncate", "-s", "4G", str(pool_file)],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )
        self.assertEqual(result, str(pool_file))

    def test_create_backing_file_rejects_path_outside_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "integration_tests" / "run"
            outside = Path(tmpdir) / "pool.img"
            run_dir.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "is not under"):
                zfs.create_backing_file(str(outside), 1, run_dir=str(run_dir))

    def test_create_pool_builds_expected_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "integration_tests" / "run"
            pool_file = run_dir / "zfs" / "pool.img"
            mount_root = run_dir / "zfs" / "mnt"
            run_dir.mkdir(parents=True)
            with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
                run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
                result = zfs.create_pool(
                    "tank",
                    str(pool_file),
                    str(mount_root),
                    create_args=("-O", "compression=zstd", "-O", "atime=off"),
                    run_dir=str(run_dir),
                )

        run.assert_called_once_with(
            [
                "zpool",
                "create",
                "-f",
                "-R",
                str(mount_root),
                "-O",
                "compression=zstd",
                "-O",
                "atime=off",
                "tank",
                str(pool_file),
            ],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )
        self.assertEqual(result, "tank")

    def test_destroy_pool_builds_expected_command(self) -> None:
        with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            zfs.destroy_pool("tank")

        run.assert_called_once_with(
            ["zpool", "destroy", "-f", "tank"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )

    def test_create_dataset_builds_expected_command(self) -> None:
        with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            result = zfs.create_dataset("tank/data", create_args=("-o", "compression=zstd"))

        run.assert_called_once_with(
            ["zfs", "create", "-o", "compression=zstd", "tank/data"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )
        self.assertEqual(result, "tank/data")

    def test_list_datasets_parses_output(self) -> None:
        with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                [],
                0,
                stdout="tank/home\ntank\ntank/data\n",
                stderr="",
            )
            datasets = zfs.list_datasets("tank")

        run.assert_called_once_with(
            ["zfs", "list", "-H", "-o", "name", "-t", "filesystem", "-r", "tank"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )
        self.assertEqual(datasets, ["tank", "tank/data", "tank/home"])

    def test_create_snapshot_builds_expected_command(self) -> None:
        with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            result = zfs.create_snapshot("tank/data@snap-001")

        run.assert_called_once_with(
            ["zfs", "snapshot", "tank/data@snap-001"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )
        self.assertEqual(result, "tank/data@snap-001")

    def test_destroy_snapshot_builds_expected_command(self) -> None:
        with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            zfs.destroy_snapshot("tank/data@snap-001")

        run.assert_called_once_with(
            ["zfs", "destroy", "tank/data@snap-001"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )

    def test_list_snapshots_parses_output(self) -> None:
        with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                [],
                0,
                stdout="tank/data@base\ntank/data@next\n",
                stderr="",
            )
            snapshots = zfs.list_snapshots("tank/data")

        run.assert_called_once_with(
            ["zfs", "list", "-H", "-o", "name", "-s", "creation", "-t", "snapshot", "-r", "tank/data"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )
        self.assertEqual(snapshots, ["tank/data@base", "tank/data@next"])

    def test_get_dataset_property_reads_value(self) -> None:
        with mock.patch("integration_tests.harness.zfs.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout="/mnt/tank/data\n", stderr="")
            value = zfs.get_dataset_property("tank/data", "mountpoint")

        run.assert_called_once_with(
            ["zfs", "get", "-H", "-o", "value", "mountpoint", "tank/data"],
            check=True,
            text=True,
            capture_output=True,
            env=mock.ANY,
        )
        self.assertEqual(value, "/mnt/tank/data")

    def test_preserves_stderr_from_failed_subprocesses(self) -> None:
        error = subprocess.CalledProcessError(
            1,
            ["zfs", "snapshot", "tank/data@snap-001"],
            stderr="permission denied",
        )
        with mock.patch("integration_tests.harness.zfs.subprocess.run", side_effect=error):
            with self.assertRaises(subprocess.CalledProcessError) as excinfo:
                zfs.create_snapshot("tank/data@snap-001")

        self.assertEqual(excinfo.exception.stderr, "permission denied")


class ZFSHarnessStreamTests(unittest.TestCase):
    def test_open_zfs_send_builds_full_args(self) -> None:
        stdout = mock.Mock()
        process = mock.Mock()
        process.stdout = stdout
        with mock.patch("integration_tests.harness.zfs.subprocess.Popen") as popen:
            popen.return_value = process
            result = zfs.open_zfs_send("tank/data@snap-001")

        popen.assert_called_once_with(
            ["zfs", "send", "tank/data@snap-001"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=mock.ANY,
        )
        self.assertEqual(result, zfs.ZFSSendProcess(process=process, stdout=stdout))

    def test_open_zfs_send_builds_incremental_args(self) -> None:
        stdout = mock.Mock()
        process = mock.Mock()
        process.stdout = stdout
        with mock.patch("integration_tests.harness.zfs.subprocess.Popen") as popen:
            popen.return_value = process
            result = zfs.open_zfs_send(
                "tank/data@snap-002",
                parent_snapshot="tank/data@snap-001",
            )

        popen.assert_called_once_with(
            ["zfs", "send", "-i", "tank/data@snap-001", "tank/data@snap-002"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=mock.ANY,
        )
        self.assertEqual(result, zfs.ZFSSendProcess(process=process, stdout=stdout))

    def test_open_zfs_send_raises_without_stdout(self) -> None:
        process = mock.Mock()
        process.stdout = None
        with mock.patch("integration_tests.harness.zfs.subprocess.Popen") as popen:
            popen.return_value = process
            with self.assertRaises(zfs.StreamError):
                zfs.open_zfs_send("tank/data@snap-001")

        process.kill.assert_called_once_with()

    def test_open_zfs_receive_builds_expected_args(self) -> None:
        stdin = mock.Mock()
        process = mock.Mock()
        process.stdin = stdin
        with mock.patch("integration_tests.harness.zfs.subprocess.Popen") as popen:
            popen.return_value = process
            result = zfs.open_zfs_receive("tank/restore", force=True)

        popen.assert_called_once_with(
            ["zfs", "receive", "-F", "tank/restore"],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=mock.ANY,
        )
        self.assertEqual(result, zfs.ZFSReceiveProcess(process=process, stdin=stdin))

    def test_open_zfs_receive_raises_without_stdin(self) -> None:
        process = mock.Mock()
        process.stdin = None
        with mock.patch("integration_tests.harness.zfs.subprocess.Popen") as popen:
            popen.return_value = process
            with self.assertRaises(zfs.StreamError):
                zfs.open_zfs_receive("tank/restore")

        process.kill.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
