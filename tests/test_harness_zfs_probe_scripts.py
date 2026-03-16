"""Tests for standalone ZFS probe scripts."""

from __future__ import annotations

import importlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock


full_probe = importlib.import_module(
    "integration_tests.scripts.run_zfs_snapshot_send_receive"
)
incremental_probe = importlib.import_module(
    "integration_tests.scripts.run_zfs_incremental"
)
retention_probe = importlib.import_module(
    "integration_tests.scripts.run_zfs_retention"
)


class RecordingLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def __enter__(self) -> "RecordingLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write(self, message: str, *, level: str = "INFO") -> None:
        self.entries.append((level, message))


class FullZFSProbeScriptTests(unittest.TestCase):
    def test_full_probe_runs_send_receive_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(tmpdir)
            log = RecordingLog()
            source_dataset = "tank/data"
            receive_parent = "tank/restore"
            receive_dataset = "tank/restore/data"
            source_snapshot = "tank/data@probe-full"
            received_snapshot = "tank/restore/data@probe-full"
            events: list[str] = []

            zfs_mock = mock.Mock()
            zfs_mock.create_snapshot.side_effect = _record(events, "create_snapshot")
            zfs_mock.list_datasets.side_effect = _record(
                events,
                "list_datasets",
                [receive_parent, receive_dataset],
            )
            zfs_mock.list_snapshots.side_effect = _record(
                events,
                "list_snapshots",
                [received_snapshot],
            )

            with mock.patch.object(
                full_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                full_probe, "open_log", return_value=log
            ), mock.patch.object(
                full_probe, "zfs", zfs_mock
            ), mock.patch.object(
                full_probe.support,
                "write_dataset_text",
                side_effect=_record(events, "write_dataset_text"),
            ) as write_mock, mock.patch.object(
                full_probe.support,
                "stream_send_receive",
                side_effect=_record(events, "stream_send_receive", 512),
            ) as stream_mock, mock.patch.object(
                full_probe.support,
                "read_dataset_text",
                side_effect=_record(
                    events,
                    "read_dataset_text",
                    full_probe.PROBE_CONTENT,
                ),
            ) as read_mock, mock.patch.object(
                full_probe.sys,
                "argv",
                ["run_zfs_snapshot_send_receive.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = full_probe.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            events,
            [
                "write_dataset_text",
                "create_snapshot",
                "stream_send_receive",
                "list_datasets",
                "list_snapshots",
                "read_dataset_text",
            ],
        )
        write_mock.assert_called_once_with(
            source_dataset,
            full_probe.PROBE_FILE,
            full_probe.PROBE_CONTENT,
        )
        stream_mock.assert_called_once_with(source_snapshot, receive_dataset)
        read_mock.assert_called_once_with(receive_dataset, full_probe.PROBE_FILE)
        self.assertIn(("INFO", "full ZFS send/receive probe completed"), log.entries)

    def test_full_probe_logs_verification_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(tmpdir)
            log = RecordingLog()

            zfs_mock = mock.Mock()
            zfs_mock.list_datasets.return_value = ["tank/restore", "tank/restore/data"]
            zfs_mock.list_snapshots.return_value = []

            with mock.patch.object(
                full_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                full_probe, "open_log", return_value=log
            ), mock.patch.object(
                full_probe, "zfs", zfs_mock
            ), mock.patch.object(
                full_probe.support, "write_dataset_text"
            ), mock.patch.object(
                full_probe.support, "stream_send_receive", return_value=512
            ), mock.patch.object(
                full_probe.support, "read_dataset_text"
            ), mock.patch.object(
                full_probe.sys,
                "argv",
                ["run_zfs_snapshot_send_receive.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = full_probe.main()

        self.assertEqual(result, 1)
        self.assertIn(
            (
                "ERROR",
                "full send/receive probe failed: received snapshot missing from zfs list output: tank/restore/data@probe-full",
            ),
            log.entries,
        )

    def test_full_probe_requires_receive_parent_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(tmpdir, include_receive_parent=False)
            log = RecordingLog()

            with mock.patch.object(
                full_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                full_probe, "open_log", return_value=log
            ), mock.patch.object(
                full_probe.support, "write_dataset_text"
            ) as write_mock, mock.patch.object(
                full_probe.support, "stream_send_receive"
            ) as stream_mock, mock.patch.object(
                full_probe.sys,
                "argv",
                [
                    "run_zfs_snapshot_send_receive.py",
                    "--config",
                    str(Path(tmpdir) / "test_zfs.toml"),
                ],
            ):
                result = full_probe.main()

        self.assertEqual(result, 1)
        write_mock.assert_not_called()
        stream_mock.assert_not_called()
        self.assertIn(
            (
                "ERROR",
                "full send/receive probe failed: zfs.receive_parent_dataset is required for the standalone full send/receive probe",
            ),
            log.entries,
        )


class IncrementalZFSProbeScriptTests(unittest.TestCase):
    def test_incremental_probe_runs_full_then_incremental_send(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(tmpdir)
            log = RecordingLog()
            receive_dataset = "tank/restore/data-incremental"
            base_snapshot = "tank/data@probe-base"
            incremental_snapshot = "tank/data@probe-incremental"
            received_base_snapshot = "tank/restore/data-incremental@probe-base"
            received_incremental_snapshot = (
                "tank/restore/data-incremental@probe-incremental"
            )
            events: list[str] = []

            zfs_mock = mock.Mock()
            zfs_mock.create_snapshot.side_effect = _record(events, "create_snapshot")
            zfs_mock.list_snapshots.side_effect = _record(
                events,
                "list_snapshots",
                [received_base_snapshot, received_incremental_snapshot],
            )

            stream_returns = iter((256, 128))

            def stream_side_effect(*args, **kwargs):
                events.append("stream_send_receive")
                return next(stream_returns)

            with mock.patch.object(
                incremental_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                incremental_probe, "open_log", return_value=log
            ), mock.patch.object(
                incremental_probe, "zfs", zfs_mock
            ), mock.patch.object(
                incremental_probe.support,
                "write_dataset_text",
                side_effect=_record(events, "write_dataset_text"),
            ), mock.patch.object(
                incremental_probe.support,
                "append_dataset_text",
                side_effect=_record(events, "append_dataset_text"),
            ), mock.patch.object(
                incremental_probe.support,
                "stream_send_receive",
                side_effect=stream_side_effect,
            ) as stream_mock, mock.patch.object(
                incremental_probe.support,
                "read_dataset_text",
                side_effect=_record(
                    events,
                    "read_dataset_text",
                    incremental_probe.BASE_CONTENT + incremental_probe.INCREMENTAL_CONTENT,
                ),
            ), mock.patch.object(
                incremental_probe.sys,
                "argv",
                ["run_zfs_incremental.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = incremental_probe.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            events,
            [
                "write_dataset_text",
                "create_snapshot",
                "stream_send_receive",
                "append_dataset_text",
                "create_snapshot",
                "stream_send_receive",
                "list_snapshots",
                "read_dataset_text",
            ],
        )
        self.assertEqual(
            stream_mock.call_args_list,
            [
                mock.call(base_snapshot, receive_dataset),
                mock.call(
                    incremental_snapshot,
                    receive_dataset,
                    parent_snapshot=base_snapshot,
                    force=True,
                ),
            ],
        )
        self.assertIn(("INFO", "incremental ZFS probe completed"), log.entries)

    def test_incremental_probe_logs_stream_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(tmpdir)
            log = RecordingLog()

            with mock.patch.object(
                incremental_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                incremental_probe, "open_log", return_value=log
            ), mock.patch.object(
                incremental_probe, "zfs", mock.Mock()
            ), mock.patch.object(
                incremental_probe.support, "write_dataset_text"
            ), mock.patch.object(
                incremental_probe.support, "append_dataset_text"
            ), mock.patch.object(
                incremental_probe.support,
                "stream_send_receive",
                side_effect=[256, RuntimeError("incremental replay failed")],
            ), mock.patch.object(
                incremental_probe.support, "read_dataset_text"
            ), mock.patch.object(
                incremental_probe.sys,
                "argv",
                ["run_zfs_incremental.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = incremental_probe.main()

        self.assertEqual(result, 1)
        self.assertIn(
            ("ERROR", "incremental probe failed: incremental replay failed"),
            log.entries,
        )

    def test_incremental_probe_requires_receive_parent_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(tmpdir, include_receive_parent=False)
            log = RecordingLog()

            with mock.patch.object(
                incremental_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                incremental_probe, "open_log", return_value=log
            ), mock.patch.object(
                incremental_probe.support, "write_dataset_text"
            ) as write_mock, mock.patch.object(
                incremental_probe.support, "stream_send_receive"
            ) as stream_mock, mock.patch.object(
                incremental_probe.sys,
                "argv",
                [
                    "run_zfs_incremental.py",
                    "--config",
                    str(Path(tmpdir) / "test_zfs.toml"),
                ],
            ):
                result = incremental_probe.main()

        self.assertEqual(result, 1)
        write_mock.assert_not_called()
        stream_mock.assert_not_called()
        self.assertIn(
            (
                "ERROR",
                "incremental probe failed: zfs.receive_parent_dataset is required for the standalone incremental send/receive probe",
            ),
            log.entries,
        )


class RetentionZFSProbeScriptTests(unittest.TestCase):
    def test_retention_probe_prunes_parent_and_logs_expected_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(
                tmpdir,
                retention_snapshots=2,
                include_receive_parent=False,
            )
            log = RecordingLog()
            source_dataset = "tank/data"
            oldest_snapshot = "tank/data@probe-retention-1"
            latest_snapshot = "tank/data@probe-retention-3"
            remaining_snapshots = [
                "tank/data@probe-retention-2",
                latest_snapshot,
            ]
            events: list[str] = []

            zfs_mock = mock.Mock()
            zfs_mock.create_snapshot.side_effect = _record(events, "create_snapshot")
            zfs_mock.destroy_snapshot.side_effect = _record(events, "destroy_snapshot")
            zfs_mock.list_snapshots.side_effect = _record(
                events,
                "list_snapshots",
                remaining_snapshots,
            )

            def drain_side_effect(*args, **kwargs):
                events.append("drain_send_stream")
                raise RuntimeError("missing incremental parent snapshot")

            with mock.patch.object(
                retention_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                retention_probe, "open_log", return_value=log
            ), mock.patch.object(
                retention_probe, "zfs", zfs_mock
            ), mock.patch.object(
                retention_probe.support,
                "write_dataset_text",
                side_effect=_record(events, "write_dataset_text"),
            ), mock.patch.object(
                retention_probe.support,
                "append_dataset_text",
                side_effect=_record(events, "append_dataset_text"),
            ), mock.patch.object(
                retention_probe.support,
                "drain_send_stream",
                side_effect=drain_side_effect,
            ) as drain_mock, mock.patch.object(
                retention_probe.sys,
                "argv",
                ["run_zfs_retention.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = retention_probe.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            events,
            [
                "write_dataset_text",
                "create_snapshot",
                "append_dataset_text",
                "create_snapshot",
                "append_dataset_text",
                "create_snapshot",
                "destroy_snapshot",
                "list_snapshots",
                "drain_send_stream",
            ],
        )
        drain_mock.assert_called_once_with(
            latest_snapshot,
            parent_snapshot=oldest_snapshot,
        )
        self.assertIn(
            (
                "INFO",
                "incremental send failed as expected after pruning tank/data@probe-retention-1: missing incremental parent snapshot",
            ),
            log.entries,
        )
        self.assertIn(("INFO", "retention ZFS probe completed"), log.entries)
        zfs_mock.list_snapshots.assert_called_once_with(source_dataset)

    def test_retention_probe_logs_unexpected_incremental_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _probe_config(tmpdir, retention_snapshots=2)
            log = RecordingLog()

            zfs_mock = mock.Mock()
            zfs_mock.list_snapshots.return_value = [
                "tank/data@probe-retention-2",
                "tank/data@probe-retention-3",
            ]

            with mock.patch.object(
                retention_probe.support,
                "load_zfs_config",
                return_value=config,
            ), mock.patch.object(
                retention_probe, "open_log", return_value=log
            ), mock.patch.object(
                retention_probe, "zfs", zfs_mock
            ), mock.patch.object(
                retention_probe.support, "write_dataset_text"
            ), mock.patch.object(
                retention_probe.support, "append_dataset_text"
            ), mock.patch.object(
                retention_probe.support, "drain_send_stream", return_value=64
            ), mock.patch.object(
                retention_probe.sys,
                "argv",
                ["run_zfs_retention.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = retention_probe.main()

        self.assertEqual(result, 1)
        self.assertIn(
            (
                "ERROR",
                "retention probe failed: incremental send unexpectedly succeeded after pruning the incremental parent",
            ),
            log.entries,
        )


def _probe_config(
    tmpdir: str,
    *,
    retention_snapshots: int = 2,
    include_receive_parent: bool = True,
) -> dict[str, object]:
    logs_dir = Path(tmpdir) / "integration_tests" / "run" / "zfs" / "logs"
    zfs_config: dict[str, object] = {
        "pool_name": "tank",
        "source_datasets": ["data"],
        "snapshot_prefix": "probe",
    }
    if include_receive_parent:
        zfs_config["receive_parent_dataset"] = "restore"
    return {
        "paths": {"logs_dir": str(logs_dir)},
        "zfs": zfs_config,
        "backup": {"retention_snapshots": retention_snapshots},
    }


def _record(events: list[str], name: str, result=None):
    def _side_effect(*args, **kwargs):
        events.append(name)
        return result

    return _side_effect
