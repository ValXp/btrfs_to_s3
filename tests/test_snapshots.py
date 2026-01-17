"""Snapshot manager tests."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from btrfs_to_s3.snapshots import SnapshotManager, snapshot_name


class RecordingRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def run(self, args: list[str]) -> None:
        self.calls.append(args)


class SnapshotTests(unittest.TestCase):
    def test_snapshot_name_deterministic(self) -> None:
        when = datetime(2026, 1, 1, 2, 3, 4, tzinfo=timezone.utc)
        self.assertEqual(
            snapshot_name("data", when, "full"),
            "data__20260101T020304Z__full",
        )

    def test_create_snapshot_records_command(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = RecordingRunner()
            manager = SnapshotManager(
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
            manager = SnapshotManager(base_dir=base_dir, runner=runner)
            deleted = manager.prune_snapshots(
                "data", retain=1, keep_name=names[0]
            )
            deleted_names = {path.name for path in deleted}
            self.assertEqual(deleted_names, {names[1]})
            self.assertEqual(
                runner.calls,
                [["btrfs", "subvolume", "delete", str(base_dir / names[1])]],
            )


if __name__ == "__main__":
    unittest.main()
