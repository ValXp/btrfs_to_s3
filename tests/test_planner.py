"""Planner tests."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path

from btrfs_to_s3.config import (
    Config,
    GlobalConfig,
    S3Config,
    ScheduleConfig,
    SnapshotsConfig,
    SubvolumesConfig,
)
from btrfs_to_s3.planner import plan_backups
from btrfs_to_s3.state import State, SubvolumeState


def make_config() -> Config:
    return Config(
        global_cfg=GlobalConfig(
            log_level="info",
            state_path=Path("/tmp/state.json"),
            lock_path=Path("/tmp/lock"),
            spool_dir=Path("/tmp/spool"),
            spool_size_bytes=1024,
        ),
        schedule=ScheduleConfig(
            full_every_days=180,
            incremental_every_days=7,
            run_at="02:00",
        ),
        snapshots=SnapshotsConfig(base_dir=Path("/tmp/snap"), retain=2),
        subvolumes=SubvolumesConfig(paths=(Path("/srv/data/data"),)),
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
    )


class PlannerTests(unittest.TestCase):
    def test_full_due(self) -> None:
        config = make_config()
        state = State(
            subvolumes={
                "data": SubvolumeState(last_full_at="2024-01-01T00:00:00Z")
            }
        )
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        plan = plan_backups(config, state, now)
        self.assertEqual(plan[0].action, "full")

    def test_incremental_due(self) -> None:
        config = make_config()
        state = State(
            subvolumes={
                "data": SubvolumeState(
                    last_full_at="2025-12-15T00:00:00Z",
                    last_snapshot="data__20260101T000000Z__inc",
                )
            }
        )
        now = datetime(2026, 1, 10, tzinfo=timezone.utc)
        plan = plan_backups(
            config,
            state,
            now,
            available_snapshots={"data__20260101T000000Z__inc"},
        )
        self.assertEqual(plan[0].action, "inc")
        self.assertEqual(plan[0].parent_snapshot, "data__20260101T000000Z__inc")

    def test_missing_parent_falls_back_to_full(self) -> None:
        config = make_config()
        state = State(
            subvolumes={
                "data": SubvolumeState(
                    last_full_at="2025-12-15T00:00:00Z",
                    last_snapshot="data__20260101T000000Z__inc",
                )
            }
        )
        now = datetime(2026, 1, 10, tzinfo=timezone.utc)
        plan = plan_backups(config, state, now, available_snapshots=set())
        self.assertEqual(plan[0].action, "full")

    def test_incremental_not_due_skips(self) -> None:
        config = make_config()
        state = State(
            subvolumes={
                "data": SubvolumeState(
                    last_full_at="2025-12-15T00:00:00Z",
                    last_snapshot="data__20260105T000000Z__inc",
                )
            }
        )
        now = datetime(2026, 1, 8, tzinfo=timezone.utc)
        plan = plan_backups(
            config,
            state,
            now,
            available_snapshots={"data__20260105T000000Z__inc"},
        )
        self.assertEqual(plan[0].action, "skip")


if __name__ == "__main__":
    unittest.main()
