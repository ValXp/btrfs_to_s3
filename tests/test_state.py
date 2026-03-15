"""State serialization tests."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from btrfs_to_s3.state import (
    SourceState,
    State,
    StateLoadError,
    StateSaveError,
    SubvolumeState,
    load_state,
    save_state,
)


class StateTests(unittest.TestCase):
    def test_state_round_trip(self) -> None:
        state = State(
            sources={
                "data": SourceState(
                    last_snapshot="snap-1",
                    last_snapshot_name="snap-1",
                    last_manifest="man-1",
                    last_full_at="2026-01-01T00:00:00Z",
                ),
                "root": SourceState(
                    last_snapshot="snap-2",
                    last_snapshot_name="snap-2",
                ),
            },
            last_run_at="2026-01-02T00:00:00Z",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            save_state(path, state)
            loaded = load_state(path)
        self.assertEqual(loaded, state)

    def test_missing_state_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "missing.json"
            state = load_state(path)
            self.assertEqual(state, State())

    def test_invalid_json_raises_state_load_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            path.write_text("{bad json\n", encoding="utf-8")

            with self.assertRaises(StateLoadError) as exc_info:
                load_state(path)

        self.assertIn(str(path), str(exc_info.exception))
        self.assertIsInstance(exc_info.exception.__cause__, json.JSONDecodeError)

    def test_read_error_raises_state_load_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            path.write_text("{}\n", encoding="utf-8")

            with mock.patch.object(
                Path,
                "open",
                side_effect=OSError("permission denied"),
            ):
                with self.assertRaises(StateLoadError) as exc_info:
                    load_state(path)

        self.assertIn(str(path), str(exc_info.exception))
        self.assertIsInstance(exc_info.exception.__cause__, OSError)

    def test_save_error_raises_state_save_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            state = State()

            with mock.patch.object(
                Path,
                "open",
                side_effect=OSError("permission denied"),
            ):
                with self.assertRaises(StateSaveError) as exc_info:
                    save_state(path, state)

        self.assertIn(str(path), str(exc_info.exception))
        self.assertIsInstance(exc_info.exception.__cause__, OSError)

    def test_replace_error_raises_state_save_error_and_cleans_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            temp_path = path.with_suffix(".tmp")

            with mock.patch.object(
                Path,
                "replace",
                side_effect=OSError("device busy"),
            ):
                with self.assertRaises(StateSaveError) as exc_info:
                    save_state(path, State())
            self.assertFalse(temp_path.exists())

        self.assertIn(str(path), str(exc_info.exception))
        self.assertIsInstance(exc_info.exception.__cause__, OSError)

    def test_legacy_state_derives_snapshot_name_and_path(self) -> None:
        state = State.from_dict(
            {
                "subvolumes": {
                    "data": {
                        "last_snapshot": "/srv/snapshots/data__20260101T000000Z__inc",
                        "last_manifest": "manifest.json",
                    }
                }
            }
        )

        source = state.sources["data"]
        self.assertEqual(
            source.snapshot_identity,
            "/srv/snapshots/data__20260101T000000Z__inc",
        )
        self.assertEqual(
            source.snapshot_name,
            "data__20260101T000000Z__inc",
        )
        self.assertEqual(
            source.snapshot_path,
            "/srv/snapshots/data__20260101T000000Z__inc",
        )

    def test_zfs_state_round_trip(self) -> None:
        state = State(
            sources={
                "tank/data": SourceState(
                    last_snapshot=(
                        "tank/data@btrfs-to-s3-"
                        "tank_x2f_data__20260101T000000Z__inc"
                    ),
                    last_snapshot_name="tank_x2f_data__20260101T000000Z__inc",
                    last_snapshot_path=None,
                    last_manifest="manifest.json",
                )
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            save_state(path, state)
            loaded = load_state(path)

        self.assertEqual(loaded, state)

    def test_subvolumes_alias_maps_to_sources(self) -> None:
        state = State(
            subvolumes={"data": SubvolumeState(last_snapshot="snap-1")}
        )

        self.assertEqual(state.sources, state.subvolumes)
        self.assertIsInstance(state.sources["data"], SourceState)

    def test_from_dict_accepts_sources_key(self) -> None:
        state = State.from_dict(
            {
                "sources": {
                    "data": {
                        "last_snapshot": "snap-1",
                        "last_snapshot_name": "snap-1",
                    }
                }
            }
        )

        self.assertEqual(state.sources["data"].snapshot_identity, "snap-1")
        self.assertEqual(state.to_dict()["subvolumes"]["data"]["last_snapshot"], "snap-1")


if __name__ == "__main__":
    unittest.main()
