"""State serialization tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from btrfs_to_s3.state import State, SubvolumeState, load_state, save_state


class StateTests(unittest.TestCase):
    def test_state_round_trip(self) -> None:
        state = State(
            subvolumes={
                "data": SubvolumeState(
                    last_snapshot="snap-1",
                    last_manifest="man-1",
                    last_full_at="2026-01-01T00:00:00Z",
                ),
                "root": SubvolumeState(last_snapshot="snap-2"),
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


if __name__ == "__main__":
    unittest.main()
