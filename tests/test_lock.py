"""Lock file tests."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from btrfs_to_s3.lock import LockError, LockFile, _pid_is_running, _read_pid


class LockTests(unittest.TestCase):
    def test_read_pid_missing_or_empty_returns_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "missing.lock"
            self.assertEqual("unknown", _read_pid(lock_path))
            lock_path.write_text("")
            self.assertEqual("unknown", _read_pid(lock_path))

    def test_pid_is_running_handles_invalid_values(self) -> None:
        self.assertFalse(_pid_is_running("not-a-number"))
        self.assertFalse(_pid_is_running("-1"))

    def test_pid_is_running_for_current_pid(self) -> None:
        self.assertTrue(_pid_is_running(str(os.getpid())))

    def test_lock_contention_reports_pid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "btrfs_to_s3.lock"
            lock = LockFile(lock_path)
            with lock:
                with self.assertRaises(LockError) as context:
                    LockFile(lock_path).acquire()
                self.assertIn(str(os.getpid()), str(context.exception))

    def test_lock_release_removes_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "btrfs_to_s3.lock"
            lock = LockFile(lock_path)
            lock.acquire()
            self.assertTrue(lock_path.exists())
            lock.release()
            self.assertFalse(lock_path.exists())

    def test_stale_lock_is_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "btrfs_to_s3.lock"
            lock_path.write_text("999999")
            lock = LockFile(lock_path)
            lock.acquire()
            self.assertEqual(str(os.getpid()), lock_path.read_text())
            lock.release()

    def test_release_noop_when_inactive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "btrfs_to_s3.lock"
            lock = LockFile(lock_path)
            lock.release()


if __name__ == "__main__":
    unittest.main()
