"""Streamer cleanup tests."""

from __future__ import annotations

import io
import subprocess
import unittest

from btrfs_to_s3.streamer import cleanup_btrfs_send


class StreamerCleanupTests(unittest.TestCase):
    def test_cleanup_terminates_and_returns_stderr(self) -> None:
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
        error = cleanup_btrfs_send(process, stdout=stdout)
        self.assertTrue(stdout.closed)
        self.assertTrue(process.terminated)
        self.assertFalse(process.killed)
        self.assertEqual(error, "stderr output")

    def test_cleanup_kills_on_timeout(self) -> None:
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
        error = cleanup_btrfs_send(process, stdout=stdout, timeout=0.01)
        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        self.assertEqual(error, "forced stderr")


if __name__ == "__main__":
    unittest.main()
