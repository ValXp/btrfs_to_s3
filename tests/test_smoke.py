"""Basic smoke tests for package import/entrypoint."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout

import btrfs_to_s3
from btrfs_to_s3 import cli


class SmokeTests(unittest.TestCase):
    def test_import_version(self) -> None:
        self.assertIsInstance(btrfs_to_s3.__version__, str)

    def test_main_returns_zero(self) -> None:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.assertEqual(cli.main([]), 0)
        self.assertIn("btrfs_to_s3", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
