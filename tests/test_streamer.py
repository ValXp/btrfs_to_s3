"""Streamer compatibility wrapper tests."""

from __future__ import annotations

import io
import unittest
from pathlib import Path
from unittest import mock

from btrfs_to_s3.filesystems.base import SendStream
from btrfs_to_s3.streamer import (
    BtrfsSendProcess,
    cleanup_btrfs_send,
    open_btrfs_send,
)


class StreamerWrapperTests(unittest.TestCase):
    def test_btrfs_send_process_aliases_shared_send_stream(self) -> None:
        self.assertIs(BtrfsSendProcess, SendStream)

    def test_cleanup_btrfs_send_delegates_to_btrfs_backend(self) -> None:
        process = object()
        stdout = io.BytesIO(b"stream")
        with mock.patch(
            "btrfs_to_s3.streamer._SEND_OPERATIONS.cleanup_send",
            return_value="stderr output",
        ) as cleanup:
            error = cleanup_btrfs_send(process, stdout=stdout, timeout=0.25)

        cleanup.assert_called_once_with(
            process, stdout=stdout, timeout=0.25
        )
        self.assertEqual(error, "stderr output")

    def test_open_btrfs_send_delegates_to_btrfs_backend(self) -> None:
        process = mock.Mock()
        stdout = io.BytesIO(b"stream")
        stream = BtrfsSendProcess(process=process, stdout=stdout)
        with mock.patch(
            "btrfs_to_s3.streamer._SEND_OPERATIONS.open_send",
            return_value=stream,
        ) as open_send:
            result = open_btrfs_send(
                Path("/snapshots/child"),
                parent_snapshot=Path("/snapshots/parent"),
            )

        open_send.assert_called_once_with(
            Path("/snapshots/child"), Path("/snapshots/parent")
        )
        self.assertEqual(result, stream)


if __name__ == "__main__":
    unittest.main()
