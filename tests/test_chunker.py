"""Chunker tests."""

from __future__ import annotations

import hashlib
import io
import unittest

from btrfs_to_s3.chunker import chunk_stream


class ChunkerTests(unittest.TestCase):
    def test_chunk_boundaries_and_hashes(self) -> None:
        data = b"abcdefghij"
        stream = io.BytesIO(data)
        chunks = list(chunk_stream(stream, 4))
        sizes = [chunk.size for chunk in chunks]
        self.assertEqual(sizes, [4, 4, 2])
        expected_hashes = [
            hashlib.sha256(b"abcd").hexdigest(),
            hashlib.sha256(b"efgh").hexdigest(),
            hashlib.sha256(b"ij").hexdigest(),
        ]
        self.assertEqual([chunk.sha256 for chunk in chunks], expected_hashes)

    def test_total_bytes_tracked(self) -> None:
        data = b"x" * 9
        stream = io.BytesIO(data)
        chunks = list(chunk_stream(stream, 5))
        total = sum(chunk.size for chunk in chunks)
        self.assertEqual(total, len(data))


if __name__ == "__main__":
    unittest.main()
