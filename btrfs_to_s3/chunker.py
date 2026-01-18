"""Chunking of byte streams with hashes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import BinaryIO, Iterator


@dataclass(frozen=True)
class Chunk:
    index: int
    reader: "ChunkReader"

    @property
    def size(self) -> int:
        return self.reader.size

    @property
    def sha256(self) -> str:
        return self.reader.sha256


class ChunkReader:
    def __init__(self, stream: BinaryIO, limit: int, initial: bytes) -> None:
        self._stream = stream
        self._remaining = max(limit, 0)
        self._buffer = bytearray(initial)
        self._hasher = hashlib.sha256()
        self._size = 0
        self._done = False

    def read(self, size: int = -1) -> bytes:
        if self._done:
            return b""
        available = len(self._buffer) + self._remaining
        if size is None or size < 0 or size > available:
            size = available
        if size == 0:
            self._mark_done()
            return b""
        chunks: list[bytes] = []
        if self._buffer:
            take = min(size, len(self._buffer))
            chunks.append(bytes(self._buffer[:take]))
            del self._buffer[:take]
            size -= take
        if size > 0 and self._remaining > 0:
            data = self._stream.read(min(size, self._remaining))
            if data:
                chunks.append(data)
                self._remaining -= len(data)
            else:
                self._remaining = 0
        if not chunks:
            self._mark_done()
            return b""
        output = b"".join(chunks)
        self._hasher.update(output)
        self._size += len(output)
        if self._remaining == 0 and not self._buffer:
            self._mark_done()
        return output

    def _mark_done(self) -> None:
        self._done = True
        self._remaining = 0
        self._buffer.clear()

    @property
    def done(self) -> bool:
        return self._done

    @property
    def size(self) -> int:
        if not self._done:
            raise RuntimeError("chunk not fully read")
        return self._size

    @property
    def sha256(self) -> str:
        if not self._done:
            raise RuntimeError("chunk not fully read")
        return self._hasher.hexdigest()


def chunk_stream(stream: BinaryIO, chunk_size: int) -> Iterator[Chunk]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    index = 0
    previous: ChunkReader | None = None
    while True:
        if previous is not None and not previous.done:
            raise RuntimeError("previous chunk not fully read")
        initial = stream.read(1)
        if not initial:
            break
        previous = ChunkReader(stream, chunk_size - len(initial), initial)
        yield Chunk(index=index, reader=previous)
        index += 1
