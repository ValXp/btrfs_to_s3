"""Chunking of byte streams with hashes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import BinaryIO, Iterator


@dataclass(frozen=True)
class Chunk:
    index: int
    size: int
    sha256: str
    data: bytes


def chunk_stream(stream: BinaryIO, chunk_size: int) -> Iterator[Chunk]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    index = 0
    while True:
        data = stream.read(chunk_size)
        if not data:
            break
        digest = hashlib.sha256(data).hexdigest()
        yield Chunk(index=index, size=len(data), sha256=digest, data=data)
        index += 1
