"""Manifest and current pointer handling."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class SnapshotInfo:
    name: str
    path: str
    uuid: str | None = None
    parent_uuid: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "uuid": self.uuid,
            "parent_uuid": self.parent_uuid,
        }


@dataclass(frozen=True)
class ChunkEntry:
    key: str
    size: int
    sha256: str
    etag: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "size": self.size,
            "sha256": self.sha256,
            "etag": self.etag,
        }


@dataclass(frozen=True)
class Manifest:
    version: int
    subvolume: str
    kind: str
    created_at: str
    snapshot: SnapshotInfo
    parent_manifest: str | None
    chunks: tuple[ChunkEntry, ...]
    total_bytes: int
    chunk_size: int
    s3: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "subvolume": self.subvolume,
            "kind": self.kind,
            "created_at": self.created_at,
            "snapshot": self.snapshot.to_dict(),
            "parent_manifest": self.parent_manifest,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "total_bytes": self.total_bytes,
            "chunk_size": self.chunk_size,
            "s3": self.s3,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True).encode("utf-8")


@dataclass(frozen=True)
class CurrentPointer:
    manifest_key: str
    kind: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_key": self.manifest_key,
            "kind": self.kind,
            "created_at": self.created_at,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True).encode("utf-8")


def publish_manifest(
    client,
    bucket: str,
    manifest_key: str,
    current_key: str,
    manifest: Manifest,
    pointer: CurrentPointer,
    storage_class: str,
    sse: str,
) -> None:
    _put_json(
        client,
        bucket=bucket,
        key=manifest_key,
        body=manifest.to_json(),
        storage_class=storage_class,
        sse=sse,
    )
    _put_json(
        client,
        bucket=bucket,
        key=current_key,
        body=pointer.to_json(),
        storage_class=storage_class,
        sse=sse,
    )


def _put_json(
    client,
    bucket: str,
    key: str,
    body: bytes,
    storage_class: str,
    sse: str,
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        StorageClass=storage_class,
        ServerSideEncryption=sse,
    )
