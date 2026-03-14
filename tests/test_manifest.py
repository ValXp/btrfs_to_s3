"""Manifest tests."""

from __future__ import annotations

import unittest

from btrfs_to_s3.manifest import (
    MANIFEST_VERSION,
    ChunkEntry,
    CurrentPointer,
    Manifest,
    SnapshotInfo,
    publish_manifest,
)


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_object(self, **kwargs) -> None:
        self.calls.append(kwargs)


class ManifestTests(unittest.TestCase):
    def test_manifest_schema(self) -> None:
        manifest = Manifest(
            version=MANIFEST_VERSION,
            filesystem="btrfs",
            source_name="data",
            kind="full",
            created_at="2026-01-01T00:00:00Z",
            snapshot=SnapshotInfo(
                name="data__20260101T000000Z__full",
                path="/srv/snapshots/data__20260101T000000Z__full",
                identity="/srv/snapshots/data__20260101T000000Z__full",
            ),
            parent_manifest=None,
            chunks=(
                ChunkEntry(
                    key="subvol/data/full/part-00000.bin",
                    size=10,
                    sha256="deadbeef",
                    etag="etag",
                ),
            ),
            total_bytes=10,
            chunk_size=10,
            s3={"bucket": "bucket", "region": "us-east-1"},
        )
        data = manifest.to_dict()
        self.assertEqual(data["version"], MANIFEST_VERSION)
        self.assertEqual(data["filesystem"], "btrfs")
        self.assertEqual(manifest.subvolume, "data")
        self.assertEqual(data["snapshot"]["name"], "data__20260101T000000Z__full")
        self.assertEqual(
            data["snapshot"]["identity"],
            "/srv/snapshots/data__20260101T000000Z__full",
        )
        self.assertEqual(len(data["chunks"]), 1)
        self.assertEqual(data["chunks"][0]["key"], "subvol/data/full/part-00000.bin")

    def test_manifest_schema_supports_zfs_identity(self) -> None:
        manifest = Manifest(
            version=MANIFEST_VERSION,
            filesystem="zfs",
            source_name="tank/data",
            kind="incremental",
            created_at="2026-01-01T00:00:00Z",
            snapshot=SnapshotInfo(
                name="tank_x2f_data__20260101T000000Z__inc",
                path=None,
                identity="tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__inc",
            ),
            parent_manifest="manifests/base.json",
            chunks=(),
            total_bytes=0,
            chunk_size=0,
            s3={"bucket": "bucket", "region": "us-east-1"},
        )

        data = manifest.to_dict()

        self.assertEqual(data["filesystem"], "zfs")
        self.assertIsNone(data["snapshot"]["path"])
        self.assertEqual(
            data["snapshot"]["identity"],
            "tank/data@btrfs-to-s3-tank_x2f_data__20260101T000000Z__inc",
        )

    def test_manifest_accepts_legacy_subvolume_keyword(self) -> None:
        manifest = Manifest(
            version=MANIFEST_VERSION,
            filesystem="btrfs",
            subvolume="data",
            kind="full",
            created_at="2026-01-01T00:00:00Z",
            snapshot=SnapshotInfo(name="snap", path="/snap", identity="/snap"),
            parent_manifest=None,
            chunks=(),
            total_bytes=0,
            chunk_size=0,
            s3={},
        )

        self.assertEqual(manifest.source_name, "data")
        self.assertEqual(manifest.to_dict()["subvolume"], "data")

    def test_publish_order(self) -> None:
        client = FakeClient()
        manifest = Manifest(
            version=MANIFEST_VERSION,
            filesystem="btrfs",
            source_name="data",
            kind="full",
            created_at="2026-01-01T00:00:00Z",
            snapshot=SnapshotInfo(
                name="data__20260101T000000Z__full",
                path="/srv/snapshots/data__20260101T000000Z__full",
                identity="/srv/snapshots/data__20260101T000000Z__full",
            ),
            parent_manifest=None,
            chunks=(),
            total_bytes=0,
            chunk_size=0,
            s3={"bucket": "bucket", "region": "us-east-1"},
        )
        pointer = CurrentPointer(
            manifest_key="subvol/data/full/manifest.json",
            kind="full",
            created_at="2026-01-01T00:00:00Z",
        )
        publish_manifest(
            client=client,
            bucket="bucket",
            manifest_key="subvol/data/full/manifest.json",
            current_key="subvol/data/current.json",
            manifest=manifest,
            pointer=pointer,
            storage_class="STANDARD",
            sse="AES256",
        )
        self.assertEqual(client.calls[0]["Key"], "subvol/data/full/manifest.json")
        self.assertEqual(client.calls[1]["Key"], "subvol/data/current.json")


if __name__ == "__main__":
    unittest.main()
