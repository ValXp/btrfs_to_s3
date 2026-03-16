"""Tests for S3-backed discovery helpers."""

from __future__ import annotations

import json
import unittest

from btrfs_to_s3 import discovery


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.closed = False

    def read(self) -> bytes:
        return self._payload

    def close(self) -> None:
        self.closed = True


class FakeS3:
    def __init__(self, *, page_size: int = 1000) -> None:
        self.objects: dict[str, bytes] = {}
        self.page_size = page_size

    def get_object(self, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": FakeBody(self.objects[Key])}

    def list_objects_v2(
        self,
        Bucket: str,
        Prefix: str,
        ContinuationToken: str | None = None,
    ) -> dict[str, object]:
        del Bucket
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        start = int(ContinuationToken or "0")
        page = keys[start : start + self.page_size]
        response: dict[str, object] = {
            "Contents": [{"Key": key} for key in page],
            "IsTruncated": start + self.page_size < len(keys),
        }
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(start + self.page_size)
        return response


class DiscoveryTests(unittest.TestCase):
    def test_list_restorable_sources_reads_current_pointers(self) -> None:
        client = FakeS3(page_size=1)
        client.objects = {
            "backup/subvol/tank/home/current.json": json.dumps(
                {
                    "manifest_key": "backup/subvol/tank/home/full/manifest-2.json",
                    "kind": "full",
                    "created_at": "20260314T010000Z",
                }
            ).encode("utf-8"),
            "backup/subvol/tank/data/current.json": json.dumps(
                {
                    "manifest_key": (
                        "backup/subvol/tank/data/incremental/manifest-3.json"
                    ),
                    "kind": "incremental",
                    "created_at": "20260314T020000Z",
                }
            ).encode("utf-8"),
            "backup/subvol/tank/data/full/manifest-1.json": b"{}",
        }

        sources = discovery.list_restorable_sources(client, "bucket", "backup")

        self.assertEqual(
            [item.to_dict() for item in sources],
            [
                {
                    "source_name": "tank/data",
                    "current_key": "backup/subvol/tank/data/current.json",
                    "manifest_key": (
                        "backup/subvol/tank/data/incremental/manifest-3.json"
                    ),
                    "kind": "incremental",
                    "created_at": "20260314T020000Z",
                },
                {
                    "source_name": "tank/home",
                    "current_key": "backup/subvol/tank/home/current.json",
                    "manifest_key": "backup/subvol/tank/home/full/manifest-2.json",
                    "kind": "full",
                    "created_at": "20260314T010000Z",
                },
            ],
        )

    def test_list_available_manifests_marks_current_and_sorts_newest_first(
        self,
    ) -> None:
        client = FakeS3()
        client.objects = {
            "backup/subvol/data/current.json": json.dumps(
                {
                    "manifest_key": (
                        "backup/subvol/data/incremental/manifest-20260314T020000Z.json"
                    ),
                    "kind": "incremental",
                    "created_at": "20260314T020000Z",
                }
            ).encode("utf-8"),
            "backup/subvol/data/full/manifest-20260314T010000Z.json": b"{}",
            "backup/subvol/data/incremental/manifest-20260314T020000Z.json": b"{}",
            "backup/subvol/data/full/part-00000.bin": b"x",
        }

        manifests = discovery.list_available_manifests(
            client,
            "bucket",
            "backup/",
            "data",
        )

        self.assertEqual(
            [item.to_dict() for item in manifests],
            [
                {
                    "source_name": "data",
                    "key": (
                        "backup/subvol/data/incremental/manifest-20260314T020000Z.json"
                    ),
                    "kind": "incremental",
                    "created_at": "20260314T020000Z",
                    "is_current": True,
                },
                {
                    "source_name": "data",
                    "key": "backup/subvol/data/full/manifest-20260314T010000Z.json",
                    "kind": "full",
                    "created_at": "20260314T010000Z",
                    "is_current": False,
                },
            ],
        )

    def test_list_available_manifests_includes_current_when_listing_is_missing(
        self,
    ) -> None:
        client = FakeS3()
        client.objects = {
            "backup/subvol/data/current.json": json.dumps(
                {
                    "manifest_key": "backup/subvol/data/full/manifest-20260314T010000Z.json",
                    "kind": "full",
                    "created_at": "20260314T010000Z",
                }
            ).encode("utf-8"),
        }

        manifests = discovery.list_available_manifests(
            client,
            "bucket",
            "backup",
            "data",
        )

        self.assertEqual(
            [item.to_dict() for item in manifests],
            [
                {
                    "source_name": "data",
                    "key": "backup/subvol/data/full/manifest-20260314T010000Z.json",
                    "kind": "full",
                    "created_at": "20260314T010000Z",
                    "is_current": True,
                }
            ],
        )

    def test_list_restorable_sources_rejects_invalid_current_pointer(self) -> None:
        client = FakeS3()
        client.objects = {
            "backup/subvol/data/current.json": json.dumps(
                {"kind": "full", "created_at": "20260314T010000Z"}
            ).encode("utf-8")
        }

        with self.assertRaises(discovery.DiscoveryError) as context:
            discovery.list_restorable_sources(client, "bucket", "backup")

        self.assertIn("missing manifest_key", str(context.exception))


if __name__ == "__main__":
    unittest.main()
