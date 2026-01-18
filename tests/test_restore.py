"""Restore helper tests."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from btrfs_to_s3 import restore


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def read(self, size: int | None = None) -> bytes:
        if size is None:
            size = len(self._payload) - self._offset
        if size < 0:
            return b""
        start = self._offset
        end = min(len(self._payload), start + size)
        self._offset = end
        return self._payload[start:end]


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.restore_requests: list[str] = []
        self.restore_headers: dict[str, list[str | None]] = {}

    def get_object(self, Bucket: str, Key: str) -> dict[str, object]:
        if Key not in self.objects:
            raise KeyError(Key)
        payload = self.objects[Key]
        if isinstance(payload, FakeBody):
            return {"Body": payload}
        return {"Body": FakeBody(payload)}

    def head_object(self, Bucket: str, Key: str) -> dict[str, object]:
        if Key not in self.objects:
            raise KeyError(Key)
        headers = self.restore_headers.get(Key, [None])
        header = headers[0]
        if len(headers) > 1:
            headers.pop(0)
        return {"Restore": header}

    def restore_object(self, Bucket: str, Key: str, RestoreRequest: dict) -> None:
        if Key not in self.objects:
            raise KeyError(Key)
        self.restore_requests.append(Key)


class RestoreTests(unittest.TestCase):
    def test_resolve_manifest_chain_orders_full_first(self) -> None:
        client = FakeS3()
        full_manifest = {
            "kind": "full",
            "parent_manifest": None,
            "chunks": [
                {"key": "full/chunk.bin", "sha256": "abc", "size": 1}
            ],
            "s3": {"storage_class": "STANDARD"},
        }
        inc_manifest = {
            "kind": "inc",
            "parent_manifest": "full/manifest.json",
            "chunks": [
                {"key": "inc/chunk.bin", "sha256": "def", "size": 1}
            ],
            "s3": {"storage_class": "STANDARD"},
        }
        client.objects["full/manifest.json"] = json.dumps(full_manifest).encode(
            "utf-8"
        )
        client.objects["inc/manifest.json"] = json.dumps(inc_manifest).encode(
            "utf-8"
        )

        manifests = restore.resolve_manifest_chain(
            client, "bucket", "inc/manifest.json"
        )

        self.assertEqual([item.key for item in manifests], [
            "full/manifest.json",
            "inc/manifest.json",
        ])
        self.assertEqual(manifests[0].kind, "full")

    def test_missing_parent_manifest_reports_key(self) -> None:
        client = FakeS3()
        inc_manifest = {
            "kind": "inc",
            "parent_manifest": "missing/manifest.json",
            "chunks": [
                {"key": "inc/chunk.bin", "sha256": "def", "size": 1}
            ],
            "s3": {"storage_class": "STANDARD"},
        }
        client.objects["inc/manifest.json"] = json.dumps(inc_manifest).encode(
            "utf-8"
        )
        with self.assertRaises(restore.RestoreError) as context:
            restore.resolve_manifest_chain(
                client, "bucket", "inc/manifest.json"
            )
        self.assertIn("missing/manifest.json", str(context.exception))

    def test_restore_header_parsing(self) -> None:
        self.assertTrue(restore.needs_restore("GLACIER"))
        self.assertTrue(restore.needs_restore("DEEP_ARCHIVE"))
        self.assertTrue(restore.needs_restore("GLACIER_IR"))
        self.assertFalse(restore.needs_restore("STANDARD"))
        self.assertFalse(restore.needs_restore("STANDARD_IA"))

        header_ready = 'ongoing-request="false", expiry-date="Tue, 01 Jan 2030 00:00:00 GMT"'
        header_pending = 'ongoing-request="true"'
        self.assertTrue(restore.is_restore_ready(header_ready))
        self.assertFalse(restore.is_restore_ready(header_pending))
        self.assertFalse(restore.is_restore_ready(None))

    def test_restore_timeout_raises(self) -> None:
        client = FakeS3()
        client.objects["chunk.bin"] = b"payload"
        client.restore_headers["chunk.bin"] = ['ongoing-request="true"']
        chunk = restore.ChunkInfo(key="chunk.bin", sha256="x", size=None)

        time_state = {"now": 0.0}

        def time_fn() -> float:
            return time_state["now"]

        def sleep_fn(seconds: float) -> None:
            time_state["now"] += seconds

        with self.assertRaises(restore.RestoreError) as context:
            restore.ensure_chunks_restored(
                client,
                "bucket",
                [chunk],
                storage_class="GLACIER",
                restore_tier="Standard",
                timeout_seconds=2,
                sleep=sleep_fn,
                time_fn=time_fn,
            )
        self.assertIn("timeout", str(context.exception))

    def test_hash_mismatch_raises(self) -> None:
        client = FakeS3()
        client.objects["chunk.bin"] = b"payload"
        chunk = restore.ChunkInfo(key="chunk.bin", sha256="bad", size=None)
        output = io.BytesIO()

        with self.assertRaises(restore.RestoreError) as context:
            restore.download_and_verify_chunks(
                client, "bucket", [chunk], output
            )
        self.assertIn("hash mismatch", str(context.exception))

    def test_download_streams_and_verifies(self) -> None:
        class RecordingBody(FakeBody):
            def __init__(self, payload: bytes) -> None:
                super().__init__(payload)
                self.read_sizes: list[int | None] = []

            def read(self, size: int | None = None) -> bytes:
                self.read_sizes.append(size)
                return super().read(size)

        payload = b"streamed-payload"
        body = RecordingBody(payload)
        client = FakeS3()
        client.objects["chunk.bin"] = body
        chunk = restore.ChunkInfo(
            key="chunk.bin",
            sha256=hashlib.sha256(payload).hexdigest(),
            size=len(payload),
        )
        output = io.BytesIO()

        total_bytes = restore.download_and_verify_chunks(
            client,
            "bucket",
            [chunk],
            output,
            read_size=4,
        )

        self.assertEqual(output.getvalue(), payload)
        self.assertEqual(total_bytes, len(payload))
        self.assertTrue(all(size == 4 for size in body.read_sizes))
        self.assertGreater(len(body.read_sizes), 2)


class RestoreVerifyTests(unittest.TestCase):
    def test_verify_metadata_requires_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: 11111111-2222-3333-4444-555555555555\n",
                    stderr="",
                )

            restore.verify_metadata(target_path, runner=runner)

    def test_verify_metadata_invalid_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: not-a-uuid\n",
                    stderr="",
                )

            with self.assertRaises(restore.RestoreError) as context:
                restore.verify_metadata(target_path, runner=runner)
            self.assertIn("UUID", str(context.exception))

    def test_verify_content_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                Path(source_dir, "file.txt").write_text("data", encoding="utf-8")
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        Path(source_dir),
                        Path(target_dir),
                        mode="full",
                        sample_max_files=10,
                    )
                self.assertIn("missing file", str(context.exception))

    def test_verify_content_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                Path(source_dir, "file.txt").write_text("data", encoding="utf-8")
                Path(target_dir, "file.txt").write_text("tada", encoding="utf-8")
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        Path(source_dir),
                        Path(target_dir),
                        mode="full",
                        sample_max_files=10,
                    )
                self.assertIn("hash mismatch", str(context.exception))

    def test_select_sample_is_deterministic(self) -> None:
        sample = restore._select_sample(["b", "a", "c"], 2)
        self.assertEqual(sample, ["a", "b"])

    def test_verify_restore_missing_source_skips_content(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: 11111111-2222-3333-4444-555555555555\n",
                    stderr="",
                )

            restore.verify_restore(
                Path("/missing/source"),
                target_path,
                mode="full",
                sample_max_files=10,
                runner=runner,
            )


if __name__ == "__main__":
    unittest.main()
