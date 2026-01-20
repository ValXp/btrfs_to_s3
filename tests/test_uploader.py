"""Uploader tests."""

from __future__ import annotations

import io
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from btrfs_to_s3.uploader import RetryPolicy, S3Uploader, UploadError


class FakeClient:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[tuple[str, dict]] = []
        self.upload_calls = 0

    def create_multipart_upload(self, **kwargs):
        self.calls.append(("create_multipart_upload", kwargs))
        return {"UploadId": "upload-1"}

    def upload_part(self, **kwargs):
        self.calls.append(("upload_part", kwargs))
        self.upload_calls += 1
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("transient")
        return {"ETag": f"etag-{self.upload_calls}"}

    def complete_multipart_upload(self, **kwargs):
        self.calls.append(("complete_multipart_upload", kwargs))
        return {}

    def abort_multipart_upload(self, **kwargs):
        self.calls.append(("abort_multipart_upload", kwargs))
        return {}

    def put_object(self, **kwargs):
        self.calls.append(("put_object", kwargs))
        return {"ETag": "etag-put"}


class UploaderTests(unittest.TestCase):
    def test_upload_stream_empty_uses_put_object(self) -> None:
        class RecordingClient(FakeClient):
            def __init__(self) -> None:
                super().__init__()
                self.payload = None

            def put_object(self, **kwargs):
                body = kwargs["Body"]
                self.payload = body.read()
                self.calls.append(("put_object", kwargs))
                return {"ETag": "etag-put"}

        client = RecordingClient()
        uploader = S3Uploader(
            client=client,
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            multipart_threshold=50,
            retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
        )
        result = uploader.upload_stream("key", io.BytesIO(b""))
        self.assertEqual(result.size, 0)
        self.assertEqual(client.payload, b"")
        self.assertTrue(any(call[0] == "put_object" for call in client.calls))

    def test_upload_stream_small_uses_put_object(self) -> None:
        class RecordingClient(FakeClient):
            def __init__(self) -> None:
                super().__init__()
                self.payload = None

            def put_object(self, **kwargs):
                body = kwargs["Body"]
                self.payload = body.read()
                self.calls.append(("put_object", kwargs))
                return {"ETag": "etag-put"}

        client = RecordingClient()
        uploader = S3Uploader(
            client=client,
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            multipart_threshold=50,
            retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
        )
        payload = b"small-payload"
        result = uploader.upload_stream("key", io.BytesIO(payload))
        self.assertEqual(result.size, len(payload))
        self.assertEqual(client.payload, payload)
        self.assertTrue(any(call[0] == "put_object" for call in client.calls))

    def test_upload_stream_large_uses_multipart(self) -> None:
        client = FakeClient()
        payload = b"a" * (5 * 1024 * 1024 + 1)
        uploader = S3Uploader(
            client=client,
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            part_size=5 * 1024 * 1024,
            multipart_threshold=5 * 1024 * 1024,
            retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
        )
        result = uploader.upload_stream("key", io.BytesIO(payload))
        self.assertEqual(result.size, len(payload))
        upload_calls = [call for call in client.calls if call[0] == "upload_part"]
        sizes = [len(call[1]["Body"]) for call in upload_calls]
        self.assertEqual(sizes, [5 * 1024 * 1024, 1])

    def test_spooled_parts_require_dir(self) -> None:
        uploader = S3Uploader(
            client=FakeClient(),
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
        )
        with self.assertRaises(UploadError):
            next(uploader._iter_spooled_parts(io.BytesIO(b""), b"", 5, None))

    def test_spool_limits_in_flight_parts(self) -> None:
        uploader = S3Uploader(
            client=FakeClient(),
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            concurrency=4,
            spool_dir=Path("spool"),
            spool_size_bytes=9,
            retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
        )
        self.assertEqual(uploader._max_in_flight_parts(5, use_spool=True), 1)

    def test_multipart_retries_then_succeeds(self) -> None:
        client = FakeClient(failures=2)
        policy = RetryPolicy(max_attempts=5, sleep=lambda _: None, jitter=lambda d: d)
        uploader = S3Uploader(
            client=client,
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            part_size=4,
            multipart_threshold=5,
            retry_policy=policy,
        )
        result = uploader.upload_bytes("key", b"abcdefghij")
        self.assertEqual(result.size, 10)
        upload_calls = [call for call in client.calls if call[0] == "upload_part"]
        self.assertEqual(len(upload_calls), 5)
        self.assertTrue(
            any(call[0] == "complete_multipart_upload" for call in client.calls)
        )
        self.assertFalse(
            any(call[0] == "abort_multipart_upload" for call in client.calls)
        )

    def test_multipart_aborts_on_exhaustion(self) -> None:
        client = FakeClient(failures=10)
        policy = RetryPolicy(max_attempts=2, sleep=lambda _: None, jitter=lambda d: d)
        uploader = S3Uploader(
            client=client,
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            part_size=4,
            multipart_threshold=5,
            retry_policy=policy,
        )
        with self.assertRaises(UploadError):
            uploader.upload_bytes("key", b"abcdefghij")
        self.assertTrue(
            any(call[0] == "abort_multipart_upload" for call in client.calls)
        )

    def test_put_object_uses_sse(self) -> None:
        client = FakeClient()
        created: dict[str, int] = {}
        uploader = S3Uploader(
            client=client,
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            multipart_threshold=50,
            retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
        )
        uploader.upload_bytes("key", b"small")
        put_calls = [call for call in client.calls if call[0] == "put_object"]
        self.assertEqual(len(put_calls), 1)
        args = put_calls[0][1]
        self.assertEqual(args["ServerSideEncryption"], "AES256")
        self.assertEqual(args["StorageClass"], "STANDARD")

    def test_put_object_stream_handles_non_seekable(self) -> None:
        class NonSeekableStream:
            def __init__(self, payload: bytes) -> None:
                self._buffer = io.BytesIO(payload)

            def read(self, size: int = -1) -> bytes:
                return self._buffer.read(size)

            def seekable(self) -> bool:
                return False

        class RecordingClient:
            def __init__(self) -> None:
                self.payload = None

            def put_object(self, **kwargs):
                body = kwargs["Body"]
                self.payload = body.read()
                return {"ETag": "etag"}

        client = RecordingClient()
        uploader = S3Uploader(
            client=client,
            bucket="bucket",
            storage_class="STANDARD",
            sse="AES256",
            multipart_threshold=50,
            retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
        )
        payload = b"non-seekable"
        result = uploader._put_object_stream(
            "key", NonSeekableStream(payload)
        )
        self.assertEqual(result.size, len(payload))
        self.assertEqual(client.payload, payload)

    def test_multipart_part_size_is_capped(self) -> None:
        client = FakeClient()
        policy = RetryPolicy(max_attempts=2, sleep=lambda _: None, jitter=lambda d: d)
        with mock.patch("btrfs_to_s3.uploader.MAX_PART_SIZE", 5):
            uploader = S3Uploader(
                client=client,
                bucket="bucket",
                storage_class="STANDARD",
                sse="AES256",
                part_size=10,
                multipart_threshold=1,
                retry_policy=policy,
            )
            uploader.upload_bytes("key", b"abcdefghij")
        upload_calls = [call for call in client.calls if call[0] == "upload_part"]
        sizes = [len(call[1]["Body"]) for call in upload_calls]
        self.assertEqual(sizes, [5, 5])

    def test_concurrency_setting_is_used(self) -> None:
        client = FakeClient()
        created: dict[str, int] = {}

        class FakeFuture:
            def __init__(self, value: str) -> None:
                self._value = value

            def result(self) -> str:
                return self._value

        class FakeExecutor:
            created: dict[str, int] = {}

            def __init__(self, max_workers: int) -> None:
                self.created["max_workers"] = max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def submit(self, fn, *args, **kwargs):
                return FakeFuture(fn(*args, **kwargs))

        FakeExecutor.created = created

        def fake_wait(futures, return_when=None):
            return set(futures), set()

        with mock.patch(
            "btrfs_to_s3.uploader.ThreadPoolExecutor", FakeExecutor
        ), mock.patch(
            "btrfs_to_s3.uploader.wait", side_effect=fake_wait
        ):
            uploader = S3Uploader(
                client=client,
                bucket="bucket",
                storage_class="STANDARD",
                sse="AES256",
                part_size=4,
                multipart_threshold=5,
                concurrency=3,
                retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
            )
            uploader.upload_bytes("key", b"abcdefghij")
            self.assertEqual(FakeExecutor.created.get("max_workers"), 3)

    def test_spool_cleans_up_files(self) -> None:
        client = FakeClient()
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            uploader = S3Uploader(
                client=client,
                bucket="bucket",
                storage_class="STANDARD",
                sse="AES256",
                part_size=5 * 1024 * 1024,
                multipart_threshold=5,
                concurrency=2,
                spool_dir=spool_dir,
                spool_size_bytes=8 * 1024 * 1024,
                retry_policy=RetryPolicy(sleep=lambda _: None, jitter=lambda d: d),
            )
            uploader.upload_bytes("key", b"abcdefghij")
            self.assertEqual(list(spool_dir.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
