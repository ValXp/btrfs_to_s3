"""Uploader tests."""

from __future__ import annotations

import unittest
from unittest import mock

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


if __name__ == "__main__":
    unittest.main()
