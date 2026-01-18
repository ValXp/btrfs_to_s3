"""S3 upload helpers with retries."""

from __future__ import annotations

import io
import random
import time
from dataclasses import dataclass
from typing import BinaryIO, Callable, Iterator

MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
MAX_PART_SIZE = 5 * GiB
MIN_PART_SIZE = 5 * MiB


class UploadError(RuntimeError):
    """Raised when an upload fails."""


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 30.0
    sleep: Callable[[float], None] = time.sleep
    jitter: Callable[[float], float] = lambda delay: delay + random.uniform(0, 0.5)


@dataclass(frozen=True)
class UploadResult:
    key: str
    size: int
    etag: str | None


class S3Uploader:
    def __init__(
        self,
        client,
        bucket: str,
        storage_class: str,
        sse: str,
        part_size: int = 128 * 1024 * 1024,
        multipart_threshold: int = 5 * 1024 * 1024,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.storage_class = storage_class
        self.sse = sse
        self.part_size = part_size
        self.multipart_threshold = multipart_threshold
        self.retry_policy = retry_policy or RetryPolicy()

    def upload_bytes(self, key: str, data: bytes) -> UploadResult:
        if len(data) >= self.multipart_threshold:
            stream = io.BytesIO(b"")
            return self._multipart_upload_stream(key, stream, data)
        response = self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            StorageClass=self.storage_class,
            ServerSideEncryption=self.sse,
        )
        return UploadResult(key=key, size=len(data), etag=response.get("ETag"))

    def upload_stream(self, key: str, stream: BinaryIO) -> UploadResult:
        part_size = self._effective_part_size()
        threshold = max(self.multipart_threshold, MIN_PART_SIZE)
        initial = self._read_until(stream, threshold + 1)
        if not initial:
            return self._put_object_stream(key, io.BytesIO(b""))
        if len(initial) <= threshold:
            return self._put_object_stream(key, io.BytesIO(initial))
        return self._multipart_upload_stream(key, stream, initial, part_size=part_size)

    def _multipart_upload_stream(
        self,
        key: str,
        stream: BinaryIO,
        initial: bytes,
        part_size: int | None = None,
    ) -> UploadResult:
        if part_size is None:
            part_size = self._effective_part_size()
        part_size = self._effective_part_size(part_size)
        response = self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            StorageClass=self.storage_class,
            ServerSideEncryption=self.sse,
        )
        upload_id = response["UploadId"]
        parts = []
        total_size = 0
        try:
            for part_number, part_data in enumerate(
                self._iter_parts(stream, initial, part_size), start=1
            ):
                etag = self._upload_part_with_retry(
                    key=key,
                    upload_id=upload_id,
                    part_number=part_number,
                    data=part_data,
                )
                parts.append({"ETag": etag, "PartNumber": part_number})
                total_size += len(part_data)
        except Exception as exc:
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
            )
            raise UploadError(f"multipart upload failed: {exc}") from exc
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        return UploadResult(key=key, size=total_size, etag=None)

    def _upload_part_with_retry(
        self, key: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self.client.upload_part(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=data,
                )
                return response["ETag"]
            except Exception as exc:
                if attempt >= self.retry_policy.max_attempts:
                    raise UploadError("retry attempts exhausted") from exc
                delay = min(
                    self.retry_policy.max_delay,
                    self.retry_policy.base_delay * (2 ** (attempt - 1)),
                )
                self.retry_policy.sleep(self.retry_policy.jitter(delay))

    def _put_object_stream(self, key: str, stream: BinaryIO) -> UploadResult:
        size = 0
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                break
            size += len(chunk)
        stream.seek(0)
        response = self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=stream,
            StorageClass=self.storage_class,
            ServerSideEncryption=self.sse,
        )
        return UploadResult(key=key, size=size, etag=response.get("ETag"))

    def _effective_part_size(self, part_size: int | None = None) -> int:
        if part_size is None:
            part_size = self.part_size
        return min(part_size, MAX_PART_SIZE)

    def _read_until(self, stream: BinaryIO, target: int) -> bytes:
        buffer = bytearray()
        while len(buffer) < target:
            data = stream.read(target - len(buffer))
            if not data:
                break
            buffer.extend(data)
        return bytes(buffer)

    def _iter_parts(
        self, stream: BinaryIO, initial: bytes, part_size: int
    ) -> Iterator[bytes]:
        buffer = bytearray(initial)
        while True:
            while len(buffer) < part_size:
                data = stream.read(part_size - len(buffer))
                if not data:
                    break
                buffer.extend(data)
            if not buffer:
                break
            if len(buffer) <= part_size:
                part = bytes(buffer)
                buffer.clear()
            else:
                part = bytes(buffer[:part_size])
                del buffer[:part_size]
            yield part
