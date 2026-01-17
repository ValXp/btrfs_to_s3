"""S3 upload helpers with retries."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable


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
            return self._multipart_upload(key, data)
        response = self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            StorageClass=self.storage_class,
            ServerSideEncryption=self.sse,
        )
        return UploadResult(key=key, size=len(data), etag=response.get("ETag"))

    def _multipart_upload(self, key: str, data: bytes) -> UploadResult:
        response = self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            StorageClass=self.storage_class,
            ServerSideEncryption=self.sse,
        )
        upload_id = response["UploadId"]
        parts = []
        try:
            for part_number, offset in enumerate(
                range(0, len(data), self.part_size), start=1
            ):
                part_data = data[offset : offset + self.part_size]
                etag = self._upload_part_with_retry(
                    key=key,
                    upload_id=upload_id,
                    part_number=part_number,
                    data=part_data,
                )
                parts.append({"ETag": etag, "PartNumber": part_number})
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
        return UploadResult(key=key, size=len(data), etag=None)

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
