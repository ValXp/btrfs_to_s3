"""S3 upload helpers with retries."""

from __future__ import annotations

import io
import random
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
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


@dataclass(frozen=True)
class SpoolPart:
    path: Path
    size: int


class S3Uploader:
    def __init__(
        self,
        client,
        bucket: str,
        storage_class: str,
        sse: str,
        part_size: int = 128 * 1024 * 1024,
        multipart_threshold: int = 5 * 1024 * 1024,
        concurrency: int = 1,
        spool_dir: Path | None = None,
        spool_size_bytes: int = 0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.storage_class = storage_class
        self.sse = sse
        self.part_size = part_size
        self.multipart_threshold = multipart_threshold
        self.concurrency = max(1, concurrency)
        self.spool_dir = spool_dir
        self.spool_size_bytes = spool_size_bytes
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
        use_spool = self._use_spool()
        if use_spool:
            part_size = min(part_size, self.spool_size_bytes)
            if part_size < MIN_PART_SIZE:
                raise UploadError("spool_size_bytes must be >= 5 MiB")
        response = self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            StorageClass=self.storage_class,
            ServerSideEncryption=self.sse,
        )
        upload_id = response["UploadId"]
        parts: dict[int, str] = {}
        total_size = 0
        max_in_flight = self._max_in_flight_parts(part_size, use_spool)
        in_flight: dict[object, tuple[int, bytes | SpoolPart]] = {}
        try:
            with ThreadPoolExecutor(max_workers=max_in_flight) as executor:
                part_iter: Iterator[bytes | SpoolPart]
                if use_spool:
                    part_iter = self._iter_spooled_parts(
                        stream, initial, part_size, self.spool_dir
                    )
                else:
                    part_iter = self._iter_parts(stream, initial, part_size)
                for part_number, part_data in enumerate(part_iter, start=1):
                    part_len = (
                        part_data.size
                        if isinstance(part_data, SpoolPart)
                        else len(part_data)
                    )
                    total_size += part_len
                    future = executor.submit(
                        self._upload_part_with_retry,
                        key,
                        upload_id,
                        part_number,
                        part_data,
                    )
                    in_flight[future] = (part_number, part_data)
                    if len(in_flight) >= max_in_flight:
                        self._drain_in_flight(in_flight, parts, use_spool)
                while in_flight:
                    self._drain_in_flight(in_flight, parts, use_spool)
        except Exception as exc:
            if use_spool:
                self._cleanup_in_flight(in_flight)
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
            MultipartUpload={"Parts": self._ordered_parts(parts)},
        )
        return UploadResult(key=key, size=total_size, etag=None)

    def _upload_part_with_retry(
        self,
        key: str,
        upload_id: str,
        part_number: int,
        data: bytes | SpoolPart,
    ) -> str:
        attempt = 0
        while True:
            attempt += 1
            try:
                if isinstance(data, SpoolPart):
                    with data.path.open("rb") as handle:
                        response = self.client.upload_part(
                            Bucket=self.bucket,
                            Key=key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=handle,
                        )
                else:
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
        close_body = False
        if getattr(stream, "seekable", None) and stream.seekable():
            size = self._copy_stream(stream)
            stream.seek(0)
            body = stream
        else:
            temp = tempfile.TemporaryFile()
            size = self._copy_stream(stream, temp)
            temp.seek(0)
            body = temp
            close_body = True
        try:
            response = self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                StorageClass=self.storage_class,
                ServerSideEncryption=self.sse,
            )
        finally:
            if close_body:
                body.close()
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

    def _copy_stream(
        self, stream: BinaryIO, destination: BinaryIO | None = None
    ) -> int:
        size = 0
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                break
            if destination is not None:
                destination.write(chunk)
            size += len(chunk)
        return size

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

    def _iter_spooled_parts(
        self,
        stream: BinaryIO,
        initial: bytes,
        part_size: int,
        spool_dir: Path | None,
    ) -> Iterator[SpoolPart]:
        if spool_dir is None:
            raise UploadError("spool_dir required when spooling")
        spool_dir.mkdir(parents=True, exist_ok=True)
        buffer = bytearray(initial)
        while True:
            if not buffer:
                data = stream.read(64 * 1024)
                if not data:
                    break
                buffer.extend(data)
            with tempfile.NamedTemporaryFile(
                dir=spool_dir, delete=False
            ) as handle:
                size = 0
                if buffer:
                    take = min(len(buffer), part_size)
                    handle.write(buffer[:take])
                    size += take
                    del buffer[:take]
                while size < part_size:
                    data = stream.read(min(8 * MiB, part_size - size))
                    if not data:
                        break
                    handle.write(data)
                    size += len(data)
                path = Path(handle.name)
            if size == 0:
                path.unlink(missing_ok=True)
                break
            yield SpoolPart(path=path, size=size)

    def _use_spool(self) -> bool:
        return self.spool_dir is not None and self.spool_size_bytes > 0

    def _max_in_flight_parts(self, part_size: int, use_spool: bool) -> int:
        if not use_spool:
            return self.concurrency
        limit = max(1, self.spool_size_bytes // max(part_size, 1))
        return max(1, min(self.concurrency, limit))

    def _ordered_parts(self, parts: dict[int, str]) -> list[dict[str, object]]:
        return [
            {"ETag": etag, "PartNumber": part_number}
            for part_number, etag in sorted(parts.items())
        ]

    def _drain_in_flight(
        self,
        in_flight: dict[object, tuple[int, bytes | SpoolPart]],
        parts: dict[int, str],
        use_spool: bool,
    ) -> None:
        done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            part_number, part_data = in_flight.pop(future)
            try:
                etag = future.result()
            except Exception:
                if use_spool:
                    self._cleanup_spool_part(part_data)
                raise
            parts[part_number] = etag
            if use_spool:
                self._cleanup_spool_part(part_data)

    def _cleanup_spool_part(self, part_data: bytes | SpoolPart) -> None:
        if isinstance(part_data, SpoolPart):
            part_data.path.unlink(missing_ok=True)

    def _cleanup_in_flight(
        self, in_flight: dict[object, tuple[int, bytes | SpoolPart]]
    ) -> None:
        for _part_number, part_data in in_flight.values():
            self._cleanup_spool_part(part_data)
