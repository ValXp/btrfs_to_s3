"""AWS S3 helpers for the test harness."""

from __future__ import annotations

from typing import Any, Iterable
import boto3


def create_s3_client(region: str):
    """Create a boto3 S3 client."""
    return boto3.client("s3", region_name=region)


def head_object(client, bucket: str, key: str) -> dict[str, Any]:
    """Fetch object metadata from S3."""
    return client.head_object(Bucket=bucket, Key=key)


def get_object(client, bucket: str, key: str) -> dict[str, Any]:
    """Fetch an object from S3."""
    return client.get_object(Bucket=bucket, Key=key)


def read_object(client, bucket: str, key: str) -> bytes:
    """Return the object body as bytes."""
    response = get_object(client, bucket, key)
    body = response["Body"].read()
    return body


def list_objects(client, bucket: str, prefix: str) -> list[dict[str, Any]]:
    """List objects under a prefix, handling pagination."""
    objects: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        params: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        response = client.list_objects_v2(**params)
        objects.extend(response.get("Contents", []))
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
    return objects


def delete_objects(
    client,
    bucket: str,
    keys: Iterable[str],
) -> dict[str, Any]:
    """Delete objects by key and return aggregated results."""
    deleted: list[str] = []
    errors: list[dict[str, Any]] = []
    key_list = list(keys)
    for chunk in _chunked(key_list, 1000):
        response = client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": key} for key in chunk],
                "Quiet": True,
            },
        )
        deleted.extend(item["Key"] for item in response.get("Deleted", []))
        errors.extend(response.get("Errors", []))
    return {"deleted": deleted, "errors": errors}


def get_storage_class(head: dict[str, Any]) -> str:
    """Return storage class, defaulting to STANDARD when absent."""
    return head.get("StorageClass", "STANDARD")


def check_storage_and_sse(
    head: dict[str, Any],
    *,
    expected_storage_class: str | None = None,
    expected_sse: str | None = None,
) -> list[str]:
    """Validate storage class and SSE headers from head_object output."""
    errors: list[str] = []
    if expected_storage_class:
        actual_storage = get_storage_class(head)
        if actual_storage != expected_storage_class:
            errors.append(
                f"storage class {actual_storage!r} != {expected_storage_class!r}"
            )
    if expected_sse:
        actual_sse = head.get("ServerSideEncryption")
        if actual_sse != expected_sse:
            errors.append(f"sse {actual_sse!r} != {expected_sse!r}")
    return errors


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]
