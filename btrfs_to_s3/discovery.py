"""S3-backed discovery helpers for restorable sources and manifests."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any

from botocore.exceptions import ClientError


_CURRENT_POINTER_SUFFIX = "/current.json"
_MANIFEST_NAME = re.compile(
    r"^(?P<kind>full|incremental)/manifest-(?P<created_at>.+)\.json$"
)


class DiscoveryError(RuntimeError):
    """Raised when S3-backed discovery fails."""


@dataclass(frozen=True)
class RestorableSource:
    source_name: str
    current_key: str
    manifest_key: str
    kind: str
    created_at: str

    def to_dict(self) -> dict[str, str]:
        return {
            "source_name": self.source_name,
            "current_key": self.current_key,
            "manifest_key": self.manifest_key,
            "kind": self.kind,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class ManifestListing:
    source_name: str
    key: str
    kind: str
    created_at: str
    is_current: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "key": self.key,
            "kind": self.kind,
            "created_at": self.created_at,
            "is_current": self.is_current,
        }


def has_aws_credentials() -> bool:
    try:
        import boto3
        from botocore.exceptions import BotoCoreError
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 operations") from exc

    try:
        return boto3.Session().get_credentials() is not None
    except BotoCoreError:
        return False


def get_s3_client(region: str):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 operations") from exc
    return boto3.client("s3", region_name=region)


def list_restorable_sources(
    client,
    bucket: str,
    prefix: str,
) -> list[RestorableSource]:
    root_prefix = f"{_normalize_prefix(prefix)}subvol/"
    sources: list[RestorableSource] = []
    for key in _list_keys(client, bucket, root_prefix):
        if not key.endswith(_CURRENT_POINTER_SUFFIX):
            continue
        source_name = _source_name_from_current_key(key, root_prefix)
        payload = _read_json_object(client, bucket, key)
        sources.append(
            RestorableSource(
                source_name=source_name,
                current_key=key,
                manifest_key=_require_string(payload, "manifest_key", key),
                kind=_require_kind(payload, key),
                created_at=_require_string(payload, "created_at", key),
            )
        )
    return sorted(sources, key=lambda item: item.source_name)


def list_available_manifests(
    client,
    bucket: str,
    prefix: str,
    source_name: str,
) -> list[ManifestListing]:
    normalized_prefix = _normalize_prefix(prefix)
    source_prefix = f"{normalized_prefix}subvol/{source_name}/"
    current_key = f"{source_prefix}current.json"
    current_manifest_key: str | None = None
    current_kind: str | None = None
    current_created_at: str | None = None

    try:
        current_payload = _read_json_object(client, bucket, current_key)
    except Exception as exc:
        if not _is_missing_key_error(exc):
            raise
    else:
        current_manifest_key = _require_string(
            current_payload, "manifest_key", current_key
        )
        current_kind = _require_kind(current_payload, current_key)
        current_created_at = _require_string(
            current_payload, "created_at", current_key
        )

    manifests: list[ManifestListing] = []
    listed_keys: set[str] = set()
    for key in _list_keys(client, bucket, source_prefix):
        if key == current_key:
            continue
        parsed = _parse_manifest_key(key, source_prefix)
        if parsed is None:
            continue
        listed_keys.add(key)
        manifests.append(
            ManifestListing(
                source_name=source_name,
                key=key,
                kind=parsed["kind"],
                created_at=parsed["created_at"],
                is_current=key == current_manifest_key,
            )
        )

    if (
        current_manifest_key is not None
        and current_manifest_key not in listed_keys
        and current_kind is not None
        and current_created_at is not None
    ):
        manifests.append(
            ManifestListing(
                source_name=source_name,
                key=current_manifest_key,
                kind=current_kind,
                created_at=current_created_at,
                is_current=True,
            )
        )

    return sorted(
        manifests,
        key=lambda item: (item.created_at, item.key),
        reverse=True,
    )


def _normalize_prefix(prefix: str) -> str:
    normalized = prefix.rstrip("/")
    return f"{normalized}/" if normalized else ""


def _list_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        params: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token is not None:
            params["ContinuationToken"] = token
        try:
            response = client.list_objects_v2(**params)
        except Exception as exc:
            raise DiscoveryError(
                f"failed to list objects under {prefix}: {exc}"
            ) from exc
        contents = response.get("Contents", [])
        if not isinstance(contents, list):
            raise DiscoveryError(f"{prefix}: list_objects_v2 returned invalid Contents")
        for item in contents:
            if not isinstance(item, dict):
                raise DiscoveryError(
                    f"{prefix}: list_objects_v2 returned invalid object entry"
                )
            key = item.get("Key")
            if isinstance(key, str) and key:
                keys.append(key)
        if not response.get("IsTruncated"):
            return keys
        token = response.get("NextContinuationToken")
        if not isinstance(token, str) or not token:
            raise DiscoveryError(
                f"{prefix}: truncated list_objects_v2 response missing continuation token"
            )


def _source_name_from_current_key(key: str, root_prefix: str) -> str:
    if not key.startswith(root_prefix) or not key.endswith(_CURRENT_POINTER_SUFFIX):
        raise DiscoveryError(f"unexpected current pointer key: {key}")
    source_name = key[len(root_prefix) : -len(_CURRENT_POINTER_SUFFIX)]
    if not source_name:
        raise DiscoveryError(f"invalid current pointer key: {key}")
    return source_name


def _read_json_object(client, bucket: str, key: str) -> dict[str, Any]:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if _is_missing_key_error(exc):
            raise
        raise DiscoveryError(f"failed to fetch {key}: {exc}") from exc
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise DiscoveryError(f"{key}: get_object response missing Body")
    try:
        payload = body.read()
    except Exception as exc:
        raise DiscoveryError(f"failed to read {key}: {exc}") from exc
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DiscoveryError(f"{key}: invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise DiscoveryError(f"{key}: expected JSON object")
    return data


def _require_string(payload: dict[str, Any], field: str, key: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise DiscoveryError(f"{key}: missing {field}")
    return value


def _require_kind(payload: dict[str, Any], key: str) -> str:
    kind = _require_string(payload, "kind", key)
    if kind not in {"full", "incremental"}:
        raise DiscoveryError(f"{key}: invalid kind {kind!r}")
    return kind


def _parse_manifest_key(
    key: str,
    source_prefix: str,
) -> dict[str, str] | None:
    if not key.startswith(source_prefix):
        return None
    remainder = key[len(source_prefix) :]
    match = _MANIFEST_NAME.fullmatch(remainder)
    if match is None:
        return None
    return {
        "kind": match.group("kind"),
        "created_at": match.group("created_at"),
    }


def _is_missing_key_error(exc: Exception) -> bool:
    if isinstance(exc, KeyError):
        return True
    if not isinstance(exc, ClientError):
        return False
    code = exc.response.get("Error", {}).get("Code")
    return code in {"NoSuchKey", "404", "NotFound"}
