"""Manifest loading and validation helpers."""

from __future__ import annotations

from typing import Any
import json
import os


DEFAULT_SCHEMA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "expected", "manifest_schema.json")
)
DEFAULT_FULL_SCHEMA_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        "expected",
        "manifest_schema_full.json",
    )
)

CURRENT_POINTER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["manifest_key", "kind", "created_at"],
    "properties": {
        "manifest_key": {"type": "string"},
        "kind": {"type": "string", "enum": ["full", "incremental"]},
        "created_at": {"type": "string"},
    },
}


def load_manifest(path: str) -> dict[str, Any]:
    """Load a manifest JSON file."""
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected JSON object")
    return data


def load_json_bytes(payload: bytes, label: str) -> dict[str, Any]:
    """Load JSON payload bytes into a dict."""
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError(f"{label}: expected JSON object")
    return data


def load_schema(path: str = DEFAULT_SCHEMA_PATH) -> dict[str, Any]:
    """Load the manifest schema."""
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected JSON object")
    return data


def validate_manifest(
    manifest: dict[str, Any],
    *,
    schema: dict[str, Any] | None = None,
) -> list[str]:
    """Validate manifest structure and chunk ordering."""
    errors: list[str] = []
    if schema is None:
        schema = load_schema()
    errors.extend(_validate_schema(manifest, schema, path="$"))
    errors.extend(_validate_chunks(manifest))
    return errors


def validate_current_pointer(pointer: dict[str, Any]) -> list[str]:
    """Validate current.json pointer payload."""
    return _validate_schema(pointer, CURRENT_POINTER_SCHEMA, path="$")


def _validate_chunks(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    chunks = manifest.get("chunks")
    if not isinstance(chunks, list):
        return errors
    has_index = any(
        isinstance(chunk, dict) and "index" in chunk for chunk in chunks
    )
    last_index: int | None = None
    for position, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            errors.append(f"chunks[{position}]: expected object")
            continue
        if has_index:
            index = chunk.get("index")
            if not isinstance(index, int):
                errors.append(f"chunks[{position}]: missing or invalid index")
            else:
                if last_index is not None and index <= last_index:
                    errors.append(
                        f"chunks[{position}]: index {index} not greater than {last_index}"
                    )
                last_index = index
        if not _has_hash(chunk):
            errors.append(f"chunks[{position}]: missing hash field")
    return errors


def _has_hash(chunk: dict[str, Any]) -> bool:
    for key in ("hash", "sha256", "checksum"):
        value = chunk.get(key)
        if isinstance(value, str) and value:
            return True
    return False


def _validate_schema(value: Any, schema: dict[str, Any], *, path: str) -> list[str]:
    errors: list[str] = []
    expected_type = schema.get("type")
    if expected_type:
        if not _matches_type(value, expected_type):
            errors.append(f"{path}: expected {expected_type}")
            return errors

    enum = schema.get("enum")
    if enum is not None and value not in enum:
        errors.append(f"{path}: value {value!r} not in {enum!r}")

    if expected_type == "object":
        if not isinstance(value, dict):
            return errors
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                errors.append(f"{path}: missing required key {key!r}")
        properties = schema.get("properties", {})
        for key, subschema in properties.items():
            if key in value:
                errors.extend(
                    _validate_schema(value[key], subschema, path=f"{path}.{key}")
                )
    elif expected_type == "array":
        if not isinstance(value, list):
            return errors
        min_items = schema.get("minItems")
        if isinstance(min_items, int) and len(value) < min_items:
            errors.append(f"{path}: expected at least {min_items} items")
        items_schema = schema.get("items")
        if isinstance(items_schema, dict):
            for index, item in enumerate(value):
                errors.extend(
                    _validate_schema(item, items_schema, path=f"{path}[{index}]")
                )
    elif expected_type == "integer":
        minimum = schema.get("minimum")
        if isinstance(minimum, int) and isinstance(value, int) and value < minimum:
            errors.append(f"{path}: expected >= {minimum}")

    return errors


def _matches_type(value: Any, expected: Any) -> bool:
    if isinstance(expected, list):
        return any(_matches_type(value, item) for item in expected)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int)
    if expected == "number":
        return isinstance(value, (int, float))
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    return True
