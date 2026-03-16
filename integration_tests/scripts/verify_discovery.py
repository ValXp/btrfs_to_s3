"""Verify S3-backed discovery commands against raw S3 state."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.aws import create_s3_client, list_objects, read_object
from harness.config import load_config
from harness.env import load_env
from harness.filesystem import normalize_s3_prefix
from harness.logs import open_log
from harness.runner import run_tool
from harness import manifest as manifest_lib


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify S3-backed list-sources/list-manifests commands."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    aws_cfg = config["aws"]

    env_path = os.path.join(os.path.dirname(config_path), "test.env")
    if os.path.exists(env_path):
        load_env(env_path, override=False)

    log_path = os.path.join(paths["logs_dir"], "verify_discovery.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        client = create_s3_client(aws_cfg["region"])
        prefix = normalize_s3_prefix(aws_cfg["prefix"])
        try:
            expected_sources = _expected_sources(client, aws_cfg["bucket"], prefix)
            actual_sources = _run_list_sources(config_path)
        except (ValueError, subprocess.CalledProcessError) as exc:
            log.write(f"list-sources failed: {exc}", level="ERROR")
            return 1

        log.write(f"list-sources output: {json.dumps(actual_sources, sort_keys=True)}")
        if actual_sources != expected_sources:
            log.write(
                "list-sources output mismatch: "
                f"expected={json.dumps(expected_sources, sort_keys=True)} "
                f"actual={json.dumps(actual_sources, sort_keys=True)}",
                level="ERROR",
            )
            return 1

        current_map = {
            entry["source_name"]: entry
            for entry in expected_sources
        }
        for source in actual_sources:
            source_name = source["source_name"]
            try:
                expected_manifests = _expected_manifests(
                    client,
                    aws_cfg["bucket"],
                    prefix,
                    current_map[source_name],
                )
                actual_manifests = _run_list_manifests(config_path, source_name)
            except (ValueError, subprocess.CalledProcessError) as exc:
                log.write(
                    f"list-manifests failed for {source_name}: {exc}",
                    level="ERROR",
                )
                return 1

            log.write(
                f"list-manifests[{source_name}] output: "
                f"{json.dumps(actual_manifests, sort_keys=True)}"
            )
            if actual_manifests != expected_manifests:
                log.write(
                    f"list-manifests output mismatch for {source_name}: "
                    f"expected={json.dumps(expected_manifests, sort_keys=True)} "
                    f"actual={json.dumps(actual_manifests, sort_keys=True)}",
                    level="ERROR",
                )
                return 1

        log.write(f"verified discovery for {len(actual_sources)} sources")
        return 0


def _run_list_sources(config_path: str) -> list[dict[str, object]]:
    result = run_tool(config_path, ["list-sources"], dry_run=False)
    if result is None:
        raise ValueError("list-sources returned no result")
    return _normalize_sources_output(_load_json_output(result.stdout, "list-sources"))


def _run_list_manifests(
    config_path: str,
    source_name: str,
) -> list[dict[str, object]]:
    result = run_tool(
        config_path,
        ["list-manifests", "--source", source_name],
        dry_run=False,
    )
    if result is None:
        raise ValueError("list-manifests returned no result")
    return _normalize_manifests_output(
        _load_json_output(result.stdout, f"list-manifests[{source_name}]")
    )


def _load_json_output(stdout: str, label: str) -> list[object]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} produced invalid JSON: {exc}") from exc
    if not isinstance(payload, list):
        raise ValueError(f"{label} did not produce a JSON array")
    return payload


def _expected_sources(
    client,
    bucket: str,
    prefix: str,
) -> list[dict[str, object]]:
    root_prefix = f"{prefix}subvol/"
    expected: list[dict[str, object]] = []
    for obj in list_objects(client, bucket, root_prefix):
        key = obj.get("Key")
        if not isinstance(key, str) or not key.endswith("/current.json"):
            continue
        source_name = key[len(root_prefix) : -len("/current.json")]
        if not source_name:
            raise ValueError(f"invalid current pointer key {key!r}")
        current = manifest_lib.load_json_bytes(read_object(client, bucket, key), key)
        expected.append(
            {
                "source_name": source_name,
                "current_key": key,
                "manifest_key": _require_str(current, "manifest_key", key),
                "kind": _require_kind(current, key),
                "created_at": _require_str(current, "created_at", key),
            }
        )
    return sorted(expected, key=lambda item: str(item["source_name"]))


def _expected_manifests(
    client,
    bucket: str,
    prefix: str,
    current: dict[str, object],
) -> list[dict[str, object]]:
    source_name = str(current["source_name"])
    source_prefix = f"{prefix}subvol/{source_name}/"
    current_key = f"{source_prefix}current.json"
    current_manifest_key = str(current["manifest_key"])
    current_kind = str(current["kind"])
    current_created_at = str(current["created_at"])

    expected: list[dict[str, object]] = []
    listed_keys: set[str] = set()
    for obj in list_objects(client, bucket, source_prefix):
        key = obj.get("Key")
        if not isinstance(key, str) or key == current_key:
            continue
        parsed = _parse_manifest_key(key, source_prefix)
        if parsed is None:
            continue
        listed_keys.add(key)
        expected.append(
            {
                "source_name": source_name,
                "key": key,
                "kind": parsed["kind"],
                "created_at": parsed["created_at"],
                "is_current": key == current_manifest_key,
            }
        )
    if current_manifest_key not in listed_keys:
        expected.append(
            {
                "source_name": source_name,
                "key": current_manifest_key,
                "kind": current_kind,
                "created_at": current_created_at,
                "is_current": True,
            }
        )
    return sorted(
        expected,
        key=lambda item: (str(item["created_at"]), str(item["key"])),
        reverse=True,
    )


def _normalize_sources_output(payload: list[object]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for index, entry in enumerate(payload):
        if not isinstance(entry, dict):
            raise ValueError(f"list-sources entry {index} is not an object")
        key = f"list-sources[{index}]"
        normalized.append(
            {
                "source_name": _require_str(entry, "source_name", key),
                "current_key": _require_str(entry, "current_key", key),
                "manifest_key": _require_str(entry, "manifest_key", key),
                "kind": _require_kind(entry, key),
                "created_at": _require_str(entry, "created_at", key),
            }
        )
    return sorted(normalized, key=lambda item: str(item["source_name"]))


def _normalize_manifests_output(payload: list[object]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for index, entry in enumerate(payload):
        if not isinstance(entry, dict):
            raise ValueError(f"list-manifests entry {index} is not an object")
        key = f"list-manifests[{index}]"
        is_current = entry.get("is_current")
        if not isinstance(is_current, bool):
            raise ValueError(f"{key}: missing is_current")
        normalized.append(
            {
                "source_name": _require_str(entry, "source_name", key),
                "key": _require_str(entry, "key", key),
                "kind": _require_kind(entry, key),
                "created_at": _require_str(entry, "created_at", key),
                "is_current": is_current,
            }
        )
    return sorted(
        normalized,
        key=lambda item: (str(item["created_at"]), str(item["key"])),
        reverse=True,
    )


def _require_str(payload: dict[str, object], field: str, label: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label}: missing {field}")
    return value


def _require_kind(payload: dict[str, object], label: str) -> str:
    kind = _require_str(payload, "kind", label)
    if kind not in {"full", "incremental"}:
        raise ValueError(f"{label}: invalid kind {kind!r}")
    return kind


def _parse_manifest_key(
    key: str,
    source_prefix: str,
) -> dict[str, str] | None:
    if not key.startswith(source_prefix):
        return None
    remainder = key[len(source_prefix) :]
    parts = remainder.split("/", 1)
    if len(parts) != 2:
        return None
    kind, filename = parts
    if kind not in {"full", "incremental"}:
        return None
    if not filename.startswith("manifest-") or not filename.endswith(".json"):
        return None
    created_at = filename[len("manifest-") : -len(".json")]
    if not created_at:
        return None
    return {"kind": kind, "created_at": created_at}


if __name__ == "__main__":
    raise SystemExit(main())
