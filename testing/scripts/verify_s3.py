"""Verify S3 object layout and metadata."""

from __future__ import annotations

import argparse
import os
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.aws import (
    check_storage_and_sse,
    create_s3_client,
    head_object,
    list_objects,
    read_object,
)
from harness.config import load_config
from harness.env import load_env
from harness.logs import open_log
from harness import manifest as manifest_lib


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify S3 objects.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    aws_cfg = config["aws"]

    env_path = os.path.join(os.path.dirname(config_path), "test.env")
    if os.path.exists(env_path):
        load_env(env_path, override=False)

    log_path = os.path.join(paths["logs_dir"], "verify_s3.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        client = create_s3_client(aws_cfg["region"])
        prefix = aws_cfg["prefix"]
        if prefix and not prefix.endswith("/"):
            prefix = f"{prefix}/"
        objects = list_objects(client, aws_cfg["bucket"], prefix)
        if not objects:
            log.write("no objects found under prefix", level="ERROR")
            return 1

        errors: list[str] = []
        metadata_cache: dict[str, dict[str, object]] = {}
        manifest_storage = aws_cfg.get("storage_class_manifest", aws_cfg["storage_class"])
        chunk_storage = aws_cfg.get("storage_class_chunks", aws_cfg["storage_class"])
        for obj in objects:
            key = obj["Key"]
            role, layout_error = _classify_key(key, prefix)
            if layout_error:
                errors.append(f"{key}: {layout_error}")
                continue
            expected_storage = (
                manifest_storage if role in ("manifest", "current") else chunk_storage
            )
            metadata = head_object(client, aws_cfg["bucket"], key)
            metadata_cache[key] = metadata
            errors.extend(
                f"{key}: {error}"
                for error in check_storage_and_sse(
                    metadata,
                    expected_storage_class=expected_storage,
                    expected_sse=aws_cfg["sse"],
                )
            )

        if errors:
            for error in errors:
                log.write(error, level="ERROR")
            return 1

        missing = _missing_subvolumes(objects, prefix, config["btrfs"])
        if missing:
            for name in missing:
                log.write(f"missing subvolume objects for {name}", level="ERROR")
            return 1

        chunk_errors = _verify_manifest_chunks(
            client,
            aws_cfg["bucket"],
            prefix,
            config["btrfs"],
            metadata_cache,
        )
        if chunk_errors:
            for error in chunk_errors:
                log.write(error, level="ERROR")
            return 1

        log.write(f"verified {len(objects)} objects under {prefix}")
        return 0


def _missing_subvolumes(
    objects: list[dict[str, object]],
    prefix: str,
    btrfs_cfg: dict[str, object],
) -> list[str]:
    subvolumes = btrfs_cfg.get("subvolumes", [])
    if not isinstance(subvolumes, list):
        return []
    normalized = prefix
    if normalized and not normalized.endswith("/"):
        normalized += "/"
    seen: set[str] = set()
    for obj in objects:
        key = obj.get("Key")
        if not isinstance(key, str):
            continue
        if normalized and not key.startswith(normalized):
            continue
        remainder = key[len(normalized) :] if normalized else key
        if not remainder.startswith("subvol/"):
            continue
        parts = remainder.split("/", 3)
        if len(parts) > 1 and parts[1]:
            seen.add(parts[1])
    return [name for name in subvolumes if name not in seen]


def _classify_key(key: str, prefix: str) -> tuple[str | None, str | None]:
    remainder = key[len(prefix) :] if prefix else key
    if not remainder.startswith("subvol/"):
        return None, "missing subvol prefix"
    parts = remainder.split("/")
    if len(parts) < 3:
        return None, "expected subvol/<name>/<kind|current.json>"
    if not parts[1]:
        return None, "missing subvolume name"
    if parts[2] == "current.json":
        if len(parts) != 3:
            return None, "current.json must be at subvol/<name>/current.json"
        return "current", None
    if parts[2] not in ("full", "incremental"):
        return None, "expected full or incremental segment"
    if len(parts) < 4 or not parts[-1]:
        return None, "missing object name"
    if parts[-1].endswith(".json"):
        if not parts[-1].startswith("manifest-"):
            return None, "unexpected json object name"
        return "manifest", None
    return "chunk", None


def _verify_manifest_chunks(
    client,
    bucket: str,
    prefix: str,
    btrfs_cfg: dict[str, object],
    metadata_cache: dict[str, dict[str, object]],
) -> list[str]:
    errors: list[str] = []
    subvolumes = btrfs_cfg.get("subvolumes", [])
    if not isinstance(subvolumes, list):
        return ["btrfs.subvolumes must be a list"]
    for subvolume in subvolumes:
        if not isinstance(subvolume, str) or not subvolume:
            errors.append("invalid subvolume name in config")
            continue
        current_key = f"{prefix}subvol/{subvolume}/current.json"
        try:
            current_payload = read_object(client, bucket, current_key)
            current = manifest_lib.load_json_bytes(current_payload, current_key)
        except Exception as exc:
            errors.append(f"{current_key}: fetch failed: {exc}")
            continue
        manifest_key = current.get("manifest_key")
        if not isinstance(manifest_key, str) or not manifest_key:
            errors.append(f"{current_key}: missing manifest_key")
            continue
        try:
            manifest_payload = read_object(client, bucket, manifest_key)
            manifest = manifest_lib.load_json_bytes(manifest_payload, manifest_key)
        except Exception as exc:
            errors.append(f"{manifest_key}: fetch failed: {exc}")
            continue
        chunks = manifest.get("chunks")
        if not isinstance(chunks, list):
            errors.append(f"{manifest_key}: missing chunks list")
            continue
        for index, chunk in enumerate(chunks):
            if not isinstance(chunk, dict):
                errors.append(f"{manifest_key}: chunk {index} not an object")
                continue
            chunk_key = chunk.get("key")
            size = chunk.get("size")
            if not isinstance(chunk_key, str) or not chunk_key:
                errors.append(f"{manifest_key}: chunk {index} missing key")
                continue
            if not isinstance(size, int):
                errors.append(f"{manifest_key}: chunk {index} missing size")
                continue
            metadata = metadata_cache.get(chunk_key)
            if metadata is None:
                try:
                    metadata = head_object(client, bucket, chunk_key)
                    metadata_cache[chunk_key] = metadata
                except Exception as exc:
                    errors.append(f"{chunk_key}: head failed: {exc}")
                    continue
            actual_size = metadata.get("ContentLength")
            if actual_size != size:
                errors.append(
                    f"{chunk_key}: size {actual_size} != manifest {size}"
                )
    return errors


if __name__ == "__main__":
    raise SystemExit(main())
