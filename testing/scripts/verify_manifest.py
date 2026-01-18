"""Validate a manifest JSON file."""

from __future__ import annotations

import argparse
import os
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.aws import create_s3_client, read_object
from harness.config import load_config
from harness.logs import open_log
from harness import manifest as manifest_lib


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify manifest JSON.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--schema", default=manifest_lib.DEFAULT_SCHEMA_PATH)
    parser.add_argument(
        "--s3-schema", default=manifest_lib.DEFAULT_FULL_SCHEMA_PATH
    )
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    manifest_path = args.manifest
    if manifest_path is None:
        manifest_path = os.path.join(paths["run_dir"], "manifest.json")
    manifest_path = os.path.abspath(manifest_path)
    schema_path = os.path.abspath(args.schema)
    s3_schema_path = os.path.abspath(args.s3_schema)

    log_path = os.path.join(paths["logs_dir"], "verify_manifest.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        log.write(f"validating manifest {manifest_path}")
        try:
            manifest = manifest_lib.load_manifest(manifest_path)
            schema = manifest_lib.load_schema(schema_path)
            errors = manifest_lib.validate_manifest(manifest, schema=schema)
        except Exception as exc:
            log.write(f"validation failed: {exc}", level="ERROR")
            return 1

        if errors:
            for error in errors:
                log.write(error, level="ERROR")
            return 1

        aws_cfg = config["aws"]
        client = create_s3_client(aws_cfg["region"])
        prefix = aws_cfg.get("prefix", "")
        if prefix and not prefix.endswith("/"):
            prefix = f"{prefix}/"
        s3_schema = manifest_lib.load_schema(s3_schema_path)
        subvolumes = config["btrfs"]["subvolumes"]
        for subvolume in subvolumes:
            current_key = f"{prefix}subvol/{subvolume}/current.json"
            log.write(f"fetching current pointer {current_key}")
            try:
                current_payload = read_object(
                    client, aws_cfg["bucket"], current_key
                )
                current = manifest_lib.load_json_bytes(
                    current_payload, current_key
                )
            except Exception as exc:
                log.write(f"current pointer fetch failed: {exc}", level="ERROR")
                return 1

            pointer_errors = manifest_lib.validate_current_pointer(current)
            if pointer_errors:
                for error in pointer_errors:
                    log.write(f"{current_key}: {error}", level="ERROR")
                return 1

            manifest_key = current.get("manifest_key")
            if not isinstance(manifest_key, str) or not manifest_key:
                log.write(
                    f"{current_key}: missing manifest_key",
                    level="ERROR",
                )
                return 1

            log.write(f"fetching manifest {manifest_key}")
            try:
                payload = read_object(
                    client, aws_cfg["bucket"], manifest_key
                )
                s3_manifest = manifest_lib.load_json_bytes(
                    payload, manifest_key
                )
                s3_errors = manifest_lib.validate_manifest(
                    s3_manifest, schema=s3_schema
                )
            except Exception as exc:
                log.write(f"s3 manifest validation failed: {exc}", level="ERROR")
                return 1

            if s3_errors:
                for error in s3_errors:
                    log.write(f"{manifest_key}: {error}", level="ERROR")
                return 1

        log.write("manifest validation passed")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
