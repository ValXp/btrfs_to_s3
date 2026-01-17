"""Verify S3 object layout and metadata."""

from __future__ import annotations

import argparse
import os
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.aws import check_storage_and_sse, create_s3_client, head_object, list_objects
from harness.config import load_config
from harness.env import load_env
from harness.logs import open_log


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
        objects = list_objects(client, aws_cfg["bucket"], aws_cfg["prefix"])
        if not objects:
            log.write("no objects found under prefix", level="ERROR")
            return 1

        errors: list[str] = []
        for obj in objects:
            key = obj["Key"]
            metadata = head_object(client, aws_cfg["bucket"], key)
            errors.extend(
                f"{key}: {error}"
                for error in check_storage_and_sse(
                    metadata,
                    expected_storage_class=aws_cfg["storage_class"],
                    expected_sse=aws_cfg["sse"],
                )
            )

        if errors:
            for error in errors:
                log.write(error, level="ERROR")
            return 1

        log.write(f"verified {len(objects)} objects under {aws_cfg['prefix']}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
