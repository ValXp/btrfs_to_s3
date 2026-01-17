"""Delete all objects under the configured S3 prefix."""

from __future__ import annotations

import argparse
import os
import sys

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.aws import create_s3_client, delete_objects, list_objects
from harness.config import load_config
from harness.env import load_env
from harness.logs import open_log


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Cleanup S3 prefix objects.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    if not args.yes:
        print("Refusing to delete objects without --yes.")
        return 1

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]
    aws_cfg = config["aws"]

    env_path = os.path.join(os.path.dirname(config_path), "test.env")
    if os.path.exists(env_path):
        load_env(env_path, override=False)

    log_path = os.path.join(paths["logs_dir"], "cleanup_s3_prefix.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        client = create_s3_client(aws_cfg["region"])
        objects = list_objects(client, aws_cfg["bucket"], aws_cfg["prefix"])
        if not objects:
            log.write("no objects to delete under prefix")
            return 0

        keys = [obj["Key"] for obj in objects]
        result = delete_objects(client, aws_cfg["bucket"], keys)
        deleted = result.get("deleted", [])
        errors = result.get("errors", [])

        log.write(f"requested delete for {len(keys)} objects")
        log.write(f"deleted {len(deleted)} objects")
        if errors:
            for error in errors:
                log.write(f"delete error: {error}", level="ERROR")
            return 1

        log.write("cleanup completed")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
