"""Focused tests for backend-aware verifier scripts."""

from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


verify_manifest = importlib.import_module("integration_tests.scripts.verify_manifest")
verify_retention = importlib.import_module("integration_tests.scripts.verify_retention")
verify_s3 = importlib.import_module("integration_tests.scripts.verify_s3")


class RecordingLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def __enter__(self) -> "RecordingLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write(self, message: str, *, level: str = "INFO") -> None:
        self.entries.append((level, message))


class VerifyManifestScriptTests(unittest.TestCase):
    def test_main_uses_zfs_sources_for_pointer_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            log = RecordingLog()
            payloads = {
                "prefix/subvol/tank/data/current.json": json.dumps(
                    {
                        "manifest_key": "prefix/subvol/tank/data/full/manifest-1.json",
                        "kind": "full",
                        "created_at": "2026-03-14T00:00:00Z",
                    }
                ).encode("utf-8"),
                "prefix/subvol/tank/data/full/manifest-1.json": b"{}",
                "prefix/subvol/tank/home/current.json": json.dumps(
                    {
                        "manifest_key": "prefix/subvol/tank/home/full/manifest-1.json",
                        "kind": "full",
                        "created_at": "2026-03-14T00:01:00Z",
                    }
                ).encode("utf-8"),
                "prefix/subvol/tank/home/full/manifest-1.json": b"{}",
            }

            with mock.patch.object(
                verify_manifest, "load_config", return_value=config
            ), mock.patch.object(
                verify_manifest, "open_log", return_value=log
            ), mock.patch.object(
                verify_manifest, "create_s3_client", return_value=object()
            ), mock.patch.object(
                verify_manifest,
                "read_object",
                side_effect=lambda client, bucket, key: payloads[key],
            ) as read_object_mock, mock.patch.object(
                verify_manifest.manifest_lib, "load_manifest", return_value={}
            ), mock.patch.object(
                verify_manifest.manifest_lib, "load_schema", return_value={}
            ), mock.patch.object(
                verify_manifest.manifest_lib, "validate_manifest", return_value=[]
            ), mock.patch.object(
                verify_manifest.manifest_lib,
                "validate_current_pointer",
                return_value=[],
            ), mock.patch.object(
                verify_manifest.sys,
                "argv",
                ["verify_manifest.py", "--config", config_path],
            ):
                result = verify_manifest.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            [call.args[2] for call in read_object_mock.call_args_list],
            [
                "prefix/subvol/tank/data/current.json",
                "prefix/subvol/tank/data/full/manifest-1.json",
                "prefix/subvol/tank/home/current.json",
                "prefix/subvol/tank/home/full/manifest-1.json",
            ],
        )


class VerifyS3ScriptTests(unittest.TestCase):
    def test_main_accepts_zfs_dataset_keys_and_verifies_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config["aws"]["storage_class_manifest"] = "STANDARD"
            config["aws"]["storage_class_chunks"] = "STANDARD_IA"
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            log = RecordingLog()
            objects = [
                {"Key": "prefix/subvol/tank/data/current.json"},
                {"Key": "prefix/subvol/tank/data/full/manifest-1.json"},
                {"Key": "prefix/subvol/tank/data/full/part-00000.bin"},
                {"Key": "prefix/subvol/tank/home/current.json"},
                {"Key": "prefix/subvol/tank/home/full/manifest-1.json"},
                {"Key": "prefix/subvol/tank/home/full/part-00000.bin"},
            ]
            metadata = {
                "prefix/subvol/tank/data/current.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "prefix/subvol/tank/data/full/manifest-1.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "prefix/subvol/tank/data/full/part-00000.bin": {
                    "ContentLength": 11,
                    "StorageClass": "STANDARD_IA",
                    "ServerSideEncryption": "AES256",
                },
                "prefix/subvol/tank/home/current.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "prefix/subvol/tank/home/full/manifest-1.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "prefix/subvol/tank/home/full/part-00000.bin": {
                    "ContentLength": 13,
                    "StorageClass": "STANDARD_IA",
                    "ServerSideEncryption": "AES256",
                },
            }
            payloads = {
                "prefix/subvol/tank/data/current.json": json.dumps(
                    {
                        "manifest_key": "prefix/subvol/tank/data/full/manifest-1.json",
                        "kind": "full",
                        "created_at": "2026-03-14T00:00:00Z",
                    }
                ).encode("utf-8"),
                "prefix/subvol/tank/data/full/manifest-1.json": json.dumps(
                    {
                        "chunks": [
                            {
                                "key": "prefix/subvol/tank/data/full/part-00000.bin",
                                "size": 11,
                            }
                        ]
                    }
                ).encode("utf-8"),
                "prefix/subvol/tank/home/current.json": json.dumps(
                    {
                        "manifest_key": "prefix/subvol/tank/home/full/manifest-1.json",
                        "kind": "full",
                        "created_at": "2026-03-14T00:01:00Z",
                    }
                ).encode("utf-8"),
                "prefix/subvol/tank/home/full/manifest-1.json": json.dumps(
                    {
                        "chunks": [
                            {
                                "key": "prefix/subvol/tank/home/full/part-00000.bin",
                                "size": 13,
                            }
                        ]
                    }
                ).encode("utf-8"),
            }

            with mock.patch.object(
                verify_s3, "load_config", return_value=config
            ), mock.patch.object(
                verify_s3, "open_log", return_value=log
            ), mock.patch.object(
                verify_s3, "create_s3_client", return_value=object()
            ), mock.patch.object(
                verify_s3, "list_objects", return_value=objects
            ), mock.patch.object(
                verify_s3,
                "head_object",
                side_effect=lambda client, bucket, key: metadata[key],
            ), mock.patch.object(
                verify_s3,
                "read_object",
                side_effect=lambda client, bucket, key: payloads[key],
            ), mock.patch.object(
                verify_s3.sys,
                "argv",
                ["verify_s3.py", "--config", config_path],
            ):
                result = verify_s3.main()

        self.assertEqual(result, 0)


class VerifyRetentionScriptTests(unittest.TestCase):
    def test_main_uses_zfs_snapshot_listing_for_each_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            log = RecordingLog()

            def list_snapshots(source: str) -> list[str]:
                return [
                    f"{source}@probe-20260314T000000Z__full",
                    f"{source}@probe-20260314T010000Z__inc",
                    f"{source}@manual-snapshot",
                ]

            with mock.patch.object(
                verify_retention, "load_config", return_value=config
            ), mock.patch.object(
                verify_retention, "open_log", return_value=log
            ), mock.patch.object(
                verify_retention.zfs, "list_snapshots", side_effect=list_snapshots
            ) as list_snapshots_mock, mock.patch.object(
                verify_retention.sys,
                "argv",
                ["verify_retention.py", "--config", config_path],
            ):
                result = verify_retention.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            [call.args[0] for call in list_snapshots_mock.call_args_list],
            ["tank/data", "tank/home"],
        )
        self.assertIn(("INFO", "subvolume tank/data snapshots=2"), log.entries)
        self.assertIn(("INFO", "subvolume tank/home snapshots=2"), log.entries)


def _zfs_config(tmpdir: str) -> dict[str, object]:
    run_dir = Path(tmpdir) / "integration_tests" / "run" / "zfs"
    return {
        "filesystem": {"backend": "zfs"},
        "paths": {
            "run_dir": str(run_dir),
            "logs_dir": str(run_dir / "logs"),
            "scratch_dir": str(run_dir / "scratch"),
            "lock_dir": str(run_dir / "lock"),
        },
        "zfs": {
            "pool_name": "tank",
            "pool_file": str(run_dir / "pool.img"),
            "pool_size_gib": 4,
            "mount_root": str(run_dir / "mnt"),
            "source_datasets": ["data", "home"],
            "receive_parent_dataset": "restore",
            "snapshot_prefix": "probe",
        },
        "aws": {
            "region": "us-east-1",
            "bucket": "bucket",
            "prefix": "prefix/",
            "storage_class": "STANDARD",
            "sse": "AES256",
        },
        "backup": {
            "chunk_size_mib": 64,
            "concurrency": 4,
            "retention_snapshots": 2,
        },
    }


if __name__ == "__main__":
    unittest.main()
