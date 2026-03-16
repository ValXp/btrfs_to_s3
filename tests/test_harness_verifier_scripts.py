"""Focused tests for backend-aware verifier scripts."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


manifest_lib = importlib.import_module("integration_tests.harness.manifest")
cleanup_s3_prefix = importlib.import_module(
    "integration_tests.scripts.cleanup_s3_prefix"
)
verify_manifest = importlib.import_module("integration_tests.scripts.verify_manifest")
verify_discovery = importlib.import_module(
    "integration_tests.scripts.verify_discovery"
)
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
    def test_main_loads_test_env_before_creating_s3_client(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config["zfs"]["source_datasets"] = ["data"]
            config_path = Path(tmpdir) / "test_zfs.toml"
            config_path.write_text("", encoding="utf-8")
            (Path(tmpdir) / "test.env").write_text(
                "AWS_ACCESS_KEY_ID=from-test-env\n"
                "AWS_SECRET_ACCESS_KEY=secret-from-test-env\n",
                encoding="utf-8",
            )
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
            }
            captured_env: dict[str, str] = {}

            def create_client(region: str) -> object:
                self.assertEqual(region, "us-east-1")
                captured_env["AWS_ACCESS_KEY_ID"] = os.environ.get(
                    "AWS_ACCESS_KEY_ID", ""
                )
                captured_env["AWS_SECRET_ACCESS_KEY"] = os.environ.get(
                    "AWS_SECRET_ACCESS_KEY", ""
                )
                return object()

            with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
                verify_manifest, "load_config", return_value=config
            ), mock.patch.object(
                verify_manifest, "open_log", return_value=log
            ), mock.patch.object(
                verify_manifest, "create_s3_client", side_effect=create_client
            ), mock.patch.object(
                verify_manifest,
                "read_object",
                side_effect=lambda client, bucket, key: payloads[key],
            ), mock.patch.object(
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
                ["verify_manifest.py", "--config", str(config_path)],
            ):
                result = verify_manifest.main()

        self.assertEqual(result, 0)
        self.assertEqual(captured_env["AWS_ACCESS_KEY_ID"], "from-test-env")
        self.assertEqual(
            captured_env["AWS_SECRET_ACCESS_KEY"], "secret-from-test-env"
        )

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


class VerifyDiscoveryScriptTests(unittest.TestCase):
    def test_main_validates_cli_discovery_output_against_s3_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            log = RecordingLog()
            current_payloads = {
                "prefix/subvol/tank/data/current.json": json.dumps(
                    {
                        "manifest_key": (
                            "prefix/subvol/tank/data/incremental/"
                            "manifest-20260314T010000Z.json"
                        ),
                        "kind": "incremental",
                        "created_at": "20260314T010000Z",
                    }
                ).encode("utf-8"),
                "prefix/subvol/tank/home/current.json": json.dumps(
                    {
                        "manifest_key": (
                            "prefix/subvol/tank/home/full/"
                            "manifest-20260314T000500Z.json"
                        ),
                        "kind": "full",
                        "created_at": "20260314T000500Z",
                    }
                ).encode("utf-8"),
            }
            objects_by_prefix = {
                "prefix/subvol/": [
                    {"Key": "prefix/subvol/tank/data/current.json"},
                    {"Key": "prefix/subvol/tank/home/current.json"},
                ],
                "prefix/subvol/tank/data/": [
                    {"Key": "prefix/subvol/tank/data/current.json"},
                    {
                        "Key": (
                            "prefix/subvol/tank/data/full/"
                            "manifest-20260314T000000Z.json"
                        )
                    },
                    {
                        "Key": (
                            "prefix/subvol/tank/data/incremental/"
                            "manifest-20260314T010000Z.json"
                        )
                    },
                ],
                "prefix/subvol/tank/home/": [
                    {"Key": "prefix/subvol/tank/home/current.json"},
                    {
                        "Key": (
                            "prefix/subvol/tank/home/full/"
                            "manifest-20260314T000500Z.json"
                        )
                    },
                ],
            }
            tool_results = [
                subprocess.CompletedProcess(
                    ["cmd"],
                    0,
                    json.dumps(
                        [
                            {
                                "source_name": "tank/data",
                                "current_key": "prefix/subvol/tank/data/current.json",
                                "manifest_key": (
                                    "prefix/subvol/tank/data/incremental/"
                                    "manifest-20260314T010000Z.json"
                                ),
                                "kind": "incremental",
                                "created_at": "20260314T010000Z",
                            },
                            {
                                "source_name": "tank/home",
                                "current_key": "prefix/subvol/tank/home/current.json",
                                "manifest_key": (
                                    "prefix/subvol/tank/home/full/"
                                    "manifest-20260314T000500Z.json"
                                ),
                                "kind": "full",
                                "created_at": "20260314T000500Z",
                            },
                        ]
                    ),
                    "",
                ),
                subprocess.CompletedProcess(
                    ["cmd"],
                    0,
                    json.dumps(
                        [
                            {
                                "source_name": "tank/data",
                                "key": (
                                    "prefix/subvol/tank/data/incremental/"
                                    "manifest-20260314T010000Z.json"
                                ),
                                "kind": "incremental",
                                "created_at": "20260314T010000Z",
                                "is_current": True,
                            },
                            {
                                "source_name": "tank/data",
                                "key": (
                                    "prefix/subvol/tank/data/full/"
                                    "manifest-20260314T000000Z.json"
                                ),
                                "kind": "full",
                                "created_at": "20260314T000000Z",
                                "is_current": False,
                            },
                        ]
                    ),
                    "",
                ),
                subprocess.CompletedProcess(
                    ["cmd"],
                    0,
                    json.dumps(
                        [
                            {
                                "source_name": "tank/home",
                                "key": (
                                    "prefix/subvol/tank/home/full/"
                                    "manifest-20260314T000500Z.json"
                                ),
                                "kind": "full",
                                "created_at": "20260314T000500Z",
                                "is_current": True,
                            },
                        ]
                    ),
                    "",
                ),
            ]

            def list_for_prefix(client, bucket, prefix):
                del client, bucket
                return list(objects_by_prefix[prefix])

            with mock.patch.object(
                verify_discovery, "load_config", return_value=config
            ), mock.patch.object(
                verify_discovery, "open_log", return_value=log
            ), mock.patch.object(
                verify_discovery, "create_s3_client", return_value=object()
            ), mock.patch.object(
                verify_discovery, "list_objects", side_effect=list_for_prefix
            ), mock.patch.object(
                verify_discovery,
                "read_object",
                side_effect=lambda client, bucket, key: current_payloads[key],
            ), mock.patch.object(
                verify_discovery,
                "run_tool",
                side_effect=tool_results,
            ) as run_tool_mock, mock.patch.object(
                verify_discovery.sys,
                "argv",
                ["verify_discovery.py", "--config", config_path],
            ):
                result = verify_discovery.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            run_tool_mock.call_args_list,
            [
                mock.call(config_path, ["list-sources"], dry_run=False),
                mock.call(
                    config_path,
                    ["list-manifests", "--source", "tank/data"],
                    dry_run=False,
                ),
                mock.call(
                    config_path,
                    ["list-manifests", "--source", "tank/home"],
                    dry_run=False,
                ),
            ],
        )


class FullManifestSchemaTests(unittest.TestCase):
    def test_full_schema_accepts_btrfs_and_zfs_snapshot_shapes(self) -> None:
        schema = manifest_lib.load_schema(manifest_lib.DEFAULT_FULL_SCHEMA_PATH)
        manifests = {
            "btrfs": {
                "version": 2,
                "filesystem": "btrfs",
                "subvolume": "data",
                "kind": "full",
                "created_at": "2026-03-14T00:00:00Z",
                "snapshot": {
                    "name": "data__20260314T000000Z__full",
                    "path": "/srv/snapshots/data__20260314T000000Z__full",
                    "identity": "/srv/snapshots/data__20260314T000000Z__full",
                    "uuid": None,
                    "parent_uuid": None,
                },
                "parent_manifest": None,
                "chunks": [
                    {
                        "key": "prefix/subvol/data/full/part-00000.bin",
                        "size": 10,
                        "sha256": "deadbeef",
                        "etag": "etag-1",
                    }
                ],
                "total_bytes": 10,
                "chunk_size": 10,
                "s3": {"storage_class": "STANDARD"},
            },
            "zfs": {
                "version": 2,
                "filesystem": "zfs",
                "subvolume": "tank/data",
                "kind": "incremental",
                "created_at": "2026-03-14T00:01:00Z",
                "snapshot": {
                    "name": "tank_x2f_data__20260314T000100Z__inc",
                    "path": None,
                    "identity": (
                        "tank/data@"
                        "btrfs-to-s3-tank_x2f_data__20260314T000100Z__inc"
                    ),
                    "uuid": None,
                    "parent_uuid": None,
                },
                "parent_manifest": "prefix/subvol/tank/data/full/manifest-1.json",
                "chunks": [
                    {
                        "key": "prefix/subvol/tank/data/inc/part-00000.bin",
                        "size": 5,
                        "sha256": "feedface",
                        "etag": None,
                    }
                ],
                "total_bytes": 5,
                "chunk_size": 5,
                "s3": {"storage_class": "STANDARD"},
            },
        }

        for backend, manifest in manifests.items():
            with self.subTest(backend=backend):
                self.assertEqual(
                    manifest_lib.validate_manifest(manifest, schema=schema),
                    [],
                )


class VerifyS3ScriptTests(unittest.TestCase):
    def test_main_does_not_scan_shared_parent_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _btrfs_config(tmpdir, prefix="btrfs-to-s3-test/")
            config_path = str(Path(tmpdir) / "test.toml")
            log = RecordingLog()
            listed_prefixes: list[str] = []
            objects_by_prefix = {
                "btrfs-to-s3-test/subvol/data/": [
                    {"Key": "btrfs-to-s3-test/subvol/data/current.json"},
                    {
                        "Key": (
                            "btrfs-to-s3-test/subvol/data/full/manifest-1.json"
                        )
                    },
                    {"Key": "btrfs-to-s3-test/subvol/data/full/part-00000.bin"},
                ],
                "btrfs-to-s3-test/subvol/root/": [
                    {"Key": "btrfs-to-s3-test/subvol/root/current.json"},
                    {
                        "Key": (
                            "btrfs-to-s3-test/subvol/root/full/manifest-1.json"
                        )
                    },
                    {"Key": "btrfs-to-s3-test/subvol/root/full/part-00000.bin"},
                ],
                "btrfs-to-s3-test/subvol/home/": [
                    {"Key": "btrfs-to-s3-test/subvol/home/current.json"},
                    {
                        "Key": (
                            "btrfs-to-s3-test/subvol/home/full/manifest-1.json"
                        )
                    },
                    {"Key": "btrfs-to-s3-test/subvol/home/full/part-00000.bin"},
                ],
            }
            metadata = {
                "btrfs-to-s3-test/subvol/data/current.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/data/full/manifest-1.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/data/full/part-00000.bin": {
                    "ContentLength": 11,
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/root/current.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/root/full/manifest-1.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/root/full/part-00000.bin": {
                    "ContentLength": 13,
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/home/current.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/home/full/manifest-1.json": {
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
                "btrfs-to-s3-test/subvol/home/full/part-00000.bin": {
                    "ContentLength": 17,
                    "StorageClass": "STANDARD",
                    "ServerSideEncryption": "AES256",
                },
            }
            payloads = {
                "btrfs-to-s3-test/subvol/data/current.json": json.dumps(
                    {
                        "manifest_key": (
                            "btrfs-to-s3-test/subvol/data/full/manifest-1.json"
                        ),
                        "kind": "full",
                        "created_at": "2026-03-14T00:00:00Z",
                    }
                ).encode("utf-8"),
                "btrfs-to-s3-test/subvol/data/full/manifest-1.json": json.dumps(
                    {
                        "chunks": [
                            {
                                "key": (
                                    "btrfs-to-s3-test/subvol/data/full/part-00000.bin"
                                ),
                                "size": 11,
                            }
                        ]
                    }
                ).encode("utf-8"),
                "btrfs-to-s3-test/subvol/root/current.json": json.dumps(
                    {
                        "manifest_key": (
                            "btrfs-to-s3-test/subvol/root/full/manifest-1.json"
                        ),
                        "kind": "full",
                        "created_at": "2026-03-14T00:01:00Z",
                    }
                ).encode("utf-8"),
                "btrfs-to-s3-test/subvol/root/full/manifest-1.json": json.dumps(
                    {
                        "chunks": [
                            {
                                "key": (
                                    "btrfs-to-s3-test/subvol/root/full/part-00000.bin"
                                ),
                                "size": 13,
                            }
                        ]
                    }
                ).encode("utf-8"),
                "btrfs-to-s3-test/subvol/home/current.json": json.dumps(
                    {
                        "manifest_key": (
                            "btrfs-to-s3-test/subvol/home/full/manifest-1.json"
                        ),
                        "kind": "full",
                        "created_at": "2026-03-14T00:02:00Z",
                    }
                ).encode("utf-8"),
                "btrfs-to-s3-test/subvol/home/full/manifest-1.json": json.dumps(
                    {
                        "chunks": [
                            {
                                "key": (
                                    "btrfs-to-s3-test/subvol/home/full/part-00000.bin"
                                ),
                                "size": 17,
                            }
                        ]
                    }
                ).encode("utf-8"),
            }

            def list_for_prefix(client, bucket, prefix):
                del client, bucket
                listed_prefixes.append(prefix)
                if prefix == "btrfs-to-s3-test/":
                    raise AssertionError("verify_s3 should not scan shared parent")
                return objects_by_prefix[prefix]

            with mock.patch.object(
                verify_s3, "load_config", return_value=config
            ), mock.patch.object(
                verify_s3, "open_log", return_value=log
            ), mock.patch.object(
                verify_s3, "create_s3_client", return_value=object()
            ), mock.patch.object(
                verify_s3, "list_objects", side_effect=list_for_prefix
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
        self.assertEqual(
            listed_prefixes,
            [
                "btrfs-to-s3-test/subvol/data/",
                "btrfs-to-s3-test/subvol/root/",
                "btrfs-to-s3-test/subvol/home/",
            ],
        )

    def test_main_accepts_zfs_dataset_keys_and_verifies_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config["aws"]["storage_class_manifest"] = "STANDARD"
            config["aws"]["storage_class_chunks"] = "STANDARD_IA"
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            log = RecordingLog()
            objects_by_prefix = {
                "prefix/subvol/tank/data/": [
                    {"Key": "prefix/subvol/tank/data/current.json"},
                    {"Key": "prefix/subvol/tank/data/full/manifest-1.json"},
                    {"Key": "prefix/subvol/tank/data/full/part-00000.bin"},
                ],
                "prefix/subvol/tank/home/": [
                    {"Key": "prefix/subvol/tank/home/current.json"},
                    {"Key": "prefix/subvol/tank/home/full/manifest-1.json"},
                    {"Key": "prefix/subvol/tank/home/full/part-00000.bin"},
                ],
            }
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
                verify_s3,
                "list_objects",
                side_effect=lambda client, bucket, prefix: objects_by_prefix[prefix],
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


class CleanupS3PrefixScriptTests(unittest.TestCase):
    def test_main_only_deletes_owned_source_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _btrfs_config(tmpdir, prefix="btrfs-to-s3-test/")
            config_path = str(Path(tmpdir) / "test.toml")
            log = RecordingLog()
            listed_prefixes: list[str] = []
            objects_by_prefix = {
                "btrfs-to-s3-test/subvol/data/": [
                    {"Key": "btrfs-to-s3-test/subvol/data/current.json"},
                ],
                "btrfs-to-s3-test/subvol/root/": [
                    {"Key": "btrfs-to-s3-test/subvol/root/current.json"},
                ],
                "btrfs-to-s3-test/subvol/home/": [
                    {"Key": "btrfs-to-s3-test/subvol/home/current.json"},
                ],
            }

            def list_for_prefix(client, bucket, prefix):
                del client, bucket
                listed_prefixes.append(prefix)
                if prefix == "btrfs-to-s3-test/":
                    raise AssertionError(
                        "cleanup_s3_prefix should not scan shared parent"
                    )
                return objects_by_prefix[prefix]

            with mock.patch.object(
                cleanup_s3_prefix, "load_config", return_value=config
            ), mock.patch.object(
                cleanup_s3_prefix, "open_log", return_value=log
            ), mock.patch.object(
                cleanup_s3_prefix, "create_s3_client", return_value=object()
            ), mock.patch.object(
                cleanup_s3_prefix, "list_objects", side_effect=list_for_prefix
            ), mock.patch.object(
                cleanup_s3_prefix,
                "delete_objects",
                return_value={
                    "deleted": [
                        "btrfs-to-s3-test/subvol/data/current.json",
                        "btrfs-to-s3-test/subvol/root/current.json",
                        "btrfs-to-s3-test/subvol/home/current.json",
                    ],
                    "errors": [],
                },
            ) as delete_objects_mock, mock.patch.object(
                cleanup_s3_prefix.sys,
                "argv",
                ["cleanup_s3_prefix.py", "--config", config_path, "--yes"],
            ):
                result = cleanup_s3_prefix.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            listed_prefixes,
            [
                "btrfs-to-s3-test/subvol/data/",
                "btrfs-to-s3-test/subvol/root/",
                "btrfs-to-s3-test/subvol/home/",
            ],
        )
        self.assertEqual(
            delete_objects_mock.call_args.args[2],
            [
                "btrfs-to-s3-test/subvol/data/current.json",
                "btrfs-to-s3-test/subvol/root/current.json",
                "btrfs-to-s3-test/subvol/home/current.json",
            ],
        )


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


def _btrfs_config(
    tmpdir: str,
    *,
    prefix: str = "prefix/",
) -> dict[str, object]:
    run_dir = Path(tmpdir) / "integration_tests" / "run"
    mount_dir = run_dir / "mnt"
    return {
        "paths": {
            "run_dir": str(run_dir),
            "logs_dir": str(run_dir / "logs"),
            "scratch_dir": str(run_dir / "scratch"),
            "lock_dir": str(run_dir / "lock"),
            "mount_dir": str(mount_dir),
            "data_dir": str(mount_dir / "data"),
            "snapshots_dir": str(mount_dir / "snapshots"),
        },
        "btrfs": {
            "subvolumes": ["data", "root", "home"],
        },
        "aws": {
            "region": "us-east-1",
            "bucket": "bucket",
            "prefix": prefix,
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
