"""Tests for backend-aware harness application scripts."""

from __future__ import annotations

import importlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


harness_filesystem = importlib.import_module("integration_tests.harness.filesystem")
seed_data = importlib.import_module("integration_tests.scripts.seed_data")
run_full = importlib.import_module("integration_tests.scripts.run_full")
run_incremental = importlib.import_module("integration_tests.scripts.run_incremental")
run_restore = importlib.import_module("integration_tests.scripts.run_restore")
verify_restore = importlib.import_module("integration_tests.scripts.verify_restore")


class RecordingLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def __enter__(self) -> "RecordingLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write(self, message: str, *, level: str = "INFO") -> None:
        self.entries.append((level, message))


class HarnessFilesystemTests(unittest.TestCase):
    def test_zfs_source_specs_and_restore_base_are_backend_aware(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)

            specs = harness_filesystem.source_specs(config)
            restore_base = harness_filesystem.restore_base_dir(config)

        self.assertEqual(
            specs,
            [
                harness_filesystem.SourceSpec(
                    identifier="tank/data",
                    path=str(Path(tmpdir) / "integration_tests" / "run" / "zfs" / "mnt" / "data"),
                ),
                harness_filesystem.SourceSpec(
                    identifier="tank/home",
                    path=str(Path(tmpdir) / "integration_tests" / "run" / "zfs" / "mnt" / "home"),
                ),
            ],
        )
        self.assertEqual(
            restore_base,
            str(Path(tmpdir) / "integration_tests" / "run" / "zfs" / "mnt" / "restore"),
        )


class SeedDataScriptTests(unittest.TestCase):
    def test_seed_data_writes_into_zfs_mount_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            mount_root = Path(config["zfs"]["mount_root"])
            (mount_root / "data").mkdir(parents=True)
            (mount_root / "home").mkdir(parents=True)
            log = RecordingLog()

            with mock.patch.object(
                seed_data, "load_config", return_value=config
            ), mock.patch.object(
                seed_data, "open_log", return_value=log
            ), mock.patch.object(
                seed_data.sys,
                "argv",
                ["seed_data.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = seed_data.main()
                self.assertEqual(result, 0)
                self.assertTrue((mount_root / "data" / "seed.txt").exists())
                self.assertTrue((mount_root / "home" / "seed.txt").exists())


class RunFullScriptTests(unittest.TestCase):
    def test_run_full_uses_source_flag_for_zfs_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            log = RecordingLog()

            with mock.patch.object(
                run_full, "load_config", return_value=config
            ), mock.patch.object(
                run_full, "open_log", return_value=log
            ), mock.patch.object(
                run_full,
                "run_tool",
                return_value=subprocess.CompletedProcess(["cmd"], 0, "", ""),
            ) as run_tool_mock, mock.patch.object(
                run_full.sys,
                "argv",
                ["run_full.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = run_full.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            run_tool_mock.call_args_list,
            [
                mock.call(
                    str(Path(tmpdir) / "test_zfs.toml"),
                    ["backup", "--source", "tank/data"],
                    dry_run=False,
                ),
                mock.call(
                    str(Path(tmpdir) / "test_zfs.toml"),
                    ["backup", "--source", "tank/home"],
                    dry_run=False,
                ),
            ],
        )


class RunIncrementalScriptTests(unittest.TestCase):
    def test_run_incremental_uses_source_flag_for_zfs_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            log = RecordingLog()

            with mock.patch.object(
                run_incremental, "load_config", return_value=config
            ), mock.patch.object(
                run_incremental, "open_log", return_value=log
            ), mock.patch.object(
                run_incremental,
                "run_tool",
                return_value=subprocess.CompletedProcess(["cmd"], 0, "", ""),
            ) as run_tool_mock, mock.patch.object(
                run_incremental.sys,
                "argv",
                [
                    "run_incremental.py",
                    "--config",
                    str(Path(tmpdir) / "test_zfs.toml"),
                    "--skip-mutate",
                ],
            ):
                result = run_incremental.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            run_tool_mock.call_args_list,
            [
                mock.call(
                    str(Path(tmpdir) / "test_zfs.toml"),
                    ["backup", "--once", "--source", "tank/data"],
                    dry_run=False,
                ),
                mock.call(
                    str(Path(tmpdir) / "test_zfs.toml"),
                    ["backup", "--once", "--source", "tank/home"],
                    dry_run=False,
                ),
            ],
        )


class RunRestoreScriptTests(unittest.TestCase):
    def test_resolve_target_path_uses_zfs_restore_base(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            args = SimpleNamespace(target=None, target_base=None, target_name=None)

            target_path = run_restore._resolve_target_path(args, config, "tank/data")

        self.assertTrue(
            target_path.startswith(
                str(Path(tmpdir) / "integration_tests" / "run" / "zfs" / "mnt" / "restore")
            )
        )
        self.assertIn("tank__data__restore__", os.path.basename(target_path))


class VerifyRestoreScriptTests(unittest.TestCase):
    def test_resolve_zfs_source_snapshot_uses_dot_zfs_snapshot_mount(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            mount_root = Path(config["zfs"]["mount_root"])
            snapshot_name = "probe-data__20260313T221500Z__full"
            snapshot_dir = mount_root / "data" / ".zfs" / "snapshot" / snapshot_name
            snapshot_dir.mkdir(parents=True)

            with mock.patch.object(
                verify_restore.zfs_harness,
                "list_snapshots",
                return_value=[f"tank/data@{snapshot_name}"],
            ):
                result = verify_restore._resolve_source_snapshot(
                    None,
                    config,
                    "tank/data",
                )

        self.assertEqual(result, str(snapshot_dir))

    def test_verify_metadata_checks_zfs_dataset_properties(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            target_path = (
                Path(config["zfs"]["mount_root"]) / "restore" / "data-target"
            )
            target_path.mkdir(parents=True)

            property_values = {
                "type": "filesystem",
                "mounted": "yes",
                "mountpoint": str(target_path),
                "readonly": "off",
            }

            with mock.patch.object(
                verify_restore.zfs_harness,
                "get_dataset_property",
                side_effect=lambda dataset, name: property_values[name],
            ) as property_mock:
                verify_restore._verify_metadata(config, str(target_path))

        self.assertEqual(
            property_mock.call_args_list,
            [
                mock.call("tank/restore/data-target", "type"),
                mock.call("tank/restore/data-target", "mounted"),
                mock.call("tank/restore/data-target", "mountpoint"),
                mock.call("tank/restore/data-target", "readonly"),
            ],
        )


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
