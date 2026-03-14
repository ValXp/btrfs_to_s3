"""Tests for backend-aware harness application scripts."""

from __future__ import annotations

import importlib
import json
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
run_interrupt = importlib.import_module("integration_tests.scripts.run_interrupt")
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


class FakeInterruptProcess:
    def __init__(self) -> None:
        self.pid = 4242
        self.returncode = -15
        self.terminated = False

    def poll(self) -> None:
        return None

    def terminate(self) -> None:
        self.terminated = True

    def communicate(self, timeout: int | None = None) -> tuple[str, str]:
        del timeout
        return ("partial backup output", "interrupted for test")


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
            ) as open_log_mock, mock.patch.object(
                run_full.os, "makedirs"
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
        open_log_mock.assert_called_once_with(
            str(Path(config["paths"]["logs_dir"]) / "run_full.log")
        )
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
            ) as open_log_mock, mock.patch.object(
                run_incremental.os, "makedirs"
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
        open_log_mock.assert_called_once_with(
            str(Path(config["paths"]["logs_dir"]) / "run_incremental.log")
        )
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


class RunInterruptScriptTests(unittest.TestCase):
    def test_resolve_backup_args_defaults_to_first_zfs_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)

            backup_args = run_interrupt._resolve_backup_args(config, None)

        self.assertEqual(backup_args, ["backup", "--source", "tank/data"])

    def test_run_interrupt_uses_deterministic_zfs_source_and_reruns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            log = RecordingLog()
            process = FakeInterruptProcess()
            command = [
                "python3",
                "-m",
                "btrfs_to_s3",
                "backup",
                "--source",
                "tank/data",
            ]

            with mock.patch.object(
                run_interrupt, "load_config", return_value=config
            ), mock.patch.object(
                run_interrupt, "open_log", return_value=log
            ) as open_log_mock, mock.patch.object(
                run_interrupt.os, "makedirs"
            ), mock.patch.object(
                run_interrupt.runner,
                "build_command",
                return_value=command,
            ) as build_command_mock, mock.patch.object(
                run_interrupt.runner,
                "build_env",
                return_value={"PATH": os.environ.get("PATH", "")},
            ), mock.patch.object(
                run_interrupt.runner,
                "run_command",
                return_value=subprocess.CompletedProcess(command, 0, "rerun ok", ""),
            ) as run_command_mock, mock.patch.object(
                run_interrupt.time, "sleep"
            ), mock.patch.object(
                run_interrupt.subprocess, "Popen", return_value=process
            ), mock.patch.object(
                run_interrupt.sys,
                "argv",
                ["run_interrupt.py", "--config", config_path],
            ):
                result = run_interrupt.main()

        self.assertEqual(result, 0)
        open_log_mock.assert_called_once_with(
            str(Path(config["paths"]["logs_dir"]) / "run_interrupt.log")
        )
        build_command_mock.assert_called_once_with(
            config,
            config_path,
            ["backup", "--source", "tank/data"],
        )
        run_command_mock.assert_called_once()
        rerun_env = run_command_mock.call_args.kwargs["env"]
        self.assertEqual(
            rerun_env["BTRFS_TO_S3_HARNESS_RUN_DIR"],
            os.path.abspath(config["paths"]["run_dir"]),
        )
        self.assertTrue(process.terminated)
        self.assertIn(("INFO", "interrupting source tank/data"), log.entries)
        self.assertIn(("INFO", "interrupt exit code -15"), log.entries)


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

    def test_main_runs_zfs_restore_and_writes_restore_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            log = RecordingLog()

            with mock.patch.object(
                run_restore, "load_config", return_value=config
            ), mock.patch.object(
                run_restore, "open_log", return_value=log
            ) as open_log_mock, mock.patch.object(
                run_restore,
                "run_tool",
                return_value=subprocess.CompletedProcess(["cmd"], 0, "restore ok", ""),
            ) as run_tool_mock, mock.patch.object(
                run_restore.sys,
                "argv",
                ["run_restore.py", "--config", config_path, "--source", "tank/data"],
            ):
                result = run_restore.main()

            metadata_path = Path(config["paths"]["run_dir"]) / "restore_target.json"
            with metadata_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

        self.assertEqual(result, 0)
        open_log_mock.assert_called_once_with(
            str(Path(config["paths"]["logs_dir"]) / "run_restore.log")
        )
        called_args = run_tool_mock.call_args.args[1]
        self.assertEqual(called_args[0:3], ["restore", "--source", "tank/data"])
        self.assertIn("--target", called_args)
        target_path = called_args[called_args.index("--target") + 1]
        self.assertEqual(
            run_tool_mock.call_args,
            mock.call(
                config_path,
                ["restore", "--source", "tank/data", "--target", target_path],
                dry_run=False,
            ),
        )
        self.assertEqual(payload["source"], "tank/data")
        self.assertEqual(payload["target_path"], target_path)

    def test_main_passes_explicit_manifest_key_for_zfs_restore(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            manifest_key = "prefix/subvol/tank/data/incremental/manifest-2.json"
            log = RecordingLog()

            with mock.patch.object(
                run_restore, "load_config", return_value=config
            ), mock.patch.object(
                run_restore, "open_log", return_value=log
            ), mock.patch.object(
                run_restore,
                "run_tool",
                return_value=subprocess.CompletedProcess(["cmd"], 0, "restore ok", ""),
            ) as run_tool_mock, mock.patch.object(
                run_restore.sys,
                "argv",
                [
                    "run_restore.py",
                    "--config",
                    config_path,
                    "--source",
                    "tank/data",
                    "--manifest-key",
                    manifest_key,
                ],
            ):
                result = run_restore.main()

        self.assertEqual(result, 0)
        called_args = run_tool_mock.call_args.args[1]
        self.assertEqual(called_args[0:3], ["restore", "--source", "tank/data"])
        self.assertIn("--manifest-key", called_args)
        self.assertEqual(
            called_args[called_args.index("--manifest-key") + 1],
            manifest_key,
        )


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

    def test_main_verifies_zfs_restore_using_restore_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            config_path = str(Path(tmpdir) / "test_zfs.toml")
            mount_root = Path(config["zfs"]["mount_root"])
            snapshot_name = "probe-data__20260313T221500Z__full"
            source_dir = mount_root / "data" / ".zfs" / "snapshot" / snapshot_name
            target_dir = mount_root / "restore" / "data-target"
            source_dir.mkdir(parents=True)
            target_dir.mkdir(parents=True)
            (source_dir / "seed.txt").write_text("seed\n", encoding="utf-8")
            (target_dir / "seed.txt").write_text("seed\n", encoding="utf-8")

            metadata_path = Path(config["paths"]["run_dir"]) / verify_restore.RESTORE_TARGETS_FILE
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            metadata_path.write_text(
                json.dumps(
                    {
                        "targets": [
                            {
                                "source": "tank/data",
                                "target_path": str(target_dir),
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            log = RecordingLog()

            with mock.patch.object(
                verify_restore, "load_config", return_value=config
            ), mock.patch.object(
                verify_restore, "open_log", return_value=log
            ) as open_log_mock, mock.patch.object(
                verify_restore.zfs_harness,
                "list_snapshots",
                return_value=[f"tank/data@{snapshot_name}"],
            ), mock.patch.object(
                verify_restore, "_verify_metadata"
            ) as verify_metadata_mock, mock.patch.object(
                verify_restore.sys,
                "argv",
                ["verify_restore.py", "--config", config_path, "--source", "tank/data"],
            ):
                result = verify_restore.main()

        self.assertEqual(result, 0)
        open_log_mock.assert_called_once_with(
            str(Path(config["paths"]["logs_dir"]) / "verify_restore.log")
        )
        verify_metadata_mock.assert_called_once_with(config, str(target_dir))
        self.assertIn(("INFO", "restore verification passed"), log.entries)


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
