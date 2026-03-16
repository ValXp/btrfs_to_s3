"""Tests for backend-aware harness runner and orchestration."""

from __future__ import annotations

import importlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock


runner = importlib.import_module("integration_tests.harness.runner")
run_all = importlib.import_module("integration_tests.scripts.run_all")


class RecordingLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def __enter__(self) -> "RecordingLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write(self, message: str, *, level: str = "INFO") -> None:
        self.entries.append((level, message))


class HarnessRunnerTests(unittest.TestCase):
    def test_render_btrfs_tool_config_preserves_btrfs_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _btrfs_config(tmpdir)

            rendered = runner._render_tool_config(config)

        mount_dir = Path(config["paths"]["mount_dir"])
        snapshots_dir = Path(config["paths"]["snapshots_dir"])
        self.assertIn('[filesystem]\nbackend = "btrfs"', rendered)
        self.assertIn(
            f'[snapshots]\nbase_dir = "{snapshots_dir}"\nretain = 2',
            rendered,
        )
        self.assertIn(
            f'paths = ["{mount_dir / "data"}", "{mount_dir / "home"}"]',
            rendered,
        )
        self.assertIn(
            f'target_base_dir = "{mount_dir / "restore"}"',
            rendered,
        )
        self.assertNotIn("[zfs]", rendered)

    def test_render_btrfs_restore_only_tool_config_omits_subvolumes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _btrfs_config(tmpdir)

            rendered = runner._render_tool_config(config, restore_only=True)

        self.assertIn('[filesystem]\nbackend = "btrfs"', rendered)
        self.assertIn("[snapshots]", rendered)
        self.assertNotIn("[subvolumes]", rendered)

    def test_render_zfs_tool_config_uses_pool_qualified_datasets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)

            rendered = runner._render_tool_config(config)

        mount_root = Path(config["zfs"]["mount_root"])
        self.assertIn('[filesystem]\nbackend = "zfs"', rendered)
        self.assertIn('[snapshots]\nretain = 2', rendered)
        self.assertIn("[zfs]", rendered)
        self.assertIn(f'mount_root = "{mount_root}"', rendered)
        self.assertIn('source_datasets = ["tank/data", "tank/home"]', rendered)
        self.assertIn('receive_parent_dataset = "tank/restore"', rendered)
        self.assertIn('snapshot_prefix = "probe"', rendered)
        self.assertIn(
            f'target_base_dir = "{mount_root / "restore"}"',
            rendered,
        )
        self.assertNotIn("[subvolumes]", rendered)
        self.assertNotIn('\nbase_dir = "', rendered)

    def test_render_zfs_restore_only_tool_config_omits_source_datasets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)

            rendered = runner._render_tool_config(config, restore_only=True)

        self.assertIn('[filesystem]\nbackend = "zfs"', rendered)
        self.assertIn("[zfs]", rendered)
        self.assertIn('receive_parent_dataset = "tank/restore"', rendered)
        self.assertNotIn("source_datasets =", rendered)

    def test_render_backup_only_zfs_tool_config_omits_restore_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            del config["zfs"]["receive_parent_dataset"]

            rendered = runner._render_tool_config(config)

        self.assertIn('[filesystem]\nbackend = "zfs"', rendered)
        self.assertIn('source_datasets = ["tank/data", "tank/home"]', rendered)
        self.assertNotIn("receive_parent_dataset =", rendered)
        self.assertNotIn("[restore]", rendered)


class RunAllTests(unittest.TestCase):
    def test_build_steps_keeps_btrfs_flow_and_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _btrfs_config(tmpdir)

            steps, teardown = run_all._build_steps(config, skip_s3=False)

        self.assertEqual(steps[0], ("setup", "setup_btrfs.py", []))
        self.assertIn(("full", "run_full.py", []), steps)
        self.assertIn(("verify_discovery", "verify_discovery.py", []), steps)
        self.assertIn(
            ("restore_reconstructed", "run_restore.py", ["--use-incremental-manifest"]),
            steps,
        )
        self.assertIn(("verify_restore", "verify_restore.py", []), steps)
        self.assertEqual(teardown, ("teardown", "teardown_btrfs.py", []))

    def test_build_steps_uses_application_backed_zfs_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)

            steps, teardown = run_all._build_steps(config, skip_s3=False)

        self.assertEqual(
            steps,
            [
                ("setup", "setup_zfs.py", []),
                ("seed", "seed_data.py", []),
                ("full", "run_full.py", []),
                ("mutate", "mutate_data.py", []),
                ("incremental", "run_incremental.py", ["--skip-mutate"]),
                ("interrupt", "run_interrupt.py", []),
                ("verify_manifest", "verify_manifest.py", []),
                ("verify_discovery", "verify_discovery.py", []),
                ("verify_s3", "verify_s3.py", []),
                ("verify_retention", "verify_retention.py", []),
                (
                    "restore_reconstructed",
                    "run_restore.py",
                    ["--use-incremental-manifest"],
                ),
                ("restore", "run_restore.py", ["--source", "all"]),
                ("verify_restore", "verify_restore.py", ["--source", "all"]),
            ],
        )
        self.assertEqual(teardown, ("teardown", "teardown_zfs.py", []))

    def test_build_steps_zfs_skip_s3_keeps_local_fixture_steps_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)

            steps, teardown = run_all._build_steps(config, skip_s3=True)

        self.assertEqual(
            steps,
            [
                ("setup", "setup_zfs.py", []),
                ("seed", "seed_data.py", []),
                ("mutate", "mutate_data.py", []),
            ],
        )
        self.assertEqual(teardown, ("teardown", "teardown_zfs.py", []))

    def test_main_runs_zfs_application_flow_and_teardown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            log = RecordingLog()
            calls: list[tuple[str, str, list[str], bool]] = []

            def fake_run_step(
                name: str,
                script: str,
                config_path: str,
                extra_args: list[str],
                log_obj,
                *,
                allow_failure: bool = False,
            ) -> bool:
                del config_path, log_obj
                calls.append((name, script, list(extra_args), allow_failure))
                return True

            with mock.patch.object(
                run_all,
                "load_config",
                return_value=config,
            ), mock.patch.object(
                run_all,
                "open_log",
                return_value=log,
            ), mock.patch.object(
                run_all,
                "_run_step",
                side_effect=fake_run_step,
            ), mock.patch.object(
                run_all.sys,
                "argv",
                ["run_all.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = run_all.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            calls,
            [
                ("setup", "setup_zfs.py", [], False),
                ("seed", "seed_data.py", [], False),
                ("full", "run_full.py", [], False),
                ("mutate", "mutate_data.py", [], False),
                ("incremental", "run_incremental.py", ["--skip-mutate"], False),
                ("interrupt", "run_interrupt.py", [], False),
                ("verify_manifest", "verify_manifest.py", [], False),
                ("verify_discovery", "verify_discovery.py", [], False),
                ("verify_s3", "verify_s3.py", [], False),
                ("verify_retention", "verify_retention.py", [], False),
                (
                    "restore_reconstructed",
                    "run_restore.py",
                    ["--use-incremental-manifest"],
                    False,
                ),
                ("restore", "run_restore.py", ["--source", "all"], False),
                ("verify_restore", "verify_restore.py", ["--source", "all"], False),
                ("teardown", "teardown_zfs.py", [], True),
            ],
        )

    def test_main_warns_that_skip_s3_skips_zfs_application_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            log = RecordingLog()
            calls: list[str] = []

            def fake_run_step(
                name: str,
                script: str,
                config_path: str,
                extra_args: list[str],
                log_obj,
                *,
                allow_failure: bool = False,
            ) -> bool:
                del script, config_path, extra_args, log_obj, allow_failure
                calls.append(name)
                return True

            with mock.patch.object(
                run_all,
                "load_config",
                return_value=config,
            ), mock.patch.object(
                run_all,
                "open_log",
                return_value=log,
            ), mock.patch.object(
                run_all,
                "_run_step",
                side_effect=fake_run_step,
            ), mock.patch.object(
                run_all.sys,
                "argv",
                [
                    "run_all.py",
                    "--config",
                    str(Path(tmpdir) / "test_zfs.toml"),
                    "--skip-s3",
                ],
            ):
                result = run_all.main()

        self.assertEqual(result, 0)
        self.assertEqual(calls, ["setup", "seed", "mutate", "teardown"])
        self.assertIn(
            (
                "WARN",
                "--skip-s3 skips the application-backed ZFS backup/restore steps",
            ),
            log.entries,
        )

    def test_main_skips_large_scenario_for_zfs_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            log = RecordingLog()
            calls: list[str] = []

            def fake_run_step(
                name: str,
                script: str,
                config_path: str,
                extra_args: list[str],
                log_obj,
                *,
                allow_failure: bool = False,
            ) -> bool:
                del script, config_path, extra_args, log_obj, allow_failure
                calls.append(name)
                return True

            with mock.patch.object(
                run_all,
                "load_config",
                return_value=config,
            ), mock.patch.object(
                run_all,
                "open_log",
                return_value=log,
            ), mock.patch.object(
                run_all,
                "_run_step",
                side_effect=fake_run_step,
            ), mock.patch.object(
                run_all.sys,
                "argv",
                [
                    "run_all.py",
                    "--config",
                    str(Path(tmpdir) / "test_zfs.toml"),
                    "--include-large",
                ],
            ):
                result = run_all.main()

        self.assertEqual(result, 0)
        self.assertNotIn("large", calls)
        self.assertIn(
            (
                "WARN",
                "skipping large scenario because it is only defined for the Btrfs harness",
            ),
            log.entries,
        )


def _btrfs_config(tmpdir: str) -> dict[str, object]:
    run_dir = Path(tmpdir) / "integration_tests" / "run"
    mount_dir = run_dir / "mnt"
    return {
        "filesystem": {"backend": "btrfs"},
        "paths": {
            "run_dir": str(run_dir),
            "logs_dir": str(run_dir / "logs"),
            "scratch_dir": str(run_dir / "scratch"),
            "lock_dir": str(run_dir / "lock"),
            "btrfs_image": str(run_dir / "btrfs.img"),
            "mount_dir": str(mount_dir),
            "data_dir": str(mount_dir / "data"),
            "snapshots_dir": str(mount_dir / "snapshots"),
        },
        "btrfs": {
            "loopback_size_gib": 4,
            "mount_options": "compress=zstd",
            "subvolumes": ["data", "home"],
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
