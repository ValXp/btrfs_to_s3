"""Tests for the ZFS fixture setup and teardown scripts."""

from __future__ import annotations

import importlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


setup_zfs = importlib.import_module("integration_tests.scripts.setup_zfs")
teardown_zfs = importlib.import_module("integration_tests.scripts.teardown_zfs")


class RecordingLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def __enter__(self) -> "RecordingLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write(self, message: str, *, level: str = "INFO") -> None:
        self.entries.append((level, message))


class SetupZFSScriptTests(unittest.TestCase):
    def test_setup_creates_pool_datasets_and_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            state_path = Path(config["paths"]["run_dir"]) / setup_zfs.POOL_STATE_FILE
            log = RecordingLog()
            zfs_mock = mock.Mock()
            zfs_mock.create_backing_file.return_value = config["zfs"]["pool_file"]
            zfs_mock.create_pool.return_value = config["zfs"]["pool_name"]

            with mock.patch.object(setup_zfs, "load_config", return_value=config), mock.patch.object(
                setup_zfs, "open_log", return_value=log
            ), mock.patch.object(setup_zfs, "zfs", zfs_mock), mock.patch.object(
                setup_zfs, "_chown_for_user"
            ) as chown_mock, mock.patch.object(
                setup_zfs.sys,
                "argv",
                ["setup_zfs.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = setup_zfs.main()
                self.assertEqual(result, 0)
                self.assertEqual(
                    zfs_mock.mock_calls,
                    [
                        mock.call.create_backing_file(
                            config["zfs"]["pool_file"],
                            config["zfs"]["pool_size_gib"],
                            run_dir=config["paths"]["run_dir"],
                        ),
                        mock.call.create_pool(
                            config["zfs"]["pool_name"],
                            config["zfs"]["pool_file"],
                            config["zfs"]["mount_root"],
                            create_args=config["zfs"]["zpool_create_args"],
                            run_dir=config["paths"]["run_dir"],
                        ),
                        mock.call.create_dataset(
                            f'{config["zfs"]["pool_name"]}/data',
                            create_args=(
                                *config["zfs"]["zfs_create_args"],
                                "-o",
                                "snapdir=visible",
                            ),
                        ),
                        mock.call.create_dataset(
                            f'{config["zfs"]["pool_name"]}/home',
                            create_args=(
                                *config["zfs"]["zfs_create_args"],
                                "-o",
                                "snapdir=visible",
                            ),
                        ),
                        mock.call.create_dataset(
                            f'{config["zfs"]["pool_name"]}/restore',
                            create_args=tuple(config["zfs"]["zfs_create_args"]),
                        ),
                    ],
                )
                chown_mock.assert_called_once()
                state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(state["pool_status"], "created")
                self.assertEqual(
                    state["datasets"],
                    [
                        f'{config["zfs"]["pool_name"]}/data',
                        f'{config["zfs"]["pool_name"]}/home',
                        f'{config["zfs"]["pool_name"]}/restore',
                    ],
                )

    def test_setup_imports_pool_when_create_reports_existing_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            log = RecordingLog()
            create_error = subprocess.CalledProcessError(
                1,
                ["zpool", "create"],
                stderr="cannot create 'tank': pool already exists",
            )
            zfs_mock = mock.Mock()
            zfs_mock.create_backing_file.return_value = config["zfs"]["pool_file"]
            zfs_mock.create_pool.side_effect = create_error

            with mock.patch.object(setup_zfs, "load_config", return_value=config), mock.patch.object(
                setup_zfs, "open_log", return_value=log
            ), mock.patch.object(setup_zfs, "zfs", zfs_mock), mock.patch.object(
                setup_zfs, "_chown_for_user"
            ), mock.patch.object(
                setup_zfs.sys,
                "argv",
                ["setup_zfs.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = setup_zfs.main()

        self.assertEqual(result, 0)
        zfs_mock.import_pool.assert_called_once_with(
            config["zfs"]["pool_name"],
            config["zfs"]["mount_root"],
            run_dir=config["paths"]["run_dir"],
        )

    def test_setup_writes_pool_state_before_failing_dataset_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            state_path = Path(config["paths"]["run_dir"]) / setup_zfs.POOL_STATE_FILE
            log = RecordingLog()
            dataset_error = subprocess.CalledProcessError(
                1,
                ["zfs", "create"],
                stderr="permission denied",
            )
            zfs_mock = mock.Mock()
            zfs_mock.create_backing_file.return_value = config["zfs"]["pool_file"]
            zfs_mock.create_pool.return_value = config["zfs"]["pool_name"]
            zfs_mock.create_dataset.side_effect = dataset_error

            with mock.patch.object(setup_zfs, "load_config", return_value=config), mock.patch.object(
                setup_zfs, "open_log", return_value=log
            ), mock.patch.object(setup_zfs, "zfs", zfs_mock), mock.patch.object(
                setup_zfs, "_chown_for_user"
            ), mock.patch.object(
                setup_zfs.sys,
                "argv",
                ["setup_zfs.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = setup_zfs.main()
                self.assertEqual(result, 1)
                self.assertTrue(state_path.exists())
                state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(state["pool_status"], "created")
                self.assertNotIn("datasets", state)
                self.assertIn(
                    (
                        "ERROR",
                        "setup failed: permission denied",
                    ),
                    log.entries,
                )


class TeardownZFSScriptTests(unittest.TestCase):
    def test_teardown_destroys_pool_and_removes_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            state_path = Path(config["paths"]["run_dir"]) / teardown_zfs.POOL_STATE_FILE
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "pool_name": config["zfs"]["pool_name"],
                        "mount_root": config["zfs"]["mount_root"],
                    }
                ),
                encoding="utf-8",
            )
            log = RecordingLog()
            zfs_mock = mock.Mock()

            with mock.patch.object(
                teardown_zfs, "load_config", return_value=config
            ), mock.patch.object(
                teardown_zfs, "open_log", return_value=log
            ), mock.patch.object(
                teardown_zfs, "zfs", zfs_mock
            ), mock.patch.object(
                teardown_zfs.sys,
                "argv",
                ["teardown_zfs.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = teardown_zfs.main()

        self.assertEqual(result, 0)
        zfs_mock.destroy_pool.assert_called_once_with(config["zfs"]["pool_name"])
        self.assertFalse(state_path.exists())

    def test_teardown_imports_pool_after_destroy_reports_missing_active_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            state_path = Path(config["paths"]["run_dir"]) / teardown_zfs.POOL_STATE_FILE
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "pool_name": config["zfs"]["pool_name"],
                        "mount_root": config["zfs"]["mount_root"],
                    }
                ),
                encoding="utf-8",
            )
            log = RecordingLog()
            destroy_error = subprocess.CalledProcessError(
                1,
                ["zpool", "destroy"],
                stderr="cannot open 'tank': no such pool",
            )
            zfs_mock = mock.Mock()
            zfs_mock.destroy_pool.side_effect = [destroy_error, None]

            with mock.patch.object(
                teardown_zfs, "load_config", return_value=config
            ), mock.patch.object(
                teardown_zfs, "open_log", return_value=log
            ), mock.patch.object(
                teardown_zfs, "zfs", zfs_mock
            ), mock.patch.object(
                teardown_zfs.sys,
                "argv",
                ["teardown_zfs.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = teardown_zfs.main()

        self.assertEqual(result, 0)
        self.assertEqual(zfs_mock.destroy_pool.call_count, 2)
        zfs_mock.import_pool.assert_called_once_with(
            config["zfs"]["pool_name"],
            config["zfs"]["mount_root"],
            run_dir=config["paths"]["run_dir"],
        )
        self.assertFalse(state_path.exists())

    def test_teardown_treats_missing_pool_as_already_cleaned_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _zfs_config(tmpdir)
            log = RecordingLog()
            destroy_error = subprocess.CalledProcessError(
                1,
                ["zpool", "destroy"],
                stderr="cannot open 'tank': no such pool",
            )
            import_error = subprocess.CalledProcessError(
                1,
                ["zpool", "import"],
                stderr="cannot import 'tank': no such pool available",
            )
            zfs_mock = mock.Mock()
            zfs_mock.destroy_pool.side_effect = destroy_error
            zfs_mock.import_pool.side_effect = import_error

            with mock.patch.object(
                teardown_zfs, "load_config", return_value=config
            ), mock.patch.object(
                teardown_zfs, "open_log", return_value=log
            ), mock.patch.object(
                teardown_zfs, "zfs", zfs_mock
            ), mock.patch.object(
                teardown_zfs.sys,
                "argv",
                ["teardown_zfs.py", "--config", str(Path(tmpdir) / "test_zfs.toml")],
            ):
                result = teardown_zfs.main()

        self.assertEqual(result, 0)
        zfs_mock.destroy_pool.assert_called_once_with(config["zfs"]["pool_name"])
        zfs_mock.import_pool.assert_called_once_with(
            config["zfs"]["pool_name"],
            config["zfs"]["mount_root"],
            run_dir=config["paths"]["run_dir"],
        )


class SetupZFSChownTests(unittest.TestCase):
    def test_chown_for_user_recurses_when_running_as_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            nested_dir = run_dir / "nested"
            file_path = nested_dir / "state.json"
            nested_dir.mkdir(parents=True)
            file_path.write_text("{}", encoding="utf-8")
            log = RecordingLog()

            with mock.patch.object(setup_zfs.os, "geteuid", return_value=0), mock.patch.dict(
                setup_zfs.os.environ,
                {"SUDO_USER": "alice"},
                clear=True,
            ), mock.patch.object(
                setup_zfs.pwd,
                "getpwnam",
                return_value=SimpleNamespace(pw_uid=1234, pw_gid=4321),
            ), mock.patch.object(setup_zfs.os, "chown") as chown_mock:
                setup_zfs._chown_for_user([str(run_dir)], log)

        self.assertEqual(
            chown_mock.call_args_list,
            [
                mock.call(str(run_dir), 1234, 4321),
                mock.call(str(nested_dir), 1234, 4321),
                mock.call(str(file_path), 1234, 4321),
            ],
        )
        self.assertIn(("INFO", "chowned paths to alice"), log.entries)


def _zfs_config(tmpdir: str) -> dict[str, object]:
    run_dir = Path(tmpdir) / "integration_tests" / "run" / "zfs"
    return {
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
            "snapshot_prefix": "btrfs-to-s3",
            "zpool_create_args": ["-O", "compression=zstd"],
            "zfs_create_args": ["-o", "compression=zstd"],
        },
    }


if __name__ == "__main__":
    unittest.main()
