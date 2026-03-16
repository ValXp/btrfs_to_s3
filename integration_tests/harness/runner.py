"""Run the btrfs_to_s3 CLI based on harness configuration."""

from __future__ import annotations

from typing import Any, Sequence
import json
import os
import shlex
import subprocess

from .config import load_config
from .env import load_env


_CMD_ENV = "BTRFS_TO_S3_CMD"


def resolve_command(config: dict[str, Any]) -> list[str]:
    """Resolve the CLI command from config or environment override."""
    override = os.environ.get(_CMD_ENV)
    if override:
        return _parse_command_override(override)
    cmd = config["tool"]["cmd"]
    return list(cmd)


def build_command(
    config: dict[str, Any],
    config_path: str,
    extra_args: Sequence[str] | None = None,
    *,
    restore_only: bool = False,
) -> list[str]:
    """Build the full CLI command, including config path and extra args."""
    config_path = os.path.abspath(config_path)
    tool = config["tool"]
    tool_config_path = _ensure_tool_config(
        config,
        config_path,
        restore_only=restore_only,
    )
    cmd = resolve_command(config)
    if extra_args:
        cmd.extend(extra_args)
    cmd.extend([tool["config_flag"], tool_config_path])
    return cmd


def build_env(*, set_pythonpath: bool = True) -> dict[str, str]:
    """Build an environment for running the CLI."""
    env = os.environ.copy()
    if set_pythonpath:
        _ensure_repo_on_pythonpath(env)
    return env


def run_command(
    command: Sequence[str],
    *,
    dry_run: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str] | None:
    """Execute a command, optionally printing instead of running."""
    if dry_run:
        print(shlex.join(command))
        return None
    return subprocess.run(
        list(command),
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )


def run_tool(
    config_path: str,
    extra_args: Sequence[str] | None = None,
    *,
    dry_run: bool = False,
    set_pythonpath: bool = True,
    restore_only: bool = False,
) -> subprocess.CompletedProcess[str] | None:
    """Load config and run the configured CLI."""
    config_path = os.path.abspath(config_path)
    config = load_config(config_path)
    env_path = os.path.join(os.path.dirname(config_path), "test.env")
    if os.path.exists(env_path):
        load_env(env_path, override=False)
    command = build_command(
        config,
        config_path,
        extra_args,
        restore_only=restore_only,
    )
    env = build_env(set_pythonpath=set_pythonpath)
    env["BTRFS_TO_S3_HARNESS_RUN_DIR"] = os.path.abspath(config["paths"]["run_dir"])
    return run_command(command, dry_run=dry_run, env=env)


def _ensure_tool_config(
    config: dict[str, Any],
    config_path: str,
    *,
    restore_only: bool = False,
) -> str:
    paths = config["paths"]
    run_dir = os.path.abspath(paths["run_dir"])
    os.makedirs(run_dir, exist_ok=True)
    config_name = (
        "tool_config_restore_only.toml"
        if restore_only
        else "tool_config.toml"
    )
    tool_config_path = os.path.join(run_dir, config_name)
    content = _render_tool_config(config, restore_only=restore_only)
    with open(tool_config_path, "w", encoding="utf-8") as handle:
        handle.write(content)
    return tool_config_path


def _render_tool_config(
    config: dict[str, Any],
    *,
    restore_only: bool = False,
) -> str:
    backend = config["filesystem"]["backend"]
    paths = config["paths"]
    aws_cfg = config["aws"]
    backup_cfg = config["backup"]
    restore_cfg = config.get("restore")
    global_cfg = config.get("global", {})
    s3_cfg = config.get("s3", {})

    run_dir = os.path.abspath(paths["run_dir"])
    lock_dir = os.path.abspath(paths["lock_dir"])
    scratch_dir = os.path.abspath(paths["scratch_dir"])

    chunk_size_bytes = int(backup_cfg["chunk_size_mib"]) * 1024 * 1024
    spool_size_bytes = max(2 * chunk_size_bytes, 64 * 1024 * 1024)
    if isinstance(global_cfg, dict) and "spool_size_bytes" in global_cfg:
        spool_size_bytes = int(global_cfg["spool_size_bytes"])
    retention = max(1, int(backup_cfg["retention_snapshots"]))

    storage_class_chunks = aws_cfg.get("storage_class_chunks", aws_cfg["storage_class"])
    storage_class_manifest = aws_cfg.get(
        "storage_class_manifest", aws_cfg["storage_class"]
    )

    spool_enabled = False
    if isinstance(s3_cfg, dict) and "spool_enabled" in s3_cfg:
        spool_enabled = bool(s3_cfg["spool_enabled"])

    lines = [
        "[global]",
        'log_level = "info"',
        f'state_path = "{os.path.join(run_dir, "state.json")}"',
        f'lock_path = "{os.path.join(lock_dir, "btrfs_to_s3.lock")}"',
        f'spool_dir = "{scratch_dir}"',
        f"spool_size_bytes = {spool_size_bytes}",
        "",
        "[schedule]",
        "full_every_days = 1",
        "incremental_every_days = 1",
        'run_at = "02:00"',
        "",
        "[filesystem]",
        f'backend = "{backend}"',
        "",
    ]
    if backend == "btrfs":
        lines.extend(
            _render_btrfs_sections(
                config,
                retention,
                restore_only=restore_only,
            )
        )
    elif backend == "zfs":
        lines.extend(
            _render_zfs_sections(
                config,
                retention,
                restore_only=restore_only,
            )
        )
    else:
        raise ValueError(f"unsupported backend {backend!r}")
    lines.extend(
        [
            "[s3]",
            f'bucket = "{aws_cfg["bucket"]}"',
            f'region = "{aws_cfg["region"]}"',
            f'prefix = "{aws_cfg["prefix"]}"',
            f"chunk_size_bytes = {chunk_size_bytes}",
            f'storage_class_chunks = "{storage_class_chunks}"',
            f'storage_class_manifest = "{storage_class_manifest}"',
            f'concurrency = {int(backup_cfg["concurrency"])}',
            f"spool_enabled = {str(spool_enabled).lower()}",
            f'sse = "{aws_cfg["sse"]}"',
            "",
        ]
    )
    if isinstance(restore_cfg, dict) and restore_cfg:
        restore_base = restore_cfg.get("target_base_dir")
    else:
        restore_base = None
    restore_base = _resolve_restore_base(config, backend, restore_base)
    if restore_base is not None:
        lines.extend(
            [
                "[restore]",
                f'target_base_dir = "{restore_base}"',
            ]
        )
        if isinstance(restore_cfg, dict) and "verify_mode" in restore_cfg:
            lines.append(f'verify_mode = "{restore_cfg["verify_mode"]}"')
        if isinstance(restore_cfg, dict) and "sample_max_files" in restore_cfg:
            lines.append(
                f"sample_max_files = {int(restore_cfg['sample_max_files'])}"
            )
        if isinstance(restore_cfg, dict) and "wait_for_restore" in restore_cfg:
            value = bool(restore_cfg["wait_for_restore"])
            lines.append(f"wait_for_restore = {str(value).lower()}")
        if (
            isinstance(restore_cfg, dict)
            and "restore_timeout_seconds" in restore_cfg
        ):
            lines.append(
                f"restore_timeout_seconds = {int(restore_cfg['restore_timeout_seconds'])}"
            )
        if isinstance(restore_cfg, dict) and "restore_tier" in restore_cfg:
            lines.append(f'restore_tier = "{restore_cfg["restore_tier"]}"')
        lines.append("")
    return "\n".join(lines)


def _render_btrfs_sections(
    config: dict[str, Any],
    retention: int,
    *,
    restore_only: bool = False,
) -> list[str]:
    paths = config["paths"]
    btrfs_cfg = config["btrfs"]
    snapshots_dir = os.path.abspath(paths["snapshots_dir"])
    lines = [
        "[snapshots]",
        f'base_dir = "{snapshots_dir}"',
        f"retain = {retention}",
        "",
    ]
    if restore_only:
        return lines

    mount_dir = os.path.abspath(paths["mount_dir"])
    subvolume_paths = [
        os.path.join(mount_dir, name) for name in btrfs_cfg["subvolumes"]
    ]
    lines.extend(
        [
            "[subvolumes]",
            f"paths = {_format_toml_list(subvolume_paths)}",
            "",
        ]
    )
    return lines


def _render_zfs_sections(
    config: dict[str, Any],
    retention: int,
    *,
    restore_only: bool = False,
) -> list[str]:
    zfs_cfg = config["zfs"]
    pool_name = zfs_cfg["pool_name"]
    mount_root = os.path.abspath(zfs_cfg["mount_root"])
    lines = [
        "[snapshots]",
        f"retain = {retention}",
        "",
        "[zfs]",
        f'pool_name = "{pool_name}"',
        f'mount_root = "{mount_root}"',
    ]
    if not restore_only:
        source_datasets = [
            _qualify_zfs_dataset(pool_name, name)
            for name in zfs_cfg["source_datasets"]
        ]
        lines.append(f"source_datasets = {_format_toml_list(source_datasets)}")

    receive_parent_dataset = _qualify_zfs_dataset(
        pool_name,
        zfs_cfg["receive_parent_dataset"],
    )
    lines.extend(
        [
        f'receive_parent_dataset = "{receive_parent_dataset}"',
        f'snapshot_prefix = "{zfs_cfg["snapshot_prefix"]}"',
        "",
        ]
    )
    return lines


def _resolve_restore_base(
    config: dict[str, Any],
    backend: str,
    restore_base: str | None,
) -> str:
    if restore_base:
        return os.path.abspath(restore_base)
    if backend == "btrfs":
        return os.path.abspath(os.path.join(config["paths"]["mount_dir"], "restore"))
    if backend == "zfs":
        zfs_cfg = config["zfs"]
        mount_root = os.path.abspath(zfs_cfg["mount_root"])
        dataset_path = _zfs_dataset_mount_path(
            zfs_cfg["pool_name"],
            zfs_cfg["receive_parent_dataset"],
        )
        return os.path.join(mount_root, dataset_path)
    raise ValueError(f"unsupported backend {backend!r}")


def _qualify_zfs_dataset(pool_name: str, dataset_name: str) -> str:
    if dataset_name == pool_name or dataset_name.startswith(pool_name + "/"):
        return dataset_name
    return f"{pool_name}/{dataset_name}"


def _zfs_dataset_mount_path(pool_name: str, dataset_name: str) -> str:
    qualified = _qualify_zfs_dataset(pool_name, dataset_name)
    if qualified == pool_name:
        return ""
    return qualified[len(pool_name) + 1 :]


def _format_toml_list(values: Sequence[str]) -> str:
    escaped = [value.replace("\\", "\\\\") for value in values]
    quoted = ", ".join(f'"{value}"' for value in escaped)
    return f"[{quoted}]"


def _parse_command_override(raw: str) -> list[str]:
    raw = raw.strip()
    if not raw:
        raise ValueError(f"{_CMD_ENV}: override is empty")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{_CMD_ENV}: expected a JSON array of strings") from exc
    if not isinstance(parsed, list) or not parsed:
        raise ValueError(f"{_CMD_ENV}: expected a JSON array of strings")
    command: list[str] = []
    for item in parsed:
        if not isinstance(item, str) or not item:
            raise ValueError(f"{_CMD_ENV}: expected a JSON array of strings")
        command.append(item)
    return command


def _ensure_repo_on_pythonpath(env: dict[str, str]) -> None:
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )
    current = env.get("PYTHONPATH", "")
    if not current:
        env["PYTHONPATH"] = repo_root
        return
    parts = current.split(os.pathsep)
    if repo_root not in parts:
        env["PYTHONPATH"] = os.pathsep.join([repo_root, current])
