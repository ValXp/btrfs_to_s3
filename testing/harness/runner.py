"""Run the btrfs_to_s3 CLI based on harness configuration."""

from __future__ import annotations

from typing import Any, Sequence
import json
import os
import shlex
import subprocess

from .config import load_config


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
) -> list[str]:
    """Build the full CLI command, including config path and extra args."""
    config_path = os.path.abspath(config_path)
    tool = config["tool"]
    cmd = resolve_command(config)
    cmd.extend([tool["config_flag"], config_path])
    if extra_args:
        cmd.extend(extra_args)
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
) -> subprocess.CompletedProcess[str] | None:
    """Load config and run the configured CLI."""
    config_path = os.path.abspath(config_path)
    config = load_config(config_path)
    command = build_command(config, config_path, extra_args)
    env = build_env(set_pythonpath=set_pythonpath)
    return run_command(command, dry_run=dry_run, env=env)


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
