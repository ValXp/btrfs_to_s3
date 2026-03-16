"""Verify restored data against a source snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime

TESTING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if TESTING_DIR not in sys.path:
    sys.path.insert(0, TESTING_DIR)

from harness.config import load_config
from harness.filesystem import (
    backend_name,
    restore_base_dir,
    restore_target_dataset,
    source_identifiers,
    target_token,
    zfs_dataset_mount_path,
    zfs_receive_parent_dataset,
    zfs_snapshot_mount_path,
)
from harness.logs import open_log
from harness import zfs as zfs_harness


DEFAULT_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "config", "test.toml")
)
DEFAULT_SAMPLE_SIZE = 25
VERIFY_CLONE_DESTROY_ATTEMPTS = 5
VERIFY_CLONE_DESTROY_DELAY_SECONDS = 0.2
RESTORE_METADATA_FILE = "restore_target.json"
RESTORE_TARGETS_FILE = "restore_targets.json"


class _ResolvedSourceSnapshot:
    def __init__(
        self,
        path: str,
        *,
        cleanup_dataset: str | None = None,
    ) -> None:
        self.path = path
        self.cleanup_dataset = cleanup_dataset


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify restored source content.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument(
        "--source",
        "--subvolume",
        dest="source",
        default=None,
        help="Source identifier or 'all' to verify every restored source.",
    )
    parser.add_argument("--source-snapshot", default=None)
    parser.add_argument("--target", default=None)
    parser.add_argument("--target-base", default=None)
    parser.add_argument("--mode", choices=("full", "sample"), default="full")
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--skip", action="store_true")
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    config = load_config(config_path)
    paths = config["paths"]

    log_path = os.path.join(paths["logs_dir"], "verify_restore.log")
    os.makedirs(paths["logs_dir"], exist_ok=True)

    with open_log(log_path) as log:
        log.write(f"loading config from {config_path}")
        if args.skip or _config_disables_verify(config):
            log.write("restore verification skipped")
            return 0
        try:
            sources = _resolve_sources(args.source, config)
            if len(sources) > 1 and args.target:
                raise ValueError("--target is only valid for a single source")
        except ValueError as exc:
            log.write(str(exc), level="ERROR")
            return 1

        for source in sources:
            source_snapshot: _ResolvedSourceSnapshot | None = None
            try:
                target_path = _resolve_target_path(
                    args,
                    config,
                    source,
                    require_mapping=len(sources) > 1,
                )
                source_snapshot = _resolve_source_snapshot(
                    args.source_snapshot,
                    config,
                    source,
                )
            except ValueError as exc:
                log.write(str(exc), level="ERROR")
                return 1

            log.write(f"target restore: {target_path}")
            try:
                log.write(f"source snapshot: {source_snapshot.path}")

                try:
                    _verify_metadata(config, target_path)
                except (RuntimeError, subprocess.CalledProcessError) as exc:
                    log.write(str(exc), level="ERROR")
                    return 1

                try:
                    mismatch = _verify_content(
                        source_snapshot.path,
                        target_path,
                        mode=args.mode,
                        sample_size=args.sample_size,
                    )
                except RuntimeError as exc:
                    log.write(str(exc), level="ERROR")
                    return 1

                if mismatch:
                    log.write(mismatch, level="ERROR")
                    return 1
            finally:
                if source_snapshot and source_snapshot.cleanup_dataset:
                    try:
                        _cleanup_zfs_verify_clone(source_snapshot.cleanup_dataset)
                    except subprocess.CalledProcessError as exc:
                        log.write(
                            f"failed to remove verify clone "
                            f"{source_snapshot.cleanup_dataset}: {exc.stderr or exc}",
                            level="ERROR",
                        )
                        return 1

        log.write("restore verification passed")
        return 0


def _config_disables_verify(config: dict) -> bool:
    restore_cfg = config.get("restore")
    if isinstance(restore_cfg, dict):
        value = restore_cfg.get("verify")
        if isinstance(value, bool):
            return not value
    top_level = config.get("verify_restore")
    if isinstance(top_level, bool):
        return not top_level
    return False


def _resolve_sources(requested: str | None, config: dict) -> list[str]:
    sources = source_identifiers(config)
    if requested is None:
        if not sources:
            raise ValueError("config has no sources")
        return [sources[0]]
    if requested == "all":
        return list(sources)
    if requested not in sources:
        raise ValueError(
            f"source {requested} not in config list: {', '.join(sources)}"
        )
    return [requested]


def _resolve_source_snapshot(
    requested: str | None,
    config: dict,
    source: str,
) -> _ResolvedSourceSnapshot:
    if requested:
        path = os.path.abspath(requested)
        if not os.path.isdir(path):
            raise ValueError(f"source snapshot missing: {path}")
        return _ResolvedSourceSnapshot(path)

    if backend_name(config) == "zfs":
        return _resolve_zfs_source_snapshot(config, source)
    return _ResolvedSourceSnapshot(
        _resolve_btrfs_source_snapshot(config["paths"]["snapshots_dir"], source)
    )


def _resolve_btrfs_source_snapshot(snapshots_dir: str, source: str) -> str:
    snapshots_dir = os.path.abspath(snapshots_dir)

    if not os.path.isdir(snapshots_dir):
        raise ValueError(f"snapshots dir missing: {snapshots_dir}")

    candidates: list[tuple[datetime, str]] = []
    for entry in os.listdir(snapshots_dir):
        parsed = _parse_snapshot_name(entry)
        if parsed is None:
            continue
        name, created_at, _kind = parsed
        if name != source:
            continue
        path = os.path.join(snapshots_dir, entry)
        if os.path.isdir(path):
            candidates.append((created_at, path))

    if not candidates:
        raise ValueError(f"no snapshots found for {source} under {snapshots_dir}")

    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _resolve_zfs_source_snapshot(
    config: dict,
    source: str,
) -> _ResolvedSourceSnapshot:
    zfs_cfg = config["zfs"]
    snapshot_prefix = zfs_cfg["snapshot_prefix"]
    snapshots = zfs_harness.list_snapshots(source)
    candidates = [
        snapshot
        for snapshot in snapshots
        if snapshot.startswith(f"{source}@{snapshot_prefix}-")
    ]
    if not candidates:
        raise ValueError(f"no ZFS snapshots found for {source}")
    snapshot = candidates[-1]
    snapshot_path = zfs_snapshot_mount_path(config, source, snapshot)
    if not os.path.isdir(snapshot_path):
        clone_dataset = _verify_clone_dataset(config, source)
        zfs_harness.clone_snapshot(snapshot, clone_dataset)
        clone_path = zfs_dataset_mount_path(config, clone_dataset)
        if not os.path.isdir(clone_path):
            try:
                _cleanup_zfs_verify_clone(clone_dataset)
            except subprocess.CalledProcessError:
                pass
            raise ValueError(f"source snapshot clone missing: {clone_path}")
        return _ResolvedSourceSnapshot(
            clone_path,
            cleanup_dataset=clone_dataset,
        )
    return _ResolvedSourceSnapshot(snapshot_path)


def _verify_clone_dataset(config: dict, source: str) -> str:
    parent_dataset = zfs_receive_parent_dataset(config)
    token = target_token(source)
    return f"{parent_dataset}/__verify__{token}__{uuid.uuid4().hex}"


def _cleanup_zfs_verify_clone(clone_dataset: str) -> None:
    last_error: subprocess.CalledProcessError | None = None
    try:
        zfs_harness.unmount_dataset(clone_dataset, force=True)
    except subprocess.CalledProcessError as exc:
        if not zfs_harness.dataset_exists(clone_dataset):
            return
        last_error = exc

    for attempt in range(VERIFY_CLONE_DESTROY_ATTEMPTS):
        try:
            zfs_harness.destroy_dataset(
                clone_dataset,
                recursive=True,
                force=True,
            )
            return
        except subprocess.CalledProcessError as exc:
            if not zfs_harness.dataset_exists(clone_dataset):
                return
            last_error = exc
            if attempt == VERIFY_CLONE_DESTROY_ATTEMPTS - 1:
                break
            time.sleep(VERIFY_CLONE_DESTROY_DELAY_SECONDS)

    if last_error is not None:
        raise last_error


def _resolve_target_path(
    args,
    config: dict,
    source: str,
    *,
    require_mapping: bool,
) -> str:
    if args.target:
        target_path = os.path.abspath(args.target)
        if not os.path.exists(target_path):
            raise ValueError(f"restore target missing: {target_path}")
        return target_path

    paths = config["paths"]
    run_dir = os.path.abspath(paths["run_dir"])
    targets_path = os.path.join(run_dir, RESTORE_TARGETS_FILE)
    if os.path.exists(targets_path):
        try:
            with open(targets_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid restore metadata: {targets_path}") from exc
        for entry in payload.get("targets", []):
            entry_source = entry.get("source", entry.get("subvolume"))
            if entry_source == source:
                target_path = os.path.abspath(entry.get("target_path", ""))
                if target_path and os.path.exists(target_path):
                    return target_path
        if require_mapping:
            raise ValueError(f"restore target missing for {source}")

    metadata_path = os.path.join(run_dir, RESTORE_METADATA_FILE)
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid restore metadata: {metadata_path}") from exc
        target_path = os.path.abspath(payload.get("target_path", ""))
        if target_path and os.path.exists(target_path):
            return target_path

    target_base = args.target_base
    if target_base is None:
        target_base = restore_base_dir(config)
    target_base = os.path.abspath(target_base)
    target_path = os.path.join(target_base, target_token(source))
    if not os.path.exists(target_path):
        raise ValueError(
            "restore target missing; rerun run_restore.py or pass --target"
        )
    return target_path


def _verify_metadata(config: dict, target_path: str) -> None:
    if backend_name(config) == "zfs":
        _verify_zfs_metadata(config, target_path)
        return
    _verify_btrfs_metadata(target_path)


def _verify_btrfs_metadata(target_path: str) -> None:
    if not os.path.isdir(target_path):
        raise RuntimeError(f"restore target is not a directory: {target_path}")
    if not os.access(target_path, os.W_OK):
        raise RuntimeError(f"restore target is not writable: {target_path}")

    result = _run(["btrfs", "subvolume", "show", target_path])
    uuid_value = _parse_uuid(result.stdout)
    if uuid_value is None:
        raise RuntimeError("restore target has no valid UUID")


def _verify_zfs_metadata(config: dict, target_path: str) -> None:
    if not os.path.isdir(target_path):
        raise RuntimeError(f"restore target is not a directory: {target_path}")
    if not os.access(target_path, os.W_OK):
        raise RuntimeError(f"restore target is not writable: {target_path}")

    try:
        dataset = restore_target_dataset(config, target_path)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc

    dataset_type = zfs_harness.get_dataset_property(dataset, "type")
    if dataset_type != "filesystem":
        raise RuntimeError(
            f"restore target has unexpected dataset type: {dataset_type}"
        )
    mounted = zfs_harness.get_dataset_property(dataset, "mounted")
    if mounted != "yes":
        raise RuntimeError(f"restore target dataset is not mounted: {dataset}")
    mountpoint = zfs_harness.get_dataset_property(dataset, "mountpoint")
    if os.path.abspath(mountpoint) != os.path.abspath(target_path):
        raise RuntimeError(f"restore target mountpoint mismatch: {mountpoint}")
    readonly = zfs_harness.get_dataset_property(dataset, "readonly")
    if readonly != "off":
        raise RuntimeError(f"restore target is readonly: {target_path}")


def _verify_content(
    source_path: str,
    target_path: str,
    *,
    mode: str,
    sample_size: int,
) -> str | None:
    source_dirs, source_files = _collect_entries(source_path)
    target_dirs, target_files = _collect_entries(target_path)

    mismatch = _check_missing_extra(source_dirs, target_dirs, "directory")
    if mismatch:
        return mismatch
    mismatch = _check_missing_extra(source_files, target_files, "file")
    if mismatch:
        return mismatch

    files_to_check = source_files
    if mode == "sample":
        files_to_check = _sample_paths(source_files, sample_size)

    for rel_path in files_to_check:
        source_file = os.path.join(source_path, rel_path)
        target_file = os.path.join(target_path, rel_path)
        if not os.path.exists(target_file):
            return f"missing restored file: {rel_path}"
        mismatch = _compare_file(source_file, target_file, rel_path)
        if mismatch:
            return mismatch

    for rel_path in source_dirs:
        source_dir = os.path.join(source_path, rel_path)
        target_dir = os.path.join(target_path, rel_path)
        if not os.path.isdir(target_dir):
            return f"missing restored directory: {rel_path}"
        mismatch = _compare_metadata(source_dir, target_dir, rel_path)
        if mismatch:
            return mismatch

    return None


def _collect_entries(base_path: str) -> tuple[list[str], list[str]]:
    dirs: list[str] = []
    files: list[str] = []
    for root, dirnames, filenames in os.walk(base_path):
        dirnames.sort()
        filenames.sort()
        rel_root = os.path.relpath(root, base_path)
        if rel_root != ".":
            dirs.append(rel_root)
        for name in dirnames:
            rel_dir = os.path.join(rel_root, name)
            rel_dir = rel_dir if rel_root != "." else name
            if os.path.islink(os.path.join(root, name)):
                files.append(rel_dir)
        for name in filenames:
            rel_file = os.path.join(rel_root, name)
            rel_file = rel_file if rel_root != "." else name
            files.append(rel_file)

    dirs = sorted(set(dirs))
    files = sorted(set(files))
    return dirs, files


def _check_missing_extra(
    source_list: list[str],
    target_list: list[str],
    label: str,
) -> str | None:
    source_set = set(source_list)
    target_set = set(target_list)
    for path in source_list:
        if path not in target_set:
            return f"missing {label}: {path}"
    for path in target_list:
        if path not in source_set:
            return f"extra {label}: {path}"
    return None


def _sample_paths(paths: list[str], sample_size: int) -> list[str]:
    if sample_size <= 0:
        return []
    if len(paths) <= sample_size:
        return list(paths)
    scored = [(path, _stable_hash(path)) for path in paths]
    scored.sort(key=lambda item: item[1])
    return [path for path, _hash in scored[:sample_size]]


def _stable_hash(path: str) -> str:
    return hashlib.sha256(path.encode("utf-8")).hexdigest()


def _compare_file(source_file: str, target_file: str, rel_path: str) -> str | None:
    source_stat = os.lstat(source_file)
    target_stat = os.lstat(target_file)

    if source_stat.st_size != target_stat.st_size:
        return f"size mismatch for {rel_path}"

    if os.path.islink(source_file) or os.path.islink(target_file):
        if os.readlink(source_file) != os.readlink(target_file):
            return f"symlink mismatch for {rel_path}"
        return None

    if not os.path.isfile(source_file) or not os.path.isfile(target_file):
        return f"file type mismatch for {rel_path}"

    if _hash_file(source_file) != _hash_file(target_file):
        return f"hash mismatch for {rel_path}"

    return _compare_metadata(source_file, target_file, rel_path)


def _compare_metadata(source_path: str, target_path: str, rel_path: str) -> str | None:
    source_stat = os.lstat(source_path)
    target_stat = os.lstat(target_path)

    source_mode = source_stat.st_mode & 0o7777
    target_mode = target_stat.st_mode & 0o7777
    if source_mode != target_mode:
        return f"mode mismatch for {rel_path}"
    if source_stat.st_uid != target_stat.st_uid:
        return f"uid mismatch for {rel_path}"
    if source_stat.st_gid != target_stat.st_gid:
        return f"gid mismatch for {rel_path}"
    return None


def _hash_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PATH"] = _ensure_sbin_on_path(env.get("PATH", ""))
    return subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )


def _parse_uuid(output: str) -> str | None:
    for line in output.splitlines():
        if line.strip().lower().startswith("uuid:"):
            value = line.split(":", 1)[1].strip()
            try:
                uuid.UUID(value)
            except ValueError:
                return None
            return value
    return None


def _parse_snapshot_name(name: str) -> tuple[str, datetime, str] | None:
    match = re.match(r"^(?P<subvol>.+)__(?P<ts>\d{8}T\d{6}Z)__(?P<kind>full|inc)$", name)
    if not match:
        return None
    timestamp = datetime.strptime(match.group("ts"), "%Y%m%dT%H%M%SZ")
    return match.group("subvol"), timestamp, match.group("kind")


def _ensure_sbin_on_path(path: str) -> str:
    parts = [entry for entry in path.split(os.pathsep) if entry]
    for entry in ("/usr/sbin", "/sbin"):
        if entry not in parts:
            parts.append(entry)
    return os.pathsep.join(parts)


if __name__ == "__main__":
    raise SystemExit(main())
