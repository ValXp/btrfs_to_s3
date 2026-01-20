"""Restore helpers for manifest resolution and stream replay."""

from __future__ import annotations

import hashlib
import json
import os
import random
import stat
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, IO

from btrfs_to_s3.path_utils import ensure_sbin_on_path

ARCHIVAL_STORAGE_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}


class RestoreError(RuntimeError):
    """Raised when restore operations fail."""


@dataclass(frozen=True)
class ChunkInfo:
    key: str
    sha256: str
    size: int | None


@dataclass(frozen=True)
class ManifestInfo:
    key: str
    kind: str
    parent_manifest: str | None
    chunks: tuple[ChunkInfo, ...]
    s3: dict[str, Any]
    snapshot_path: str | None


def needs_restore(storage_class: str | None) -> bool:
    if not storage_class:
        return False
    return storage_class.upper() in ARCHIVAL_STORAGE_CLASSES


def is_restore_ready(restore_header: str | None) -> bool:
    if not restore_header:
        return False
    lowered = restore_header.lower()
    if 'ongoing-request="false"' in lowered:
        return True
    if 'ongoing-request="true"' in lowered:
        return False
    return False


def fetch_current_manifest_key(
    client,
    bucket: str,
    current_key: str,
) -> str:
    payload = _fetch_json(client, bucket, current_key)
    manifest_key = payload.get("manifest_key")
    if not isinstance(manifest_key, str) or not manifest_key:
        raise RestoreError(f"{current_key} missing manifest_key")
    return manifest_key


def resolve_manifest_chain(
    client,
    bucket: str,
    start_key: str,
) -> list[ManifestInfo]:
    manifests: list[ManifestInfo] = []
    seen: set[str] = set()
    current_key = start_key
    while True:
        if current_key in seen:
            raise RestoreError(f"manifest chain loop detected at {current_key}")
        seen.add(current_key)
        manifest = fetch_manifest(client, bucket, current_key)
        manifests.append(manifest)
        parent = manifest.parent_manifest
        if parent:
            current_key = parent
            continue
        break
    manifests.reverse()
    if not manifests or manifests[0].kind != "full":
        raise RestoreError("manifest chain does not end in full backup")
    return manifests


def fetch_manifest(client, bucket: str, key: str) -> ManifestInfo:
    payload = _fetch_json(client, bucket, key)
    return parse_manifest(payload, key)


def parse_manifest(payload: dict[str, Any], key: str) -> ManifestInfo:
    kind = payload.get("kind")
    if not isinstance(kind, str) or not kind:
        raise RestoreError(f"{key} missing kind")
    parent_manifest = payload.get("parent_manifest")
    if parent_manifest == "":
        parent_manifest = None
    if parent_manifest is not None and not isinstance(parent_manifest, str):
        raise RestoreError(f"{key} invalid parent_manifest")
    chunks_payload = payload.get("chunks")
    if not isinstance(chunks_payload, list) or not chunks_payload:
        raise RestoreError(f"{key} missing chunks")
    chunks: list[ChunkInfo] = []
    for chunk in chunks_payload:
        if not isinstance(chunk, dict):
            raise RestoreError(f"{key} has invalid chunk entry")
        chunk_key = chunk.get("key")
        sha256 = chunk.get("sha256")
        size = chunk.get("size")
        if not isinstance(chunk_key, str) or not chunk_key:
            raise RestoreError(f"{key} chunk missing key")
        if not isinstance(sha256, str) or not sha256:
            raise RestoreError(f"{key} chunk missing sha256")
        if size is not None and not isinstance(size, int):
            raise RestoreError(f"{key} chunk has invalid size")
        chunks.append(ChunkInfo(key=chunk_key, sha256=sha256, size=size))
    s3 = payload.get("s3", {})
    if not isinstance(s3, dict):
        raise RestoreError(f"{key} invalid s3 metadata")
    snapshot_path = _parse_snapshot_path(payload, key)
    return ManifestInfo(
        key=key,
        kind=kind,
        parent_manifest=parent_manifest,
        chunks=tuple(chunks),
        s3=s3,
        snapshot_path=snapshot_path,
    )


def ensure_chunks_restored(
    client,
    bucket: str,
    chunks: Iterable[ChunkInfo],
    *,
    storage_class: str | None,
    restore_tier: str,
    timeout_seconds: int,
    sleep: callable = time.sleep,
    time_fn: callable = time.monotonic,
) -> None:
    if not needs_restore(storage_class):
        return
    keys = [chunk.key for chunk in chunks]
    for key in keys:
        client.restore_object(
            Bucket=bucket,
            Key=key,
            RestoreRequest={
                "Days": 1,
                "GlacierJobParameters": {"Tier": restore_tier},
            },
        )
    deadline = time_fn() + timeout_seconds
    pending = set(keys)
    delay = 1.0
    while pending:
        now = time_fn()
        if now >= deadline:
            missing = ", ".join(sorted(pending))
            raise RestoreError(f"restore timeout waiting for {missing}")
        for key in list(pending):
            response = client.head_object(Bucket=bucket, Key=key)
            if is_restore_ready(response.get("Restore")):
                pending.remove(key)
        if pending:
            jitter = random.random() * 0.1 * delay
            sleep(delay + jitter)
            delay = min(delay * 2.0, 30.0)


def download_and_verify_chunks(
    client,
    bucket: str,
    chunks: Iterable[ChunkInfo],
    output: IO[bytes],
    *,
    read_size: int = 1024 * 1024,
) -> int:
    if read_size <= 0:
        raise RestoreError("read_size must be positive")
    total_bytes = 0
    for chunk in chunks:
        response = client.get_object(Bucket=bucket, Key=chunk.key)
        body = response["Body"]
        hasher = hashlib.sha256()
        while True:
            data = body.read(read_size)
            if not data:
                break
            hasher.update(data)
            output.write(data)
            total_bytes += len(data)
        digest = hasher.hexdigest()
        if digest != chunk.sha256:
            raise RestoreError(f"hash mismatch for {chunk.key}")
    return total_bytes


def restore_chain(
    client,
    bucket: str,
    manifests: Iterable[ManifestInfo],
    target: Path,
    *,
    wait_for_restore: bool,
    restore_tier: str,
    restore_timeout_seconds: int,
) -> int:
    if target.exists():
        raise RestoreError(f"target path already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    for manifest in manifests:
        if wait_for_restore:
            storage_class = manifest.s3.get("storage_class")
            ensure_chunks_restored(
                client,
                bucket,
                manifest.chunks,
                storage_class=storage_class,
                restore_tier=restore_tier,
                timeout_seconds=restore_timeout_seconds,
            )
        created, bytes_written = _apply_manifest_stream(
            client, bucket, manifest, target.parent
        )
        total_bytes += bytes_written
        if created != target:
            if created.exists():
                if target.exists():
                    _delete_subvolume(target)
                os.rename(created, target)
            else:
                raise RestoreError(f"received subvolume missing: {created}")
    if target.exists():
        _set_subvolume_writable(target)
    return total_bytes


def verify_metadata(
    target: Path,
    *,
    runner: callable = subprocess.run,
) -> None:
    if not target.is_dir():
        raise RestoreError(f"restore target is not a directory: {target}")
    if not os.access(target, os.W_OK):
        raise RestoreError(f"restore target is not writable: {target}")
    env = os.environ.copy()
    env["PATH"] = ensure_sbin_on_path(env.get("PATH", ""))
    result = runner(
        ["btrfs", "subvolume", "show", str(target)],
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )
    if _parse_uuid(result.stdout) is None:
        raise RestoreError("restore target has no valid UUID")


def verify_content(
    source: Path,
    target: Path,
    *,
    mode: str,
    sample_max_files: int,
) -> None:
    if not source.exists():
        raise RestoreError(f"source snapshot missing: {source}")
    if not source.is_dir():
        raise RestoreError(f"source snapshot is not a directory: {source}")
    source_dirs, source_files = _collect_entries(source)
    target_dirs, target_files = _collect_entries(target)

    mismatch = _check_missing_extra(source_dirs, target_dirs, "directory")
    if mismatch:
        raise RestoreError(mismatch)
    mismatch = _check_missing_extra(source_files, target_files, "file")
    if mismatch:
        raise RestoreError(mismatch)

    for rel_path in source_files:
        source_path = source / rel_path
        target_path = target / rel_path
        source_type = _entry_type(source_path)
        target_type = _entry_type(target_path)
        if source_type != target_type:
            raise RestoreError(f"type mismatch for {rel_path}")
        if source_type == "symlink":
            if os.readlink(source_path) != os.readlink(target_path):
                raise RestoreError(f"symlink mismatch for {rel_path}")

    regular_files = [
        rel_path
        for rel_path in source_files
        if _entry_type(source / rel_path) == "file"
    ]
    if mode == "full":
        files_to_check = regular_files
    elif mode == "sample":
        files_to_check = _select_sample(regular_files, sample_max_files)
    elif mode == "none":
        return
    else:
        raise RestoreError(f"unknown verify mode: {mode}")

    for rel_path in files_to_check:
        source_path = source / rel_path
        target_path = target / rel_path
        source_stat = source_path.stat()
        target_stat = target_path.stat()
        if source_stat.st_size != target_stat.st_size:
            raise RestoreError(f"size mismatch for {rel_path}")
        if _hash_file(source_path) != _hash_file(target_path):
            raise RestoreError(f"hash mismatch for {rel_path}")


def verify_restore(
    source: Path | None,
    target: Path,
    *,
    mode: str,
    sample_max_files: int,
    runner: callable = subprocess.run,
) -> None:
    if mode == "none":
        return
    verify_metadata(target, runner=runner)
    if source is None or not source.exists():
        return
    verify_content(
        source,
        target,
        mode=mode,
        sample_max_files=sample_max_files,
    )


def _apply_manifest_stream(
    client,
    bucket: str,
    manifest: ManifestInfo,
    receive_dir: Path,
) -> tuple[Path, int]:
    snapshot_path = manifest.snapshot_path
    if not snapshot_path:
        raise RestoreError(f"{manifest.key} missing snapshot path")
    subvol_name = Path(snapshot_path).name
    proc = subprocess.Popen(
        ["btrfs", "receive", str(receive_dir)],
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdin is not None
    stream_error: Exception | None = None
    try:
        bytes_written = download_and_verify_chunks(
            client, bucket, manifest.chunks, proc.stdin
        )
    except Exception as exc:
        stream_error = exc
    finally:
        proc.stdin.close()
        if stream_error is not None:
            stderr = _cleanup_btrfs_receive(proc)
            message = f"restore stream failed: {stream_error}"
            if stderr:
                message = f"{message}; btrfs receive error: {stderr}"
            raise RestoreError(message) from stream_error
    _stdout, stderr = proc.communicate()
    code = proc.returncode
    if code != 0:
        error = stderr.decode("utf-8", errors="replace").strip()
        if error:
            raise RestoreError(
                f"btrfs receive failed with exit code {code}: {error}"
            )
        raise RestoreError(f"btrfs receive failed with exit code {code}")
    return receive_dir / subvol_name, bytes_written


def _cleanup_btrfs_receive(
    process: subprocess.Popen[bytes],
    timeout: float = 5.0,
) -> str:
    try:
        if process.poll() is None:
            process.terminate()
        _stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        _stdout, stderr = process.communicate()
    return stderr.decode("utf-8", errors="replace").strip()


def _fetch_json(client, bucket: str, key: str) -> dict[str, Any]:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001 - surface key errors
        raise RestoreError(f"missing object {key}") from exc
    body = response["Body"].read()
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RestoreError(f"{key} invalid json") from exc
    if not isinstance(payload, dict):
        raise RestoreError(f"{key} must be a JSON object")
    return payload


def _parse_snapshot_path(payload: dict[str, Any], key: str) -> str | None:
    snapshot = payload.get("snapshot")
    if snapshot is None:
        return None
    if not isinstance(snapshot, dict):
        raise RestoreError(f"{key} invalid snapshot metadata")
    path = snapshot.get("path")
    if path is None:
        return None
    if not isinstance(path, str) or not path:
        raise RestoreError(f"{key} invalid snapshot path")
    return path


def _collect_entries(base_path: Path) -> tuple[list[str], list[str]]:
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


def _entry_type(path: Path) -> str:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return "missing"
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISDIR(mode):
        return "dir"
    return "other"


def _delete_subvolume(path: Path) -> None:
    env = os.environ.copy()
    env["PATH"] = ensure_sbin_on_path(env.get("PATH", ""))
    subprocess.run(
        ["btrfs", "subvolume", "delete", str(path)],
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )


def _set_subvolume_writable(path: Path) -> None:
    env = os.environ.copy()
    env["PATH"] = ensure_sbin_on_path(env.get("PATH", ""))
    subprocess.run(
        ["btrfs", "property", "set", "-f", "-ts", str(path), "ro", "false"],
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )


def _select_sample(paths: list[str], sample_max_files: int) -> list[str]:
    if sample_max_files <= 0:
        return []
    ordered = sorted(paths)
    if len(ordered) <= sample_max_files:
        return ordered
    return ordered[:sample_max_files]


def _hash_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


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
