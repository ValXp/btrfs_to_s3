"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for 3.10
    import tomli as tomllib  # type: ignore[no-redef]

TOMLDecodeError = tomllib.TOMLDecodeError

MiB = 1024**2
GiB = 1024**3
MIN_SPOOL_SIZE_BYTES = 5 * MiB

DEFAULT_LOG_LEVEL = "info"
DEFAULT_STATE_PATH = "~/.local/state/btrfs_to_s3/state.json"
DEFAULT_LOCK_PATH = "/var/lock/btrfs_to_s3.lock"
DEFAULT_SPOOL_DIR = "/mnt/ssd/btrfs_to_s3_spool"
DEFAULT_SPOOL_SIZE_BYTES = 200 * GiB
DEFAULT_FULL_EVERY_DAYS = 180
DEFAULT_INCREMENTAL_EVERY_DAYS = 7
DEFAULT_RUN_AT = "02:00"
DEFAULT_SNAPSHOT_BASE_DIR = "/srv/snapshots"
DEFAULT_SNAPSHOT_RETAIN = 2
DEFAULT_CHUNK_SIZE_BYTES = 200 * GiB
DEFAULT_STORAGE_CLASS_CHUNKS = "DEEP_ARCHIVE"
DEFAULT_STORAGE_CLASS_MANIFEST = "STANDARD"
DEFAULT_S3_CONCURRENCY = 4
DEFAULT_S3_SPOOL_ENABLED = False
DEFAULT_S3_SSE = "AES256"
DEFAULT_RESTORE_TARGET_BASE_DIR = "/srv/restore"
DEFAULT_RESTORE_VERIFY_MODE = "full"
DEFAULT_RESTORE_SAMPLE_MAX_FILES = 1000
DEFAULT_RESTORE_WAIT_FOR_RESTORE = True
DEFAULT_RESTORE_TIMEOUT_SECONDS = 72 * 60 * 60
DEFAULT_RESTORE_TIER = "Standard"
DEFAULT_FILESYSTEM_BACKEND = "btrfs"
VALID_FILESYSTEM_BACKENDS = frozenset({"btrfs", "zfs"})


class ConfigError(ValueError):
    """Raised when configuration is invalid."""


@dataclass(frozen=True)
class GlobalConfig:
    log_level: str
    state_path: Path
    lock_path: Path
    spool_dir: Path
    spool_size_bytes: int


@dataclass(frozen=True)
class ScheduleConfig:
    full_every_days: int
    incremental_every_days: int
    run_at: str


@dataclass(frozen=True)
class SnapshotsConfig:
    base_dir: Path | None
    retain: int


@dataclass(frozen=True)
class SubvolumesConfig:
    paths: tuple[Path, ...]


@dataclass(frozen=True)
class FilesystemConfig:
    backend: str


@dataclass(frozen=True)
class ZFSConfig:
    pool_name: str
    mount_root: Path
    source_datasets: tuple[str, ...]
    receive_parent_dataset: str | None
    snapshot_prefix: str


@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: str
    prefix: str
    chunk_size_bytes: int
    storage_class_chunks: str
    storage_class_manifest: str
    concurrency: int
    spool_enabled: bool
    sse: str


@dataclass(frozen=True)
class RestoreConfig:
    target_base_dir: Path
    verify_mode: str
    sample_max_files: int
    wait_for_restore: bool
    restore_timeout_seconds: int
    restore_tier: str


@dataclass(frozen=True)
class Config:
    global_cfg: GlobalConfig
    schedule: ScheduleConfig
    snapshots: SnapshotsConfig
    subvolumes: SubvolumesConfig
    s3: S3Config
    restore: RestoreConfig
    filesystem: FilesystemConfig = field(
        default_factory=lambda: FilesystemConfig(
            backend=DEFAULT_FILESYSTEM_BACKEND
        )
    )
    zfs: ZFSConfig | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Config":
        filesystem = FilesystemConfig(
            backend=_load_backend(data.get("filesystem"))
        )
        global_data = data.get("global", {})
        schedule_data = data.get("schedule", {})
        snapshots_data = data.get("snapshots", {})
        subvolumes_data = data.get("subvolumes", {})
        s3_data = data.get("s3", {})
        restore_data = data.get("restore", {})
        zfs_data = data.get("zfs")

        if not isinstance(global_data, dict):
            raise ConfigError("global must be a table")
        if not isinstance(schedule_data, dict):
            raise ConfigError("schedule must be a table")
        if not isinstance(snapshots_data, dict):
            raise ConfigError("snapshots must be a table")
        if not isinstance(subvolumes_data, dict):
            raise ConfigError("subvolumes must be a table")
        if not isinstance(s3_data, dict):
            raise ConfigError("s3 must be a table")
        if not isinstance(restore_data, dict):
            raise ConfigError("restore must be a table")

        global_cfg = GlobalConfig(
            log_level=str(global_data.get("log_level", DEFAULT_LOG_LEVEL)),
            state_path=_expand_path(
                global_data.get("state_path", DEFAULT_STATE_PATH)
            ),
            lock_path=_expand_path(global_data.get("lock_path", DEFAULT_LOCK_PATH)),
            spool_dir=_expand_path(global_data.get("spool_dir", DEFAULT_SPOOL_DIR)),
            spool_size_bytes=int(
                global_data.get("spool_size_bytes", DEFAULT_SPOOL_SIZE_BYTES)
            ),
        )
        schedule = ScheduleConfig(
            full_every_days=int(
                schedule_data.get("full_every_days", DEFAULT_FULL_EVERY_DAYS)
            ),
            incremental_every_days=int(
                schedule_data.get(
                    "incremental_every_days", DEFAULT_INCREMENTAL_EVERY_DAYS
                )
            ),
            run_at=str(schedule_data.get("run_at", DEFAULT_RUN_AT)),
        )
        snapshots = SnapshotsConfig(
            base_dir=_load_snapshot_base_dir(
                snapshots_data, filesystem.backend
            ),
            retain=int(snapshots_data.get("retain", DEFAULT_SNAPSHOT_RETAIN)),
        )
        subvolume_paths = _load_subvolume_paths(subvolumes_data)
        s3 = S3Config(
            bucket=str(s3_data.get("bucket", "")),
            region=str(s3_data.get("region", "")),
            prefix=str(s3_data.get("prefix", "")),
            chunk_size_bytes=int(
                s3_data.get("chunk_size_bytes", DEFAULT_CHUNK_SIZE_BYTES)
            ),
            storage_class_chunks=str(
                s3_data.get("storage_class_chunks", DEFAULT_STORAGE_CLASS_CHUNKS)
            ),
            storage_class_manifest=str(
                s3_data.get("storage_class_manifest", DEFAULT_STORAGE_CLASS_MANIFEST)
            ),
            concurrency=int(s3_data.get("concurrency", DEFAULT_S3_CONCURRENCY)),
            spool_enabled=bool(
                s3_data.get("spool_enabled", DEFAULT_S3_SPOOL_ENABLED)
            ),
            sse=str(s3_data.get("sse", DEFAULT_S3_SSE)),
        )
        restore = RestoreConfig(
            target_base_dir=_expand_path(
                restore_data.get(
                    "target_base_dir", DEFAULT_RESTORE_TARGET_BASE_DIR
                )
            ),
            verify_mode=str(
                restore_data.get("verify_mode", DEFAULT_RESTORE_VERIFY_MODE)
            ),
            sample_max_files=int(
                restore_data.get(
                    "sample_max_files", DEFAULT_RESTORE_SAMPLE_MAX_FILES
                )
            ),
            wait_for_restore=bool(
                restore_data.get(
                    "wait_for_restore", DEFAULT_RESTORE_WAIT_FOR_RESTORE
                )
            ),
            restore_timeout_seconds=int(
                restore_data.get(
                    "restore_timeout_seconds", DEFAULT_RESTORE_TIMEOUT_SECONDS
                )
            ),
            restore_tier=str(
                restore_data.get("restore_tier", DEFAULT_RESTORE_TIER)
            ),
        )
        config = Config(
            global_cfg=global_cfg,
            schedule=schedule,
            snapshots=snapshots,
            subvolumes=SubvolumesConfig(paths=subvolume_paths),
            s3=s3,
            restore=restore,
            filesystem=filesystem,
            zfs=_load_zfs_config(zfs_data, filesystem.backend),
        )
        validate_config(config)
        return config


def load_config(path: Path) -> Config:
    if not path.is_absolute():
        raise ConfigError(f"config path must be absolute: {path}")
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except TOMLDecodeError as exc:
        raise ConfigError(f"failed to parse config: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"failed to read config: {exc}") from exc
    return Config.from_dict(data)


def validate_config(config: Config) -> None:
    _validate_log_level(config.global_cfg.log_level)
    _validate_path(config.global_cfg.state_path, "global.state_path")
    _validate_path(config.global_cfg.lock_path, "global.lock_path")
    _validate_path(config.global_cfg.spool_dir, "global.spool_dir")
    if config.global_cfg.spool_size_bytes < MIN_SPOOL_SIZE_BYTES:
        raise ConfigError("global.spool_size_bytes must be >= 5 MiB")

    _validate_positive(config.schedule.full_every_days, "schedule.full_every_days")
    _validate_positive(
        config.schedule.incremental_every_days, "schedule.incremental_every_days"
    )
    _validate_run_at(config.schedule.run_at)

    _validate_backend(config.filesystem.backend)
    if config.snapshots.base_dir is not None:
        _validate_path(config.snapshots.base_dir, "snapshots.base_dir")
    if config.snapshots.retain < 1:
        raise ConfigError("snapshots.retain must be >= 1")

    if config.filesystem.backend == "btrfs":
        if config.snapshots.base_dir is None:
            raise ConfigError(
                'snapshots.base_dir is required when filesystem.backend = "btrfs"'
            )
        for path in config.subvolumes.paths:
            _validate_path(path, "subvolumes.paths")
        _validate_unique_btrfs_source_identifiers(config.subvolumes.paths)
    elif config.zfs is None:
        raise ConfigError(
            'zfs section is required when filesystem.backend = "zfs"'
        )
    else:
        _validate_zfs_config(config.zfs)

    if not config.s3.bucket:
        raise ConfigError("s3.bucket is required")
    if not config.s3.region:
        raise ConfigError("s3.region is required")
    if not config.s3.prefix:
        raise ConfigError("s3.prefix is required")
    _validate_positive(config.s3.chunk_size_bytes, "s3.chunk_size_bytes")
    if config.s3.concurrency < 1:
        raise ConfigError("s3.concurrency must be >= 1")
    if not isinstance(config.s3.spool_enabled, bool):
        raise ConfigError("s3.spool_enabled must be true or false")
    if not config.s3.storage_class_chunks:
        raise ConfigError("s3.storage_class_chunks is required")
    if not config.s3.storage_class_manifest:
        raise ConfigError("s3.storage_class_manifest is required")
    if not config.s3.sse:
        raise ConfigError("s3.sse is required")

    _validate_path(config.restore.target_base_dir, "restore.target_base_dir")
    if config.restore.verify_mode not in {"full", "sample", "none"}:
        raise ConfigError("restore.verify_mode must be full, sample, or none")
    _validate_positive(
        config.restore.sample_max_files, "restore.sample_max_files"
    )
    _validate_positive(
        config.restore.restore_timeout_seconds, "restore.restore_timeout_seconds"
    )
    if not config.restore.restore_tier:
        raise ConfigError("restore.restore_tier is required")


def _expand_path(raw: Any) -> Path:
    return Path(str(raw)).expanduser()


def _load_backend(raw: Any) -> str:
    if raw is None:
        return DEFAULT_FILESYSTEM_BACKEND
    if not isinstance(raw, dict):
        raise ConfigError("filesystem must be a table")
    backend = raw.get("backend")
    if not isinstance(backend, str) or not backend:
        raise ConfigError("filesystem.backend is required")
    return backend


def _load_snapshot_base_dir(
    snapshots_data: dict[str, Any],
    backend: str,
) -> Path | None:
    if "base_dir" in snapshots_data:
        return _expand_path(snapshots_data["base_dir"])
    if backend == "zfs":
        return None
    return _expand_path(DEFAULT_SNAPSHOT_BASE_DIR)


def _load_subvolume_paths(
    subvolumes_data: dict[str, Any],
) -> tuple[Path, ...]:
    raw_paths = subvolumes_data.get("paths", [])
    if raw_paths is None:
        return ()
    if not isinstance(raw_paths, list):
        raise ConfigError("subvolumes.paths must be a list of paths")
    return tuple(_expand_path(path) for path in raw_paths)


def _load_zfs_config(raw: Any, backend: str) -> ZFSConfig | None:
    if backend != "zfs":
        return None
    if not isinstance(raw, dict):
        raise ConfigError(
            'zfs section is required when filesystem.backend = "zfs"'
        )
    mount_root = raw.get("mount_root")
    source_datasets = raw.get("source_datasets", [])
    if source_datasets is None:
        source_datasets = []
    if not isinstance(source_datasets, list):
        raise ConfigError("zfs.source_datasets must be a list of dataset names")
    normalized_sources = []
    for dataset in source_datasets:
        if not isinstance(dataset, str) or not dataset:
            raise ConfigError(
                "zfs.source_datasets must be a list of dataset names"
            )
        normalized_sources.append(dataset)
    return ZFSConfig(
        pool_name=_require_non_empty_string(raw.get("pool_name"), "zfs.pool_name"),
        mount_root=_expand_path(
            _require_non_empty_string(mount_root, "zfs.mount_root")
        ),
        source_datasets=tuple(normalized_sources),
        receive_parent_dataset=_load_optional_non_empty_string(
            raw.get("receive_parent_dataset"),
            "zfs.receive_parent_dataset",
        ),
        snapshot_prefix=_require_non_empty_string(
            raw.get("snapshot_prefix"),
            "zfs.snapshot_prefix",
        ),
    )


def _require_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{field} is required")
    return value


def _load_optional_non_empty_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{field} must be a non-empty string when set")
    return value


def _validate_path(path: Path, field: str) -> None:
    if not path.is_absolute():
        raise ConfigError(f"{field} must be an absolute path: {path}")


def _validate_backend(value: str) -> None:
    if value not in VALID_FILESYSTEM_BACKENDS:
        raise ConfigError(
            f"filesystem.backend must be one of {sorted(VALID_FILESYSTEM_BACKENDS)}; got {value}"
        )


def _validate_positive(value: int, field: str) -> None:
    if value <= 0:
        raise ConfigError(f"{field} must be > 0")


def _validate_log_level(value: str) -> None:
    valid = {"debug", "info", "warning", "error", "critical"}
    if value.lower() not in valid:
        raise ConfigError(
            f"global.log_level must be one of {sorted(valid)}; got {value}"
        )


def _validate_run_at(value: str) -> None:
    parts = value.split(":")
    if len(parts) != 2:
        raise ConfigError("schedule.run_at must be HH:MM")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError as exc:
        raise ConfigError("schedule.run_at must be HH:MM") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ConfigError("schedule.run_at must be HH:MM")


def _validate_zfs_config(config: ZFSConfig) -> None:
    _validate_path(config.mount_root, "zfs.mount_root")
    if not config.pool_name:
        raise ConfigError("zfs.pool_name is required")
    _validate_unique_zfs_source_identifiers(
        config.pool_name,
        config.source_datasets,
    )
    if not config.snapshot_prefix:
        raise ConfigError("zfs.snapshot_prefix is required")


def qualify_zfs_dataset(pool_name: str, dataset: str) -> str:
    if dataset == pool_name or dataset.startswith(pool_name + "/"):
        return dataset
    return f"{pool_name}/{dataset}"


def _validate_unique_btrfs_source_identifiers(paths: tuple[Path, ...]) -> None:
    duplicates = _duplicate_identifiers(path.name for path in paths)
    if duplicates:
        raise ConfigError(
            "duplicate source identifiers in subvolumes.paths: "
            + ", ".join(duplicates)
        )


def _validate_unique_zfs_source_identifiers(
    pool_name: str,
    datasets: tuple[str, ...],
) -> None:
    duplicates = _duplicate_identifiers(
        qualify_zfs_dataset(pool_name, dataset) for dataset in datasets
    )
    if duplicates:
        raise ConfigError(
            "duplicate source identifiers in zfs.source_datasets after pool-name normalization: "
            + ", ".join(duplicates)
        )


def _duplicate_identifiers(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
            continue
        seen.add(value)
    return tuple(duplicates)
