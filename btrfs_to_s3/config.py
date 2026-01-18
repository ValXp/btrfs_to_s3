"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for 3.10
    import tomli as tomllib  # type: ignore[no-redef]

GiB = 1024**3

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
DEFAULT_S3_SSE = "AES256"
DEFAULT_RESTORE_TARGET_BASE_DIR = "/srv/restore"
DEFAULT_RESTORE_VERIFY_MODE = "full"
DEFAULT_RESTORE_SAMPLE_MAX_FILES = 1000
DEFAULT_RESTORE_WAIT_FOR_RESTORE = True
DEFAULT_RESTORE_TIMEOUT_SECONDS = 72 * 60 * 60
DEFAULT_RESTORE_TIER = "Standard"


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
    base_dir: Path
    retain: int


@dataclass(frozen=True)
class SubvolumesConfig:
    paths: tuple[Path, ...]


@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: str
    prefix: str
    chunk_size_bytes: int
    storage_class_chunks: str
    storage_class_manifest: str
    concurrency: int
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

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Config":
        global_data = data.get("global", {})
        schedule_data = data.get("schedule", {})
        snapshots_data = data.get("snapshots", {})
        subvolumes_data = data.get("subvolumes", {})
        s3_data = data.get("s3", {})
        restore_data = data.get("restore", {})

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
            base_dir=_expand_path(
                snapshots_data.get("base_dir", DEFAULT_SNAPSHOT_BASE_DIR)
            ),
            retain=int(snapshots_data.get("retain", DEFAULT_SNAPSHOT_RETAIN)),
        )
        subvolume_paths = tuple(
            _expand_path(path) for path in subvolumes_data.get("paths", [])
        )
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
    except OSError as exc:
        raise ConfigError(f"failed to read config: {exc}") from exc
    return Config.from_dict(data)


def validate_config(config: Config) -> None:
    _validate_log_level(config.global_cfg.log_level)
    _validate_path(config.global_cfg.state_path, "global.state_path")
    _validate_path(config.global_cfg.lock_path, "global.lock_path")
    _validate_path(config.global_cfg.spool_dir, "global.spool_dir")
    _validate_positive(config.global_cfg.spool_size_bytes, "global.spool_size_bytes")

    _validate_positive(config.schedule.full_every_days, "schedule.full_every_days")
    _validate_positive(
        config.schedule.incremental_every_days, "schedule.incremental_every_days"
    )
    _validate_run_at(config.schedule.run_at)

    _validate_path(config.snapshots.base_dir, "snapshots.base_dir")
    if config.snapshots.retain < 1:
        raise ConfigError("snapshots.retain must be >= 1")

    if not config.subvolumes.paths:
        raise ConfigError("subvolumes.paths must include at least one path")
    for path in config.subvolumes.paths:
        _validate_path(path, "subvolumes.paths")

    if not config.s3.bucket:
        raise ConfigError("s3.bucket is required")
    if not config.s3.region:
        raise ConfigError("s3.region is required")
    if not config.s3.prefix:
        raise ConfigError("s3.prefix is required")
    _validate_positive(config.s3.chunk_size_bytes, "s3.chunk_size_bytes")
    if config.s3.concurrency < 1:
        raise ConfigError("s3.concurrency must be >= 1")
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


def _validate_path(path: Path, field: str) -> None:
    if not path.is_absolute():
        raise ConfigError(f"{field} must be an absolute path: {path}")


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
