"""Planner for full vs incremental backups."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from btrfs_to_s3.config import Config
from btrfs_to_s3.snapshots import parse_snapshot_name
from btrfs_to_s3.state import State


@dataclass(frozen=True, init=False)
class PlanItem:
    source_name: str
    action: str
    parent_snapshot: str | None
    reason: str

    def __init__(
        self,
        *,
        source_name: str | None = None,
        subvolume: str | None = None,
        action: str,
        parent_snapshot: str | None,
        reason: str,
    ) -> None:
        if source_name is not None and subvolume is not None:
            raise TypeError("pass only one of source_name or subvolume")
        resolved_name = source_name if source_name is not None else subvolume
        if resolved_name is None:
            raise TypeError("source_name is required")
        object.__setattr__(self, "source_name", resolved_name)
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "parent_snapshot", parent_snapshot)
        object.__setattr__(self, "reason", reason)

    @property
    def subvolume(self) -> str:
        return self.source_name


def plan_backups(
    config: Config,
    state: State,
    now: datetime,
    available_snapshots: Iterable[str] | None = None,
    source_names: Iterable[str] | None = None,
) -> list[PlanItem]:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    available = (
        {_snapshot_basename(name) for name in available_snapshots}
        if available_snapshots is not None
        else None
    )
    names = (
        tuple(source_names)
        if source_names is not None
        else _config_source_names(config)
    )
    if not _scheduled_run_due(now, config.schedule.run_at, state.last_run_at):
        return [
            _scheduled_skip_plan(name, state.sources.get(name))
            for name in names
        ]
    plans: list[PlanItem] = []
    for name in names:
        source_state = state.sources.get(name)
        plans.append(
            _plan_source(
                name,
                source_state,
                config.schedule.full_every_days,
                config.schedule.incremental_every_days,
                now,
                available,
            )
        )
    return plans


def _config_source_names(config: Config) -> tuple[str, ...]:
    if config.filesystem.backend == "zfs" and config.zfs is not None:
        return config.zfs.source_datasets
    return tuple(_btrfs_source_name(path) for path in config.subvolumes.paths)


def _scheduled_run_due(
    now: datetime,
    run_at: str,
    last_run_at: str | None,
) -> bool:
    today_run_at = _scheduled_run_at(now, run_at)
    if now < today_run_at:
        return False
    last_run = _parse_iso_timestamp(last_run_at)
    if last_run is None:
        return True
    return last_run.astimezone(now.tzinfo) < today_run_at


def _scheduled_run_at(now: datetime, run_at: str) -> datetime:
    hour, minute = (int(part) for part in run_at.split(":", 1))
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _scheduled_skip_plan(name: str, source_state) -> PlanItem:
    return PlanItem(
        source_name=name,
        action="skip",
        parent_snapshot=source_state.last_snapshot if source_state else None,
        reason="scheduled_run_not_due",
    )


def _plan_source(
    name: str,
    source_state,
    full_every_days: int,
    incremental_every_days: int,
    now: datetime,
    available_snapshots: set[str] | None,
) -> PlanItem:
    last_full_at = _parse_iso_timestamp(
        source_state.last_full_at if source_state else None
    )
    if last_full_at is None or now - last_full_at >= timedelta(days=full_every_days):
        return PlanItem(
            source_name=name,
            action="full",
            parent_snapshot=None,
            reason="full_due",
        )

    last_snapshot = source_state.last_snapshot if source_state else None
    if not last_snapshot:
        return PlanItem(
            source_name=name,
            action="full",
            parent_snapshot=None,
            reason="missing_parent",
        )
    last_manifest = source_state.last_manifest if source_state else None
    if not last_manifest:
        return PlanItem(
            source_name=name,
            action="full",
            parent_snapshot=None,
            reason="missing_manifest",
        )
    last_snapshot_name = (
        source_state.snapshot_name if source_state else None
    )
    if not last_snapshot_name:
        return PlanItem(
            source_name=name,
            action="full",
            parent_snapshot=None,
            reason="missing_parent",
        )
    if available_snapshots is not None and last_snapshot_name not in available_snapshots:
        return PlanItem(
            source_name=name,
            action="full",
            parent_snapshot=None,
            reason="missing_parent",
        )

    last_snapshot_at = _parse_snapshot_timestamp(last_snapshot_name)
    if last_snapshot_at is None:
        return PlanItem(
            source_name=name,
            action="inc",
            parent_snapshot=last_snapshot,
            reason="incremental_due",
        )
    if now - last_snapshot_at < timedelta(days=incremental_every_days):
        return PlanItem(
            source_name=name,
            action="skip",
            parent_snapshot=last_snapshot,
            reason="incremental_not_due",
        )
    return PlanItem(
        source_name=name,
        action="inc",
        parent_snapshot=last_snapshot,
        reason="incremental_due",
    )


def _btrfs_source_name(path: Path) -> str:
    return path.name


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_snapshot_timestamp(name: str) -> datetime | None:
    parsed = parse_snapshot_name(name)
    if parsed is None:
        return None
    return parsed[1]


def _snapshot_basename(value: str) -> str:
    return Path(value).name
