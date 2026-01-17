"""Planner for full vs incremental backups."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from btrfs_to_s3.config import Config
from btrfs_to_s3.snapshots import parse_snapshot_name
from btrfs_to_s3.state import State


@dataclass(frozen=True)
class PlanItem:
    subvolume: str
    action: str
    parent_snapshot: str | None
    reason: str


def plan_backups(
    config: Config,
    state: State,
    now: datetime,
    available_snapshots: Iterable[str] | None = None,
) -> list[PlanItem]:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    available = set(available_snapshots) if available_snapshots is not None else None
    plans: list[PlanItem] = []
    for subvolume_path in config.subvolumes.paths:
        name = _subvolume_name(subvolume_path)
        sub_state = state.subvolumes.get(name)
        plans.append(
            _plan_subvolume(
                name,
                sub_state,
                config.schedule.full_every_days,
                config.schedule.incremental_every_days,
                now,
                available,
            )
        )
    return plans


def _plan_subvolume(
    name: str,
    sub_state,
    full_every_days: int,
    incremental_every_days: int,
    now: datetime,
    available_snapshots: set[str] | None,
) -> PlanItem:
    last_full_at = _parse_iso_timestamp(
        sub_state.last_full_at if sub_state else None
    )
    if last_full_at is None or now - last_full_at >= timedelta(days=full_every_days):
        return PlanItem(
            subvolume=name,
            action="full",
            parent_snapshot=None,
            reason="full_due",
        )

    last_snapshot = sub_state.last_snapshot if sub_state else None
    if not last_snapshot:
        return PlanItem(
            subvolume=name,
            action="full",
            parent_snapshot=None,
            reason="missing_parent",
        )
    if available_snapshots is not None and last_snapshot not in available_snapshots:
        return PlanItem(
            subvolume=name,
            action="full",
            parent_snapshot=None,
            reason="missing_parent",
        )

    last_snapshot_at = _parse_snapshot_timestamp(last_snapshot)
    if last_snapshot_at is None:
        return PlanItem(
            subvolume=name,
            action="inc",
            parent_snapshot=last_snapshot,
            reason="incremental_due",
        )
    if now - last_snapshot_at < timedelta(days=incremental_every_days):
        return PlanItem(
            subvolume=name,
            action="skip",
            parent_snapshot=last_snapshot,
            reason="incremental_not_due",
        )
    return PlanItem(
        subvolume=name,
        action="inc",
        parent_snapshot=last_snapshot,
        reason="incremental_due",
    )


def _subvolume_name(path: Path) -> str:
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
