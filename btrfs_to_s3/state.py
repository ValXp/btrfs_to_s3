"""Local state persistence.

The on-disk state file keeps the legacy ``subvolumes`` key for backward
compatibility. In-memory code should prefer the backend-neutral ``sources``
view instead.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class StateLoadError(RuntimeError):
    """Raised when a persisted state file cannot be read or parsed."""


@dataclass(frozen=True)
class SourceState:
    # last_snapshot stores the backend-specific snapshot identity.
    last_snapshot: str | None = None
    last_snapshot_name: str | None = None
    last_snapshot_path: str | None = None
    last_manifest: str | None = None
    last_full_at: str | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "SourceState":
        last_snapshot = data.get("last_snapshot")
        last_snapshot_name = data.get("last_snapshot_name")
        if last_snapshot_name is None and isinstance(last_snapshot, str):
            last_snapshot_name = Path(last_snapshot).name
        last_snapshot_path = data.get("last_snapshot_path")
        if last_snapshot_path is None and isinstance(last_snapshot, str):
            last_snapshot_path = _legacy_snapshot_path(last_snapshot)
        return SourceState(
            last_snapshot=last_snapshot,
            last_snapshot_name=last_snapshot_name,
            last_snapshot_path=last_snapshot_path,
            last_manifest=data.get("last_manifest"),
            last_full_at=data.get("last_full_at"),
        )

    @property
    def snapshot_identity(self) -> str | None:
        return self.last_snapshot

    @property
    def snapshot_name(self) -> str | None:
        if self.last_snapshot_name is not None:
            return self.last_snapshot_name
        if self.last_snapshot is None:
            return None
        return Path(self.last_snapshot).name

    @property
    def snapshot_path(self) -> str | None:
        if self.last_snapshot_path is not None:
            return self.last_snapshot_path
        if self.last_snapshot is None:
            return None
        return _legacy_snapshot_path(self.last_snapshot)

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_snapshot": self.last_snapshot,
            "last_snapshot_name": self.last_snapshot_name,
            "last_snapshot_path": self.last_snapshot_path,
            "last_manifest": self.last_manifest,
            "last_full_at": self.last_full_at,
        }


SubvolumeState = SourceState


@dataclass(frozen=True, init=False)
class State:
    sources: dict[str, SourceState] = field(default_factory=dict)
    last_run_at: str | None = None

    def __init__(
        self,
        *,
        sources: dict[str, SourceState] | None = None,
        subvolumes: dict[str, SourceState] | None = None,
        last_run_at: str | None = None,
    ) -> None:
        if sources is not None and subvolumes is not None:
            raise TypeError("pass only one of sources or subvolumes")
        object.__setattr__(self, "sources", dict(sources or subvolumes or {}))
        object.__setattr__(self, "last_run_at", last_run_at)

    @property
    def subvolumes(self) -> dict[str, SourceState]:
        return self.sources

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "State":
        raw_sources = data.get("sources")
        if raw_sources is None:
            raw_sources = data.get("subvolumes", {})
        sources = {
            name: SourceState.from_dict(value)
            for name, value in raw_sources.items()
        }
        return State(sources=sources, last_run_at=data.get("last_run_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "subvolumes": {
                name: source.to_dict()
                for name, source in self.sources.items()
            },
            "last_run_at": self.last_run_at,
        }


def load_state(path: Path) -> State:
    try:
        if not path.exists():
            return State()
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise StateLoadError(
            f"failed to parse state file {path}: {exc}"
        ) from exc
    except OSError as exc:
        raise StateLoadError(
            f"failed to read state file {path}: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise StateLoadError(f"state file {path} must be a JSON object")
    try:
        return State.from_dict(data)
    except (AttributeError, TypeError, ValueError) as exc:
        raise StateLoadError(f"invalid state file {path}: {exc}") from exc


def save_state(path: Path, state: State) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(state.to_dict(), handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp_path.replace(path)


def _legacy_snapshot_path(value: str) -> str | None:
    if "@" in value:
        return None
    separators = [sep for sep in (os.sep, os.altsep) if sep]
    if Path(value).is_absolute() or any(sep in value for sep in separators):
        return value
    return None
