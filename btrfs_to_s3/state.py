"""Local state persistence."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SubvolumeState:
    last_snapshot: str | None = None
    last_manifest: str | None = None
    last_full_at: str | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "SubvolumeState":
        return SubvolumeState(
            last_snapshot=data.get("last_snapshot"),
            last_manifest=data.get("last_manifest"),
            last_full_at=data.get("last_full_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_snapshot": self.last_snapshot,
            "last_manifest": self.last_manifest,
            "last_full_at": self.last_full_at,
        }


@dataclass(frozen=True)
class State:
    subvolumes: dict[str, SubvolumeState] = field(default_factory=dict)
    last_run_at: str | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "State":
        subvolumes = {
            name: SubvolumeState.from_dict(value)
            for name, value in data.get("subvolumes", {}).items()
        }
        return State(subvolumes=subvolumes, last_run_at=data.get("last_run_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "subvolumes": {
                name: subvolume.to_dict()
                for name, subvolume in self.subvolumes.items()
            },
            "last_run_at": self.last_run_at,
        }


def load_state(path: Path) -> State:
    if not path.exists():
        return State()
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return State.from_dict(data)


def save_state(path: Path, state: State) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(state.to_dict(), handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp_path.replace(path)
