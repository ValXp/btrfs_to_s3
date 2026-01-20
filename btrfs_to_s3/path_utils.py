"""Shared path helpers."""

from __future__ import annotations

import os


def ensure_sbin_on_path(path: str) -> str:
    parts = [entry for entry in path.split(os.pathsep) if entry]
    for entry in ("/usr/sbin", "/sbin"):
        if entry not in parts:
            parts.append(entry)
    return os.pathsep.join(parts)
