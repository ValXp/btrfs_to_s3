"""Load .env-style files into the process environment."""

from __future__ import annotations

from typing import Final
import os
import re


_ENV_KEY_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_env(path: str, *, override: bool = False) -> dict[str, str]:
    """Parse a KEY=VALUE env file and load values into os.environ."""
    loaded: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as handle:
        for line_num, raw_line in enumerate(handle, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                raise ValueError(f"{path}:{line_num}: expected KEY=VALUE")
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not _ENV_KEY_RE.fullmatch(key):
                raise ValueError(f"{path}:{line_num}: invalid key {key!r}")
            value = _strip_quotes(value)
            loaded[key] = value
            if override or key not in os.environ:
                os.environ[key] = value
    return loaded


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value
