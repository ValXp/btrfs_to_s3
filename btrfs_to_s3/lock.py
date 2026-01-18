"""Process lock handling."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class LockError(RuntimeError):
    """Raised when the lock cannot be acquired."""


@dataclass
class LockFile:
    path: Path
    pid: int | None = None
    _active: bool = False

    def acquire(self) -> "LockFile":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        pid = os.getpid()
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                existing_pid = _read_pid(self.path)
                if _pid_is_running(existing_pid):
                    raise LockError(
                        f"lock already held by pid {existing_pid}"
                    ) from None
                try:
                    self.path.unlink(missing_ok=True)
                except OSError:
                    raise LockError(
                        f"lock already held by pid {existing_pid}"
                    ) from None
                continue
            else:
                with os.fdopen(fd, "w") as handle:
                    handle.write(str(pid))
                    handle.flush()
                self.pid = pid
                self._active = True
                return self
        existing_pid = _read_pid(self.path)
        raise LockError(f"lock already held by pid {existing_pid}") from None

    def release(self) -> None:
        if not self._active:
            return
        try:
            self.path.unlink(missing_ok=True)
        finally:
            self._active = False

    def __enter__(self) -> "LockFile":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def _read_pid(path: Path) -> str:
    try:
        return path.read_text().strip() or "unknown"
    except OSError:
        return "unknown"


def _pid_is_running(pid_text: str) -> bool:
    try:
        pid = int(pid_text)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
