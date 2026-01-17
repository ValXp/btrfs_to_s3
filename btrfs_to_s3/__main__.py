"""Package entrypoint."""

from __future__ import annotations

from btrfs_to_s3.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
