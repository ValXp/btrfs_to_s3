Learnings
- FYI: `testing/config/test.env` is a tracked template even though `testing/.gitignore` ignores it, so edit with care to avoid committing secrets.
- FYI: `testing/harness/env.py` only accepts `KEY=VALUE` lines (optional `export`) and will raise on invalid entries; inline comments are treated as part of the value.
- FYI: `python` is not available in the current shell; use `python3.14` (or add a `python` shim) when running harness commands.
- FYI: `testing/scripts/setup_btrfs.py` chowns `testing/run/` to `SUDO_USER` so non-root scripts can run after setup; teardown still needs sudo.
