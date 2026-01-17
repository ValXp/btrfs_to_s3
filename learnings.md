Learnings
- FYI: `testing/config/test.env` is a tracked template even though `testing/.gitignore` ignores it, so edit with care to avoid committing secrets.
- FYI: `testing/harness/env.py` only accepts `KEY=VALUE` lines (optional `export`) and will raise on invalid entries; inline comments are treated as part of the value.
