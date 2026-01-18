Learnings
- FYI: `btrfs_to_s3` currently only implements `backup`; `integration_tests/scripts/run_restore.py` assumes the upcoming `restore` subcommand and will fail until it is available.
- FYI: `integration_tests/scripts/run_large.py` expects multi-chunk manifests, but the current `btrfs_to_s3` CLI stub always writes a single chunk, so the large scenario will fail until chunked uploads are implemented.
