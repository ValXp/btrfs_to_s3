# TODO

- [x] Make restore work from a reconstructed config without requiring the original `subvolumes.paths` or `zfs.source_datasets`, and avoid requiring `--source` when an explicit manifest key is enough.
- [x] Add S3-backed discovery/list commands for restorable sources and available manifests under a configured bucket/prefix.
- [ ] Implement remote retention so old full/incremental generations can be pruned from S3 after a newer full backup becomes current.
- [ ] Expand restore archive handling beyond `GLACIER` and `DEEP_ARCHIVE` so restore behavior matches the requirement to handle all relevant S3 storage classes.
