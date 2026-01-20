"""Restore helper tests."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from btrfs_to_s3 import restore


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def read(self, size: int | None = None) -> bytes:
        if size is None:
            size = len(self._payload) - self._offset
        if size < 0:
            return b""
        start = self._offset
        end = min(len(self._payload), start + size)
        self._offset = end
        return self._payload[start:end]


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.restore_requests: list[str] = []
        self.restore_headers: dict[str, list[str | None]] = {}

    def get_object(self, Bucket: str, Key: str) -> dict[str, object]:
        if Key not in self.objects:
            raise KeyError(Key)
        payload = self.objects[Key]
        if isinstance(payload, FakeBody):
            return {"Body": payload}
        return {"Body": FakeBody(payload)}

    def head_object(self, Bucket: str, Key: str) -> dict[str, object]:
        if Key not in self.objects:
            raise KeyError(Key)
        headers = self.restore_headers.get(Key, [None])
        header = headers[0]
        if len(headers) > 1:
            headers.pop(0)
        return {"Restore": header}

    def restore_object(self, Bucket: str, Key: str, RestoreRequest: dict) -> None:
        if Key not in self.objects:
            raise KeyError(Key)
        self.restore_requests.append(Key)


class RestoreTests(unittest.TestCase):
    def test_needs_restore_handles_empty(self) -> None:
        self.assertFalse(restore.needs_restore(None))

    def test_restore_header_unknown_is_false(self) -> None:
        self.assertFalse(restore.is_restore_ready("not-a-header"))

    def test_resolve_manifest_chain_orders_full_first(self) -> None:
        client = FakeS3()
        full_manifest = {
            "kind": "full",
            "parent_manifest": None,
            "chunks": [
                {"key": "full/chunk.bin", "sha256": "abc", "size": 1}
            ],
            "s3": {"storage_class": "STANDARD"},
        }
        inc_manifest = {
            "kind": "inc",
            "parent_manifest": "full/manifest.json",
            "chunks": [
                {"key": "inc/chunk.bin", "sha256": "def", "size": 1}
            ],
            "s3": {"storage_class": "STANDARD"},
        }
        client.objects["full/manifest.json"] = json.dumps(full_manifest).encode(
            "utf-8"
        )
        client.objects["inc/manifest.json"] = json.dumps(inc_manifest).encode(
            "utf-8"
        )

        manifests = restore.resolve_manifest_chain(
            client, "bucket", "inc/manifest.json"
        )

        self.assertEqual([item.key for item in manifests], [
            "full/manifest.json",
            "inc/manifest.json",
        ])
        self.assertEqual(manifests[0].kind, "full")

    def test_missing_parent_manifest_reports_key(self) -> None:
        client = FakeS3()
        inc_manifest = {
            "kind": "inc",
            "parent_manifest": "missing/manifest.json",
            "chunks": [
                {"key": "inc/chunk.bin", "sha256": "def", "size": 1}
            ],
            "s3": {"storage_class": "STANDARD"},
        }
        client.objects["inc/manifest.json"] = json.dumps(inc_manifest).encode(
            "utf-8"
        )
        with self.assertRaises(restore.RestoreError) as context:
            restore.resolve_manifest_chain(
                client, "bucket", "inc/manifest.json"
            )
        self.assertIn("missing/manifest.json", str(context.exception))

    def test_restore_header_parsing(self) -> None:
        self.assertTrue(restore.needs_restore("GLACIER"))
        self.assertTrue(restore.needs_restore("DEEP_ARCHIVE"))
        self.assertTrue(restore.needs_restore("GLACIER_IR"))
        self.assertFalse(restore.needs_restore("STANDARD"))
        self.assertFalse(restore.needs_restore("STANDARD_IA"))

        header_ready = 'ongoing-request="false", expiry-date="Tue, 01 Jan 2030 00:00:00 GMT"'
        header_pending = 'ongoing-request="true"'
        self.assertTrue(restore.is_restore_ready(header_ready))
        self.assertFalse(restore.is_restore_ready(header_pending))
        self.assertFalse(restore.is_restore_ready(None))

    def test_restore_timeout_raises(self) -> None:
        client = FakeS3()
        client.objects["chunk.bin"] = b"payload"
        client.restore_headers["chunk.bin"] = ['ongoing-request="true"']
        chunk = restore.ChunkInfo(key="chunk.bin", sha256="x", size=None)

        time_state = {"now": 0.0}

        def time_fn() -> float:
            return time_state["now"]

        def sleep_fn(seconds: float) -> None:
            time_state["now"] += seconds

        with self.assertRaises(restore.RestoreError) as context:
            restore.ensure_chunks_restored(
                client,
                "bucket",
                [chunk],
                storage_class="GLACIER",
                restore_tier="Standard",
                timeout_seconds=2,
                sleep=sleep_fn,
                time_fn=time_fn,
            )
        self.assertIn("timeout", str(context.exception))

    def test_resolve_manifest_chain_detects_loop(self) -> None:
        client = FakeS3()
        manifest = {
            "kind": "inc",
            "parent_manifest": "loop/manifest.json",
            "chunks": [{"key": "chunk.bin", "sha256": "abc", "size": 1}],
        }
        client.objects["loop/manifest.json"] = json.dumps(manifest).encode(
            "utf-8"
        )

        with self.assertRaises(restore.RestoreError) as context:
            restore.resolve_manifest_chain(
                client, "bucket", "loop/manifest.json"
            )
        self.assertIn("loop detected", str(context.exception))

    def test_resolve_manifest_chain_requires_full(self) -> None:
        client = FakeS3()
        manifest = {
            "kind": "inc",
            "parent_manifest": None,
            "chunks": [{"key": "chunk.bin", "sha256": "abc", "size": 1}],
        }
        client.objects["inc/manifest.json"] = json.dumps(manifest).encode(
            "utf-8"
        )

        with self.assertRaises(restore.RestoreError) as context:
            restore.resolve_manifest_chain(
                client, "bucket", "inc/manifest.json"
            )
        self.assertIn("full backup", str(context.exception))

    def test_fetch_current_manifest_key(self) -> None:
        client = FakeS3()
        client.objects["current.json"] = json.dumps(
            {"manifest_key": "snap/manifest.json"}
        ).encode("utf-8")

        result = restore.fetch_current_manifest_key(
            client, "bucket", "current.json"
        )

        self.assertEqual(result, "snap/manifest.json")

    def test_fetch_current_manifest_key_missing(self) -> None:
        client = FakeS3()
        client.objects["current.json"] = json.dumps({}).encode("utf-8")

        with self.assertRaises(restore.RestoreError) as context:
            restore.fetch_current_manifest_key(
                client, "bucket", "current.json"
            )
        self.assertIn("manifest_key", str(context.exception))

    def test_fetch_json_errors(self) -> None:
        client = FakeS3()
        client.objects["bad.json"] = b"{not json"
        with self.assertRaises(restore.RestoreError) as context:
            restore._fetch_json(client, "bucket", "bad.json")
        self.assertIn("invalid json", str(context.exception))

        client.objects["list.json"] = json.dumps([1, 2, 3]).encode("utf-8")
        with self.assertRaises(restore.RestoreError) as context:
            restore._fetch_json(client, "bucket", "list.json")
        self.assertIn("must be a JSON object", str(context.exception))

        with self.assertRaises(restore.RestoreError) as context:
            restore._fetch_json(client, "bucket", "missing.json")
        self.assertIn("missing object", str(context.exception))

    def test_parse_manifest_errors(self) -> None:
        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest({}, "manifest.json")

        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest(
                {"kind": "full", "parent_manifest": 123, "chunks": []},
                "manifest.json",
            )

        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest(
                {"kind": "full", "chunks": []},
                "manifest.json",
            )

        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest(
                {"kind": "full", "chunks": ["bad"]},
                "manifest.json",
            )

        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest(
                {"kind": "full", "chunks": [{"sha256": "x"}]},
                "manifest.json",
            )

        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest(
                {"kind": "full", "chunks": [{"key": "a"}]},
                "manifest.json",
            )

        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest(
                {"kind": "full", "chunks": [{"key": "a", "sha256": "x", "size": "bad"}]},
                "manifest.json",
            )

        with self.assertRaises(restore.RestoreError):
            restore.parse_manifest(
                {
                    "kind": "full",
                    "chunks": [{"key": "a", "sha256": "x", "size": 1}],
                    "s3": [],
                },
                "manifest.json",
            )

    def test_parse_manifest_accepts_empty_parent(self) -> None:
        payload = {
            "kind": "full",
            "parent_manifest": "",
            "chunks": [{"key": "a", "sha256": "x", "size": 1}],
        }
        manifest = restore.parse_manifest(payload, "manifest.json")
        self.assertIsNone(manifest.parent_manifest)

    def test_hash_mismatch_raises(self) -> None:
        client = FakeS3()
        client.objects["chunk.bin"] = b"payload"
        chunk = restore.ChunkInfo(key="chunk.bin", sha256="bad", size=None)
        output = io.BytesIO()

        with self.assertRaises(restore.RestoreError) as context:
            restore.download_and_verify_chunks(
                client, "bucket", [chunk], output
            )
        self.assertIn("hash mismatch", str(context.exception))

    def test_download_streams_and_verifies(self) -> None:
        class RecordingBody(FakeBody):
            def __init__(self, payload: bytes) -> None:
                super().__init__(payload)
                self.read_sizes: list[int | None] = []

            def read(self, size: int | None = None) -> bytes:
                self.read_sizes.append(size)
                return super().read(size)

        payload = b"streamed-payload"
        body = RecordingBody(payload)
        client = FakeS3()
        client.objects["chunk.bin"] = body
        chunk = restore.ChunkInfo(
            key="chunk.bin",
            sha256=hashlib.sha256(payload).hexdigest(),
            size=len(payload),
        )
        output = io.BytesIO()

        total_bytes = restore.download_and_verify_chunks(
            client,
            "bucket",
            [chunk],
            output,
            read_size=4,
        )

        self.assertEqual(output.getvalue(), payload)
        self.assertEqual(total_bytes, len(payload))
        self.assertTrue(all(size == 4 for size in body.read_sizes))
        self.assertGreater(len(body.read_sizes), 2)

    def test_download_requires_positive_read_size(self) -> None:
        client = FakeS3()
        client.objects["chunk.bin"] = b"payload"
        chunk = restore.ChunkInfo(
            key="chunk.bin",
            sha256=hashlib.sha256(b"payload").hexdigest(),
            size=None,
        )
        with self.assertRaises(restore.RestoreError) as context:
            restore.download_and_verify_chunks(
                client, "bucket", [chunk], io.BytesIO(), read_size=0
            )
        self.assertIn("read_size", str(context.exception))

    def test_ensure_chunks_restored_noop_for_standard(self) -> None:
        client = FakeS3()
        client.objects["chunk.bin"] = b"payload"
        chunk = restore.ChunkInfo(key="chunk.bin", sha256="x", size=None)

        restore.ensure_chunks_restored(
            client,
            "bucket",
            [chunk],
            storage_class="STANDARD",
            restore_tier="Standard",
            timeout_seconds=1,
        )

        self.assertEqual(client.restore_requests, [])

    def test_ensure_chunks_restored_clears_pending(self) -> None:
        client = FakeS3()
        client.objects["chunk.bin"] = b"payload"
        client.restore_headers["chunk.bin"] = ['ongoing-request="false"']
        chunk = restore.ChunkInfo(key="chunk.bin", sha256="x", size=None)

        restore.ensure_chunks_restored(
            client,
            "bucket",
            [chunk],
            storage_class="GLACIER",
            restore_tier="Standard",
            timeout_seconds=1,
            sleep=lambda _: None,
            time_fn=lambda: 0.0,
        )

        self.assertEqual(client.restore_requests, ["chunk.bin"])

    def test_stream_failure_cleans_up_receive(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(restore.ChunkInfo(key="chunk.bin", sha256="x", size=None),),
            s3={},
            snapshot_path="/snapshots/data__20260101T000000Z__full",
        )

        class FakeProcess:
            def __init__(self) -> None:
                self.stdin = io.BytesIO()
                self.terminated = False
                self.killed = False
                self._poll = None
                self.returncode = None

            def poll(self):
                return self._poll

            def terminate(self) -> None:
                self.terminated = True
                self._poll = 0
                self.returncode = 0

            def kill(self) -> None:
                self.killed = True
                self._poll = 0
                self.returncode = 0

            def communicate(self, timeout: float | None = None):
                return b"", b"receive failed"

        proc = FakeProcess()

        with mock.patch(
            "btrfs_to_s3.restore.subprocess.Popen", return_value=proc
        ), mock.patch(
            "btrfs_to_s3.restore.download_and_verify_chunks",
            side_effect=restore.RestoreError("bad chunk"),
        ):
            with self.assertRaises(restore.RestoreError) as context:
                restore._apply_manifest_stream(
                    client, "bucket", manifest, Path("/tmp")
                )
        self.assertTrue(proc.terminated)
        self.assertIn("restore stream failed", str(context.exception))
        self.assertIn("receive failed", str(context.exception))

    def test_receive_exit_includes_stderr(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(restore.ChunkInfo(key="chunk.bin", sha256="x", size=None),),
            s3={},
            snapshot_path="/snapshots/data__20260101T000000Z__full",
        )

        class FakeProcess:
            def __init__(self) -> None:
                self.stdin = io.BytesIO()
                self.returncode = 2

            def communicate(self, timeout: float | None = None):
                return b"", b"receive stderr"

        proc = FakeProcess()

        with mock.patch(
            "btrfs_to_s3.restore.subprocess.Popen", return_value=proc
        ), mock.patch(
            "btrfs_to_s3.restore.download_and_verify_chunks",
            return_value=0,
        ):
            with self.assertRaises(restore.RestoreError) as context:
                restore._apply_manifest_stream(
                    client, "bucket", manifest, Path("/tmp")
                )
        self.assertIn("exit code 2", str(context.exception))
        self.assertIn("receive stderr", str(context.exception))

    def test_receive_exit_without_stderr(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(),
            s3={},
            snapshot_path="/snapshots/data__20260101T000000Z__full",
        )

        class FakeProcess:
            def __init__(self) -> None:
                self.stdin = io.BytesIO()
                self.returncode = 1

            def communicate(self, timeout: float | None = None):
                return b"", b""

        proc = FakeProcess()

        with mock.patch(
            "btrfs_to_s3.restore.subprocess.Popen", return_value=proc
        ), mock.patch(
            "btrfs_to_s3.restore.download_and_verify_chunks",
            return_value=0,
        ):
            with self.assertRaises(restore.RestoreError) as context:
                restore._apply_manifest_stream(
                    client, "bucket", manifest, Path("/tmp")
                )
        self.assertIn("exit code 1", str(context.exception))

    def test_restore_chain_renames_and_sets_writable(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(),
            s3={},
            snapshot_path="/snapshots/data__20260101T000000Z__full",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            created = parent / "created"
            created.mkdir()
            target = parent / "target"

            with mock.patch(
                "btrfs_to_s3.restore._apply_manifest_stream",
                return_value=(created, 123),
            ), mock.patch(
                "btrfs_to_s3.restore._set_subvolume_writable"
            ) as writable:
                total = restore.restore_chain(
                    client,
                    "bucket",
                    [manifest],
                    target,
                    wait_for_restore=False,
                    restore_tier="Standard",
                    restore_timeout_seconds=1,
                )

            self.assertEqual(total, 123)
            self.assertTrue(target.exists())
            writable.assert_called_once_with(target)

    def test_restore_chain_waits_and_deletes_target(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(),
            s3={"storage_class": "GLACIER"},
            snapshot_path="/snapshots/data__20260101T000000Z__full",
        )
        second = restore.ManifestInfo(
            key="manifest2.json",
            kind="inc",
            parent_manifest="manifest.json",
            chunks=(),
            s3={},
            snapshot_path="/snapshots/data__20260102T000000Z__inc",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            target = parent / "target"
            created_first = parent / "created_first"
            created_first.mkdir()
            created_second = parent / "created"
            created_second.mkdir()

            with mock.patch(
                "btrfs_to_s3.restore._apply_manifest_stream",
                side_effect=[(created_first, 1), (created_second, 2)],
            ), mock.patch(
                "btrfs_to_s3.restore._delete_subvolume"
            ) as delete, mock.patch(
                "btrfs_to_s3.restore.ensure_chunks_restored"
            ) as ensure, mock.patch(
                "btrfs_to_s3.restore._set_subvolume_writable"
            ):
                total = restore.restore_chain(
                    client,
                    "bucket",
                    [manifest, second],
                    target,
                    wait_for_restore=True,
                    restore_tier="Standard",
                    restore_timeout_seconds=1,
                )

            self.assertEqual(total, 3)
            delete.assert_called_once_with(target)
            self.assertEqual(ensure.call_count, 2)

    def test_restore_chain_rejects_existing_target(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(),
            s3={},
            snapshot_path="/snapshots/data__20260101T000000Z__full",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "target"
            target.mkdir()
            with self.assertRaises(restore.RestoreError) as context:
                restore.restore_chain(
                    client,
                    "bucket",
                    [manifest],
                    target,
                    wait_for_restore=False,
                    restore_tier="Standard",
                    restore_timeout_seconds=1,
                )
        self.assertIn("already exists", str(context.exception))

    def test_restore_chain_requires_created_subvolume(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(),
            s3={},
            snapshot_path="/snapshots/data__20260101T000000Z__full",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            target = parent / "target"

            with mock.patch(
                "btrfs_to_s3.restore._apply_manifest_stream",
                return_value=(parent / "missing", 1),
            ):
                with self.assertRaises(restore.RestoreError) as context:
                    restore.restore_chain(
                        client,
                        "bucket",
                        [manifest],
                        target,
                        wait_for_restore=False,
                        restore_tier="Standard",
                        restore_timeout_seconds=1,
                    )
        self.assertIn("received subvolume missing", str(context.exception))

    def test_apply_manifest_stream_requires_snapshot_path(self) -> None:
        client = FakeS3()
        manifest = restore.ManifestInfo(
            key="manifest.json",
            kind="full",
            parent_manifest=None,
            chunks=(),
            s3={},
            snapshot_path=None,
        )

        with self.assertRaises(restore.RestoreError) as context:
            restore._apply_manifest_stream(
                client, "bucket", manifest, Path("/tmp")
            )
        self.assertIn("missing snapshot path", str(context.exception))

    def test_cleanup_btrfs_receive_handles_timeout(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self.terminated = False
                self.killed = False
                self._calls = 0

            def poll(self):
                return None

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True

            def communicate(self, timeout: float | None = None):
                self._calls += 1
                if self._calls == 1:
                    raise subprocess.TimeoutExpired(cmd="btrfs", timeout=1.0)
                return b"", b"stderr"

        proc = FakeProcess()

        stderr = restore._cleanup_btrfs_receive(proc, timeout=0.0)

        self.assertTrue(proc.terminated)
        self.assertTrue(proc.killed)
        self.assertEqual(stderr, "stderr")

    def test_parse_snapshot_path_none_returns_none(self) -> None:
        path = restore._parse_snapshot_path(
            {"snapshot": {"path": None}}, "manifest.json"
        )
        self.assertIsNone(path)

    def test_parse_snapshot_path_errors(self) -> None:
        with self.assertRaises(restore.RestoreError) as context:
            restore._parse_snapshot_path({"snapshot": "nope"}, "manifest.json")
        self.assertIn("invalid snapshot metadata", str(context.exception))

        with self.assertRaises(restore.RestoreError) as context:
            restore._parse_snapshot_path(
                {"snapshot": {"path": ""}}, "manifest.json"
            )
        self.assertIn("invalid snapshot path", str(context.exception))

    def test_entry_type_handles_missing_and_other(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            missing = base / "missing"
            self.assertEqual(restore._entry_type(missing), "missing")

            fifo = base / "fifo"
            os.mkfifo(fifo)
            self.assertEqual(restore._entry_type(fifo), "other")

            file_path = base / "file.txt"
            file_path.write_text("data", encoding="utf-8")
            self.assertEqual(restore._entry_type(file_path), "file")

            dir_path = base / "dir"
            dir_path.mkdir()
            self.assertEqual(restore._entry_type(dir_path), "dir")

            link_path = base / "link"
            link_path.symlink_to(file_path)
            self.assertEqual(restore._entry_type(link_path), "symlink")

    def test_collect_entries_treats_symlink_dir_as_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            target = base / "target"
            target.mkdir()
            link = base / "link"
            link.symlink_to(target)
            dirs, files = restore._collect_entries(base)
            self.assertIn("link", files)

    def test_check_missing_extra_reports_extra(self) -> None:
        message = restore._check_missing_extra(["a"], ["a", "b"], "file")
        self.assertEqual(message, "extra file: b")

    def test_delete_subvolume_runs_btrfs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            with mock.patch(
                "btrfs_to_s3.restore.subprocess.run"
            ) as runner:
                restore._delete_subvolume(target)
        runner.assert_called_once()

    def test_set_subvolume_writable_runs_btrfs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            with mock.patch(
                "btrfs_to_s3.restore.subprocess.run"
            ) as runner:
                restore._set_subvolume_writable(target)
        runner.assert_called_once()


class RestoreVerifyTests(unittest.TestCase):
    def test_verify_metadata_uses_sbin_on_path(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)
            captured: dict[str, str] = {}

            def runner(*args, **kwargs):
                captured["path"] = kwargs["env"]["PATH"]
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: 11111111-2222-3333-4444-555555555555\n",
                    stderr="",
                )

            with mock.patch.dict(os.environ, {"PATH": "/bin"}):
                restore.verify_metadata(target_path, runner=runner)
        self.assertIn("/usr/sbin", captured["path"])
        self.assertIn("/sbin", captured["path"])

    def test_verify_metadata_requires_directory(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir) / "file.txt"
            target_path.write_text("data", encoding="utf-8")

            with self.assertRaises(restore.RestoreError) as context:
                restore.verify_metadata(target_path, runner=mock.Mock())
            self.assertIn("not a directory", str(context.exception))

    def test_verify_metadata_requires_writable(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: 11111111-2222-3333-4444-555555555555\n",
                    stderr="",
                )

            with mock.patch("os.access", return_value=False):
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_metadata(target_path, runner=runner)
            self.assertIn("not writable", str(context.exception))

    def test_verify_metadata_requires_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: 11111111-2222-3333-4444-555555555555\n",
                    stderr="",
                )

            restore.verify_metadata(target_path, runner=runner)

    def test_verify_metadata_invalid_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: not-a-uuid\n",
                    stderr="",
                )

            with self.assertRaises(restore.RestoreError) as context:
                restore.verify_metadata(target_path, runner=runner)
            self.assertIn("UUID", str(context.exception))

    def test_verify_content_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                Path(source_dir, "file.txt").write_text("data", encoding="utf-8")
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        Path(source_dir),
                        Path(target_dir),
                        mode="full",
                        sample_max_files=10,
                    )
                self.assertIn("missing file", str(context.exception))

    def test_verify_content_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                Path(source_dir, "file.txt").write_text("data", encoding="utf-8")
                Path(target_dir, "file.txt").write_text("tada", encoding="utf-8")
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        Path(source_dir),
                        Path(target_dir),
                        mode="full",
                        sample_max_files=10,
                    )
                self.assertIn("hash mismatch", str(context.exception))

    def test_verify_content_source_missing(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            with self.assertRaises(restore.RestoreError) as context:
                restore.verify_content(
                    Path("/missing/source"),
                    Path(target_dir),
                    mode="full",
                    sample_max_files=10,
                )
        self.assertIn("source snapshot missing", str(context.exception))

    def test_verify_content_source_not_dir(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                source_path = Path(source_dir, "file.txt")
                source_path.write_text("data", encoding="utf-8")
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        source_path,
                        Path(target_dir),
                        mode="full",
                        sample_max_files=10,
                    )
                self.assertIn("not a directory", str(context.exception))

    def test_verify_content_type_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                Path(source_dir, "dir").mkdir()
                Path(source_dir, "link").symlink_to(Path(source_dir, "dir"))
                Path(target_dir, "dir").mkdir()
                Path(target_dir, "link").write_text("data", encoding="utf-8")
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        Path(source_dir),
                        Path(target_dir),
                        mode="full",
                        sample_max_files=10,
                    )
                self.assertIn("type mismatch", str(context.exception))

    def test_verify_content_symlink_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                source_path = Path(source_dir)
                target_path = Path(target_dir)
                (source_path / "dir").mkdir()
                (target_path / "dir").mkdir()
                (source_path / "link").symlink_to(source_path / "dir")
                (target_path / "link").symlink_to(target_path / "dir")
                (target_path / "dir").rmdir()
                (target_path / "dir").mkdir()
                with mock.patch(
                    "os.readlink",
                    side_effect=["/a", "/b"],
                ):
                    with self.assertRaises(restore.RestoreError) as context:
                        restore.verify_content(
                            source_path,
                            target_path,
                            mode="full",
                            sample_max_files=10,
                        )
                self.assertIn("symlink mismatch", str(context.exception))

    def test_verify_content_size_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                Path(source_dir, "file.txt").write_text("data", encoding="utf-8")
                Path(target_dir, "file.txt").write_text("longer", encoding="utf-8")
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        Path(source_dir),
                        Path(target_dir),
                        mode="full",
                        sample_max_files=10,
                    )
                self.assertIn("size mismatch", str(context.exception))

    def test_verify_content_modes(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            with tempfile.TemporaryDirectory() as target_dir:
                Path(source_dir, "file.txt").write_text("data", encoding="utf-8")
                Path(target_dir, "file.txt").write_text("data", encoding="utf-8")
                restore.verify_content(
                    Path(source_dir),
                    Path(target_dir),
                    mode="sample",
                    sample_max_files=0,
                )
                restore.verify_content(
                    Path(source_dir),
                    Path(target_dir),
                    mode="none",
                    sample_max_files=0,
                )
                with self.assertRaises(restore.RestoreError) as context:
                    restore.verify_content(
                        Path(source_dir),
                        Path(target_dir),
                        mode="unknown",
                        sample_max_files=0,
                    )
                self.assertIn("unknown verify mode", str(context.exception))

    def test_select_sample_is_deterministic(self) -> None:
        sample = restore._select_sample(["b", "a", "c"], 2)
        self.assertEqual(sample, ["a", "b"])

    def test_select_sample_empty_when_zero(self) -> None:
        sample = restore._select_sample(["a", "b"], 0)
        self.assertEqual(sample, [])

    def test_select_sample_returns_ordered(self) -> None:
        sample = restore._select_sample(["b", "a"], 3)
        self.assertEqual(sample, ["a", "b"])

    def test_verify_restore_missing_source_skips_content(self) -> None:
        with tempfile.TemporaryDirectory() as target_dir:
            target_path = Path(target_dir)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0],
                    0,
                    stdout="UUID: 11111111-2222-3333-4444-555555555555\n",
                    stderr="",
                )

            restore.verify_restore(
                Path("/missing/source"),
                target_path,
                mode="full",
                sample_max_files=10,
                runner=runner,
            )

    def test_verify_restore_mode_none_skips_metadata(self) -> None:
        runner = mock.Mock()
        restore.verify_restore(
            None,
            Path("/tmp/target"),
            mode="none",
            sample_max_files=10,
            runner=runner,
        )
        runner.assert_not_called()

    def test_parse_uuid_missing_returns_none(self) -> None:
        self.assertIsNone(restore._parse_uuid("no uuid here"))



if __name__ == "__main__":
    unittest.main()
