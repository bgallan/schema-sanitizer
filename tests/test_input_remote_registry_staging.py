"""Staged remote registry input tests."""

from __future__ import annotations

import json
import threading

import pytest
from conftest import require_native
from input_contract_shared import *  # noqa: F403

import schema_sanitizer as ss


def test_remote_chunk_prefetch_iterator_stages_next_chunk_and_cleans_up() -> None:
    """Verify remote chunk prefetch overlaps staging and closes unused chunks."""
    from schema_sanitizer.api_impl.source_plan.remote import iter_staged_remote_chunks

    class FakeStaged:
        """Fake staged chunk with cleanup tracking."""

        def __init__(self, start: int):
            """Store chunk start."""
            self.start = start
            self.closed = False

        def close(self) -> None:
            """Mark the staged chunk as closed."""
            self.closed = True

    class FakeManifest:
        """Fake remote manifest with chunk staging hooks."""

        chunk_size = 1
        files = [object(), object()]
        input_format = "json"
        memory_limit_bytes = 1

        def __init__(self) -> None:
            """Initialize call tracking."""
            self.calls: list[int] = []
            self.staged: dict[int, FakeStaged] = {}
            self.second_started = threading.Event()

        def stage_chunk(self, start: int) -> FakeStaged:
            """Return one fake staged chunk."""
            self.calls.append(start)
            if start == 1:
                self.second_started.set()
            staged = FakeStaged(start)
            self.staged[start] = staged
            return staged

    manifest = FakeManifest()
    with iter_staged_remote_chunks(manifest) as chunks:
        first = next(chunks)
        assert first.start == 0
        assert manifest.second_started.wait(timeout=2.0)
        assert manifest.calls == [0, 1]
        assert manifest.staged[1].closed is False

    assert manifest.staged[0].closed is False
    assert manifest.staged[1].closed is True


def test_remote_json_directory_to_jsonl_uses_bounded_registry_staging(
    monkeypatch, tmp_path
) -> None:
    """Verify remote JSONL output retains only bounded staged registry chunks."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl import routing as remote_routing
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    staged_calls = []

    async def fake_list_remote_directory_files(uri, suffixes):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only the requested chunk into local child files."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        remote_routing,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

    out_path = tmp_path / "out.jsonl"
    ss.to_jsonl(
        "s3://bucket/partition/",
        out_path,
        input_format="json",
        input_mode="directory",
    )

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert [row["id"] for row in rows] == ["a", "b"]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.json",
        "s3://bucket/partition/b.json",
    ]
    assert staged_calls == [["a.json", "b.json"], ["a.json", "b.json"]]


def test_remote_json_directory_to_pyarrow_uses_bounded_registry_staging(
    monkeypatch,
    tmp_path,
) -> None:
    """Verify remote analytical conversion stages chunks but streams them natively."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl import routing as remote_routing
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    staged_calls = []

    async def fake_list_remote_directory_files(uri, suffixes):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage each requested chunk into local child files."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-arrow-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        remote_routing,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

    result = ss.to_pyarrow(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
    )

    rows = result.clean_data.to_pylist()
    assert [row["id"] for row in rows] == ["a", "b"]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.json",
        "s3://bucket/partition/b.json",
    ]
    assert staged_calls == [["a.json", "b.json"], ["a.json", "b.json"]]


def test_remote_json_directory_to_jsonl_uses_bounded_staging_with_registry(
    monkeypatch,
    tmp_path,
) -> None:
    """Verify remote registry-backed writes avoid all-chunks staged retention."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl import routing as remote_routing
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    registry_seed = tmp_path / "registry-seed.json"
    registry_seed.write_text('{"id": "seed"}\n', encoding="utf-8")
    registry_json = ss.to_pyarrow(registry_seed, input_format="json").schema_registry_json
    staged_calls = []

    async def fake_list_remote_directory_files(uri, suffixes):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage each requested remote chunk into local child files."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-single-pass-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    registry_stream_calls = 0
    real_registry_stream = source_plan_registry_stream.open_source_plan_registry_stream

    def tracking_registry_stream(*args, **kwargs):
        """Track native registry stream use while preserving behavior."""
        nonlocal registry_stream_calls
        registry_stream_calls += 1
        return real_registry_stream(*args, **kwargs)

    monkeypatch.setattr(
        remote_routing,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )
    monkeypatch.setattr(
        source_plan_registry_stream,
        "open_source_plan_registry_stream",
        tracking_registry_stream,
    )

    out_path = tmp_path / "remote-single-pass.jsonl"
    ss.to_jsonl(
        "s3://bucket/partition/",
        out_path,
        input_format="json",
        input_mode="directory",
        schema_registry=registry_json,
    )

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert registry_stream_calls == 1
    assert [row["id"] for row in rows] == ["a", "b"]
    assert staged_calls == [["a.json", "b.json"], ["a.json", "b.json"]]
