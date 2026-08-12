"""Input source-plan stream tests."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.input_contract import *  # noqa: F403


def test_source_plan_sequence_probe_flattens_path_sources_once() -> None:
    """Pure path-source sequences should use one native probe, not a Python merge loop."""
    from types import SimpleNamespace

    import schema_sanitizer.input_impl.source_plan as path_sources_impl
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.source_plan import probing as source_plan_probe
    from schema_sanitizer.input_impl.source_plan import (
        PreparedSourceBatch,
        SourceDescriptor,
        _native_path_source_plan,
    )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        path_sources_impl, "PATH_SOURCE_PLAN_CREATE", lambda sources, *_args: tuple(sources)
    )
    calls: list[list[tuple[str, str, str]]] = []

    class FakeRawContext:
        """Raw context that captures one native path-source probe."""

        def registry_probe_path_sources_best_effort(
            self,
            sources,
            _call_options,
            *,
            registry_json,
            field_name_policy,
            schema_mode,
        ):
            """Capture the flattened source list."""
            calls.append(list(sources))
            assert registry_json == "{}"
            assert field_name_policy == "lower_snake"
            assert schema_mode == "additive"
            return SimpleNamespace(
                schema_registry_json='{"flattened":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-05T00:00:00Z",
                field_names=("id",),
            )

    def child(path: str, source_file: str, route: str):
        """Build one child plan for the sequence fixture."""
        return _native_path_source_plan(
            source_batch=PreparedSourceBatch(
                (SourceDescriptor("json", path, source_file),),
                input_format="json",
            ),
            input_format="json",
            route_name=route,
        )

    plan = source_plan_model.NativeSourcePlan(
        kind=source_plan_model.SEQUENCE,
        payload=(
            child("/tmp/a.json", "gs://bucket/a.json", "child-a"),
            child("/tmp/b.json", "gs://bucket/b.json", "child-b"),
        ),
        input_format="json",
        route_name="sequence",
    )

    raw = source_plan_probe.probe_source_plan_registry(
        FakeRawContext(),
        plan,
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert raw.schema_registry_json == '{"flattened":true}'
    assert calls == [
        [
            ("json", "/tmp/a.json", "gs://bucket/a.json"),
            ("json", "/tmp/b.json", "gs://bucket/b.json"),
        ]
    ]
    monkeypatch.undo()


def test_source_plan_plain_stream_uses_native_path_source_payload() -> None:
    """Plain stream source plans should pass the reusable native payload capsule."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl import execution_context as execution_context_impl

    native_payload = object()
    captured_sources: list[object] = []

    class FakeRawContext:
        """Raw context that supports the native path-source capsule."""

        def to_sink_path_sources(
            self,
            _sink,
            sources,
            _call_options,
            *,
            include_source_file,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture the exact source payload passed to native."""
            assert include_source_file is True
            assert first_row_columns == {}
            assert timestamp_columns == ()
            captured_sources.append(sources)
            return "raw-stream"

    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor

    plan = source_plan_model.NativeSourcePlan(
        kind=source_plan_model.PATH_SOURCES,
        payload=None,
        input_format="json",
        route_name="native_manifest_paths",
        source_batch=PreparedSourceBatch(
            (SourceDescriptor("json", "/tmp/a.json", "gs://bucket/a.json"),),
            input_format="json",
        ),
        native_payload=native_payload,
    )

    raw = execution_context_impl._open_source_plan_sink_stream_or_none(
        FakeRawContext(),
        plan,
        "options",
        sink="stream",
        include_source_file=True,
    )

    assert raw == "raw-stream"
    assert captured_sources == [native_payload]


def test_remote_source_plan_stream_uses_native_chunk_provider(monkeypatch) -> None:
    """Remote source-plan streams should pull chunks lazily instead of flattening all chunks."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl import execution_context as execution_context_impl
    from schema_sanitizer.api_impl.input.directory_preparation import (
        RemoteNativeDirectorySourceManifest,
    )
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging
    from schema_sanitizer.input_impl.prepared import NativeDirectorySourceManifest
    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor
    from schema_sanitizer.sources import RemoteFile

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []

    def fake_path_source_plan_create(sources, *_args):
        """Return a visible native-plan stand-in."""
        chunk = tuple(sources)
        native_chunks.append(chunk)
        return ("native-plan", chunk)

    import schema_sanitizer.input_impl.source_plan as path_sources_impl

    monkeypatch.setattr(
        path_sources_impl,
        "PATH_SOURCE_PLAN_CREATE",
        fake_path_source_plan_create,
    )

    class FakeStage:
        """Fake staged remote chunk."""

        def __init__(self, name: str) -> None:
            """Create a one-file staged native manifest."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                PreparedSourceBatch(
                    (
                        SourceDescriptor(
                            "json",
                            f"/tmp/{name}.json",
                            f"s3://bucket/{name}.json",
                        ),
                    ),
                    input_format="json",
                )
            )

        def close(self) -> None:
            """Record staged chunk cleanup."""
            events.append(f"close:{self.name}")

    class FakeStagedChunks:
        """Context manager yielding fake staged chunks."""

        def __init__(self, stages: list[FakeStage]) -> None:
            """Store staged chunks."""
            self._stages = stages

        def __enter__(self):
            """Record when lazy staging starts."""
            events.append("enter")
            return iter(self._stages)

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Record context cleanup."""
            events.append("exit")
            return False

        def __iter__(self):
            """Iterate over staged chunks."""
            return iter(self._stages)

    class FakeRaw:
        """Fake raw sink that owns the provider like native does."""

        def __init__(self, provider) -> None:
            """Store provider."""
            self.provider = provider

        def close(self) -> None:
            """Close provider through the raw sink."""
            self.provider.close()
            events.append("raw-close")

    class FakeRawContext:
        """Raw context exposing the plain native chunk-provider sink."""

        def to_sink_path_source_chunk_provider(
            self,
            sink,
            provider,
            call_options,
            *,
            include_source_file,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture the provider handoff."""
            assert sink == "stream"
            assert call_options == "options"
            assert include_source_file is True
            assert first_row_columns == {}
            assert timestamp_columns == ()
            events.append("provider-open")
            return FakeRaw(provider)

    stages = [FakeStage("a"), FakeStage("b")]
    plan = source_plan_model.NativeSourcePlan(
        kind=source_plan_model.REMOTE_CHUNKS,
        payload=RemoteNativeDirectorySourceManifest(
            [
                RemoteFile("s3://bucket/a.json", "a.json", None),
                RemoteFile("s3://bucket/b.json", "b.json", None),
            ],
            input_format="json",
            chunk_size=1,
        ),
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )
    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: FakeStagedChunks(stages),
    )

    raw = execution_context_impl._open_source_plan_sink_stream_or_none(
        FakeRawContext(),
        plan,
        "options",
        sink="stream",
        include_source_file=True,
    )

    assert isinstance(raw, FakeRaw)
    assert events == ["provider-open"]
    assert raw.provider.next_sources() == (
        "native-plan",
        (("json", "/tmp/a.json", "s3://bucket/a.json"),),
    )
    assert events == ["provider-open", "enter"]
    assert raw.provider.next_sources() == (
        "native-plan",
        (("json", "/tmp/b.json", "s3://bucket/b.json"),),
    )
    assert events == ["provider-open", "enter", "close:a"]
    assert native_chunks == [
        (("json", "/tmp/a.json", "s3://bucket/a.json"),),
        (("json", "/tmp/b.json", "s3://bucket/b.json"),),
    ]
    raw.close()
    assert events == [
        "provider-open",
        "enter",
        "close:a",
        "close:b",
        "exit",
        "raw-close",
    ]


def test_remote_json_directory_preparation_allows_native_non_utf8_directory(
    monkeypatch,
) -> None:
    """Verify non-UTF-8 remote directories prepare a lazy native source plan."""
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.api_impl.source_plan import attached as source_plan_attached
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.sources import RemoteFile

    def fail_native_stage(*args, **kwargs):
        """Fail if remote directories stage eagerly during preparation."""
        raise AssertionError("remote directories should not stage during preparation")

    def fake_list_remote_directory(*_args, **_kwargs):
        """Return one remote child without using the removed sync facade."""
        return (RemoteFile("s3://bucket/partition/row.json", "row.json"),)

    monkeypatch.setattr(
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fail_native_stage,
    )

    prepared = public_input.prepare_public_input(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
        input_text_encoding="iso8859-1",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )

    manifest = source_plan_attached.remote_native_multisource_manifest_from_data(prepared.data)
    assert manifest is not None
    assert manifest.input_text_encoding == "iso8859-1"


def test_remote_directory_staging_respects_download_concurrency(monkeypatch) -> None:
    """Verify remote directory staging caps active downloads while preserving row order."""
    from schema_sanitizer.remote_impl import directory_downloads as remote_downloads
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.sources import RemoteFile

    active_downloads = 0
    max_active_downloads = 0

    files = [
        RemoteFile(f"s3://bucket/partition/{index}.jsonl", f"{index}.jsonl") for index in range(5)
    ]

    async def fake_client(files, *, memory_limit_bytes, threading_mode="single"):
        """Return a reusable fake provider client from bounded provider metadata."""
        assert len(files) == 1
        assert files[0].uri == "s3://bucket/partition/0.jsonl"
        assert memory_limit_bytes == 32 * 1024 * 1024
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(
        client,
        file,
        local_path,
        *,
        storage_reservation=None,
    ):
        """Write a payload while tracking active download count."""
        assert storage_reservation is not None
        nonlocal active_downloads, max_active_downloads
        assert client is not None
        active_downloads += 1
        max_active_downloads = max(max_active_downloads, active_downloads)
        await asyncio.sleep(0.01)
        Path(local_path).write_text(f'{{"file":"{file.name}"}}\n', encoding="utf-8")
        active_downloads -= 1

    monkeypatch.setattr(remote_downloads, "provider_client_for_downloads", fake_client)
    monkeypatch.setattr(remote_downloads, "close_provider_client", fake_close)
    monkeypatch.setattr(remote_downloads, "download_file_to_path", fake_download)

    staged = remote_staging.stage_remote_files_to_directory(
        files,
        memory_limit_bytes=32 * 1024 * 1024,
        threading_mode="multi",
    )
    try:
        assert max_active_downloads == 2
        root = Path(staged.path)
        assert [
            (root / f"{index}.jsonl").read_text(encoding="utf-8").strip() for index in range(5)
        ] == [
            '{"file":"0.jsonl"}',
            '{"file":"1.jsonl"}',
            '{"file":"2.jsonl"}',
            '{"file":"3.jsonl"}',
            '{"file":"4.jsonl"}',
        ]
    finally:
        staged.close()


def test_remote_directory_staging_does_not_retry_memory_limit_failure(monkeypatch) -> None:
    """Verify post-download memory-limit failures are not retried as remote I/O."""
    from contextlib import contextmanager

    from schema_sanitizer.errors import SchemaSanitizerResourceError
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.providers import s3_sync
    from schema_sanitizer.sources import RemoteFile

    downloads = 0

    files = [RemoteFile("s3://bucket/partition/row.jsonl", "row.jsonl", None)]

    @contextmanager
    def fake_client():
        """Yield one inert blocking S3 client."""
        yield object()

    def fake_download(
        _context,
        file,
        local_path,
        *,
        memory_limit_bytes,
        storage_reservation=None,
    ):
        """Write an oversized payload through the current streaming contract."""
        assert storage_reservation is not None
        nonlocal downloads
        assert memory_limit_bytes == 8
        assert file.name == "row.jsonl"
        downloads += 1
        Path(local_path).write_bytes(b'{"payload":"too large"}\n')

    monkeypatch.setattr(s3_sync, "open_client", fake_client)
    monkeypatch.setattr(sync_backend, "_download_with_context", fake_download)

    with pytest.raises(SchemaSanitizerResourceError, match="memory_limit_bytes"):
        remote_staging.stage_remote_files_to_directory(
            files,
            memory_limit_bytes=8,
        )
    assert downloads == 1
