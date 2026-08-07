"""Native remote registry input tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest


def _remote_plan():
    """Build a two-file remote source plan."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.input.directory_preparation import (
        RemoteNativeDirectorySourceManifest,
    )
    from schema_sanitizer.sources import RemoteFile

    manifest = RemoteNativeDirectorySourceManifest(
        [
            RemoteFile("s3://bucket/a.json", "a.json", None),
            RemoteFile("s3://bucket/b.json", "b.json", None),
        ],
        input_format="json",
        chunk_size=1,
    )
    return source_plan_model.NativeSourcePlan(
        kind=source_plan_model.REMOTE_CHUNKS,
        payload=manifest,
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )


def _fake_staging(events: list[str], label: str):
    """Build a context factory yielding two staged one-file manifests."""
    from schema_sanitizer.input_impl.prepared import NativeDirectorySourceManifest
    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor

    class FakeStage:
        """One staged remote source owned by the test context."""

        def __init__(self, name: str) -> None:
            """Create one staged source manifest."""
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
            """Record release of the staged source."""
            events.append(f"close:{self.name}")

    class FakeStagedChunks:
        """Context manager yielding deterministic staged chunks."""

        def __enter__(self):
            """Enter staging and return its chunk iterator."""
            events.append(f"enter:{label}")
            return iter([FakeStage(f"{label}-a"), FakeStage(f"{label}-b")])

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Record staging cleanup without suppressing failures."""
            events.append(f"exit:{label}")
            return False

    return FakeStagedChunks()


def _patch_native_plan(monkeypatch, chunks: list[tuple[tuple[str, str, str], ...]]) -> None:
    """Expose provider chunks without constructing a real native capsule."""
    import schema_sanitizer.input_impl.source_plan as path_sources_impl

    def create(sources, *_args):
        """Record one provider chunk and return a stand-in plan."""
        chunk = tuple(sources)
        chunks.append(chunk)
        return ("native-plan", chunk)

    monkeypatch.setattr(path_sources_impl, "PATH_SOURCE_PLAN_CREATE", create)


def test_remote_registry_stream_uses_current_native_auto_provider(monkeypatch) -> None:
    """Remote registry output uses one paired-provider native route."""
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)

    class FakeRaw:
        """Opened native result retaining the streaming provider."""

        diagnostics = {"route": "auto-provider"}
        native_registry_state = "auto-state"
        schema_registry_json = '{"auto":true}'
        schema_drifts_json = "[]"

        def __init__(self, stream_provider) -> None:
            """Retain the provider until the raw result closes."""
            self._stream_provider = stream_provider

        def close(self) -> None:
            """Close the native result and its streaming provider."""
            events.append("raw-close")
            self._stream_provider.close()

    class FakeRawContext:
        """Current ABI context consuming paired remote providers."""

        auto_calls = 0

        def to_registry_sink_path_source_chunk_provider_auto_registry(
            self,
            sink,
            probe_provider,
            stream_provider,
            call_options,
            **options,
        ):
            """Consume the probe provider and return an opened stream."""
            assert sink == "stream"
            assert call_options == "options"
            assert options == {
                "registry_json": "{}",
                "field_name_policy": "lower_snake",
                "schema_mode": "additive",
                "first_row_columns": {},
                "timestamp_columns": ("ingestion_timestamp",),
                "native_registry_state": None,
                "skip_invalid_json_sources": True,
            }
            self.auto_calls += 1
            while probe_provider.next_sources() is not None:
                pass
            probe_provider.close()
            return FakeRaw(stream_provider)

    contexts = [
        _fake_staging(events, "probe"),
        _fake_staging(events, "stream"),
    ]
    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: contexts.pop(0),
    )
    raw_context = FakeRawContext()

    opened = source_plan_registry_stream.open_source_plan_registry_stream(
        raw_context,
        _remote_plan(),
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
    )

    assert opened.schema_registry_json == '{"auto":true}'
    assert opened.native_registry_state == "auto-state"
    assert raw_context.auto_calls == 1
    assert len(native_chunks) == 2
    assert events == [
        "enter:probe",
        "close:probe-b",
        "exit:probe",
    ]
    opened.close()
    assert events == [
        "enter:probe",
        "close:probe-b",
        "exit:probe",
        "raw-close",
        "close:probe-a",
    ]


def test_remote_registry_probe_is_owned_by_native_chunk_provider(monkeypatch) -> None:
    """Registry inference makes one native call and lets it pull every chunk."""
    from schema_sanitizer.api_impl.source_plan import probing as source_plan_probe
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)

    class FakeRawContext:
        """Current ABI context consuming one registry probe provider."""

        def registry_probe_path_source_chunk_provider(
            self,
            provider,
            call_options,
            **options,
        ):
            """Consume all chunks inside one native registry probe call."""
            assert call_options == "options"
            assert options["skip_invalid_json_sources"] is True
            while provider.next_sources() is not None:
                pass
            provider.close()
            return SimpleNamespace(
                schema_registry_json='{"native":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-11T00:00:00Z",
                field_names=("id",),
                native_registry_state="compiled-state",
            )

    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: _fake_staging(events, "probe"),
    )
    raw = source_plan_probe.probe_source_plan_registry(
        FakeRawContext(),
        _remote_plan(),
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert raw.schema_registry_json == '{"native":true}'
    assert raw.native_registry_state == "compiled-state"
    assert len(native_chunks) == 2
    assert events == [
        "enter:probe",
        "close:probe-a",
        "close:probe-b",
        "exit:probe",
    ]


def test_remote_auto_provider_failure_closes_both_providers(monkeypatch) -> None:
    """A native opening failure releases active probe and stream staging contexts."""
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)

    class FakeRawContext:
        """Current ABI context failing after opening both providers."""

        def to_registry_sink_path_source_chunk_provider_auto_registry(
            self,
            _sink,
            probe_provider,
            stream_provider,
            _call_options,
            **_options,
        ):
            """Open both providers before simulating a native failure."""
            assert probe_provider.next_sources() is not None
            assert stream_provider.next_sources() is not None
            raise RuntimeError("native open failed")

    contexts = [
        _fake_staging(events, "probe"),
        _fake_staging(events, "stream"),
    ]
    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: contexts.pop(0),
    )

    with pytest.raises(RuntimeError, match="native open failed"):
        source_plan_registry_stream.open_source_plan_registry_stream(
            FakeRawContext(),
            _remote_plan(),
            "options",
            registry_json="{}",
            field_name_policy="lower_snake",
            schema_mode="additive",
            first_row_columns={},
            timestamp_columns=(),
        )

    assert sorted(event for event in events if event.startswith("close:")) == [
        "close:probe-a",
        "close:stream-a",
    ]
    assert "exit:probe" in events
    assert "exit:stream" in events


def test_remote_probe_prefix_resume_uses_exact_file_offset(monkeypatch) -> None:
    """Streaming reuses the probe prefix and resumes at the first unstaged file."""
    from schema_sanitizer.api_impl.source_plan import remote as remote_source_plan
    from schema_sanitizer.input_impl.prepared import NativeDirectorySourceManifest
    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor

    opened_at: list[int] = []
    closed: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)
    manifest = _remote_plan().payload

    class FakeStage:
        """Own one staged source from the requested manifest suffix."""

        def __init__(self, source_index: int) -> None:
            """Create a one-file native manifest for the source index."""
            source = manifest.files[source_index]
            self.name = source.name
            self.manifest = NativeDirectorySourceManifest(
                PreparedSourceBatch(
                    (
                        SourceDescriptor(
                            "json",
                            f"/tmp/{source.name}",
                            source.uri,
                        ),
                    ),
                    input_format="json",
                )
            )

        def close(self) -> None:
            """Record ownership release."""
            closed.append(self.name)

    class FakeStagedChunks:
        """Yield the exact manifest suffix requested by the provider."""

        def __init__(self, start: int) -> None:
            """Remember the first file index to stage."""
            self.start = start

        def __enter__(self):
            """Return one staged chunk per remaining source file."""
            return iter(FakeStage(index) for index in range(self.start, len(manifest.files)))

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Do not suppress provider failures."""
            return False

    def open_chunks(_manifest, *, start=0):
        """Record the exact resume offset requested by each provider."""
        opened_at.append(start)
        return FakeStagedChunks(start)

    monkeypatch.setattr(remote_source_plan, "open_staged_remote_chunks", open_chunks)
    probe = remote_source_plan.RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=manifest,
        retain_consumed_chunks=1,
    )
    stream = remote_source_plan.RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=manifest,
        retained_chunk_donor=probe,
    )

    while probe.next_sources() is not None:
        pass
    probe.close()
    probe_chunks = tuple(native_chunks)
    native_chunks.clear()

    while stream.next_sources() is not None:
        pass
    stream.close()

    assert opened_at == [0, 1]
    assert [chunk[0][2] for chunk in probe_chunks] == [
        "s3://bucket/a.json",
        "s3://bucket/b.json",
    ]
    assert [chunk[0][2] for chunk in native_chunks] == [
        "s3://bucket/a.json",
        "s3://bucket/b.json",
    ]
    assert sorted(closed) == ["a.json", "b.json", "b.json"]
