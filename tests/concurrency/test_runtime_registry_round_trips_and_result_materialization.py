"""Tests stream and result resource lifecycle contracts."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


def test_registry_probe_path_sources_native_state_round_trip(
    tmp_path: Path, require_native: None
) -> None:
    from schema_sanitizer.core_impl.execution import ExecutionContext

    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first_path.write_text('{"a": 1}\n', encoding="utf-8")
    second_path.write_text('{"b": 2}\n', encoding="utf-8")
    ctx = ExecutionContext()

    first = ctx.registry_probe_path_sources(
        [("json", str(first_path), str(first_path))],
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )
    second = ctx.registry_probe_path_sources(
        [("json", str(second_path), str(second_path))],
        None,
        registry_json=first.schema_registry_json,
        field_name_policy="lower_snake",
        schema_mode="additive",
        native_registry_state=first.native_registry_state,
    )

    assert first.native_registry_state is not None
    assert second.native_registry_state is not None
    assert set(second.field_names) >= {"a", "b"}


def test_registry_probe_path_source_chunk_provider_native_round_trip(
    tmp_path: Path,
    require_native: None,
) -> None:
    from schema_sanitizer.core_impl.execution import ExecutionContext

    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first_path.write_text('{"a": 1}\n', encoding="utf-8")
    second_path.write_text('{"b": 2}\n', encoding="utf-8")

    class Provider:
        """Simple two-chunk path-source provider."""

        def __init__(self) -> None:
            self.index = 0
            self.closed = 0

        def next_sources(self):
            chunks = [
                [("json", str(first_path), str(first_path))],
                [("json", str(second_path), str(second_path))],
            ]
            if self.index >= len(chunks):
                return None
            out = chunks[self.index]
            self.index += 1
            return out

        def close(self) -> None:
            self.closed += 1

    provider = Provider()
    ctx = ExecutionContext()

    result = ctx.registry_probe_path_source_chunk_provider(
        provider,
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert provider.index == 2
    assert provider.closed == 1
    assert result.native_registry_state is not None
    assert set(result.field_names) >= {"a", "b"}


def test_registry_sink_path_source_chunk_provider_auto_registry_native_round_trip(
    tmp_path: Path,
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.streams import Stream
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.resource_lifecycle import _close_suppressing_errors

    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first_path.write_text('{"id": 1}\n', encoding="utf-8")
    second_path.write_text('{"id": 2, "name": "two"}\n', encoding="utf-8")
    chunks = [
        [("json", str(first_path), str(first_path))],
        [("json", str(second_path), str(second_path))],
    ]

    class Provider:
        """Simple replayable path-source chunk provider."""

        def __init__(self) -> None:
            self.index = 0
            self.closed = 0

        def next_sources(self):
            if self.index >= len(chunks):
                return None
            out = chunks[self.index]
            self.index += 1
            return out

        def close(self) -> None:
            self.closed += 1

    probe_provider = Provider()
    stream_provider = Provider()
    raw = ExecutionContext().to_registry_sink_path_source_chunk_provider_auto_registry(
        "stream",
        probe_provider,
        stream_provider,
        None,
        registry_json="{}",
        field_name_policy="lower_alpha",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
        skip_invalid_json_sources=True,
    )
    stream = Stream(raw)
    try:
        table = pa.Table.from_batches(stream, schema=stream.schema)
    finally:
        _close_suppressing_errors(stream)
        _close_suppressing_errors(raw)

    rows = table.to_pylist()
    assert [row["id"] for row in rows] == [1, 2]
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "two"
    assert rows[0]["schema_registry"]
    assert rows[0]["schema_drifts"]
    assert rows[1]["schema_registry"] is None
    assert raw.native_registry_state is not None
    assert probe_provider.index == 2
    assert stream_provider.index == 2
    assert probe_provider.closed == 1
    assert stream_provider.closed == 1


def test_registry_probe_arrow_sources_uses_native_registry_state(monkeypatch) -> None:
    from schema_sanitizer.core_impl import execution as execution_context
    from schema_sanitizer.core_impl import probes as probe_dependencies

    calls: list[tuple[object, ...]] = []

    class FakeNative:
        """Minimal native module with Arrow-source probe variants."""

        @staticmethod
        def context_registry_probe_from_arrow_sources_registry_state(*args):
            calls.append(args)
            return {
                "schema": b"\x00\x00\x00\x00",
                "diagnostics_json": "{}",
                "schema_registry_json": '{"arrow_state":true}',
                "schema_drifts_json": "[]",
                "conversion_timestamp": "2026-07-05T00:00:00Z",
                "native_registry_state": "next-arrow-state",
            }

        @staticmethod
        def context_registry_probe_from_arrow_sources(*_args):
            raise AssertionError("JSON Arrow-source registry probe should not be used")

    monkeypatch.setattr(execution_context, "_native", SimpleNamespace(context_new=lambda: "ctx"))
    ctx = execution_context.ExecutionContext()
    monkeypatch.setattr(probe_dependencies, "_native", FakeNative)
    monkeypatch.setattr(
        probe_dependencies, "_options_capsule", lambda options: f"prepared:{options}"
    )

    result = ctx.registry_probe_arrow_sources(
        [("factory", "s3://bucket/a.parquet")],
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        native_registry_state="arrow-state",
    )

    assert result.schema_registry_json == '{"arrow_state":true}'
    assert result.native_registry_state == "next-arrow-state"
    assert calls == [
        (
            "ctx",
            [("factory", "s3://bucket/a.parquet")],
            "prepared:options",
            "arrow-state",
            "lower_snake",
            "additive",
        )
    ]


def test_registry_probe_arrow_sources_native_state_round_trip(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.core_impl.execution import ExecutionContext

    first_table = pa.table({"a": [1]})
    second_table = pa.table({"b": [2]})
    ctx = ExecutionContext()

    first = ctx.registry_probe_arrow_sources(
        [(first_table.to_reader(), "memory://first.parquet")],
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )
    second = ctx.registry_probe_arrow_sources(
        [(second_table.to_reader(), "memory://second.parquet")],
        None,
        registry_json=first.schema_registry_json,
        field_name_policy="lower_snake",
        schema_mode="additive",
        native_registry_state=first.native_registry_state,
    )

    assert first.native_registry_state is not None
    assert second.native_registry_state is not None
    assert set(second.field_names) >= {"a", "b"}


def test_registry_sink_arrow_source_chunk_provider_auto_registry_native_round_trip(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.streams import Stream
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.resource_lifecycle import _close_suppressing_errors

    chunks = [
        [(pa.table({"id": [1]}).to_reader(), "memory://first.parquet")],
        [(pa.table({"id": [2], "name": ["two"]}).to_reader(), "memory://second.parquet")],
    ]

    class Provider:
        """Simple replayable Arrow-source chunk provider."""

        def __init__(self) -> None:
            self.index = 0
            self.closed = 0

        def next_sources(self):
            if self.index >= len(chunks):
                return None
            out = chunks[self.index]
            self.index += 1
            return out

        def close(self) -> None:
            self.closed += 1

    probe_provider = Provider()
    stream_provider = Provider()
    raw = ExecutionContext().to_registry_sink_arrow_source_chunk_provider_auto_registry(
        "stream",
        probe_provider,
        stream_provider,
        None,
        registry_json="{}",
        field_name_policy="lower_alpha",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
    )
    stream = Stream(raw)
    try:
        table = pa.Table.from_batches(stream, schema=stream.schema)
    finally:
        _close_suppressing_errors(stream)
        _close_suppressing_errors(raw)

    rows = table.to_pylist()
    assert [row["id"] for row in rows] == [1, 2]
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "two"
    assert rows[0]["source_file"] == "memory://first.parquet"
    assert rows[1]["source_file"] == "memory://second.parquet"
    assert rows[0]["schema_registry"]
    assert rows[0]["schema_drifts"]
    assert rows[1]["schema_registry"] is None
    assert raw.native_registry_state is not None
    assert probe_provider.index == 2
    assert stream_provider.index == 2
    assert probe_provider.closed == 1
    assert stream_provider.closed == 1


def test_sink_result_table_materialization_closes_stream_backed_raw(monkeypatch) -> None:
    import schema_sanitizer.api_impl.results as sink_result

    class TableStream:
        """Test helper for TableStream."""

        def __arrow_c_stream__(self):
            return object()

    class Raw:
        """Test helper for Raw."""

        table = TableStream()
        diagnostics = None
        closed = False

        def close(self):
            self.closed = True

    raw = Raw()

    def fake_table_from_stream_like(obj, *, feature):
        """Return fake table from stream like for the test."""
        assert isinstance(obj, TableStream)
        assert feature == "sink table output"
        return "materialized-table"

    monkeypatch.setattr(
        sink_result._pyarrow_streams, "table_from_stream_like", fake_table_from_stream_like
    )

    result = sink_result.SinkResult(raw)

    assert result.table == "materialized-table"
    assert raw.closed is True


def test_sink_result_table_materialization_preserves_diagnostics_until_close(monkeypatch) -> None:
    import schema_sanitizer.api_impl.results as sink_result

    class TableStream:
        """Test helper for TableStream."""

        def __arrow_c_stream__(self):
            return object()

    class Diagnostics:
        """Test helper for Diagnostics."""

        skipped_rows = 7

    class Raw:
        """Test helper for Raw."""

        table = TableStream()

        def __init__(self) -> None:
            self.diagnostics = Diagnostics()
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            self.main_closed += 1

        def close(self) -> None:
            self.closed += 1

    raw = Raw()

    def fake_table_from_stream_like(obj, *, feature):
        """Return fake table from stream like for the test."""
        assert isinstance(obj, TableStream)
        assert feature == "sink table output"
        return "materialized-table"

    monkeypatch.setattr(
        sink_result._pyarrow_streams, "table_from_stream_like", fake_table_from_stream_like
    )

    result = sink_result.SinkResult(raw)

    assert result.table == "materialized-table"
    assert raw.main_closed == 1
    assert raw.closed == 0
    assert raw.diagnostics.skipped_rows == 7

    result.close()
    assert raw.closed == 1
