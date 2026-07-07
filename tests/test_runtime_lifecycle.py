"""Tests stream and result resource lifecycle contracts."""

from __future__ import annotations

import gc
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import require_native


def test_opened_registry_stream_materialization_uses_raw_arrow_stream(monkeypatch) -> None:
    """Verify source-plan analytical output avoids Python batch iteration."""
    from schema_sanitizer.api_impl import source_plan_registry_output as output
    from schema_sanitizer.api_impl.source_plan import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw Arrow stream helper."""

        closed = 0

        def __arrow_c_stream__(self):
            """Expose a fake Arrow C stream."""
            return object()

        def close(self) -> None:
            """Close the raw stream."""
            self.closed += 1

    class PythonStream:
        """Python iterator wrapper that must not be consumed."""

        closed = 0

        def __iter__(self):
            """Fail if analytical materialization uses Python batch iteration."""
            raise AssertionError("Python batch iteration should not be used")

        def close(self) -> None:
            """Close the Python stream wrapper."""
            self.closed += 1

    raw = RawStream()
    python_stream = PythonStream()

    def fake_table_from_stream_like(obj, *, feature):
        """Assert direct raw-stream materialization."""
        assert obj is raw
        assert feature == "to_pyarrow"
        return "arrow-table"

    monkeypatch.setattr(
        output._pyarrow_streams, "table_from_stream_like", fake_table_from_stream_like
    )

    opened = OpenedSourcePlanRegistryStream(
        stream=python_stream,
        raw_stream=raw,
        schema_registry_json="{}",
        schema_drifts_json="[]",
    )

    result = output.materialize_opened_registry_stream(opened, target="pyarrow")

    assert result.clean_data == "arrow-table"
    assert python_stream.closed == 1
    assert raw.closed == 0


def test_opened_registry_stream_materialization_closes_raw_stream(monkeypatch) -> None:
    """Verify raw streams are closed after direct materialization."""
    from schema_sanitizer.api_impl import source_plan_registry_output as output
    from schema_sanitizer.api_impl.source_plan import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw Arrow stream helper."""

        def __init__(self) -> None:
            """Initialize close counter."""
            self.closed = 0

        def __arrow_c_stream__(self):
            """Expose a fake Arrow C stream."""
            return object()

        def close(self) -> None:
            """Close the raw stream."""
            self.closed += 1

    raw = RawStream()

    def fake_table_from_stream_like(obj, *, feature):
        """Return a fake table without closing so opened.close is exercised."""
        assert obj is raw
        assert feature == "to_pyarrow"
        return "arrow-table"

    monkeypatch.setattr(
        output._pyarrow_streams, "table_from_stream_like", fake_table_from_stream_like
    )

    opened = OpenedSourcePlanRegistryStream(
        stream=None,
        raw_stream=raw,
        schema_registry_json="{}",
        schema_drifts_json="[]",
        close_items=[raw],
    )

    result = output.materialize_opened_registry_stream(opened, target="pyarrow")

    assert result.clean_data == "arrow-table"
    assert raw.closed == 1


def test_opened_registry_file_output_uses_raw_stream(monkeypatch, tmp_path: Path) -> None:
    """Verify source-plan file output transfers raw streams to native file writers."""
    from schema_sanitizer.api_impl import source_plan_registry_output as output
    from schema_sanitizer.api_impl.ingest_runtime_types import Result
    from schema_sanitizer.api_impl.source_plan import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw stream with close tracking."""

        def __init__(self) -> None:
            """Initialize close counter and diagnostics."""
            self.closed = 0
            self.diagnostics = SimpleNamespace()

        def close(self) -> None:
            """Close the raw stream."""
            self.closed += 1

    raw = RawStream()
    writer = object()
    out = tmp_path / "out.jsonl"
    seen: dict[str, object] = {}

    def fail_output_stream(self: object) -> object:
        """Reject the Python stream-wrapper path."""
        raise AssertionError("opened file output should consume raw stream directly")

    def fake_write_raw_stream_to_file(
        raw_arg: object, out_path: object, **kwargs: object
    ) -> Result:
        """Record direct raw writer arguments."""
        seen.update(kwargs)
        assert raw_arg is raw
        assert out_path == out
        raw.close()
        return Result(SimpleNamespace(diagnostics=raw.diagnostics), clean_data=None)

    monkeypatch.setattr(OpenedSourcePlanRegistryStream, "output_stream", fail_output_stream)
    monkeypatch.setattr(output, "write_raw_stream_to_file", fake_write_raw_stream_to_file)

    opened = OpenedSourcePlanRegistryStream(
        stream=None,
        raw_stream=raw,
        schema_registry_json='{"registry":true}',
        schema_drifts_json="[]",
        native_registry_state="state",
        diagnostics=raw.diagnostics,
        close_items=[raw],
    )

    result = output.write_opened_registry_stream_to_file(
        opened,
        out,
        writer=writer,
        feature="to_jsonl",
    )

    assert seen["writer"] is writer
    assert seen["feature"] == "to_jsonl"
    assert seen["first_row_columns"] is None
    assert result.schema_registry_json == '{"registry":true}'
    assert result.schema_drifts_json == "[]"
    assert result.native_registry_state == "state"
    assert raw.closed == 1


def test_opened_registry_file_output_keeps_existing_stream_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify existing Python stream wrappers keep the fallback writer path."""
    from schema_sanitizer.api_impl import source_plan_registry_output as output
    from schema_sanitizer.api_impl.source_plan import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw stream with close tracking."""

        def __init__(self) -> None:
            """Initialize close counter."""
            self.closed = 0

        def close(self) -> None:
            """Close the raw stream."""
            self.closed += 1

    class PythonStream:
        """Existing stream wrapper with close tracking."""

        def __init__(self) -> None:
            """Initialize close counter."""
            self.closed = 0

        def close(self) -> None:
            """Close the stream wrapper."""
            self.closed += 1

    raw = RawStream()
    stream = PythonStream()
    out = tmp_path / "out.csv"
    seen: dict[str, object] = {}

    def fail_write_raw_stream_to_file(*_args: object, **_kwargs: object) -> object:
        """Reject raw direct writing when a wrapper already exists."""
        raise AssertionError("existing stream wrappers should use writer fallback")

    def fake_writer(stream_arg: object, out_path: object, **kwargs: object) -> dict[str, int]:
        """Record fallback writer arguments."""
        seen.update(kwargs)
        assert stream_arg is stream
        assert out_path == out
        return {"materialized_rows": 0, "batches": 0}

    monkeypatch.setattr(output, "write_raw_stream_to_file", fail_write_raw_stream_to_file)

    opened = OpenedSourcePlanRegistryStream(
        stream=stream,
        raw_stream=raw,
        schema_registry_json="{}",
        schema_drifts_json="[]",
    )

    result = output.write_opened_registry_stream_to_file(
        opened,
        out,
        writer=fake_writer,
        feature="to_csv",
    )

    assert result.schema_registry_json == "{}"
    assert seen["feature"] == "to_csv"
    assert stream.closed == 1
    assert raw.closed == 0


def test_abi3_sink_output_close_releases_stream_wrappers() -> None:
    """Verify abi3 sink output close releases stream wrappers."""
    from schema_sanitizer.core_impl.runtime_support import SinkOutput

    main_capsule = object()
    out = SinkOutput(
        sink="stream",
        main_stream_capsule=main_capsule,
        diagnostics_json='{"skipped_rows":1}',
    )

    assert out.__arrow_c_stream__() is main_capsule
    assert out.diagnostics.skipped_rows == 1

    out.close()

    with pytest.raises(AttributeError):
        out.__arrow_c_stream__()


def test_probe_results_decode_pyarrow_schema_lazily(monkeypatch) -> None:
    """Verify native probe results only decode PyArrow schemas when requested."""
    from schema_sanitizer.core_impl import runtime_support
    from schema_sanitizer.core_impl.runtime_support import RegistryProbeResult, SchemaProbeResult

    calls: list[bytes] = []

    def fake_decode(payload: bytes) -> str:
        """Track lazy schema decode calls."""
        calls.append(payload)
        return f"schema:{payload.decode()}"

    monkeypatch.setattr(runtime_support, "_pyarrow_schema_from_logical_schema_payload", fake_decode)

    schema_result = SchemaProbeResult.from_native(
        {"schema": b"schema-probe", "diagnostics_json": "{}"}
    )
    registry_result = RegistryProbeResult.from_native(
        {
            "schema": b"registry-probe",
            "diagnostics_json": "{}",
            "schema_registry_json": '{"schema_generation":1}',
            "schema_drifts_json": "[]",
            "conversion_timestamp": "2026-07-02T00:00:00Z",
        }
    )

    assert calls == []
    assert registry_result.schema_registry_json == '{"schema_generation":1}'
    assert registry_result.schema_drifts_json == "[]"
    assert schema_result.schema == "schema:schema-probe"
    assert schema_result.schema == "schema:schema-probe"
    assert registry_result.schema == "schema:registry-probe"
    assert registry_result.schema == "schema:registry-probe"
    assert calls == [b"schema-probe", b"registry-probe"]


def test_probe_result_field_names_do_not_decode_pyarrow_schema(monkeypatch) -> None:
    """Verify native probe field names are available without PyArrow schema decoding."""
    from schema_sanitizer.core_impl import runtime_support
    from schema_sanitizer.core_impl.runtime_support import RegistryProbeResult, SchemaProbeResult

    def payload_for_names(*names: str) -> bytes:
        """Build a tiny logical schema payload with int64 nullable fields."""
        out = bytearray()
        out.extend(len(names).to_bytes(4, "little"))
        for name in names:
            raw = name.encode("utf-8")
            out.extend(len(raw).to_bytes(4, "little"))
            out.extend(raw)
            out.append(1)
            out.append(2)
        return bytes(out)

    calls: list[bytes] = []

    def fake_decode(payload: bytes) -> str:
        """Track lazy schema decode calls."""
        calls.append(payload)
        return "decoded"

    monkeypatch.setattr(runtime_support, "_pyarrow_schema_from_logical_schema_payload", fake_decode)

    schema_payload = payload_for_names("id", "value")
    registry_payload = payload_for_names("source", "schema_registry")
    schema_result = SchemaProbeResult.from_native(
        {"schema": schema_payload, "diagnostics_json": "{}"}
    )
    registry_result = RegistryProbeResult.from_native(
        {
            "schema": registry_payload,
            "diagnostics_json": "{}",
            "schema_registry_json": "{}",
            "schema_drifts_json": "[]",
            "conversion_timestamp": "2026-07-02T00:00:00Z",
        }
    )

    assert schema_result.field_names == ("id", "value")
    assert registry_result.field_names == ("source", "schema_registry")
    assert registry_result.has_any_field_name({"missing", "schema_registry"})
    assert calls == []
    assert schema_result.schema == "decoded"
    assert registry_result.schema == "decoded"
    assert calls == [schema_payload, registry_payload]


def test_registry_probe_path_sources_uses_native_registry_state(monkeypatch) -> None:
    """Verify path-source probes prefer the native-state ABI when available."""
    from schema_sanitizer.core_impl import runtime

    calls: list[tuple[object, ...]] = []

    class FakeNative:
        """Minimal native module with path-source probe variants."""

        @staticmethod
        def context_registry_probe_from_path_sources_registry_state(*args):
            """Capture the native-state probe call."""
            calls.append(args)
            return {
                "schema": b"\x00\x00\x00\x00",
                "diagnostics_json": "{}",
                "schema_registry_json": '{"state":true}',
                "schema_drifts_json": "[]",
                "conversion_timestamp": "2026-07-05T00:00:00Z",
                "native_registry_state": "next-state",
            }

        @staticmethod
        def context_registry_probe_from_path_sources(*_args):
            """Fail if the JSON-registry path is used."""
            raise AssertionError("JSON registry probe should not be used")

    ctx = runtime.ExecutionContext.__new__(runtime.ExecutionContext)
    ctx._capsule = "ctx"
    monkeypatch.setattr(runtime, "_native", FakeNative)
    monkeypatch.setattr(runtime, "_options_capsule", lambda options: f"prepared:{options}")

    result = ctx.registry_probe_path_sources(
        [("json", "/tmp/a.json", "s3://bucket/a.json")],
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        native_registry_state="state",
    )

    assert result.schema_registry_json == '{"state":true}'
    assert result.native_registry_state == "next-state"
    assert calls == [
        (
            "ctx",
            [("json", "/tmp/a.json", "s3://bucket/a.json")],
            "prepared:options",
            "state",
            "lower_snake",
            "additive",
        )
    ]


def test_registry_probe_path_sources_native_state_round_trip(tmp_path: Path) -> None:
    """Verify native path-source probes accept registry-state capsules."""
    require_native()
    from schema_sanitizer.core_impl.runtime import ExecutionContext

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
) -> None:
    """Verify native registry probes consume lazy path-source chunks."""
    require_native()
    from schema_sanitizer.core_impl.runtime import ExecutionContext

    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first_path.write_text('{"a": 1}\n', encoding="utf-8")
    second_path.write_text('{"b": 2}\n', encoding="utf-8")

    class Provider:
        """Simple two-chunk path-source provider."""

        def __init__(self) -> None:
            """Initialize chunk state."""
            self.index = 0
            self.closed = 0

        def next_sources(self):
            """Return one path source per chunk."""
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
            """Record native provider cleanup."""
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
) -> None:
    """Verify native provider auto-registry probes and streams through paired providers."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.ingest_lifecycle import _close_suppressing_errors
    from schema_sanitizer.api_impl.ingest_runtime_types import Stream
    from schema_sanitizer.core_impl.runtime import ExecutionContext

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
            """Initialize chunk state."""
            self.index = 0
            self.closed = 0

        def next_sources(self):
            """Return one path source per chunk."""
            if self.index >= len(chunks):
                return None
            out = chunks[self.index]
            self.index += 1
            return out

        def close(self) -> None:
            """Record native provider cleanup."""
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
    """Verify Arrow-source probes prefer the native-state ABI when available."""
    from schema_sanitizer.core_impl import runtime

    calls: list[tuple[object, ...]] = []

    class FakeNative:
        """Minimal native module with Arrow-source probe variants."""

        @staticmethod
        def context_registry_probe_from_arrow_sources_registry_state(*args):
            """Capture the native-state probe call."""
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
            """Fail if the JSON-registry path is used."""
            raise AssertionError("JSON Arrow-source registry probe should not be used")

    ctx = runtime.ExecutionContext.__new__(runtime.ExecutionContext)
    ctx._capsule = "ctx"
    monkeypatch.setattr(runtime, "_native", FakeNative)
    monkeypatch.setattr(runtime, "_options_capsule", lambda options: f"prepared:{options}")

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


def test_registry_probe_arrow_sources_native_state_round_trip() -> None:
    """Verify native Arrow-source probes accept registry-state capsules."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.core_impl.runtime import ExecutionContext

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


def test_registry_sink_arrow_source_chunk_provider_auto_registry_native_round_trip() -> None:
    """Verify native Arrow-source provider auto-registry probes and streams."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.ingest_lifecycle import _close_suppressing_errors
    from schema_sanitizer.api_impl.ingest_runtime_types import Stream
    from schema_sanitizer.core_impl.runtime import ExecutionContext

    chunks = [
        [(pa.table({"id": [1]}).to_reader(), "memory://first.parquet")],
        [(pa.table({"id": [2], "name": ["two"]}).to_reader(), "memory://second.parquet")],
    ]

    class Provider:
        """Simple replayable Arrow-source chunk provider."""

        def __init__(self) -> None:
            """Initialize chunk state."""
            self.index = 0
            self.closed = 0

        def next_sources(self):
            """Return one Arrow source per chunk."""
            if self.index >= len(chunks):
                return None
            out = chunks[self.index]
            self.index += 1
            return out

        def close(self) -> None:
            """Record native provider cleanup."""
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
    """Verify sink result table materialization closes stream backed raw."""
    from schema_sanitizer.api_impl import ingest_runtime_types as types

    class TableStream:
        """Test helper for TableStream."""

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

    class Raw:
        """Test helper for Raw."""

        table = TableStream()
        diagnostics = None
        closed = False

        def close(self):
            """Close the test helper."""
            self.closed = True

    raw = Raw()

    def fake_table_from_stream_like(obj, *, feature):
        """Return fake table from stream like for the test."""
        assert isinstance(obj, TableStream)
        assert feature == "sink table output"
        return "materialized-table"

    monkeypatch.setattr(
        types._pyarrow_streams, "table_from_stream_like", fake_table_from_stream_like
    )

    result = types.SinkResult(raw)

    assert result.table == "materialized-table"
    assert raw.closed is True


def test_sink_result_table_materialization_preserves_diagnostics_until_close(monkeypatch) -> None:
    """Verify sink result table materialization preserves diagnostics until close."""
    from schema_sanitizer.api_impl import ingest_runtime_types as types

    class TableStream:
        """Test helper for TableStream."""

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

    class Diagnostics:
        """Test helper for Diagnostics."""

        skipped_rows = 7

    class Raw:
        """Test helper for Raw."""

        table = TableStream()

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.diagnostics = Diagnostics()
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    raw = Raw()

    def fake_table_from_stream_like(obj, *, feature):
        """Return fake table from stream like for the test."""
        assert isinstance(obj, TableStream)
        assert feature == "sink table output"
        return "materialized-table"

    monkeypatch.setattr(
        types._pyarrow_streams, "table_from_stream_like", fake_table_from_stream_like
    )

    result = types.SinkResult(raw)

    assert result.table == "materialized-table"
    assert raw.main_closed == 1
    assert raw.closed == 0
    assert raw.diagnostics.skipped_rows == 7

    result.close()
    assert raw.closed == 1


def test_abi3_sink_output_close_main_stream_preserves_diagnostics() -> None:
    """Verify abi3 sink output close main stream preserves diagnostics."""
    from schema_sanitizer.core_impl.runtime_support import SinkOutput

    main_capsule = object()
    out = SinkOutput(
        sink="stream",
        main_stream_capsule=main_capsule,
        diagnostics_json='{"skipped_rows":1}',
    )

    out.close_main_stream()

    with pytest.raises(AttributeError):
        out.__arrow_c_stream__()
    assert out.diagnostics.skipped_rows == 1


def test_arrow_c_stream_close_prefers_main_stream_only_close() -> None:
    """Verify arrow c stream close prefers main stream only close."""
    from schema_sanitizer.api_impl.ingest_runtime_types import ArrowCStream

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

    raw = Raw()
    stream = ArrowCStream(raw)

    stream.close()

    assert raw.main_closed == 1
    assert raw.closed == 0


def test_arrow_c_stream_close_releases_keepalive_reference() -> None:
    """Verify arrow c stream close releases keepalive reference."""
    from schema_sanitizer.api_impl.ingest_runtime_types import ArrowCStream

    class Raw:
        """Test helper for Raw."""

        closed = False

        def close(self):
            """Close the test helper."""
            self.closed = True

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

    class Keepalive:
        """Test helper for Keepalive."""

        closed = False

        def close(self):
            """Close the test helper."""
            self.closed = True

    raw = Raw()
    keepalive = Keepalive()
    stream = ArrowCStream(raw)
    object.__setattr__(stream, "_keepalive", keepalive)

    stream.close()

    assert raw.closed is True
    assert keepalive.closed is True
    assert not hasattr(stream, "_keepalive")


def test_sink_result_close_clears_owned_references() -> None:
    """Verify sink result close clears owned references."""
    from schema_sanitizer.api_impl.ingest_runtime_types import SinkResult

    class Closable:
        """Test helper for Closable."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    raw = Closable()
    stream = Closable()
    result = SinkResult(raw)
    result._stream = stream

    result.close()
    result.close()

    assert raw.closed == 1
    assert stream.closed == 1
    assert result.raw is None
    assert result._stream is None


def test_stream_close_deduplicates_reader_raw_close(monkeypatch) -> None:
    """Verify stream close deduplicates reader raw close."""
    from schema_sanitizer.api_impl import ingest_runtime_types as types
    from schema_sanitizer.api_impl.ingest_runtime_types import Stream

    class Reader:
        """Test helper for Reader."""

        schema = None

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    reader = Reader()

    def fake_is_record_batch_reader(obj, *, feature):
        """Return fake is record batch reader for the test."""
        assert obj is reader
        assert feature == "Stream construction"
        return True

    monkeypatch.setattr(
        types._pyarrow_streams,
        "is_record_batch_reader",
        fake_is_record_batch_reader,
    )

    stream = Stream(reader)
    stream.close()
    stream.close()

    assert reader.closed == 1
    assert stream._raw is None
    assert stream._reader is None


def test_stream_close_main_stream_preserves_diagnostics(monkeypatch) -> None:
    """Verify stream close main stream preserves diagnostics."""
    from schema_sanitizer.api_impl import ingest_runtime_types as types
    from schema_sanitizer.api_impl.ingest_runtime_types import Stream

    class Reader:
        """Test helper for Reader."""

        schema = None

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.main_closed = 0
            self.closed = 0

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    reader = Reader()
    raw = Raw()

    def fake_is_record_batch_reader(obj, *, feature):
        """Return fake is record batch reader for the test."""
        assert obj is raw
        assert feature == "Stream construction"
        return False

    def fake_reader_from_stream_like(obj, *, feature):
        """Return fake reader from stream like for the test."""
        assert obj is raw
        assert feature == "Stream construction"
        return reader

    monkeypatch.setattr(
        types._pyarrow_streams,
        "is_record_batch_reader",
        fake_is_record_batch_reader,
    )
    monkeypatch.setattr(
        types._pyarrow_streams,
        "reader_from_stream_like",
        fake_reader_from_stream_like,
    )

    stream = Stream(raw)
    stream.close_main_stream()
    stream.close_main_stream()

    assert reader.closed == 1
    assert raw.main_closed == 1
    assert raw.closed == 0
    assert stream._reader is None
    assert stream._raw is None


def test_result_drop_closes_private_resource_owner() -> None:
    """Verify result drop closes private resource owner."""
    from types import SimpleNamespace

    from schema_sanitizer.api_impl.ingest_runtime_types import Result

    class Owner:
        """Test helper for Owner."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    owner = Owner()
    result = Result(SimpleNamespace(diagnostics=None), clean_data=None)
    object.__setattr__(result, "_resource_owner", owner)

    del result
    gc.collect()

    assert owner.closed == 1


def test_arrow_c_stream_drop_closes_main_stream_and_keepalive() -> None:
    """Verify arrow c stream drop closes main stream and keepalive."""
    from schema_sanitizer.api_impl.ingest_runtime_types import ArrowCStream

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    class Keepalive:
        """Test helper for Keepalive."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    raw = Raw()
    keepalive = Keepalive()
    stream = ArrowCStream(raw)
    object.__setattr__(stream, "_keepalive", keepalive)

    del stream
    gc.collect()

    assert raw.main_closed == 1
    assert raw.closed == 0
    assert keepalive.closed == 1


def test_result_drop_closes_private_keepalive() -> None:
    """Verify result drop closes private keepalive."""
    from types import SimpleNamespace

    from schema_sanitizer.api_impl.ingest_runtime_types import Result

    class Keepalive:
        """Test helper for Keepalive."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    keepalive = Keepalive()
    result = Result(SimpleNamespace(diagnostics=None), clean_data=None)
    object.__setattr__(result, "_keepalive", keepalive)

    del result
    gc.collect()

    assert keepalive.closed == 1


def test_abi3_runtime_support_destructors_suppress_cleanup_errors() -> None:
    """Verify abi3 runtime support destructors suppress cleanup errors."""
    from schema_sanitizer.core_impl.runtime_support import SinkOutput

    sink = SinkOutput(sink="stream", diagnostics_json="{}")
    sink.__del__()
