"""Tests stream and result resource lifecycle contracts."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


def test_opened_registry_stream_materialization_uses_raw_arrow_stream(monkeypatch) -> None:
    from schema_sanitizer.api_impl.source_plan import registry as output
    from schema_sanitizer.api_impl.source_plan.registry import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw Arrow stream helper."""

        closed = 0

        def __arrow_c_stream__(self):
            return object()

        def close(self) -> None:
            self.closed += 1

    class PythonStream:
        """Python iterator wrapper that must not be consumed."""

        closed = 0

        def __iter__(self):
            raise AssertionError("Python batch iteration should not be used")

        def close(self) -> None:
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
    from schema_sanitizer.api_impl.source_plan import registry as output
    from schema_sanitizer.api_impl.source_plan.registry import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw Arrow stream helper."""

        def __init__(self) -> None:
            self.closed = 0

        def __arrow_c_stream__(self):
            return object()

        def close(self) -> None:
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
    from schema_sanitizer.api_impl.results import Result
    from schema_sanitizer.api_impl.source_plan import registry as output
    from schema_sanitizer.api_impl.source_plan.registry import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw stream with close tracking."""

        def __init__(self) -> None:
            self.closed = 0
            self.diagnostics = SimpleNamespace()

        def close(self) -> None:
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
    from schema_sanitizer.api_impl.source_plan import registry as output
    from schema_sanitizer.api_impl.source_plan.registry import OpenedSourcePlanRegistryStream

    class RawStream:
        """Raw stream with close tracking."""

        def __init__(self) -> None:
            self.closed = 0

        def close(self) -> None:
            self.closed += 1

    class PythonStream:
        """Existing stream wrapper with close tracking."""

        def __init__(self) -> None:
            self.closed = 0

        def close(self) -> None:
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
    from schema_sanitizer.core_impl.native_results import SinkOutput

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
    from schema_sanitizer.core_impl import native_results as native_schema_results
    from schema_sanitizer.core_impl.native_results import (
        RegistryProbeResult,
        SchemaProbeResult,
    )

    calls: list[bytes] = []

    def fake_decode(payload: bytes) -> str:
        """Track lazy schema decode calls."""
        calls.append(payload)
        return f"schema:{payload.decode()}"

    monkeypatch.setattr(native_schema_results, "pyarrow_schema_from_payload", fake_decode)

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
    from schema_sanitizer.core_impl import native_results as native_schema_results
    from schema_sanitizer.core_impl.native_results import (
        RegistryProbeResult,
        SchemaProbeResult,
    )

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

    monkeypatch.setattr(native_schema_results, "pyarrow_schema_from_payload", fake_decode)

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
    from schema_sanitizer.core_impl import execution as execution_context
    from schema_sanitizer.core_impl import probes as probe_dependencies

    calls: list[tuple[object, ...]] = []

    class FakeNative:
        """Minimal native module with path-source probe variants."""

        @staticmethod
        def context_registry_probe_from_path_sources_registry_state(*args):
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
            raise AssertionError("JSON registry probe should not be used")

    monkeypatch.setattr(execution_context, "_native", SimpleNamespace(context_new=lambda: "ctx"))
    ctx = execution_context.ExecutionContext()
    monkeypatch.setattr(probe_dependencies, "_native", FakeNative)
    monkeypatch.setattr(
        probe_dependencies, "_options_capsule", lambda options: f"prepared:{options}"
    )

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
