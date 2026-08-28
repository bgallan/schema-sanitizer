"""Tests Arrow-source schema discovery at the native provider boundary."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.usefixtures("require_native")


def test_arrow_source_probe_prefers_schema_protocol_without_opening_stream() -> None:
    """A schema-capable source must not open its data-bearing stream to probe."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.core_impl.execution import ExecutionContext

    class SchemaFirstSource:
        """Expose an Arrow schema while making stream access observable."""

        def __init__(self) -> None:
            self.schema = pa.schema([("id", pa.int64())])
            self.schema_calls = 0
            self.stream_calls = 0

        def __arrow_c_schema__(self):
            self.schema_calls += 1
            return self.schema.__arrow_c_schema__()

        def __arrow_c_stream__(self, requested_schema=None):
            del requested_schema
            self.stream_calls += 1
            raise AssertionError("schema probe opened the Arrow data stream")

    source = SchemaFirstSource()
    result = ExecutionContext().registry_probe_arrow_sources(
        [(source, "memory://schema-first.parquet")],
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert result.field_names == ("id",)
    assert source.schema_calls == 1
    assert source.stream_calls == 0


def test_arrow_source_probe_keeps_stream_only_protocol_fallback() -> None:
    """A source without the schema protocol must retain stream schema discovery."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.core_impl.execution import ExecutionContext

    class StreamOnlySource:
        """Expose only the original Arrow stream protocol."""

        def __init__(self) -> None:
            self.reader = pa.table({"id": [1]}).to_reader()
            self.stream_calls = 0

        def __arrow_c_stream__(self, requested_schema=None):
            self.stream_calls += 1
            return self.reader.__arrow_c_stream__(requested_schema)

    source = StreamOnlySource()
    result = ExecutionContext().registry_probe_arrow_sources(
        [(source, "memory://stream-only.parquet")],
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert not hasattr(source, "__arrow_c_schema__")
    assert result.field_names == ("id",)
    assert source.stream_calls == 1
