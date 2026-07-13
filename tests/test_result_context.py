"""Tests Result and ExecutionContext behavior."""

from __future__ import annotations

import pytest

pa = pytest.importorskip("pyarrow")

import schema_sanitizer
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options


def test_result_stats_reflect_finalized_materialization() -> None:
    """Verify result stats reflect finalized materialization."""
    result = ExecutionContext().to_table(
        [{"a": "bad"}, {"a": 2}],
        options=normalize_call_options(
            schema_contract=pa.schema([("a", pa.int64())]),
            schema_mode="strict",
            on_error="skip_row",
            parse_integers=False,
        ),
        format="python",
        source="python",
    )

    assert result.stats["materialized_rows"] == 1
    assert result.stats["batches"] == 1
    assert result.stats["skipped_rows"] == 1


def test_execution_context_accepts_explicit_text_source() -> None:
    """Verify execution context accepts explicit text source."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    result = ExecutionContext().to_table('[{"a": 1}, {"a": 2}]', format="json", source="text")

    assert result.clean_data.to_pylist() == [{"a": 1}, {"a": 2}]


def test_execution_context_normalizes_uppercase_text_formats() -> None:
    """Verify execution context normalizes uppercase text formats."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    ctx = ExecutionContext()

    json_result = ctx.to_table('[{"a": 1}]', format=" JSON ", source=" text ")
    csv_result = ctx.to_table("a\n1\n", format=" CSV ", source=" text ")
    source_result = ctx.to_table('[{"a": 1}]', format="json", source=" TEXT ")

    assert json_result.clean_data.to_pylist() == [{"a": 1}]
    assert csv_result.clean_data.to_pylist() == [{"a": "1"}]
    assert source_result.clean_data.to_pylist() == [{"a": 1}]


def test_execution_context_rejects_unknown_options() -> None:
    """Verify execution context rejects unknown options."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    with pytest.raises(TypeError, match="unknown"):
        ExecutionContext(unknown=object())


def test_table_sink_does_not_expose_single_use_stream() -> None:
    """Verify table sink does not expose single use stream."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    out = ExecutionContext().to_sink([{"a": 1}], sink="table", format="python", source="python")

    assert out.stream is None
    assert out.table.to_pylist() == [{"a": 1}]


def test_execution_context_normalizes_sink_selector() -> None:
    """Verify execution context normalizes sink selector."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    out = ExecutionContext().to_sink([{"a": 1}], sink=" TABLE ", format="python", source="python")

    assert out.table.to_pylist() == [{"a": 1}]


def test_execution_context_selector_arguments_must_be_strings() -> None:
    """Verify execution context selector arguments must be strings."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    ctx = ExecutionContext()

    with pytest.raises(TypeError, match="format"):
        ctx.to_table([{"a": 1}], format=None)
    with pytest.raises(TypeError, match="source"):
        ctx.to_table([{"a": 1}], source=None)
    with pytest.raises(TypeError, match="sink"):
        ctx.to_sink([{"a": 1}], sink=None)


def test_result_only_exposes_explicit_result_properties() -> None:
    """Verify result only exposes explicit result properties."""

    class Raw:
        """Test helper for Raw."""

        table = None
        diagnostics = None

    result = schema_sanitizer.Result(Raw())

    for name in (
        "table",
        "schema",
        "num_rows",
        "to_pylist",
    ):
        with pytest.raises(AttributeError):
            getattr(result, name)
