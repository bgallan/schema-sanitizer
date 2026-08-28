"""Regression coverage for memory metadata collection limits precede large reservations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_metadata_collection_limits_precede_large_reservations() -> None:
    """Hostile Python collection sizes must be rejected before native reserve()."""
    source = (ROOT / "cpp/src/api/python_abi3/metadata/columns/columns.cc").read_text(
        encoding="utf-8"
    )

    dict_parser = source.split("bool append_utf8_columns_from_dict", 1)[1].split(
        "bool py_value_to_metadata_span", 1
    )[0]
    span_parser = source.split("bool append_row_span_columns_from_dict", 1)[1].split(
        "bool append_timestamp_columns", 1
    )[0]
    timestamp_parser = source.split("bool append_timestamp_columns", 1)[1].split(
        "bool append_registry_metadata_columns", 1
    )[0]

    assert dict_parser.index("ensure_item_budget") < dict_parser.index("out->reserve")
    assert span_parser.index("ensure_item_budget") < span_parser.index("out->reserve")
    assert span_parser.index("ensure_item_budget(total_spans") < span_parser.index(
        "column.spans.reserve"
    )
    assert timestamp_parser.index("ensure_item_budget") < timestamp_parser.index("out->reserve")
    assert "kMaxMetadataInputUtf8Bytes" in source


def test_generated_metadata_budget_precedes_batch_allocations() -> None:
    """Metadata expansion must be estimated before vectors allocate their payloads."""
    builder = (ROOT / "cpp/src/api/python_abi3/metadata/stream/array_builder.cc").read_text(
        encoding="utf-8"
    )
    lifecycle = (ROOT / "cpp/src/api/python_abi3/metadata/stream/stream.cc").read_text(
        encoding="utf-8"
    )
    body = builder.split("sanitize::Status build_metadata_array", 1)[1]

    budget = body.index("validate_generated_metadata_budget")
    assert budget < body.index("timestamp_columns.reserve")
    assert budget < body.index("utf8_columns.reserve")
    assert budget < body.index("children.resize")
    assert "memory_budget_from_limit(memory_limit_bytes)" in lifecycle
    assert "budget.metadata_bytes" in lifecycle
    assert "saturating_capacity_bytes" in lifecycle


def test_metadata_root_offset_is_rejected_before_child_generation() -> None:
    """A sliced struct root cannot index past newly generated zero-offset children."""
    source = (ROOT / "cpp/src/api/python_abi3/metadata/stream/stream.cc").read_text(
        encoding="utf-8"
    )
    compact_source = " ".join(source.split())
    validator = compact_source.split("sanitize::Status validate_metadata_base_array", 1)[1]

    assert validator.index("base.offset != 0") < validator.index("base.offset + base.length")
    assert "root offset must be zero" in validator


def test_serializers_wipe_and_release_large_transient_buffers() -> None:
    """Serial and parallel output buffers are wiped without retaining giant peaks."""
    jsonl = (ROOT / "cpp/src/internal/json_output/jsonl_stream_writer.cc").read_text(
        encoding="utf-8"
    )
    csv = (ROOT / "cpp/src/internal/csv/csv_stream_writer.cc").read_text(encoding="utf-8")
    ordered = (ROOT / "cpp/src/internal/output/ordered_text_output.hh").read_text(encoding="utf-8")

    assert "secure_zero_memory(buffer.data(), buffer.size())" in jsonl
    assert "secure_zero_memory(bytes.data(), bytes.size())" in ordered
    assert "fragment.wipe()" in ordered
    assert "clear_decode_buffer" in csv
    assert "secure_zero_memory(buffer.data(), buffer.size())" in csv
    assert "write statistics overflow" in ordered


def test_native_row_span_count_is_rejected_before_iteration(require_native: None) -> None:
    """A synthetic huge sequence must fail by count without reading any element."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    class HugeSequence:
        """Helper class used by this regression."""

        def __len__(self) -> int:
            return 1_000_001

        def __getitem__(self, index: int) -> Any:
            raise AssertionError(f"element {index} must not be read")

    with pytest.raises(ValueError, match="row-span entry count"):
        native_core.metadata_stream_wrap(object(), {}, {}, {"source_file": HugeSequence()}, ())


def test_native_generated_metadata_batch_budget_rejects_before_materialization(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Repeated metadata cannot allocate beyond the configured batch budget."""
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import (
        JSONL_STREAM_WRITE_WITH_METADATA,
    )

    stream = ExecutionContext().to_sink_python(
        "stream", ({"id": index} for index in range(32)), None
    )

    with pytest.raises(RuntimeError, match="metadata batch exceeds byte safety limit"):
        JSONL_STREAM_WRITE_WITH_METADATA(
            stream,
            str(tmp_path / "bounded.jsonl"),
            {},
            {"source_file": "a moderately long metadata value"},
            {},
            (),
            64,
        )
