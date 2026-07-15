"""Regression coverage for bounded Parquet reader and writer scratch."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]
WRITER = ROOT / "cpp/src/internal/parquet/stream_writer"
READER = ROOT / "cpp/src/internal/parquet/footer_reader"
BUDGET = ROOT / "cpp/src/internal/memory/memory_budget.hh"


def test_parquet_writer_limits_come_from_the_single_budget() -> None:
    """Writer pages, row groups and footer share the canonical budget source."""
    budget = BUDGET.read_text(encoding="utf-8")
    config = (WRITER / "stream_writer_configuration.cc.inc").read_text(
        encoding="utf-8"
    )
    api = (WRITER / "stream_writer_api.cc.inc").read_text(encoding="utf-8")
    for field in (
        "parquet_row_group_bytes",
        "parquet_row_group_rows",
        "parquet_page_bytes",
        "parquet_footer_bytes",
    ):
        assert field in budget
    assert "getenv" not in config
    assert "memory_budget_from_limit(options.memory_limit_bytes)" in api


def test_row_group_estimator_stops_collecting_at_the_byte_limit() -> None:
    """Verify the defensive regression contract."""
    collection = (WRITER / "stream_writer_collection.cc.inc").read_text()
    function = collection.split("determine_bounded_row_group_count", 1)[1].split(
        "collect_column_page_data", 1
    )[0]
    loop = function.split("for (std::int64_t row_index", 1)[1]
    assert "prefix = saturating_size_add(prefix, row_bytes)" in loop
    assert "if (prefix >= byte_limit)" in loop
    assert "for (const auto bytes : per_row_bytes)" not in function


def test_dictionary_candidate_avoids_per_value_index_scratch() -> None:
    """Verify the defensive regression contract."""
    encodings = (WRITER / "stream_writer_value_encodings.cc.inc").read_text()
    types = (WRITER / "stream_writer_types.cc.inc").read_text()
    assert "std::vector<std::uint32_t> indices" not in encodings
    assert "for_each_dictionary_item" in encodings
    assert "estimated_dictionary_scratch_bytes" in encodings
    assert "kMaxDictionaryCandidateEntries" in types
    assert "kMaxDictionaryCandidateScratchBytes" in types


def test_delta_encoders_do_not_duplicate_full_pages() -> None:
    """Verify the defensive regression contract."""
    encodings = (WRITER / "stream_writer_value_encodings.cc.inc").read_text()
    assert "signed_delta_values_for_column" not in encodings
    assert "std::vector<std::int64_t> lengths" not in encodings
    assert "encode_delta_binary_packed_sequence" in encodings
    assert "std::array<std::int64_t, kBlockSize> block_deltas" in encodings


def test_page_size_arithmetic_is_saturating() -> None:
    """Verify the defensive regression contract."""
    pages = (WRITER / "stream_writer_pages.cc.inc").read_text()
    assert "saturating_size_add" in pages
    assert "saturating_size_multiply" in pages
    assert "byte_array_value_offsets" not in pages


def test_native_reader_limits_come_from_the_operation_budget() -> None:
    """Verify the defensive regression contract."""
    limits = (READER / "runtime/native_buffer_limits.cc.inc").read_text()
    state = (
        READER / "native_stream/schema/native_stream_arrow_state.cc.inc"
    ).read_text()
    assert "memory_budget_from_limit(memory_limit_bytes)" in limits
    assert "parquet_reader_buffer_bytes" in limits
    assert "parquet_reader_rows" in limits
    assert "max_buffer_bytes" in state
    assert "max_batch_rows" in state
    assert "getenv" not in limits


def test_footer_metadata_budget_rejects_before_retention(tmp_path: Path) -> None:
    """Verify the defensive regression contract."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE

    rows = [{f"column_{index}": index for index in range(128)}]
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    with pytest.raises(RuntimeError, match="footer metadata|memory|limit"):
        PARQUET_STREAM_WRITE(
            stream,
            str(tmp_path / "bounded.parquet"),
            "uncompressed",
            -1,
            1024,
        )


def test_streamed_dictionary_and_delta_encodings_remain_readable(tmp_path: Path) -> None:
    """Verify the defensive regression contract."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_runtime import native_core
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE

    rows = [{"id": index, "name": f"group-{index % 7}"} for index in range(5_000)]
    output = tmp_path / "encodings.parquet"
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    PARQUET_STREAM_WRITE(stream, str(output), "uncompressed", -1, 64 << 20)
    footer = json.loads(native_core.parquet_footer_info_json(str(output)))
    columns = {
        column["path_in_schema"][-1]: column
        for column in footer["row_groups"][0]["columns"]
    }
    assert footer["num_rows"] == len(rows)
    assert 5 in columns["id"]["encodings"]
    assert 8 in columns["name"]["encodings"]
