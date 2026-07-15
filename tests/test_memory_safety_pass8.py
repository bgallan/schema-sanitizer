"""Regression coverage for the eighth defensive memory-hardening pass."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]


def test_materialization_validity_is_allocated_only_after_first_null() -> None:
    """Null-free native columns must not pay for redundant validity bitmaps."""
    source = (
        ROOT / "cpp/src/internal/materialization/builders/detail.hh"
    ).read_text(encoding="utf-8")
    validity = source.split("void push_validity", 1)[1].split(
        "static const void *validity_buffer", 1
    )[0]

    assert "if (validity_.empty())" in validity
    assert "if (valid)" in validity
    assert "++length_;\n        return;" in validity
    assert "validity_.assign(byte_count, uint8_t{0xff})" in validity
    assert "validity_[byte_index] &= static_cast<uint8_t>(~bit_mask)" in validity


def test_materialization_late_null_round_trip(tmp_path: Path) -> None:
    """Rows preceding the first null remain valid after lazy bitmap creation."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import JSONL_STREAM_WRITE

    rows = [{"value": 1}, {"value": 2}, {"value": None}, {"value": 4}]
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    output = tmp_path / "late-null.jsonl"

    JSONL_STREAM_WRITE(stream, str(output))

    assert [json.loads(line) for line in output.read_text().splitlines()] == rows


def test_materialization_nested_late_null_round_trip(tmp_path: Path) -> None:
    """Lazy validity also preserves struct and list rows preceding a null."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import JSONL_STREAM_WRITE

    rows = [
        {"obj": {"x": 1}, "items": [1, 2]},
        {"obj": {"x": 2}, "items": [3]},
        {"obj": None, "items": None},
        {"obj": {"x": 4}, "items": [4, 5]},
    ]
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    output = tmp_path / "nested-late-null.jsonl"

    JSONL_STREAM_WRITE(stream, str(output))

    assert [json.loads(line) for line in output.read_text().splitlines()] == rows


def test_xml_row_stream_uses_borrowed_slices_and_bounded_retention() -> None:
    """Execution should not duplicate every XML row or pin exceptional buffers."""
    header = (
        ROOT / "cpp/src/internal/parsing/streaming/xml/row_scanner.hh"
    ).read_text(encoding="utf-8")
    scanner = (
        ROOT / "cpp/src/internal/parsing/streaming/xml/row_scanner_buffer.cc"
    ).read_text(encoding="utf-8")
    lifecycle = (
        ROOT / "cpp/src/internal/parsing/streaming/xml/row_scanner.cc"
    ).read_text(encoding="utf-8")
    frontend = (ROOT / "cpp/src/frontends/xml/frontend.cc").read_text(
        encoding="utf-8"
    )

    assert "std::string_view text" in header
    assert "retained_buffer_limit" in scanner
    assert "buffer_.capacity() > retained_buffer_limit()" in scanner
    assert "secure_zero_memory(buffer_.data(), buffer_.size())" in scanner
    assert "std::memmove" in scanner
    assert ".text = std::string_view(buffer_).substr" in lifecycle
    assert "storage->raw_rows.emplace_back(slice.text)" in frontend


def test_xml_borrowed_row_slice_round_trip_with_secure_cleanup(
    tmp_path: Path
) -> None:
    """Borrowed XML slices remain valid until parsing finishes."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "rows.xml"
    output = tmp_path / "rows.jsonl"
    source.write_text(
        "<rows>"
        "<row><id>1</id><payload>abcdefghijklmnopqrstuvwxyz</payload></row>"
        "<row><id>2</id><payload>ABCDEFGHIJKLMNOPQRSTUVWXYZ</payload></row>"
        "</rows>",
        encoding="utf-8",
    )

    ss.to_jsonl(
        source,
        output,
        input_format="xml",
        xml_row_tag="row",
        memory_limit_bytes=1 << 20,
    )

    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert [row["id"] for row in rows] == ["1", "2"]
    assert rows[0]["payload"] == "abcdefghijklmnopqrstuvwxyz"
    assert rows[1]["payload"] == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def test_csv_multichunk_segments_release_owners_and_exceptional_capacity() -> None:
    """Materialized records must not retain every contributing source chunk."""
    scanner = (
        ROOT / "cpp/src/internal/parsing/streaming/csv/scanner.cc"
    ).read_text(encoding="utf-8")
    record = (
        ROOT / "cpp/src/internal/parsing/streaming/csv/record_buffer.cc"
    ).read_text(encoding="utf-8")

    assert "void CsvStreamingScanner::clear_segments() noexcept" in scanner
    assert "kMaxRetainedSegments = 1024" in scanner
    assert "segments_.swap(empty)" in scanner
    assert "kMaxCsvRecordSegments" in record
    assert "CSV record spans too many input chunks" in record
    assert "scanner_.clear_segments();" in record
    assert record.index("scanner_.clear_segments();") < record.index(
        "return make_text_slice"
    )


def test_csv_record_spanning_many_chunks_round_trip(tmp_path: Path) -> None:
    """Releasing segment owners must not invalidate the arena-backed record."""
    require_native()
    import schema_sanitizer as ss

    payload = "x" * 4096
    source = tmp_path / "rows.csv"
    output = tmp_path / "rows.jsonl"
    source.write_text(f'id,payload\n1,"{payload}"\n', encoding="utf-8")

    ss.to_jsonl(
        source,
        output,
        input_format="csv",
        memory_limit_bytes=1 << 20,
    )

    row = json.loads(output.read_text().splitlines()[0])
    assert row["id"] == "1"
    assert row["payload"] == payload


def test_json_multichunk_metadata_amplification_is_bounded() -> None:
    """Tiny source chunks cannot amplify one JSON value into unbounded metadata."""
    header = (
        ROOT / "cpp/src/internal/parsing/streaming/json/value_span_scanner.hh"
    ).read_text(encoding="utf-8")
    buffer = (
        ROOT / "cpp/src/internal/parsing/streaming/json/value_span_buffer.cc"
    ).read_text(encoding="utf-8")

    assert "kMaxSegments = 65'536" in header
    assert "segments_.size() >= kMaxSegments" in buffer
    assert "JSON value spans too many input chunks" in buffer


def test_segment_chunk_size_is_internal_and_not_public(tmp_path: Path) -> None:
    """Chunk fragmentation remains internal to the single memory budget."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "rows.jsonl"
    output = tmp_path / "rows-out.jsonl"
    source.write_text('{"payload":"ok"}\n', encoding="utf-8")
    with pytest.raises(TypeError, match="read_chunk_bytes"):
        ss.to_jsonl(
            source,
            output,
            input_format="jsonl",
            read_chunk_bytes=1,
            memory_limit_bytes=1 << 20,
        )
