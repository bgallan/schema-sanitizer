"""Tests lazy validity allocation and bounded retention across coalescing, hostile folder
chunks, XML borrowed slices, multichunk CSV records, JSON metadata amplification, and
secure cleanup. Null bitmaps appear only on the first null, oversized rows fail under
the single budget, and segment owners or scratch capacity release on success and
exception."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from _support.resource_fakes import CapsuleStream

ROOT = Path(__file__).resolve().parents[2]


def test_coalescer_allocates_validity_only_after_first_null() -> None:
    """Verify coalescer allocates validity only after first null."""
    append = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_append.cc").read_text()
    estimate = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_export.cc").read_text()
    validity = append.split("sanitize::Status append_validity", 1)[1].split(
        "sanitize::Status ensure_child_count", 1
    )[0]
    assert "bool has_null = false" in validity
    assert validity.index("if (!out->validity.empty() || has_null)") < validity.index(
        "out->validity.resize"
    )
    assert "const bool needs_validity" in estimate


def test_coalescer_uses_the_single_memory_budget() -> None:
    """Verify coalescer uses the single memory budget."""
    stream = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_stream.cc").read_text()
    state = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_stream_internal.hh").read_text()
    assert "memory_budget_from_limit(memory_limit_bytes)" in stream
    assert "single row exceeds hard batch byte limit" in stream
    assert "retained bytes exceed hard batch limit" in stream
    assert "std::size_t max_batch_bytes" in state
    assert "getenv" not in stream


def test_folder_reader_checks_hostile_chunk_before_retaining_it() -> None:
    """Verify folder reader checks hostile chunk before retaining it."""
    from schema_sanitizer.errors import SchemaSanitizerResourceError
    from schema_sanitizer.input_impl.directory_inputs import FolderFile, read_folder_file_bytes

    requested: list[int] = []

    class OversizedReader:
        """Return an oversized chunk regardless of the requested read bound."""

        def read(self, size: int = -1, /) -> bytes:
            """Read bounded data from the oversized reader test double."""
            requested.append(size)
            return b"x" * 4096

        def close(self) -> None:
            """Close the resources owned by the oversized reader test double."""
            return None

    file = FolderFile("hostile.bin", "hostile.bin", None, OversizedReader)
    with pytest.raises(SchemaSanitizerResourceError, match="hostile.bin"):
        read_folder_file_bytes(file, memory_limit_bytes=32, stage="bounded read")
    assert requested == [33]


def test_folder_reader_wipes_temporary_accumulator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify folder reader wipes temporary accumulator."""
    from schema_sanitizer.input_impl import directory_inputs

    observed: list[bytes] = []
    original_zero = directory_inputs._zero_bytearray_range

    def capture(buffer: bytearray, start: int, end: int) -> None:
        """Record the accumulator after each zeroing pass."""
        original_zero(buffer, start, end)
        observed.append(bytes(buffer))

    monkeypatch.setattr(directory_inputs, "_zero_bytearray_range", capture)
    file = directory_inputs.FolderFile(
        display_name="secret.bin",
        name="secret.bin",
        size=None,
        open_binary=lambda: io.BytesIO(b"secret"),
    )
    assert (
        directory_inputs.read_folder_file_bytes(file, memory_limit_bytes=64, stage="bounded read")
        == b"secret"
    )
    assert observed == [b"\x00" * 6]


def test_native_coalescer_preserves_late_nulls_without_eager_validity(
    tmp_path: Path, require_native: None
) -> None:
    """Verify native coalescer preserves late nulls without eager validity."""
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import COALESCING_STREAM_WRAP, JSONL_STREAM_WRITE

    rows = [
        {"id": 1, "payload": "first", "nested": {"value": 10}},
        {"id": 2, "payload": None, "nested": None},
        {"id": 3, "payload": "last", "nested": {"value": None}},
    ]
    source = ExecutionContext().to_sink_python("stream", rows, None)
    capsule = COALESCING_STREAM_WRAP(source, 1 << 20)
    output = tmp_path / "late-nulls.jsonl"
    JSONL_STREAM_WRITE(CapsuleStream(capsule), str(output), 1 << 20)
    assert [json.loads(line) for line in output.read_text().splitlines()] == rows


def test_native_coalescer_rejects_one_row_over_budget(tmp_path: Path, require_native: None) -> None:
    """Verify native coalescer rejects one row over budget."""
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import (
        COALESCING_STREAM_WRAP,
        PARQUET_STREAM_WRITE,
    )

    source = ExecutionContext().to_sink_python("stream", [{"payload": "x" * 4096}], None)
    capsule = COALESCING_STREAM_WRAP(source, 512)
    with pytest.raises(
        RuntimeError, match="(single row exceeds hard batch byte limit|logical byte limit)"
    ):
        PARQUET_STREAM_WRITE(
            CapsuleStream(capsule), str(tmp_path / "bounded.parquet"), "uncompressed", -1, 512
        )


def test_materialization_validity_is_allocated_only_after_first_null() -> None:
    """Null-free native columns do not pay for redundant validity bitmaps."""
    source = (ROOT / "cpp/src/internal/materialization/builders/detail.hh").read_text(
        encoding="utf-8"
    )
    validity = source.split("void push_validity", 1)[1].split(
        "static const void *validity_buffer", 1
    )[0]
    assert "if (validity_.empty())" in validity
    assert "if (valid)" in validity
    assert "++length_;\n        return;" in validity
    assert "validity_.assign(byte_count, uint8_t{0xff})" in validity
    assert "validity_[byte_index] &= static_cast<uint8_t>(~bit_mask)" in validity


def test_materialization_late_null_round_trip(tmp_path: Path, require_native: None) -> None:
    """Rows preceding the first null remain valid after lazy bitmap creation."""
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import JSONL_STREAM_WRITE

    rows = [{"value": 1}, {"value": 2}, {"value": None}, {"value": 4}]
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    output = tmp_path / "late-null.jsonl"
    JSONL_STREAM_WRITE(stream, str(output))
    assert [json.loads(line) for line in output.read_text().splitlines()] == rows


def test_materialization_nested_late_null_round_trip(tmp_path: Path, require_native: None) -> None:
    """Lazy validity preserves struct and list rows preceding a null."""
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
    """Execution does not duplicate every XML row or pin exceptional buffers."""
    header = (ROOT / "cpp/src/internal/parsing/streaming/xml/row_scanner.hh").read_text(
        encoding="utf-8"
    )
    scanner = (ROOT / "cpp/src/internal/parsing/streaming/xml/row_scanner_buffer.cc").read_text(
        encoding="utf-8"
    )
    lifecycle = (ROOT / "cpp/src/internal/parsing/streaming/xml/row_scanner.cc").read_text(
        encoding="utf-8"
    )
    frontend = (ROOT / "cpp/src/frontends/xml/frontend.cc").read_text(encoding="utf-8")
    assert "std::string_view text" in header
    assert "retained_buffer_limit" in scanner
    assert "buffer_.capacity() > retained_buffer_limit()" in scanner
    assert "secure_zero_memory(buffer_.data(), buffer_.size())" in scanner
    assert "std::memmove" in scanner
    assert ".text = std::string_view(buffer_).substr" in lifecycle
    assert "storage->raw_arena.append(slice.text)" in frontend
    assert "std::pmr::vector<std::string_view> raw_rows" in (
        ROOT / "cpp/src/frontends/xml/frontend_internal.hh"
    ).read_text(encoding="utf-8")


def test_xml_borrowed_row_slice_round_trip_with_secure_cleanup(
    tmp_path: Path, require_native: None
) -> None:
    """Borrowed XML slices remain valid until parsing finishes."""
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
    """Materialized records do not retain every contributing source chunk."""
    scanner = (ROOT / "cpp/src/internal/parsing/streaming/csv/scanner.cc").read_text(
        encoding="utf-8"
    )
    record = (ROOT / "cpp/src/internal/parsing/streaming/csv/record_buffer.cc").read_text(
        encoding="utf-8"
    )
    assert "void CsvStreamingScanner::clear_segments() noexcept" in scanner
    assert "kMaxRetainedSegments = 1024" in scanner
    assert "segments_.swap(empty)" in scanner
    assert "scanner_.max_record_segments_" in record
    assert "CSV record spans too many input chunks" in record
    assert "scanner_.clear_segments();" in record
    assert record.index("scanner_.clear_segments();") < record.index("return make_text_slice")


def test_csv_record_spanning_many_chunks_round_trip(tmp_path: Path, require_native: None) -> None:
    """Releasing segment owners does not invalidate the arena-backed record."""
    import schema_sanitizer as ss

    payload = "x" * 4096
    source = tmp_path / "rows.csv"
    output = tmp_path / "rows.jsonl"
    source.write_text(f'id,payload\n1,"{payload}"\n', encoding="utf-8")
    ss.to_jsonl(source, output, input_format="csv", memory_limit_bytes=1 << 20)
    row = json.loads(output.read_text().splitlines()[0])
    assert row["id"] == "1"
    assert row["payload"] == payload


def test_json_multichunk_metadata_amplification_is_bounded() -> None:
    """Tiny source chunks cannot amplify one JSON value into unbounded metadata."""
    header = (ROOT / "cpp/src/internal/parsing/streaming/json/value_span_scanner.hh").read_text(
        encoding="utf-8"
    )
    buffer = (ROOT / "cpp/src/internal/parsing/streaming/json/value_span_buffer.cc").read_text(
        encoding="utf-8"
    )
    assert "kMaxSegments = 65'536" in header
    assert "segments_.size() >= kMaxSegments" in buffer
    assert "JSON value spans too many input chunks" in buffer


def test_segment_chunk_size_is_internal_and_not_public(
    tmp_path: Path, require_native: None
) -> None:
    """Chunk fragmentation remains internal to the single memory budget."""
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
