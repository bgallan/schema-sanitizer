"""Protect maintenance layout revision 79."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_runtime_python_owners_are_direct_and_bounded() -> None:
    """Execution contexts and stream wrappers must not regress into micro-packages."""
    owners = [
        ROOT / "src/schema_sanitizer/api_impl/execution_context.py",
        ROOT / "src/schema_sanitizer/api_impl/streams.py",
    ]
    for owner in owners:
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    assert not (ROOT / "src/schema_sanitizer/api_impl/execution_context").exists()
    assert not (ROOT / "src/schema_sanitizer/stream_impl.py").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/streams").exists()


def test_parquet_writer_domains_are_consolidated_without_mega_fragments() -> None:
    """Schema nodes, collection, and page handling each have one bounded owner."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    expected = {
        "stream_writer_schema_nodes.cc.inc",
        "stream_writer_collection.cc.inc",
        "stream_writer_pages.cc.inc",
    }
    for name in expected:
        owner = writer / name
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    for removed in ("schema", "collection", "pages"):
        assert not (writer / removed).exists()


def test_parquet_page_splitter_is_single_pass() -> None:
    """Page boundaries must be computed before materializing each slice once."""
    text = (ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_pages.cc.inc").read_text(
        encoding="utf-8"
    )
    compact_text = " ".join(text.split()).replace("( ", "(")
    assert "build_page_slice_index" in text
    assert "page_row_incremental_bytes" in text
    assert "ranges.emplace_back" in text
    assert (
        "slice_column_page_data(column, page_data, index, range.begin, range.end, "
        "range.value_begin, range.value_end, range.byte_begin, range.byte_end)" in compact_text
    )
    assert "ColumnPageData candidate" not in text
    assert "best = std::move(candidate)" not in text


def test_parquet_writer_uses_cxx23_endian_primitives() -> None:
    """Little-endian helpers use the standard C++23 byte-order primitives."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    entry = (writer / "stream_writer.cc").read_text(encoding="utf-8")
    write_values = (writer / "stream_writer_arrow_values.cc.inc").read_text(encoding="utf-8")
    statistics = (writer / "stream_writer_statistics.cc.inc").read_text(encoding="utf-8")
    assert "#include <bit>" in entry
    assert "std::endian::native" in write_values
    assert "std::byteswap" in write_values
    assert "std::byteswap" in statistics
