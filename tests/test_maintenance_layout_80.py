"""Protect ownership and performance changes introduced by maintenance layout 80."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_parquet_python_routes_have_direct_bounded_owners() -> None:
    """Arrow-source and direct-route behavior stay in two cohesive modules."""
    parquet = ROOT / "src/schema_sanitizer/api_impl/parquet"
    for name, symbols in (
        (
            "arrow_sources.py",
            (
                "class ParquetArrowSource",
                "class ParquetArrowSourceChunkProvider",
                "def parquet_arrow_stream_factory_or_none",
            ),
        ),
        (
            "direct_routes.py",
            (
                "def parquet_direct_sink_raw_or_none",
                "def parquet_direct_registry_sink_raw_or_none",
                "def should_retry_native_parquet_reader_failure",
            ),
        ),
    ):
        owner = parquet / name
        text = owner.read_text(encoding="utf-8")
        assert len(text.splitlines()) <= 500
        for symbol in symbols:
            assert symbol in text
    assert not (parquet / "arrow_sources").exists()
    assert not (parquet / "direct_routes").exists()


def test_arrow_source_chunks_do_not_copy_descriptor_slices() -> None:
    """Chunk iteration uses a view over the canonical descriptor tuple."""
    text = (ROOT / "src/schema_sanitizer/api_impl/parquet/arrow_sources.py").read_text(
        encoding="utf-8"
    )
    assert "self._sources = tuple(sources)" in text
    assert "chunk = islice(self._sources, self._index, end)" in text
    assert "self._sources[self._index" not in text


def test_parquet_writer_encoding_phases_have_bounded_owners() -> None:
    """Statistics and adaptive encodings stay consolidated without mega-files."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    expected = {
        "stream_writer_statistics.cc.inc": 500,
        "stream_writer_value_encodings.cc.inc": 500,
    }
    for name, limit in expected.items():
        path = writer / name
        assert path.is_file()
        assert len(path.read_text(encoding="utf-8").splitlines()) <= limit
    for removed in (
        "stream_writer_min_max_statistics.cc.inc",
        "stream_writer_column_statistics.cc.inc",
        "stream_writer_dictionary_encoding.cc.inc",
        "stream_writer_delta_binary_encoding.cc.inc",
        "stream_writer_adaptive_encoding.cc.inc",
    ):
        assert not (writer / removed).exists()


def test_dictionary_encoding_borrows_page_values() -> None:
    """Dictionary discovery stores views and copies each unique value once."""
    text = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    assert "std::vector<std::string_view> dictionary" in text
    assert "BorrowedStringLookupMap<std::uint32_t> index_by_value" in text
    assert "out.dictionary_values.reserve(dictionary_bytes)" in text
    assert "kInitialUniqueReserve = 4096" in text
    assert "append_item(std::string" not in text
    assert "std::vector<std::string> dictionary" not in text


def test_delta_length_encoding_has_no_payload_staging_buffer() -> None:
    """Delta-length byte arrays append source slices after encoding lengths."""
    text = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    assert "std::string bytes" not in text
    assert "out.append(values.substr(offset" in text
    assert "lengths.size() * sizeof(std::uint32_t)" in text


def test_byte_stream_split_uses_cxx23_uninitialized_fill() -> None:
    """BYTE_STREAM_SPLIT fills its output directly through the C++23 string API."""
    text = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    assert "resize_and_overwrite" in text
    assert "std::string out(values.size()," not in text
