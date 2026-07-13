"""Protect input buffering and adaptive Parquet page-header ownership."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from schema_sanitizer.core_impl.generated_bytes import BufferedGeneratedBytesReader

ROOT = Path(__file__).resolve().parents[1]


class _ChunkReader(BufferedGeneratedBytesReader):
    """Small deterministic generated reader used to verify cursor semantics."""

    def __init__(self, chunks: list[bytes]):
        """Store deterministic source chunks."""
        self._chunks = chunks
        self._index = 0
        super().__init__("_ChunkReader", default_chunk_bytes=8)

    def _append_next(self, target_bytes: int) -> bool:
        """Append one deterministic source chunk."""
        del target_bytes
        if self._index >= len(self._chunks):
            return False
        self._buffer.extend(self._chunks[self._index])
        self._index += 1
        return True

    def _reset_reader(self) -> None:
        """Reset the deterministic source cursor."""
        self._index = 0


def test_generated_byte_reader_uses_an_amortized_cursor() -> None:
    """Small reads advance a cursor instead of deleting the bytearray prefix."""
    reader = _ChunkReader([b"abcdefghij"])
    assert reader.read(2) == b"ab"
    assert len(reader._buffer) == 10
    assert reader._buffer_offset == 2
    assert b"".join(iter(lambda: reader.read(2), b"")) == b"cdefghij"
    assert reader._buffer == bytearray()
    assert reader._buffer_offset == 0
    assert reader.seek(0) == 0
    assert reader.read(10) == b"abcdefghij"

    source = (ROOT / "src/schema_sanitizer/core_impl/generated_bytes.py").read_text(
        encoding="utf-8"
    )
    assert "self._buffer_offset" in source
    assert "del self._buffer[:max_bytes]" not in source


def test_text_transcoding_belongs_to_input_selection() -> None:
    """Encoding policy and its path transcoder have one bounded owner."""
    selection = ROOT / "src/schema_sanitizer/input_impl/selection.py"
    preparation = ROOT / "src/schema_sanitizer/api_impl/input/preparation.py"
    selection_source = selection.read_text(encoding="utf-8")
    preparation_source = preparation.read_text(encoding="utf-8")

    assert "class TranscodingPathByteReader" in selection_source
    assert "def prepare_native_text_data" in selection_source
    assert "prepare_native_text_data(" in preparation_source
    assert "TranscodingPathByteReader(" not in preparation_source
    assert importlib.util.find_spec("schema_sanitizer.core_impl.transcoding_reader") is None
    assert len(selection_source.splitlines()) <= 500


def test_page_headers_are_read_with_a_growing_window() -> None:
    """The native reader must not allocate/read the 1 MiB ceiling per page."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    page_io = (reader / "pages/footer_reader_page_read.cc.inc").read_text(encoding="utf-8")
    footer_reader = (reader / "footer_reader.cc").read_text(encoding="utf-8")
    thrift = reader / "thrift"

    assert "kInitialPageHeaderBytes = 256" in page_io
    assert "while (!parsed.ok() && window_size < maximum_window)" in page_io
    assert "window_size = std::min(maximum_window, window_size * 2)" in page_io
    assert "bytes.resize(window_size)" in page_io
    assert "std::string bytes(maximum_window" not in page_io
    assert not (reader / "pages/footer_reader_page_io.cc.inc").exists()
    assert '#include "thrift/compact_reader.cc.inc"' in footer_reader
    assert (thrift / "compact_reader.cc.inc").is_file()
    assert not (thrift / "compact_reader_values.cc.inc").exists()
    assert not (thrift / "compact_reader_skip.cc.inc").exists()
