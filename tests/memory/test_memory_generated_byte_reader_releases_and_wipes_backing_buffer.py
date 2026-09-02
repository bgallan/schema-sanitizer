"""Checks secure generated-byte cleanup together with JSONL writer limits, null-free CSV
streams, recursive Arrow-to-JSON validation, and tracking-pool wipe behavior. Backing
buffers are overwritten before release, redundant wipes are avoided, and validation
occurs before serialization can retain nested data."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_generated_byte_reader_releases_and_wipes_backing_buffer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing a sensitive generated stream must wipe and detach its allocation."""
    from schema_sanitizer.core_impl.generated_bytes import BufferedGeneratedBytesReader

    class _Reader(BufferedGeneratedBytesReader):
        """Helper class used by this regression."""

        def __init__(self) -> None:
            """Initialize the reader test double."""
            super().__init__("memory regression", default_chunk_bytes=16)

        def _append_next(self, target_bytes: int) -> bool:
            """Report end of generated input without appending bytes."""
            del target_bytes
            return False

        def _reset_reader(self) -> None:
            """Reset the generated-byte reader to its initial state."""
            return None

    reader = _Reader()
    reader._buffer.extend(b"secret payload")
    retained = reader._buffer

    reader.close()

    # Clearing the existing allocation avoids allocating a replacement during
    # terminal cleanup while still wiping every sensitive byte in place.
    assert reader._buffer is retained
    assert bytes(retained) == b""
    assert reader._buffer == bytearray()


def test_generated_byte_reader_overwrites_before_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The detached allocation is explicitly zeroed before its logical clear."""
    from schema_sanitizer.core_impl import generated_bytes

    observed: list[bytes] = []
    original_clear = generated_bytes._zero_bytearray_range

    def capture(buffer: bytearray, start: int, end: int) -> None:
        """Helper used by this regression."""
        original_clear(buffer, start, end)
        observed.append(bytes(buffer))

    class _Reader(generated_bytes.BufferedGeneratedBytesReader):
        """Helper class used by this regression."""

        def __init__(self) -> None:
            """Initialize the reader test double."""
            super().__init__("memory regression", default_chunk_bytes=16)

        def _append_next(self, target_bytes: int) -> bool:
            """Report end of generated input without appending bytes."""
            del target_bytes
            return False

        def _reset_reader(self) -> None:
            """Reset the generated-byte reader to its initial state."""
            return None

    monkeypatch.setattr(generated_bytes, "_zero_bytearray_range", capture)
    reader = _Reader()
    reader._buffer.extend(b"secret")

    reader.close()

    assert observed == [b"\x00" * 6]


def test_jsonl_writer_enforces_logical_buffer_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Foreign Arrow offsets cannot make the writer read beyond its byte budget."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.jsonl_sink import write_jsonl_stream

    batch = pa.record_batch({"payload": pa.array(["abcd"])})
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    with pytest.raises(RuntimeError, match="logical byte limit"):
        write_jsonl_stream(
            source,
            tmp_path / "bounded.jsonl",
            feature="memory regression",
            memory_limit_bytes=1,
        )


def test_csv_nested_stream_omits_validity_without_nulls(require_native: None) -> None:
    """Rendered nested UTF-8 columns allocate validity only after the first null."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.csv_sink import native_csv_nested_reader

    batch = pa.record_batch({"items": pa.array([[1, 2], [3]])})
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    reader = native_csv_nested_reader(source, pa=pa)
    output = reader.read_next_batch().column(0)

    assert output.to_pylist() == ["[1,2]", "[3]"]
    assert output.buffers()[0] is None


def test_arrow_json_validation_is_recursive_and_precedes_serialization() -> None:
    """The JSONL and CSV paths share one recursive hostile-metadata gate."""
    validator = (ROOT / "cpp/src/internal/json_output/schema/array_validation.cc").read_text(
        encoding="utf-8"
    )
    writer = (ROOT / "cpp/src/internal/json_output/jsonl_stream_writer.cc").read_text(
        encoding="utf-8"
    )
    nested_csv = (ROOT / "cpp/src/api/python_abi3/csv/nested_stream/nested_stream.cc").read_text(
        encoding="utf-8"
    )

    assert "validate_array_slice_impl" in validator
    assert "validate_dictionary_indices" in validator
    assert "fixed-size list child offset overflow" in validator
    ordered_output = (ROOT / "cpp/src/internal/output/ordered_text_output.hh").read_text(
        encoding="utf-8"
    )
    assert ordered_output.index("validate_batch(batch->value())") < ordered_output.index(
        "executor->Submit"
    )
    assert "append_value(" in writer
    get_next = nested_csv.split("int get_next", maxsplit=1)[1]
    assert get_next.index("jsonl::validate_array_slice") < get_next.index("build_nested_utf8_array")


def test_tracking_pool_avoids_redundant_secure_wipes() -> None:
    """Nested pools delegate one final overwrite to a parent that guarantees it."""
    header = (ROOT / "cpp/src/internal/memory/memory_pool.hh").read_text(encoding="utf-8")
    default_pool = (ROOT / "cpp/src/internal/memory/memory_pool.cc").read_text(encoding="utf-8")
    tracking = (ROOT / "cpp/src/internal/memory/tracking_memory_pool.cc.inc").read_text(
        encoding="utf-8"
    )

    assert "wipes_memory_on_free() const noexcept" in header
    assert "return secure_memory_cleanup_enabled();" in default_pool
    assert "!parent_->wipes_memory_on_free()" in tracking
    assert tracking.count("secure_zero_memory(buffer") == 1
