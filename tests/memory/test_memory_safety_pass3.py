"""Regression coverage for the third defensive memory-hardening pass."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[2]


def test_options_decoder_rejects_impossible_string_count_before_reserve() -> None:
    """A four-byte hostile count must be invalid, not an allocation request."""
    require_native()
    from schema_sanitizer.core_impl import native_options
    from schema_sanitizer.core_impl.native_runtime import native_core

    payload = bytearray(native_options._encode_options_bytes(native_options.Options()))
    payload[11:15] = (0xFFFFFFFF).to_bytes(4, "little")

    with pytest.raises(RuntimeError, match="true_tokens"):
        native_core.options_prepare_bytes(bytes(payload))


def test_logical_schema_decoder_checks_physical_bytes_before_reserve() -> None:
    """A declared field collection must fit in the remaining wire payload."""
    require_native()
    from schema_sanitizer.core_impl.logical_schema import LogicalSchemaPayload

    payload = (65_536).to_bytes(4, "little")
    with pytest.raises(ValueError, match="field count exceeds remaining bytes"):
        LogicalSchemaPayload(payload)


def test_options_string_iterable_is_bounded_without_list_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """String-list encoding must enforce its limit while consuming an iterator."""
    from schema_sanitizer.core_impl import native_options

    monkeypatch.setattr(native_options, "_MAX_STRING_LIST_ITEMS", 2)
    consumed: list[int] = []

    def values():
        """Yield enough values to cross the patched defensive limit."""
        for index in range(3):
            consumed.append(index)
            yield str(index)

    with pytest.raises(ValueError, match="exceeds safety limit"):
        native_options._append_vec_string(bytearray(), values())
    assert consumed == [0, 1, 2]


def test_native_parquet_writer_bounds_retained_row_group_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The writer must reject a pathological row-group count before growing footer state."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import direct_writers

    batch = pa.record_batch({"id": pa.array(range(64), type=pa.int64())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    with pytest.raises(RuntimeError, match="row-group count exceeds safety limit"):
        direct_writers.try_write_parquet_direct_native(
            stream,
            tmp_path / "bounded.parquet",
            first_row_columns=None,
            all_row_columns=None,
            row_span_columns=None,
            timestamp_columns=(),
            parquet_compression="uncompressed",
            memory_limit_bytes=64 * 1024,
        )


def test_schema_payload_and_page_indexes_avoid_duplicate_materialization() -> None:
    """Keep one schema codec and generate page-index lists from retained pages."""
    payload_source = (ROOT / "cpp/src/api/python_abi3/arrow_direct/schema/payload.cc").read_text(
        encoding="utf-8"
    )
    page_index_source = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_page_indexes.cc.inc"
    ).read_text(encoding="utf-8")
    interner_source = (ROOT / "cpp/src/sanitize/detail/intern.hh").read_text(encoding="utf-8")

    assert "serialize_logical_schema_bytes" in payload_source
    assert "void append_u32" not in payload_source
    assert "std::vector<std::string> min_values" not in page_index_source
    assert "std::pmr::deque<std::pmr::string>" in interner_source


def test_metadata_stream_omits_redundant_validity_bitmap() -> None:
    """Non-null generated metadata should not allocate one validity bit per row."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.metadata_native import CapsuleArrowStream
    from schema_sanitizer.core_impl.native_symbols import METADATA_STREAM_WRAP

    batch = pa.record_batch({"id": pa.array([1, 2, 3], type=pa.int64())})
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    capsule = METADATA_STREAM_WRAP(source, {}, {"tag": "x"}, {}, ())
    output = pa.RecordBatchReader.from_stream(CapsuleArrowStream(capsule)).read_next_batch()
    tag = output.column(output.schema.get_field_index("tag"))

    assert tag.to_pylist() == ["x", "x", "x"]
    assert tag.buffers()[0] is None


def test_metadata_and_schema_payload_boundaries_are_explicit_results() -> None:
    """Foreign metadata and schema serialization must fail before unsafe allocation."""
    metadata_builder = (
        ROOT / "cpp/src/api/python_abi3/metadata/stream/array_builder.cc"
    ).read_text(encoding="utf-8")
    metadata_lifecycle = (ROOT / "cpp/src/api/python_abi3/metadata/stream/stream.cc").read_text(
        encoding="utf-8"
    )
    schema_header = (ROOT / "cpp/src/internal/planning/options_schema_serialization.hh").read_text(
        encoding="utf-8"
    )
    schema_codec = (ROOT / "cpp/src/internal/planning/options_schema_serialization.cc").read_text(
        encoding="utf-8"
    )

    array_builder = metadata_builder.split("sanitize::Status build_metadata_array", maxsplit=1)[1]
    assert "validate_metadata_base_array(*stream_state, base)" in array_builder
    assert array_builder.index(
        "validate_metadata_base_array(*stream_state, base)"
    ) < array_builder.index("state->children.resize")
    assert "base.offset + base.length > max_slots" in metadata_lifecycle
    assert "sanitize::Result<std::string>" in schema_header
    assert "catch (const std::bad_alloc &)" in schema_codec
