"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from conftest import require_native

_GENERATED_METADATA_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _write_csv(path: Path, text: str = "a,b\n1,2\n3,4\n") -> Path:
    """Write csv."""
    path.write_text(text, encoding="utf-8")
    return path


def _without_generated_metadata(row: dict[str, object]) -> dict[str, object]:
    """Return row data excluding generated file-converter metadata columns."""
    return {k: v for k, v in row.items() if k not in _GENERATED_METADATA_COLUMNS}


def _without_generated_metadata_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Return rows excluding generated file-converter metadata columns."""
    return [_without_generated_metadata(row) for row in rows]


def _native_parquet_zlib_available(pa: object, tmp_path: Path) -> bool:
    """Return whether the compiled native Parquet writer can emit gzip pages."""
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output

    write = native_parquet_output.PARQUET_STREAM_WRITE
    if write is None:
        return False
    batch = pa.record_batch({"text": pa.array(["probe"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    try:
        write(stream, str(tmp_path / "native-zlib-probe.parquet"), "gzip", -1, -1)
    except RuntimeError as exc:
        if "zlib is not available" in str(exc):
            return False
        raise
    return True


def test_parquet_native_file_output_dictionary_encodes_repeated_byte_arrays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify repeated string/binary values use Parquet dictionary encoding."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "name": pa.array(["alpha", "beta", "alpha", None, "alpha"], type=pa.string()),
            "payload": pa.array([b"x", b"x", None, b"y", b"x"], type=pa.binary()),
            "unique": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary-encoded.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_dictionary_encodes_repeated_byte_arrays",
        parquet_compression="uncompressed",
    )
    table = pq.read_table(out)
    assert table.to_pylist() == [
        {"name": "alpha", "payload": b"x", "unique": "a"},
        {"name": "beta", "payload": b"x", "unique": "b"},
        {"name": "alpha", "payload": None, "unique": "c"},
        {"name": None, "payload": b"y", "unique": "d"},
        {"name": "alpha", "payload": b"x", "unique": "e"},
    ]
    parquet_file = pq.ParquetFile(out)
    name_meta = parquet_file.metadata.row_group(0).column(0)
    payload_meta = parquet_file.metadata.row_group(0).column(1)
    unique_meta = parquet_file.metadata.row_group(0).column(2)
    assert "RLE_DICTIONARY" in name_meta.encodings
    assert name_meta.dictionary_page_offset is not None
    assert "DELTA_LENGTH_BYTE_ARRAY" in payload_meta.encodings
    assert payload_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" not in unique_meta.encodings
    assert "DELTA_LENGTH_BYTE_ARRAY" in unique_meta.encodings
    assert unique_meta.dictionary_page_offset is None
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_preserves_null_dictionary_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify dictionary values that are null materialize as Parquet nulls."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "name": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, None, 0], type=pa.int8()),
                pa.array([None, "x"], type=pa.string()),
            ),
            "score": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, None, 0], type=pa.int8()),
                pa.array([None, 7], type=pa.int64()),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary-null-values.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_preserves_null_dictionary_values",
    )

    assert native_file_output.last_parquet_stream_route() == "native"
    assert pq.read_table(out).to_pylist() == [
        {"name": None, "score": None},
        {"name": "x", "score": 7},
        {"name": None, "score": None},
        {"name": None, "score": None},
    ]
    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).statistics.null_count == 3
    assert parquet_file.metadata.row_group(0).column(1).statistics.null_count == 3


def test_parquet_native_file_output_accepts_coalesced_empty_byte_arrays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify coalesced empty string/binary buffers write through native Parquet."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.pyarrow.metadata_native import CapsuleArrowStream
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output
    from schema_sanitizer.core_impl.native_symbols import COALESCING_STREAM_WRAP

    wrap = COALESCING_STREAM_WRAP
    if wrap is None:
        pytest.skip("native coalescing stream wrapper is unavailable")

    schema = pa.schema(
        [
            pa.field("text", pa.string()),
            pa.field("payload", pa.binary()),
        ]
    )
    batches = [
        pa.record_batch(
            {
                "text": pa.array(["", ""], type=pa.string()),
                "payload": pa.array([b"", b""], type=pa.binary()),
            },
            schema=schema,
        ),
        pa.record_batch(
            {
                "text": pa.array([""], type=pa.string()),
                "payload": pa.array([b""], type=pa.binary()),
            },
            schema=schema,
        ),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    stream = CapsuleArrowStream(wrap(reader, 1024))
    out = tmp_path / "coalesced-empty-byte-arrays.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_accepts_coalesced_empty_byte_arrays",
    )

    assert native_file_output.last_parquet_stream_route() == "native"
    assert pq.read_table(out).to_pylist() == [
        {"text": "", "payload": b""},
        {"text": "", "payload": b""},
        {"text": "", "payload": b""},
    ]


def test_parquet_native_file_output_skips_dictionary_when_payload_is_larger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify dictionary encoding is used only when it reduces value payload size."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "short_text": pa.array(["a", "a", "b", "c", "d"], type=pa.string()),
            "small_int": pa.array([1, 1, 2, 3, 4], type=pa.int32()),
            "long_text": pa.array(["alphabet" * 8, "alphabet" * 8, "x", "y", "z"]),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary-profit.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_skips_dictionary_when_payload_is_larger",
        parquet_compression="uncompressed",
    )
    assert pq.read_table(out).to_pylist() == batch.to_pylist()
    parquet_file = pq.ParquetFile(out)
    short_text_meta = parquet_file.metadata.row_group(0).column(0)
    small_int_meta = parquet_file.metadata.row_group(0).column(1)
    long_text_meta = parquet_file.metadata.row_group(0).column(2)
    assert "RLE_DICTIONARY" not in short_text_meta.encodings
    assert short_text_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" not in small_int_meta.encodings
    assert small_int_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" in long_text_meta.encodings
    assert long_text_meta.dictionary_page_offset is not None
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_dictionary_encodes_repeated_fixed_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify repeated fixed-width values use Parquet dictionary encoding."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "id": pa.array([7, 8, 7, None, 7], type=pa.int64()),
            "score": pa.array([1.5, 2.5, 1.5, None, 1.5], type=pa.float64()),
            "amount": pa.array(
                [Decimal("1.00"), Decimal("2.00"), Decimal("1.00"), None, None],
                type=pa.decimal128(10, 2),
            ),
            "flag": pa.array([True, False, True, None, None], type=pa.bool_()),
            "unique": pa.array([1, 2, 3, 4, None], type=pa.int32()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "fixed-dictionary-encoded.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_dictionary_encodes_repeated_fixed_values",
        parquet_compression="uncompressed",
    )
    assert pq.read_table(out).to_pylist() == [
        {"id": 7, "score": 1.5, "amount": Decimal("1.00"), "flag": True, "unique": 1},
        {"id": 8, "score": 2.5, "amount": Decimal("2.00"), "flag": False, "unique": 2},
        {"id": 7, "score": 1.5, "amount": Decimal("1.00"), "flag": True, "unique": 3},
        {"id": None, "score": None, "amount": None, "flag": None, "unique": 4},
        {"id": 7, "score": 1.5, "amount": None, "flag": None, "unique": None},
    ]
    parquet_file = pq.ParquetFile(out)
    id_meta = parquet_file.metadata.row_group(0).column(0)
    score_meta = parquet_file.metadata.row_group(0).column(1)
    amount_meta = parquet_file.metadata.row_group(0).column(2)
    assert "DELTA_BINARY_PACKED" in id_meta.encodings
    assert id_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" in score_meta.encodings
    assert score_meta.dictionary_page_offset is not None
    assert "RLE_DICTIONARY" in amount_meta.encodings
    assert amount_meta.dictionary_page_offset is not None
    for column_index in (3, 4):
        metadata = parquet_file.metadata.row_group(0).column(column_index)
        assert "RLE_DICTIONARY" not in metadata.encodings
        assert metadata.dictionary_page_offset is None
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_dictionary_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer decodes dictionary columns."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "name": pa.array(
                ["one", "two", "one", None],
                type=pa.dictionary(pa.int8(), pa.string()),
            ),
            "flag": pa.array(
                [True, False, True, None],
                type=pa.dictionary(pa.int8(), pa.bool_()),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_dictionary_stream",
    )

    assert pq.read_table(out).to_pylist() == [
        {"name": "one", "flag": True},
        {"name": "two", "flag": False},
        {"name": "one", "flag": True},
        {"name": None, "flag": None},
    ]
    assert native_file_output.last_parquet_stream_route() == "native"
