"""Native Parquet sink encoding, layout, compression, and routing contracts.

It validates statistics, page indexes, row groups, compression, dictionaries, scalar and
nested encodings, routing, and native flat-stream output.
"""

from __future__ import annotations

import datetime as dt
import logging
import math
from decimal import Decimal
from pathlib import Path

import pytest
from _support.sinks import fail_pyarrow_sink
from _support.sinks import native_parquet_zlib_available as _native_parquet_zlib_available
from _support.sinks import without_generated_metadata_rows as _without_generated_metadata_rows
from _support.sinks import write_csv as _write_csv

import schema_sanitizer as ss
from schema_sanitizer.core_impl.schema_registry import merge_schema_registry


def test_parquet_native_file_output_writes_float_statistics_without_nan_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes float statistics without nan bounds."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "reading": pa.array([float("nan"), 1.5, None, -2.0], type=pa.float64()),
            "empty_reading": pa.array(
                [float("nan"), None, float("nan"), None],
                type=pa.float32(),
            ),
            "zero": pa.array([-0.0, 0.0, None, None], type=pa.float64()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "float-stats.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_float_statistics_without_nan_bounds",
    )

    parquet_file = pq.ParquetFile(out)
    reading_stats = parquet_file.metadata.row_group(0).column(0).statistics
    assert reading_stats.null_count == 1
    assert reading_stats.min == -2.0
    assert reading_stats.max == 1.5
    empty_stats = parquet_file.metadata.row_group(0).column(1).statistics
    assert empty_stats.null_count == 2
    assert not empty_stats.has_min_max
    zero_stats = parquet_file.metadata.row_group(0).column(2).statistics
    assert zero_stats.null_count == 2
    assert zero_stats.min == -0.0
    assert zero_stats.max == 0.0
    assert math.copysign(1.0, zero_stats.min) == -1.0
    assert math.copysign(1.0, zero_stats.max) == 1.0


def test_parquet_native_file_output_skips_column_index_without_page_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output skips column index without page bounds."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"value": pa.array([float("nan"), float("nan")], type=pa.float64())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "nan-only-no-column-index.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_skips_column_index_without_page_bounds",
    )

    metadata = pq.ParquetFile(out).metadata.row_group(0).column(0)
    assert not metadata.has_column_index
    assert metadata.has_offset_index
    stats = metadata.statistics
    assert stats is not None
    assert not stats.has_min_max
    rows = pq.read_table(out).to_pylist()
    assert len(rows) == 2
    assert all(math.isnan(row["value"]) for row in rows)


def test_parquet_native_file_output_splits_large_batches_into_row_groups(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output splits large batches into row groups."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    row_count = 400
    batch = pa.record_batch(
        {
            "id": pa.array(range(1, row_count + 1), type=pa.int64()),
            "name": pa.array([f"name-{index}" for index in range(row_count)], type=pa.string()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "split-row-groups.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_splits_large_batches_into_row_groups",
        memory_limit_bytes=256 * 1024,
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_rows == row_count
    assert parquet_file.metadata.num_row_groups == 4
    assert [parquet_file.metadata.row_group(i).num_rows for i in range(4)] == [
        128,
        128,
        128,
        16,
    ]
    assert pq.read_table(out).to_pylist() == [
        {"id": index + 1, "name": f"name-{index}"} for index in range(row_count)
    ]


def test_parquet_native_file_output_respects_uncompressed_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output respects uncompressed override."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"text": pa.array(["same"] * 32, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "uncompressed.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_respects_uncompressed_override",
        parquet_compression="uncompressed",
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"
    assert pq.read_table(out).to_pylist() == [{"text": "same"}] * 32


def test_parquet_native_snappy_reduces_repeated_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native snappy reduces repeated payload."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    values = [f"shared-prefix-{'abc123' * 32}-{index:08d}" for index in range(1024)]
    batch = pa.record_batch({"text": pa.array(values, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "snappy.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_snappy_reduces_repeated_payload",
        parquet_compression="snappy",
    )

    parquet_file = pq.ParquetFile(out)
    column = parquet_file.metadata.row_group(0).column(0)
    assert column.compression == "SNAPPY"
    assert column.total_compressed_size < column.total_uncompressed_size
    assert pq.read_table(out).column("text").to_pylist() == values


def test_parquet_native_file_output_defaults_to_gzip_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output defaults to gzip when available."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    if not _native_parquet_zlib_available(pa, tmp_path):
        pytest.skip("native Parquet writer was built without zlib")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"text": pa.array(["compressible"] * 128, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "gzip.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_defaults_to_gzip_when_available",
    )

    parquet_file = pq.ParquetFile(out)
    compression = parquet_file.metadata.row_group(0).column(0).compression
    if compression == "UNCOMPRESSED":
        pytest.skip("native Parquet writer was built without zlib")
    assert compression == "GZIP"
    assert pq.read_table(out).to_pylist() == [{"text": "compressible"}] * 128


def test_parquet_native_file_output_accepts_gzip_level(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output accepts gzip level."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    if not _native_parquet_zlib_available(pa, tmp_path):
        pytest.skip("native Parquet writer was built without zlib")
    batch = pa.record_batch({"text": pa.array(["compressible"] * 128, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "gzip-level.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_accepts_gzip_level",
        parquet_compression="gzip",
        parquet_gzip_level=9,
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "GZIP"
    assert pq.read_table(out).to_pylist() == [{"text": "compressible"}] * 128


def test_parquet_native_file_output_rejects_invalid_gzip_level(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output rejects invalid gzip level."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    if not _native_parquet_zlib_available(pa, tmp_path):
        pytest.skip("native Parquet writer was built without zlib")
    batch = pa.record_batch({"text": pa.array(["x"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    with pytest.raises(ValueError, match="parquet_gzip_level"):
        native_file_output.write_parquet_native_first_stream(
            stream,
            tmp_path / "bad-gzip-level.parquet",
            feature="test_parquet_native_file_output_rejects_invalid_gzip_level",
            parquet_compression="gzip",
            parquet_gzip_level=10,
        )


@pytest.mark.parametrize("compression", ["brotli", "none"])
def test_parquet_native_file_output_rejects_unknown_compression(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    compression: str,
    require_native: None,
) -> None:
    """Verify Parquet native file output rejects unknown compression."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    batch = pa.record_batch({"text": pa.array(["x"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    with pytest.raises(ValueError, match="parquet_compression"):
        native_file_output.write_parquet_native_first_stream(
            stream,
            tmp_path / "bad-compression.parquet",
            feature="test_parquet_native_file_output_rejects_unknown_compression",
            parquet_compression=compression,
        )


def test_to_parquet_public_compression_option_writes_uncompressed(
    tmp_path: Path, require_native: None
) -> None:
    """Verify to Parquet public compression option writes uncompressed."""
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n{"text":"same"}\n', encoding="utf-8")
    out = tmp_path / "public-uncompressed.parquet"

    result = ss.to_parquet(source, out, input_format="jsonl", parquet_compression="uncompressed")

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"
    assert result.stats["file_output_route"] == "native_direct"
    assert result.stats["file_metadata_route"] == "none"


def test_to_parquet_public_gzip_level_option_writes_gzip(
    tmp_path: Path, require_native: None
) -> None:
    """Verify to Parquet public gzip level option writes gzip."""
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n{"text":"same"}\n', encoding="utf-8")
    out = tmp_path / "public-gzip.parquet"

    ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        parquet_compression="gzip",
        parquet_gzip_level=9,
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "GZIP"


def test_to_parquet_public_compression_option_rejects_unknown(tmp_path: Path) -> None:
    """Verify to Parquet public compression option rejects unknown."""
    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="parquet_compression"):
        ss.to_parquet(
            source, tmp_path / "out.parquet", input_format="jsonl", parquet_compression="brotli"
        )


def test_to_parquet_public_gzip_level_option_rejects_out_of_range(tmp_path: Path) -> None:
    """Verify to Parquet public gzip level option rejects out of range."""
    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="parquet_gzip_level"):
        ss.to_parquet(
            source,
            tmp_path / "out.parquet",
            input_format="jsonl",
            parquet_gzip_level=10,
        )


def test_to_parquet_public_compression_option_reaches_pyarrow_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify to Parquet public compression option reaches PyArrow fallback."""
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import stream_output
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output

    monkeypatch.setattr(
        stream_output,
        "try_write_raw_native_file_output",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        native_parquet_output,
        "try_write_parquet_direct_native",
        lambda *_args, **_kwargs: False,
    )
    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n{"text":"same"}\n', encoding="utf-8")
    out = tmp_path / "pyarrow-uncompressed.parquet"

    ss.to_parquet(source, out, input_format="jsonl", parquet_compression="uncompressed")

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"


def test_to_parquet_writes_file(tmp_path: Path, require_native: None) -> None:
    """Verify to Parquet writes file."""
    pq = pytest.importorskip("pyarrow.parquet")

    out = tmp_path / "out.parquet"
    result = ss.to_parquet(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out,
        input_format="csv",
    )
    assert isinstance(result, ss.Result)
    assert result.clean_data is None
    rows = pq.read_table(out).to_pylist()
    assert _without_generated_metadata_rows(rows) == [{"a": "1", "b": "2"}]


def test_parquet_sink_native_coalesces_flat_arrow_batches(
    tmp_path: Path, require_native: None
) -> None:
    """Verify Parquet sink native coalesces flat arrow batches."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.parquet.sink import _write_coalesced_batches

    batches = [
        pa.record_batch(
            {
                "id": pa.array([index], type=pa.int64()),
                "name": pa.array([f"name-{index}"], type=pa.string()),
            }
        )
        for index in range(8)
    ]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    out = tmp_path / "flat.parquet"
    writer = pq.ParquetWriter(out, batches[0].schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=batches[0].schema,
            pa=pa,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == [
        {"id": index, "name": f"name-{index}"} for index in range(8)
    ]


def test_parquet_sink_native_coalesces_nested_arrow_batches(
    tmp_path: Path, require_native: None
) -> None:
    """Verify Parquet sink native coalesces nested arrow batches."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.parquet.sink import _write_coalesced_batches

    payload_type = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("flags", pa.list_(pa.bool_())),
        ]
    )
    item_type = pa.struct([pa.field("score", pa.float64()), pa.field("label", pa.string())])
    schema = pa.schema(
        [
            pa.field("payload", payload_type),
            pa.field("items", pa.list_(item_type)),
        ]
    )
    rows = [
        {
            "payload": (
                None
                if index == 5
                else {
                    "id": index,
                    "name": None if index == 2 else f"name-{index}",
                    "flags": None if index == 1 else [True, False] if index % 2 == 0 else [],
                }
            ),
            "items": (
                None
                if index == 4
                else (
                    []
                    if index % 3 == 0
                    else [
                        {"score": index + 0.5, "label": f"a-{index}"},
                        {"score": index + 1.5, "label": None if index == 7 else f"b-{index}"},
                    ]
                )
            ),
        }
        for index in range(8)
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    batches = [table.slice(index, 1).to_batches()[0] for index in range(8)]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    out = tmp_path / "nested.parquet"
    writer = pq.ParquetWriter(out, schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=schema,
            pa=pa,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == rows


def test_parquet_sink_native_coalesces_dictionary_arrow_batches(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Verify Parquet sink native coalesces dictionary arrow batches."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.parquet.sink import _write_coalesced_batches

    schema = pa.schema([pa.field("coded", pa.dictionary(pa.int8(), pa.string()))])
    dictionary = pa.array(["value-0", "value-1"], type=pa.string())
    batches = [
        pa.record_batch(
            [pa.DictionaryArray.from_arrays(pa.array([index % 2], type=pa.int8()), dictionary)],
            schema=schema,
        )
        for index in range(8)
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    out = tmp_path / "dictionary.parquet"
    writer = pq.ParquetWriter(out, schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=schema,
            pa=pa,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == [{"coded": f"value-{index % 2}"} for index in range(8)]


def test_parquet_sink_rejects_changed_dictionary_during_native_coalescing(
    require_native: None,
) -> None:
    """Verify Parquet sink rejects changed dictionary during native coalescing."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.parquet.sink import _write_coalesced_batches

    schema = pa.schema([pa.field("coded", pa.dictionary(pa.int8(), pa.string()))])
    batches = [
        pa.record_batch(
            [
                pa.DictionaryArray.from_arrays(
                    pa.array([0], type=pa.int8()),
                    pa.array(["a", "b"], type=pa.string()),
                )
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.DictionaryArray.from_arrays(
                    pa.array([0], type=pa.int8()),
                    pa.array(["b", "a"], type=pa.string()),
                )
            ],
            schema=schema,
        ),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)

    class Writer:
        """Track whether unsafe dictionary coalescing writes any batch."""

        wrote = False

        def write_batch(self, _batch: object) -> None:
            """Record an attempted native sink batch write."""
            self.wrote = True

    writer = Writer()
    with pytest.raises(Exception, match="dictionary values changed"):
        _write_coalesced_batches(writer, reader, schema=schema, pa=pa)

    assert writer.wrote is False


def test_to_parquet_omits_null_and_empty_container_only_fields(
    tmp_path: Path, require_native: None
) -> None:
    """Verify to Parquet omits null and empty container only fields."""
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"null_value":null,"writer":{},"items":[],"null_wrapper":{"child":null},'
        '"wrapper":{"child":{}},"null_items":[null],"nested_items":[{}]}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.parquet"

    ss.to_parquet(source, out, input_format="jsonl")

    table = pq.read_table(out)
    assert "nullvalue" not in table.schema.names
    assert "writer" not in table.schema.names
    assert "items" not in table.schema.names
    assert "nullwrapper" not in table.schema.names
    assert "wrapper" not in table.schema.names
    assert "nullitems" not in table.schema.names
    assert "nesteditems" not in table.schema.names
    assert _without_generated_metadata_rows(table.to_pylist()) == [{}]


def test_to_parquet_writes_mixed_null_empty_and_populated_values(
    tmp_path: Path, require_native: None
) -> None:
    """Verify to Parquet writes mixed null empty and populated values."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"score":null,"writer":{},"items":[]}\n'
        '{"score":3,"writer":{"name":"Alex"},"items":[1,2]}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.parquet"

    ss.to_parquet(source, out, input_format="jsonl")

    table = pq.read_table(out)
    assert pa.types.is_int64(table.schema.field("score").type)
    assert pa.types.is_struct(table.schema.field("writer").type)
    assert pa.types.is_list(table.schema.field("items").type)
    assert _without_generated_metadata_rows(table.to_pylist()) == [
        {"items": None, "score": None, "writer": None},
        {"items": [1, 2], "score": 3, "writer": {"name": "Alex"}},
    ]


def test_registry_keeps_existing_fields_for_empty_containers(
    tmp_path: Path, require_native: None
) -> None:
    """Verify registry keeps existing fields for empty containers."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    previous = merge_schema_registry(
        inferred_schema=pa.schema(
            [
                pa.field("items", pa.list_(pa.int64())),
                pa.field("score", pa.int64()),
                pa.field("writer", pa.struct([pa.field("id", pa.int64())])),
            ]
        ),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    source = tmp_path / "rows.jsonl"
    source.write_text('{"items":[],"score":null,"writer":{}}\n', encoding="utf-8")
    out = tmp_path / "out.parquet"

    result = ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=previous.schema_registry,
    )

    row = _without_generated_metadata_rows(pq.read_table(out).to_pylist())[0]
    assert row == {"items": None, "score": None, "writer": None}
    assert result.schema_drifts == []
    assert (
        result.schema_registry["schema_generation"] == previous.schema_registry["schema_generation"]
    )


def test_empty_first_partition_does_not_destabilize_registry(
    tmp_path: Path, require_native: None
) -> None:
    """Verify empty first partition does not destabilize registry."""
    pq = pytest.importorskip("pyarrow.parquet")

    empty_source = tmp_path / "empty.jsonl"
    empty_source.write_text(
        '{"items":[],"score":null,"writer":{}}\n',
        encoding="utf-8",
    )
    empty_out = tmp_path / "empty.parquet"
    empty_result = ss.to_parquet(
        empty_source,
        empty_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
    )

    populated_source = tmp_path / "populated.jsonl"
    populated_source.write_text(
        '{"items":[1],"score":3,"writer":{"id":2}}\n',
        encoding="utf-8",
    )
    populated_out = tmp_path / "populated.parquet"
    populated_result = ss.to_parquet(
        populated_source,
        populated_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
        schema_registry=empty_result.schema_registry,
    )

    populated_names = pq.read_table(populated_out).schema.names
    assert {"items", "score", "writer"}.issubset(populated_names)
    assert not any(name.startswith(("items_v", "score_v", "writer_v")) for name in populated_names)
    assert [drift["output_name"] for drift in populated_result.schema_drifts] == [
        "items",
        "score",
        "writer",
    ]

    replay_out = tmp_path / "replay.parquet"
    replay_result = ss.to_parquet(
        empty_source,
        replay_out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=populated_result.schema_registry,
    )

    replay_row = _without_generated_metadata_rows(pq.read_table(replay_out).to_pylist())[0]
    assert replay_row == {"items": None, "score": None, "writer": None}
    assert replay_result.schema_drifts == []
    assert (
        replay_result.schema_registry["schema_generation"]
        == populated_result.schema_registry["schema_generation"]
    )


def test_parquet_native_file_output_dictionary_encodes_repeated_byte_arrays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output dictionary encodes repeated byte arrays."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

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


def test_parquet_native_file_output_preserves_null_dictionary_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output preserves null dictionary values."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output accepts coalesced empty byte arrays."""
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

    assert pq.read_table(out).to_pylist() == [
        {"text": "", "payload": b""},
        {"text": "", "payload": b""},
        {"text": "", "payload": b""},
    ]


def test_parquet_native_file_output_skips_dictionary_when_payload_is_larger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output skips dictionary when payload is larger."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

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


def test_parquet_native_file_output_dictionary_encodes_repeated_fixed_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output dictionary encodes repeated fixed values."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

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


def test_parquet_native_file_output_writes_dictionary_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes dictionary stream."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

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


def test_parquet_native_file_output_writes_integer_width_logical_types(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes integer width logical types."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "i8": pa.array([-5, 7, None, -5], type=pa.int8()),
            "u8": pa.array([250, 1, None, 250], type=pa.uint8()),
            "i16": pa.array([-300, 12, None, -300], type=pa.int16()),
            "u16": pa.array([65000, 2, None, 65000], type=pa.uint16()),
            "u32": pa.array([4_000_000_000, 7, None, 4_000_000_000], type=pa.uint32()),
            "u64": pa.array(
                [2**63 + 5, 9, None, 2**63 + 5],
                type=pa.uint64(),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "integer-logical-types.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_integer_width_logical_types",
    )

    table = pq.read_table(out)
    assert table.schema == batch.schema
    assert table.to_pylist() == [
        {
            "i8": -5,
            "u8": 250,
            "i16": -300,
            "u16": 65000,
            "u32": 4_000_000_000,
            "u64": 2**63 + 5,
        },
        {"i8": 7, "u8": 1, "i16": 12, "u16": 2, "u32": 7, "u64": 9},
        {"i8": None, "u8": None, "i16": None, "u16": None, "u32": None, "u64": None},
        {
            "i8": -5,
            "u8": 250,
            "i16": -300,
            "u16": 65000,
            "u32": 4_000_000_000,
            "u64": 2**63 + 5,
        },
    ]


def test_parquet_native_file_output_writes_timestamp_nanos(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes timestamp nanos."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "event_at": pa.array(
                [1_640_995_200_000_000_123, None, 1_640_995_200_000_000_456],
                type=pa.timestamp("ns"),
            )
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "timestamp-nanos.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_timestamp_nanos",
    )

    table = pq.read_table(out)
    assert table.schema == batch.schema
    assert table.to_pylist() == batch.to_pylist()
    parquet_file = pq.ParquetFile(out)
    assert "Timestamp" in str(parquet_file.schema)


def test_parquet_native_file_output_writes_schema_sanitizer_logical_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes schema sanitizer logical surface."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    struct_type = pa.struct([pa.field("x", pa.int64()), pa.field("s", pa.string())])
    batch = pa.record_batch(
        {
            "empty": pa.array([None, None], type=pa.null()),
            "ok": pa.array([True, None], type=pa.bool_()),
            "id": pa.array([7, None], type=pa.int64()),
            "score": pa.array([1.5, None], type=pa.float64()),
            "text": pa.array(["value", None], type=pa.string()),
            "event_at": pa.array(
                [1_640_995_200_000_000, None],
                type=pa.timestamp("us"),
            ),
            "event_date": pa.array([19_723, None], type=pa.date32()),
            "clock": pa.array([3723, None], type=pa.time32("s")),
            "payload": pa.array(
                [{"x": 10, "s": "nested"}, None],
                type=struct_type,
            ),
            "items": pa.array([[1, None, 3], None], type=pa.list_(pa.int64())),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "schema-sanitizer-logical-surface.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_schema_sanitizer_logical_surface",
    )

    table = pq.read_table(out)
    assert table.schema.field("empty").type == pa.null()
    assert table.schema.field("ok").type == pa.bool_()
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.field("text").type == pa.string()
    assert table.schema.field("event_at").type == pa.timestamp("us")
    assert table.schema.field("event_date").type == pa.date32()
    assert table.schema.field("clock").type == pa.time32("ms")
    assert table.schema.field("payload").type == struct_type
    assert table.schema.field("items").type == pa.list_(pa.int64())
    assert table.to_pylist() == [
        {
            "empty": None,
            "ok": True,
            "id": 7,
            "score": 1.5,
            "text": "value",
            "event_at": dt.datetime(2022, 1, 1),
            "event_date": dt.date(2024, 1, 1),
            "clock": dt.time(1, 2, 3),
            "payload": {"x": 10, "s": "nested"},
            "items": [1, None, 3],
        },
        {
            "empty": None,
            "ok": None,
            "id": None,
            "score": None,
            "text": None,
            "event_at": None,
            "event_date": None,
            "clock": None,
            "payload": None,
            "items": None,
        },
    ]
    parquet_schema = str(pq.ParquetFile(out).schema)
    assert "Null" in parquet_schema
    assert "Time(isAdjustedToUTC=false, timeUnit=milliseconds)" in parquet_schema


def test_parquet_native_file_output_writes_nested_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes nested stream."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct([pa.field("id", pa.int64()), pa.field("name", pa.string())]),
            ),
            pa.field("scores", pa.list_(pa.int64())),
            pa.field("large_scores", pa.large_list(pa.int64())),
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
                            pa.field("amount", pa.decimal128(10, 2)),
                            pa.field("label", pa.string()),
                            pa.field("flags", pa.list_(pa.bool_())),
                        ]
                    )
                ),
            ),
            pa.field("matrix", pa.list_(pa.list_(pa.int64()))),
        ]
    )
    rows = [
        {
            "payload": {"id": 1, "name": "one"},
            "scores": [1, 2],
            "large_scores": [100, 200],
            "items": [
                {
                    "score": 10,
                    "amount": Decimal("12.34"),
                    "label": "a",
                    "flags": [True, False],
                }
            ],
            "matrix": [[1, 2], [], None, [3]],
        },
        {"payload": None, "scores": [], "large_scores": [], "items": [], "matrix": []},
        {
            "payload": {"id": None, "name": None},
            "scores": None,
            "large_scores": None,
            "items": None,
            "matrix": None,
        },
        {
            "payload": {"id": 4, "name": None},
            "scores": [None, 5],
            "large_scores": [None, 500],
            "items": [
                None,
                {"score": None, "amount": None, "label": "b", "flags": []},
                {
                    "score": 7,
                    "amount": Decimal("-0.01"),
                    "label": None,
                    "flags": None,
                },
            ],
            "matrix": [None, [None, 5], []],
        },
    ]
    batch = pa.Table.from_pylist(rows, schema=schema).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "nested.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_nested_stream",
    )

    assert pq.read_table(out).to_pylist() == rows


def test_parquet_native_file_output_writes_map_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes map stream."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("attrs", pa.map_(pa.string(), pa.int64())),
            pa.field("series", pa.map_(pa.string(), pa.list_(pa.int64()))),
        ]
    )
    rows = [
        {
            "attrs": [("one", 1), ("two", None)],
            "series": [("a", [1, 2]), ("b", []), ("c", None)],
        },
        {"attrs": [], "series": []},
        {"attrs": None, "series": None},
        {"attrs": [("negative", -3)], "series": [("x", [None, 5])]},
    ]
    batch = pa.record_batch(
        [
            pa.array([row["attrs"] for row in rows], type=schema.field("attrs").type),
            pa.array([row["series"] for row in rows], type=schema.field("series").type),
        ],
        schema=schema,
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "map.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_map_stream",
    )

    assert pq.read_table(out).to_pylist() == rows


def test_parquet_native_file_output_writes_fixed_size_list_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes fixed size list stream."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("vec", pa.list_(pa.int64(), 2)),
            pa.field(
                "items",
                pa.list_(pa.struct([pa.field("x", pa.int64())]), 2),
            ),
            pa.field("empty", pa.list_(pa.int64(), 0)),
        ]
    )
    rows = [
        {"vec": [1, 2], "items": [{"x": 1}, {"x": None}], "empty": []},
        {"vec": [None, 4], "items": [None, {"x": 2}], "empty": []},
        {"vec": None, "items": None, "empty": None},
    ]
    batch = pa.Table.from_pylist(rows, schema=schema).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "fixed-size-list.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_fixed_size_list_stream",
    )

    assert pq.read_table(out).to_pylist() == rows


def test_parquet_native_file_output_writes_generated_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes generated metadata."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"id": pa.array([1, 2], type=pa.int64())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "native-metadata.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_generated_metadata",
        first_row_columns={"schema_registry": "{}"},
        all_row_columns={"source_file": "/tmp/source.jsonl"},
        timestamp_columns=("ingestion_timestamp",),
    )

    table = pq.read_table(out)
    timestamp_type = table.schema.field("ingestion_timestamp").type
    assert pa.types.is_timestamp(timestamp_type)
    assert timestamp_type.unit == "us"
    rows = table.to_pylist()
    assert rows[0]["id"] == 1
    assert rows[0]["schema_registry"] == "{}"
    assert rows[0]["source_file"] == "/tmp/source.jsonl"
    assert rows[0]["ingestion_timestamp"] is not None
    assert rows[1]["id"] == 2
    assert rows[1]["schema_registry"] is None
    assert rows[1]["source_file"] == "/tmp/source.jsonl"
    assert rows[1]["ingestion_timestamp"] is not None


def test_parquet_native_file_output_splits_large_pages_without_dictionary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output splits large pages without dictionary."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    values = [f"source-file-{index:04d}/" + ("x" * 512) for index in range(400)]
    batch = pa.record_batch({"source_file": pa.array(values, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "page-split.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_splits_large_pages_without_dictionary",
        parquet_compression="uncompressed",
        memory_limit_bytes=1024 * 1024,
    )
    assert pq.read_table(out).to_pylist() == batch.to_pylist()
    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_row_groups == 1
    metadata = parquet_file.metadata.row_group(0).column(0)
    assert "RLE_DICTIONARY" not in metadata.encodings
    assert metadata.dictionary_page_offset is None
    assert metadata.has_column_index
    assert metadata.has_offset_index


def test_parquet_native_file_output_splits_row_groups_by_byte_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output splits row groups by byte budget."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    rows = [f"{index:03d}-" + ("payload" * 586) for index in range(800)]
    batch = pa.record_batch({"message": pa.array(rows, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "row-group-byte-budget.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_splits_row_groups_by_byte_budget",
        parquet_compression="uncompressed",
        memory_limit_bytes=4 * 1024 * 1024,
    )
    assert pq.read_table(out).to_pylist() == batch.to_pylist()
    assert pq.ParquetFile(out).metadata.num_row_groups > 1


def test_parquet_native_file_output_skips_delta_encoding_on_int64_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output skips delta encoding on int64 overflow."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    values = [-(2**63), 2**63 - 1, 0, None]
    batch = pa.record_batch({"value": pa.array(values, type=pa.int64())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "int64-delta-overflow.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_skips_delta_encoding_on_int64_overflow",
    )

    metadata = pq.ParquetFile(out).metadata.row_group(0).column(0)
    assert "DELTA_BINARY_PACKED" not in metadata.encodings
    assert pq.read_table(out).to_pylist() == [
        {"value": values[0]},
        {"value": values[1]},
        {"value": values[2]},
        {"value": None},
    ]


def test_parquet_native_file_output_preserves_sliced_batch_offsets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output preserves sliced batch offsets."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("scores", pa.list_(pa.int64())),
            pa.field("fixed", pa.list_(pa.int64(), 2)),
            pa.field(
                "payload",
                pa.struct([pa.field("x", pa.int64()), pa.field("s", pa.string())]),
            ),
        ]
    )
    rows = [
        {
            "id": 0,
            "name": "zero",
            "scores": [0],
            "fixed": [0, 10],
            "payload": {"x": 0, "s": "zero"},
        },
        {
            "id": 1,
            "name": "one",
            "scores": [1, 2],
            "fixed": [1, 11],
            "payload": {"x": 1, "s": "one"},
        },
        {
            "id": 2,
            "name": None,
            "scores": [],
            "fixed": [None, 12],
            "payload": None,
        },
        {
            "id": 3,
            "name": "three",
            "scores": None,
            "fixed": [3, None],
            "payload": {"x": None, "s": "three"},
        },
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    batch = table.slice(1, 2).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "sliced.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_preserves_sliced_batch_offsets",
    )

    assert pq.read_table(out).to_pylist() == rows[1:3]


def test_parquet_native_file_output_is_duckdb_readable_across_row_groups(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output is duckdb readable across row groups."""
    duckdb = pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field(
                "payload",
                pa.struct([pa.field("x", pa.int64()), pa.field("s", pa.string())]),
            ),
            pa.field("scores", pa.list_(pa.int64())),
        ]
    )
    rows = [
        {
            "id": index,
            "name": f"name-{index}",
            "payload": {"x": index * 10, "s": f"payload-{index}"},
            "scores": [index, index + 1],
        }
        for index in range(5)
    ]
    batch = pa.Table.from_pylist(rows, schema=schema).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "duckdb-readable.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_is_duckdb_readable_across_row_groups",
    )

    with duckdb.connect() as connection:
        assert connection.execute(
            """
            SELECT count(*), sum(id), max(payload.x), sum(list_sum(scores))
            FROM read_parquet(?)
            """,
            [str(out)],
        ).fetchone() == (5, 10, 40, 25)
        assert connection.execute(
            """
            SELECT payload.s
            FROM read_parquet(?)
            WHERE id = 3
            """,
            [str(out)],
        ).fetchone() == ("payload-3",)


def test_parquet_native_file_output_uses_native_writer_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output uses native writer when available."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fake_native_write(
        stream: object,
        output_path: str,
        compression: str,
        gzip_level: int,
        memory_limit_bytes: int,
        threading_mode: int,
    ) -> None:
        """Write a marker file through the fake native Parquet writer."""
        assert hasattr(stream, "__arrow_c_stream__")
        assert (compression, gzip_level, memory_limit_bytes, threading_mode) == (
            "gzip",
            -1,
            -1,
            0,
        )
        Path(output_path).write_bytes(b"native-parquet")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", fake_native_write)
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1", "2"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "direct.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_uses_native_writer_when_available",
    )

    assert out.read_bytes() == b"native-parquet"


def test_parquet_native_file_output_falls_back_when_gzip_lacks_zlib(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output falls back when gzip lacks zlib."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    fallback_calls: list[Path] = []

    def fake_native_write(
        _stream: object,
        _output_path: str,
        _compression: str,
        _gzip_level: int,
        _memory_limit_bytes: int,
        _threading_mode: int,
    ) -> None:
        """Simulate a native build without zlib."""
        raise RuntimeError(
            "native Parquet writer: gzip compression requested but zlib is not available"
        )

    def fake_pyarrow_sink(_stream: object, output_path: Path, **_kwargs: object) -> None:
        """Record PyArrow fallback and write a marker file."""
        fallback_calls.append(output_path)
        output_path.write_bytes(b"pyarrow-parquet")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", fake_native_write)
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fake_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "fallback.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_falls_back_when_gzip_lacks_zlib",
    )

    assert fallback_calls == [out]
    assert out.read_bytes() == b"pyarrow-parquet"


def test_parquet_native_file_output_retries_pyarrow_after_native_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    require_native: None,
) -> None:
    """Verify Parquet native file output retries PyArrow after native failure."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def failing_native_write(
        stream: object,
        _output_path: str,
        _compression: str,
        _gzip_level: int,
        _memory_limit_bytes: int,
        _threading_mode: int,
    ) -> None:
        """Consume part of the stream before simulating a native writer bug."""
        assert hasattr(stream, "read_next_batch")
        stream.read_next_batch()
        raise RuntimeError("native Parquet writer: simulated fatal bug")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", failing_native_write)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.file_conversion.writers")
    batches = [
        pa.record_batch({"a": pa.array(["1", "2"])}),
        pa.record_batch({"a": pa.array(["3"])}),
    ]
    stream = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    out = tmp_path / "native-failure-fallback.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_retries_pyarrow_after_native_failure",
        parquet_compression="uncompressed",
    )

    assert pq.read_table(out).column("a").to_pylist() == ["1", "2", "3"]
    assert "retrying Parquet output with PyArrow" in caplog.text


def test_raw_parquet_file_output_retries_pyarrow_after_native_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify raw Parquet file output retries PyArrow after native failure."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output
    from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file

    def failing_native_write(
        stream: object,
        _output_path: str,
        _compression: str,
        _gzip_level: int,
        _memory_limit_bytes: int,
        _threading_mode: int,
    ) -> None:
        """Fail after reading one batch from each native attempt."""
        assert hasattr(stream, "read_next_batch")
        stream.read_next_batch()
        raise RuntimeError("native Parquet writer: simulated raw fatal bug")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", failing_native_write)
    batches = [
        pa.record_batch({"a": pa.array(["1"])}),
        pa.record_batch({"a": pa.array(["2", "3"])}),
    ]
    raw = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    out = tmp_path / "raw-native-failure-fallback.parquet"

    write_raw_stream_to_file(
        raw,
        out,
        writer=native_file_output.write_parquet_native_first_stream,
        feature="test_raw_parquet_file_output_retries_pyarrow_after_native_failure",
        first_row_columns=None,
        parquet_compression="uncompressed",
    )

    assert pq.read_table(out).column("a").to_pylist() == ["1", "2", "3"]


def test_parquet_native_file_output_writes_metadata_without_pyarrow_sink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes metadata without PyArrow sink."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    captured: dict[str, object] = {}

    def fake_native_write(
        stream: object,
        output_path: str,
        first_row_columns: dict[str, object],
        all_row_columns: dict[str, object],
        row_span_columns: dict[str, list[tuple[int, str | None]]],
        timestamp_columns: tuple[str, ...],
        compression: str,
        gzip_level: int,
        memory_limit_bytes: int,
        threading_mode: int,
    ) -> None:
        """Write a marker file through the fake native metadata Parquet writer."""
        assert hasattr(stream, "__arrow_c_stream__")
        captured["first_row_columns"] = first_row_columns
        captured["all_row_columns"] = all_row_columns
        captured["row_span_columns"] = row_span_columns
        captured["timestamp_columns"] = timestamp_columns
        assert (compression, gzip_level, memory_limit_bytes, threading_mode) == (
            "gzip",
            -1,
            -1,
            0,
        )
        Path(output_path).write_bytes(b"native-parquet-metadata")

    monkeypatch.setattr(
        native_parquet_output, "PARQUET_STREAM_WRITE_WITH_METADATA", fake_native_write
    )
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1", "2"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "direct-metadata.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_metadata_without_pyarrow_sink",
        first_row_columns={"schema_registry": "{}"},
        all_row_columns={"source_file": "/tmp/source.parquet"},
        timestamp_columns=("ingestion_timestamp",),
    )

    assert out.read_bytes() == b"native-parquet-metadata"
    assert captured == {
        "first_row_columns": {"schema_registry": "{}"},
        "all_row_columns": {"source_file": "/tmp/source.parquet"},
        "row_span_columns": {},
        "timestamp_columns": ("ingestion_timestamp",),
    }


def test_parquet_native_file_output_writes_supported_flat_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Verify Parquet native file output writes supported flat stream."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "id": pa.array([1, None, 3], type=pa.int64()),
            "name": pa.array(["one", None, "three"], type=pa.string()),
            "payload": pa.array([b"bb", None, b"a"], type=pa.binary()),
            "score": pa.array([1.5, None, 3.25], type=pa.float64()),
            "ok": pa.array([True, None, False], type=pa.bool_()),
            "amount": pa.array(
                [Decimal("123.45"), None, Decimal("-0.10")],
                type=pa.decimal128(10, 2),
            ),
            "big_amount": pa.array(
                [
                    Decimal("123456789012345678901234567890.1234"),
                    None,
                    Decimal("-1.0000"),
                ],
                type=pa.decimal256(40, 4),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "native-flat.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_supported_flat_stream",
    )

    assert pq.read_table(out).to_pylist() == [
        {
            "id": 1,
            "name": "one",
            "payload": b"bb",
            "score": 1.5,
            "ok": True,
            "amount": Decimal("123.45"),
            "big_amount": Decimal("123456789012345678901234567890.1234"),
        },
        {
            "id": None,
            "name": None,
            "payload": None,
            "score": None,
            "ok": None,
            "amount": None,
            "big_amount": None,
        },
        {
            "id": 3,
            "name": "three",
            "payload": b"a",
            "score": 3.25,
            "ok": False,
            "amount": Decimal("-0.10"),
            "big_amount": Decimal("-1.0000"),
        },
    ]
    parquet_file = pq.ParquetFile(out)
    id_stats = parquet_file.metadata.row_group(0).column(0).statistics
    assert id_stats.null_count == 1
    assert id_stats.min == 1
    assert id_stats.max == 3
    name_stats = parquet_file.metadata.row_group(0).column(1).statistics
    assert name_stats.null_count == 1
    assert name_stats.min == "one"
    assert name_stats.max == "three"
    payload_stats = parquet_file.metadata.row_group(0).column(2).statistics
    assert payload_stats.null_count == 1
    assert payload_stats.min == b"a"
    assert payload_stats.max == b"bb"
    score_stats = parquet_file.metadata.row_group(0).column(3).statistics
    assert score_stats.null_count == 1
    assert score_stats.min == 1.5
    assert score_stats.max == 3.25
    score_meta = parquet_file.metadata.row_group(0).column(3)
    assert any(encoding in score_meta.encodings for encoding in ("BYTE_STREAM_SPLIT", "PLAIN"))
    ok_stats = parquet_file.metadata.row_group(0).column(4).statistics
    assert ok_stats.null_count == 1
    assert ok_stats.min is False
    assert ok_stats.max is True
    amount_stats = parquet_file.metadata.row_group(0).column(5).statistics
    assert amount_stats.null_count == 1
    assert amount_stats.min == Decimal("-0.10")
    assert amount_stats.max == Decimal("123.45")
    big_amount_stats = parquet_file.metadata.row_group(0).column(6).statistics
    assert big_amount_stats.null_count == 1
    assert big_amount_stats.min == Decimal("-1.0000")
    assert big_amount_stats.max == Decimal("123456789012345678901234567890.1234")
