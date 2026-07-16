"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native
from sinks_shared import fail_pyarrow_sink


def test_parquet_native_file_output_splits_large_pages_without_dictionary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify oversized column payloads are split into readable non-dictionary pages."""
    require_native()
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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_splits_row_groups_by_byte_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify row groups are bounded by estimated uncompressed column bytes."""
    require_native()
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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_skips_delta_encoding_on_int64_overflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify extreme int64 deltas fall back to a non-overflowing encoding."""
    require_native()
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
    assert native_file_output.last_parquet_stream_route() == "native"
    assert pq.read_table(out).to_pylist() == [
        {"value": values[0]},
        {"value": values[1]},
        {"value": values[2]},
        {"value": None},
    ]


def test_parquet_native_file_output_preserves_sliced_batch_offsets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify sliced Arrow batches preserve nested values."""
    require_native()
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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_is_duckdb_readable_across_row_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify DuckDB can scan native Parquet output with nested row groups."""
    require_native()
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
    assert native_file_output.last_parquet_stream_route() == "native"
