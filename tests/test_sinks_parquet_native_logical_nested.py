"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import require_native


def test_parquet_native_file_output_writes_integer_width_logical_types(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet preserves small and unsigned integer schemas."""
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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_timestamp_nanos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet writes Arrow timestamp[ns] as Parquet NANOS."""
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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_schema_sanitizer_logical_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet covers every schema-sanitizer logical kind."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_nested_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer produces readable nested output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_map_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer produces readable map output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_fixed_size_list_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer handles fixed-size lists."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

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
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_generated_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer handles metadata-wrapped streams."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.pyarrow.file_metadata import last_metadata_route
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

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
    assert native_file_output.last_parquet_stream_route() == "native"
    assert last_metadata_route() == "native"
