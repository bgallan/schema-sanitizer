"""Parquet API/runtime tests split by contract area."""

from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import require_native

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")

# Split from test_parquet_native_scalar_runtime.py: test_native_parquet_stream_materializes_fixed_size_binary, test_native_parquet_stream_preserves_required_scalar_nullability, test_native_parquet_writer_rejects_null_in_required_field, ...


@_requires_pyarrow
def test_native_parquet_stream_materializes_fixed_size_binary(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes fixed-size binary columns."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "fixed-size-binary.parquet"
    plain_values = [
        None if index % 10 == 0 else index.to_bytes(4, "little") for index in range(600)
    ]
    table = pa.table(
        {
            "plain_token": pa.array(
                plain_values,
                type=pa.binary(4),
            ),
            "dict_token": pa.array(
                [b"same", b"same", b"same", None, b"same"] * 120,
                type=pa.binary(4),
            ),
        }
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    columns = {
        tuple(column["path_in_schema"]): column for column in info["row_groups"][0]["columns"]
    }
    assert columns[("plain_token",)]["physical_type"] == 7
    assert columns[("plain_token",)]["fixed_type_length"] == 4
    assert columns[("plain_token",)]["native_arrow_format"] == "w:4"
    assert columns[("plain_token",)]["native_read_value_buffer_kind"] == "fixed_width"
    assert columns[("plain_token",)]["native_read_value_width_bytes"] == 4
    assert columns[("plain_token",)]["native_read_arrow_n_buffers"] == 2
    assert columns[("dict_token",)]["physical_type"] == 7
    assert columns[("dict_token",)]["fixed_type_length"] == 4
    assert columns[("dict_token",)]["native_arrow_format"] == "w:4"
    assert columns[("dict_token",)]["native_read_value_buffer_kind"] == "dictionary_fixed_width"
    assert columns[("dict_token",)]["native_read_value_width_bytes"] == 4
    assert columns[("dict_token",)]["native_read_arrow_n_buffers"] == 2

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_preserves_required_scalar_nullability(
    tmp_path: Path,
) -> None:
    """Verify native Parquet reader preserves required scalar field nullability."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "required-scalars.parquet"
    schema = pa.schema(
        [
            pa.field("ok", pa.bool_(), nullable=False),
            pa.field("i8", pa.int8(), nullable=False),
            pa.field("u16", pa.uint16(), nullable=False),
            pa.field("n", pa.int64(), nullable=False),
            pa.field("f", pa.float32(), nullable=False),
            pa.field("g", pa.float64(), nullable=False),
            pa.field("s", pa.string(), nullable=False),
            pa.field("payload", pa.binary(), nullable=False),
            pa.field("fixed_payload", pa.binary(4), nullable=False),
            pa.field("amount", pa.decimal128(10, 2), nullable=False),
            pa.field("day", pa.date32(), nullable=False),
            pa.field("ts", pa.timestamp("us"), nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array([True, False, True], type=pa.bool_()),
            pa.array([-1, 2, 3], type=pa.int8()),
            pa.array([1, 65000, 3], type=pa.uint16()),
            pa.array([10, 20, 30], type=pa.int64()),
            pa.array([1.25, 2.5, 3.75], type=pa.float32()),
            pa.array([1.5, 2.25, 3.125], type=pa.float64()),
            pa.array(["alpha", "bravo", "charlie"], type=pa.string()),
            pa.array([b"a", b"bb", b"ccc"], type=pa.binary()),
            pa.array([b"abcd", b"wxyz", b"1234"], type=pa.binary(4)),
            pa.array(
                [Decimal("1.23"), Decimal("4.56"), Decimal("7.89")],
                type=pa.decimal128(10, 2),
            ),
            pa.array(
                [dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(2026, 1, 3)],
                type=pa.date32(),
            ),
            pa.array(
                [
                    dt.datetime(2026, 1, 1, 1, 2, 3),
                    dt.datetime(2026, 1, 1, 1, 2, 4),
                    dt.datetime(2026, 1, 1, 1, 2, 5),
                ],
                type=pa.timestamp("us"),
            ),
        ],
        schema=schema,
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    row_group = info["row_groups"][0]
    assert row_group["num_rows"] == 3
    assert all(column["max_definition_level"] == 0 for column in row_group["columns"])
    assert all(column["native_read_total_nulls"] == 0 for column in row_group["columns"])
    assert all(column["native_read_arrow_null_count"] == 0 for column in row_group["columns"])
    assert all(column["native_read_has_validity_buffer"] == 0 for column in row_group["columns"])

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_writer_rejects_null_in_required_field(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify native Parquet writer does not materialize nulls in required fields."""
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "required-null.parquet"
    schema = pa.schema([pa.field("n", pa.int64(), nullable=False)])
    table = pa.Table.from_arrays([pa.array([1, None], type=pa.int64())], schema=schema)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.file_conversion.writers")

    with pytest.raises(pa.ArrowInvalid, match="non-nullable"):
        write_parquet_native_first_stream(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
            path,
            feature="test",
            parquet_compression="uncompressed",
        )
    assert "required field contains null" in caplog.text


@_requires_pyarrow
def test_native_parquet_stream_reads_empty_file_schema(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream handles empty files with footer schema only."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "empty.parquet"
    table = pa.table(
        {
            "a": pa.array([], type=pa.int64()),
            "b": pa.array([], type=pa.string()),
        }
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["num_rows"] == 0
    assert info["row_group_count"] == 0
    assert info["native_reader_ready"] == 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.num_rows == 0
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_reads_empty_struct_file_schema(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream handles empty supported struct schemas."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "empty-struct.parquet"
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("score", pa.float64(), nullable=False),
                    ]
                ),
            )
        ]
    )
    table = pa.Table.from_pylist([], schema=schema)
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["num_rows"] == 0
    assert info["row_group_count"] == 0
    assert info["native_reader_ready"] == 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.num_rows == 0
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_pyarrow_empty_row_group_parquet_falls_back_cleanly(
    tmp_path: Path,
) -> None:
    """Verify empty row groups remain readable through the PyArrow fallback."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "empty-row-group.parquet"
    table = pa.table(
        {
            "a": pa.array([], type=pa.int64()),
            "b": pa.array([], type=pa.string()),
        }
    )
    pq.write_table(table, path, row_group_size=1)
    metadata = pq.ParquetFile(path).metadata
    assert metadata.num_rows == 0
    assert metadata.num_row_groups == 1
    assert metadata.row_group(0).num_rows == 0
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["num_rows"] == 0
    assert info["row_group_count"] == 1
    assert info["row_groups"][0]["num_rows"] == 0
    assert all(not column["pages"] for column in info["row_groups"][0]["columns"])
    assert info["native_reader_ready"] == 0
    assert any("file was not written" in blocker for blocker in info["native_reader_blockers"])

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.num_rows == 0
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("file was not written" in blocker for blocker in diagnostics["blockers"])


@_requires_pyarrow
def test_native_parquet_stream_reads_empty_supported_list_file_schema(
    tmp_path: Path,
) -> None:
    """Verify native reader handles empty files with supported list schemas."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "empty-list.parquet"
    table = pa.Table.from_pylist(
        [],
        schema=pa.schema(
            [
                pa.field("scores", pa.list_(pa.int64())),
                pa.field(
                    "items",
                    pa.list_(
                        pa.struct(
                            [
                                pa.field("score", pa.int64()),
                                pa.field("label", pa.string()),
                            ]
                        )
                    ),
                ),
                pa.field("nested_scores", pa.list_(pa.list_(pa.int64()))),
                pa.field("deep_scores", pa.list_(pa.list_(pa.list_(pa.int64())))),
                pa.field(
                    "very_deep_scores",
                    pa.list_(pa.list_(pa.list_(pa.list_(pa.int64())))),
                ),
                pa.field("labels", pa.map_(pa.string(), pa.int64())),
                pa.field("nested_labels", pa.list_(pa.map_(pa.string(), pa.int64()))),
            ]
        ),
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["num_rows"] == 0
    assert info["row_group_count"] == 0
    assert info["native_reader_ready"] == 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.num_rows == 0
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
