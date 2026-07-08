"""Tests public Parquet API behavior."""

from __future__ import annotations

import datetime as dt
import gc
import json
import logging
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from conftest import read_test_parquet, require_native

import schema_sanitizer as ss

if TYPE_CHECKING:
    import pyarrow as pa_types

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")


def _sample_table() -> pa_types.Table:
    """Return sample table for the test."""
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


@_requires_pyarrow
def test_parquet_path_auto(tmp_path: Path) -> None:
    """Verify parquet path auto."""
    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    result = read_test_parquet(path)
    assert result.clean_data.num_rows == 3
    assert result.clean_data.schema.names == ["a", "b"]


@_requires_pyarrow
def test_parquet_path_with_temporal_values(tmp_path: Path) -> None:
    """Verify parquet path with temporal values."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "d": pa.array([dt.date(2024, 1, 2)], type=pa.date32()),
                "ts": pa.array([dt.datetime(2024, 1, 2, 3, 4, 5)], type=pa.timestamp("us")),
            }
        ),
        path,
    )

    result = read_test_parquet(path)

    assert result.clean_data.num_rows == 1
    assert result.clean_data.schema.names == ["d", "ts"]
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["schema_elements"][1]["logical_type"] == "date"
    assert info["schema_elements"][2]["logical_type"] == "timestamp"
    assert info["schema_elements"][2]["logical_type_time_unit"] == "micros"
    assert info["schema_elements"][2]["logical_type_is_adjusted_to_utc"] == 0
    assert info["row_groups"][0]["columns"][0]["path_in_schema"] == ["d"]
    assert info["row_groups"][0]["columns"][1]["path_in_schema"] == ["ts"]
    assert info["row_groups"][0]["columns"][0]["native_arrow_format"] == "tdD"
    assert info["row_groups"][0]["columns"][1]["native_arrow_format"] == "tsu:"


@_requires_pyarrow
def test_native_parquet_footer_info_maps_utc_timestamp_timezone(tmp_path: Path) -> None:
    """Verify adjusted UTC Parquet timestamps expose an Arrow timezone."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array(
                    [dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        ),
        path,
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["schema_elements"][1]["logical_type"] == "timestamp"
    assert info["schema_elements"][1]["logical_type_time_unit"] == "micros"
    assert info["schema_elements"][1]["logical_type_is_adjusted_to_utc"] == 1
    assert info["row_groups"][0]["columns"][0]["native_arrow_format"] == "tsu:UTC"


@_requires_pyarrow
def test_read_parquet_path_materializes_table(tmp_path: Path) -> None:
    """Verify read parquet path materializes table."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()

    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == _sample_table().to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("file was not written" in blocker for blocker in diagnostics["blockers"])


@_requires_pyarrow
def test_native_parquet_reader_logs_not_ready_fallback(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify unsupported native Parquet attempts leave useful fallback diagnostics."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    path = tmp_path / "pyarrow.parquet"
    pq.write_table(_sample_table(), path)
    caplog.set_level(logging.DEBUG, logger="schema_sanitizer.adapters.pyarrow_parquet_direct")

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == _sample_table().to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("file was not written" in blocker for blocker in diagnostics["blockers"])
    assert "Native Parquet reader skipped; retrying input with PyArrow" in caplog.text


@_requires_pyarrow
def test_parquet_file_like_records_non_native_source_diagnostics() -> None:
    """Verify file-like Parquet inputs explain why native reader was bypassed."""
    from io import BytesIO

    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    data = BytesIO()
    pq.write_table(_sample_table(), data)
    data.seek(0)

    factory = open_parquet_record_batch_stream_factory(data, source="stream", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == _sample_table().to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_parquetfile_iter_batches"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is False
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "source_not_path"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_parquet_buffer_records_non_native_source_diagnostics() -> None:
    """Verify in-memory Parquet buffers use the safe non-native fallback."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    sink = pa.BufferOutputStream()
    pq.write_table(_sample_table(), sink)
    data = sink.getvalue().to_pybytes()

    factory = open_parquet_record_batch_stream_factory(data, source="text", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == _sample_table().to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_parquetfile_iter_batches"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is False
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "source_not_path"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_parquet_buffer_projection_materializes_requested_columns() -> None:
    """Verify buffer-backed Parquet projections expose the projected schema."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    table = pa.table({"a": [1, 2], "b": ["x", "y"], "c": [True, False]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)

    factory = open_parquet_record_batch_stream_factory(
        sink.getvalue().to_pybytes(),
        source="text",
        feature="test",
        columns=["b"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == [{"b": "x"}, {"b": "y"}]
    assert last_parquet_stream_factory_route() == "pyarrow_parquetfile_iter_batches"


@_requires_pyarrow
def test_native_parquet_stream_materializes_plain_fixed_width_rows(
    tmp_path: Path,
) -> None:
    """Verify the native Parquet stream materializes supported fixed-width pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native.parquet"
    table = pa.table({"a": [10, 20, None, 40], "b": [1000, -5, 7, None]})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert {
        tuple(column["path_in_schema"]): column["native_read_value_buffer_kind"]
        for column in info["row_groups"][0]["columns"]
    } == {("a",): "fixed_width", ("b",): "fixed_width"}

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []
    assert diagnostics["row_group_count"] == 1
    assert diagnostics["num_rows"] == 4


@_requires_pyarrow
def test_native_parquet_stream_respects_small_batch_size_with_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    """Verify native Parquet falls back when row-group batches exceed batch_size."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-small-batches.parquet"
    table = pa.table({"a": pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        batch_size=2,
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    batches = list(reader)

    assert [batch.num_rows for batch in batches] == [2, 2, 1]
    assert pa.Table.from_batches(batches).to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("requested batch_size is 2" in blocker for blocker in diagnostics["blockers"])


@_requires_pyarrow
def test_native_parquet_stream_projection_uses_native_route(
    tmp_path: Path,
) -> None:
    """Verify projected scalar reads stay on the native Parquet route."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-projection.parquet"
    table = pa.table(
        {
            "a": pa.array([1, 2, 3], type=pa.int64()),
            "b": pa.array(["x", "y", "z"], type=pa.string()),
            "c": pa.array([True, False, True], type=pa.bool_()),
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

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["b"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == [{"b": "x"}, {"b": "y"}, {"b": "z"}]
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_parquet_filter_uses_dataset_scanner_instead_of_native_route(
    tmp_path: Path,
) -> None:
    """Verify dataset filters are honored through the PyArrow scanner route."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    ds = pytest.importorskip("pyarrow.dataset")
    path = tmp_path / "native-filter.parquet"
    table = pa.table(
        {
            "a": pa.array([1, 2, 3], type=pa.int64()),
            "b": pa.array(["x", "y", "z"], type=pa.string()),
        }
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["b"],
        filters=ds.field("a") > 1,
    )
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == [{"b": "y"}, {"b": "z"}]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is False
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "filter_requires_dataset_scanner"


@_requires_pyarrow
def test_parquet_filter_rejects_buffer_source() -> None:
    """Verify filters are never silently ignored for non-dataset sources."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        open_parquet_record_batch_stream_factory,
    )

    ds = pytest.importorskip("pyarrow.dataset")
    sink = pa.BufferOutputStream()
    pq.write_table(_sample_table(), sink)

    with pytest.raises(ValueError, match="filters require a path-backed source"):
        open_parquet_record_batch_stream_factory(
            sink.getvalue().to_pybytes(),
            source="text",
            feature="test",
            filters=ds.field("a") > 1,
        )


@_requires_pyarrow
def test_native_parquet_stream_materializes_plain_boolean_rows(
    tmp_path: Path,
) -> None:
    """Verify the native Parquet stream materializes PLAIN boolean pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-bool.parquet"
    table = pa.table({"ok": pa.array([True, None, False, True], type=pa.bool_())})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    column = info["row_groups"][0]["columns"][0]
    page = column["pages"][0]
    assert info["native_reader_ready"] == 1
    assert column["native_arrow_format"] == "b"
    assert column["native_read_value_buffer_kind"] == "bit_packed_boolean"
    assert column["native_read_arrow_n_buffers"] == 2
    assert page["value_encoding"] == 0
    assert page["decoded_value_preview"] == ["true", "false", "true"]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_rle_dictionary_strings(
    tmp_path: Path,
) -> None:
    """Verify the native Parquet stream materializes RLE dictionary strings."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native.parquet"
    table = pa.table({"s": ["same"] * 500})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["row_groups"][0]["columns"][0]["native_read_value_buffer_kind"] == (
        "dictionary_byte_array"
    )
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_rle_dictionary_fixed_width(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes fixed-width dictionary pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    values = [7] * 500
    table = pa.table({"n": pa.array(values, type=pa.int64())})
    pq.write_table(table, src)

    ss.to_parquet(
        src,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(out)

    assert info is not None
    column = next(
        column for column in info["row_groups"][0]["columns"] if column["path_in_schema"] == ["n"]
    )
    assert info["native_reader_ready"] == 1
    assert column["native_read_value_buffer_kind"] == "dictionary_fixed_width"
    assert column["native_read_value_width_bytes"] == 8
    assert column["native_read_arrow_n_buffers"] == 2
    dictionary_page = column["pages"][0]
    data_page = column["pages"][1]
    assert dictionary_page["is_dictionary_page"] == 1
    assert dictionary_page["decoded_value_preview"] == ["7"]
    assert data_page["value_encoding"] == 8
    assert data_page["decoded_value_preview"] == ["7"] * 8
    assert data_page["materialized_value_bytes"] == len(values) * 8
    assert data_page["materialized_offset_bytes"] == 0

    factory = open_parquet_record_batch_stream_factory(out, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    assert [
        {key: value for key, value in row.items() if key not in generated}
        for row in reader.read_all().to_pylist()
    ] == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_integer_logical_widths(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream writes Arrow-width integer buffers."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "integer-logical-widths.parquet"
    table = pa.table(
        {
            "i8": pa.array([-5, 7, None, -1], type=pa.int8()),
            "u8": pa.array([250, 1, None, 2], type=pa.uint8()),
            "i16": pa.array([-300, 12, None, -2], type=pa.int16()),
            "u16": pa.array([65000, 2, None, 3], type=pa.uint16()),
            "u32": pa.array([4_000_000_000, 7, None, 8], type=pa.uint32()),
            "u64": pa.array([2**63 + 5, 9, None, 10], type=pa.uint64()),
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
    widths = {
        tuple(column["path_in_schema"]): column["native_read_value_width_bytes"]
        for column in info["row_groups"][0]["columns"]
    }
    assert widths == {
        ("i8",): 1,
        ("u8",): 1,
        ("i16",): 2,
        ("u16",): 2,
        ("u32",): 4,
        ("u64",): 8,
    }

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_decimal_fixed_bytes(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream converts decimal fixed bytes to Arrow order."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "decimal-fixed-bytes.parquet"
    table = pa.table(
        {
            "plain_amount": pa.array(
                [Decimal("123.45"), Decimal("-0.10"), None, Decimal("999.99")],
                type=pa.decimal128(10, 2),
            ),
            "dict_amount": pa.array(
                [Decimal("1.00"), Decimal("2.00"), Decimal("1.00"), None],
                type=pa.decimal128(10, 2),
            ),
            "big_amount": pa.array(
                [
                    Decimal("123456789012345678901234567890.1234"),
                    Decimal("-1.0000"),
                    None,
                    Decimal("2.5000"),
                ],
                type=pa.decimal256(40, 4),
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
    assert columns[("plain_amount",)]["native_arrow_format"] == "d:10,2,128"
    assert columns[("plain_amount",)]["native_read_value_buffer_kind"] == "fixed_width"
    assert columns[("plain_amount",)]["native_read_value_width_bytes"] == 16
    assert columns[("dict_amount",)]["native_arrow_format"] == "d:10,2,128"
    assert columns[("dict_amount",)]["native_read_value_buffer_kind"] == "dictionary_fixed_width"
    assert columns[("dict_amount",)]["native_read_value_width_bytes"] == 16
    assert columns[("big_amount",)]["native_arrow_format"] == "d:40,4,256"
    assert columns[("big_amount",)]["native_read_value_buffer_kind"] == "fixed_width"
    assert columns[("big_amount",)]["native_read_value_width_bytes"] == 32

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_fixed_size_binary(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes fixed-size binary columns."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

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
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

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
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "required-null.parquet"
    schema = pa.schema([pa.field("n", pa.int64(), nullable=False)])
    table = pa.Table.from_arrays([pa.array([1, None], type=pa.int64())], schema=schema)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.native_file_output")

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
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

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
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

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
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
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
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

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


@_requires_pyarrow
def test_native_parquet_stream_projects_empty_file_schema(
    tmp_path: Path,
) -> None:
    """Verify native empty-file reads honor projected column order."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "empty-projected.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("profile", pa.struct([pa.field("name", pa.string())])),
            pa.field("scores", pa.list_(pa.int64())),
        ]
    )
    table = pa.Table.from_pylist([], schema=schema)
    expected = table.select(["scores", "id"])
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["scores", "id"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(expected.schema)
    assert out.num_rows == 0
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_projects_empty_file_past_unprojected_complex_repeated(
    tmp_path: Path,
) -> None:
    """Verify empty-file projection ignores unsupported unprojected fields."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "empty-projected-complex.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("items", pa.list_(pa.struct([pa.field("ids", pa.list_(pa.int64()))]))),
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
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["id"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.select(["id"]).schema)
    assert out.num_rows == 0
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_footer_info_accepts_empty_list_struct_list_chain_readiness(
    tmp_path: Path,
) -> None:
    """Verify empty list-struct nested-list files can use the native reader."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "empty-complex-repeated.parquet"
    schema = pa.schema(
        [pa.field("items", pa.list_(pa.struct([pa.field("ids", pa.list_(pa.list_(pa.int64())))])))]
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
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.num_rows == 0
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_reads_multiple_row_groups(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream returns all row groups in order."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "multi-row-group.parquet"
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    batches = [
        pa.record_batch(
            [pa.array([1, 2], type=pa.int64()), pa.array(["x", "y"])],
            schema=schema,
        ),
        pa.record_batch(
            [pa.array([3], type=pa.int64()), pa.array(["z"])],
            schema=schema,
        ),
    ]
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(schema, batches),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["row_group_count"] == 2
    assert [row_group["num_rows"] for row_group in info["row_groups"]] == [2, 1]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_reads_list_columns_across_row_groups(
    tmp_path: Path,
) -> None:
    """Verify native list arrays reset offsets correctly per row group."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "multi-row-group-list.parquet"
    schema = pa.schema([pa.field("items", pa.list_(pa.int64()))])
    batches = [
        pa.record_batch(
            [pa.array([[1, 2], None], type=pa.list_(pa.int64()))],
            schema=schema,
        ),
        pa.record_batch(
            [pa.array([[], [3, 4, 5]], type=pa.list_(pa.int64()))],
            schema=schema,
        ),
    ]
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(schema, batches),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["row_group_count"] == 2
    assert [row_group["num_rows"] for row_group in info["row_groups"]] == [2, 2]
    assert [
        row_group["columns"][0]["repeated_level_offsets"] for row_group in info["row_groups"]
    ] == [[0, 2, 2], [0, 0, 3]]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == [
        {"items": [1, 2]},
        {"items": None},
        {"items": []},
        {"items": [3, 4, 5]},
    ]
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_reads_multiple_pages_with_null_spans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native Parquet stream materializes split pages and null spans."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_PAGE_BYTES", "96")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES", "1048576")
    path = tmp_path / "multi-page-null-spans.parquet"
    rows = 80
    table = pa.table(
        {
            "a": pa.array(
                [None if row % 7 == 0 else row * 1000003 for row in range(rows)],
                type=pa.int64(),
            ),
            "b": pa.array(
                [
                    None if row % 5 == 0 else f"value-{row:03d}-with-page-split-padding"
                    for row in range(rows)
                ],
                type=pa.string(),
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
    row_group = info["row_groups"][0]
    assert row_group["num_rows"] == rows
    for column in row_group["columns"]:
        data_pages = [page for page in column["pages"] if page["is_dictionary_page"] == 0]
        assert len(data_pages) > 1
        assert column["native_read_data_page_count"] == len(data_pages)
        assert len(column["native_read_page_spans"]) == len(data_pages)
        assert sum(span["row_count"] for span in column["native_read_page_spans"]) == rows
        assert sum(span["null_count"] for span in column["native_read_page_spans"]) > 0
        assert [span["first_row_index"] for span in column["native_read_page_spans"]] == [
            location["first_row_index"] for location in column["offset_index_locations"]
        ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_reader_memory_budget_blocks_native_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native Parquet reader refuses row groups over its buffer budget."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "budget.parquet"
    table = pa.table(
        {
            "a": pa.array([1, 2, 3], type=pa.int64()),
            "b": pa.array(["wide-value-000", "wide-value-001", "wide-value-002"]),
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

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_READER_MAX_BUFFER_BYTES", "1")
    limited_info = native_parquet_footer_info(path)

    assert limited_info is not None
    assert limited_info["native_reader_ready"] == 0
    assert any(
        "native buffer estimate" in blocker and "exceeds configured limit 1" in blocker
        for blocker in limited_info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"


@_requires_pyarrow
def test_read_parquet_retries_pyarrow_after_native_reader_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify native Parquet reader failure falls back to a PyArrow stream."""
    from schema_sanitizer.api_impl import parquet_direct

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    def fail_native_reader(*_args: object, **_kwargs: object) -> object:
        """Simulate a fatal native direct Parquet reader failure."""
        raise RuntimeError("native Parquet reader: simulated fatal bug")

    monkeypatch.setattr(parquet_direct, "_call_core", fail_native_reader)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.parquet_direct")

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == _sample_table().to_pylist()
    assert parquet_direct.last_parquet_direct_route() == "pyarrow"
    assert "retrying input with PyArrow" in caplog.text


@_requires_pyarrow
def test_to_parquet_retries_pyarrow_after_native_reader_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify Parquet conversion retries with PyArrow after native reader failure."""
    from schema_sanitizer.api_impl import parquet_direct

    require_native()
    path = tmp_path / "data.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(_sample_table(), path)

    def fail_native_reader(*_args: object, **_kwargs: object) -> object:
        """Simulate a fatal native direct Parquet reader failure."""
        raise RuntimeError("native Parquet reader: simulated fatal conversion bug")

    monkeypatch.setattr(parquet_direct, "_call_core", fail_native_reader)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.parquet_direct")

    ss.to_parquet(
        path,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert [{key: value for key, value in row.items() if key not in generated} for row in rows] == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]
    assert "schema_registry" in rows[0]
    assert "schema_drifts" in rows[0]
    assert "retrying input with PyArrow" in caplog.text


@_requires_pyarrow
def test_native_parquet_footer_info_reads_pyarrow_file(tmp_path: Path) -> None:
    """Verify native Parquet footer parsing reads bounded file metadata."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["num_rows"] == 3
    assert info["row_group_count"] == 1
    assert info["schema_element_count"] >= 3
    assert isinstance(info["created_by"], str)
    assert info["native_reader_ready"] == 0
    assert (
        "file was not written by schema-sanitizer native parquet writer"
        in info["native_reader_blockers"]
    )
    assert [element["name"] for element in info["schema_elements"]] == [
        "schema",
        "a",
        "b",
    ]
    assert info["schema_elements"][1]["physical_type"] == 2
    assert info["schema_elements"][2]["physical_type"] == 6
    assert info["row_groups"][0]["num_rows"] == 3
    assert [column["path_in_schema"] for column in info["row_groups"][0]["columns"]] == [
        ["a"],
        ["b"],
    ]
    assert all(column["num_values"] == 3 for column in info["row_groups"][0]["columns"])
    assert all("data_page_offset" in column for column in info["row_groups"][0]["columns"])
    for column in info["row_groups"][0]["columns"]:
        assert column["pages"][0]["type"] == 2
        assert column["pages"][0]["is_dictionary_page"] == 1
        assert column["pages"][0]["value_encoding"] == 0
        assert column["pages"][1]["type"] == 0
        assert column["pages"][1]["is_dictionary_page"] == 0
        assert column["pages"][1]["num_values"] == 3
        assert column["pages"][1]["value_encoding"] == 8
        assert column["pages"][1]["payload_verified"] == 0


@_requires_pyarrow
def test_spark_int96_parquet_uses_pyarrow_fallback(tmp_path: Path) -> None:
    """Verify Spark-style INT96 timestamps stay readable through fallback."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
    )

    require_native()
    path = tmp_path / "spark-int96.parquet"
    value = dt.datetime(2024, 1, 1, 1, 2, 3, 123456)
    pq.write_table(
        pa.table({"ts": pa.array([value], type=pa.timestamp("ns"))}),
        path,
        flavor="spark",
        use_deprecated_int96_timestamps=True,
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert info["schema_elements"][1]["physical_type"] == 3
    assert any("unsupported physical type" in blocker for blocker in info["native_reader_blockers"])

    result = read_test_parquet(path)

    assert result.clean_data.schema.field("ts").type == pa.timestamp("us")
    assert result.clean_data.to_pylist() == [{"ts": value}]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("unsupported physical type" in blocker for blocker in diagnostics["blockers"])


@_requires_pyarrow
def test_spark_flavored_nested_parquet_uses_pyarrow_fallback(tmp_path: Path) -> None:
    """Verify Spark-flavored nested Parquet remains readable through fallback."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
    )

    require_native()
    path = tmp_path / "spark-flavored-nested.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "profile": pa.array(
                [{"name": "a", "score": 1.5}, {"name": "b", "score": None}],
                type=pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("score", pa.float64()),
                    ]
                ),
            ),
            "tags": pa.array([["alpha", "beta"], ["gamma"]], type=pa.list_(pa.string())),
        }
    )
    pq.write_table(table, path, flavor="spark", compression="snappy")

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert any(
        "nested or repeated column is not yet native materializable" in blocker
        or "unsupported compression" in blocker
        for blocker in info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_bigquery_compatible_standard_parquet_uses_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    """Verify BigQuery-style logical scalars without Arrow metadata stay readable."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
    )

    require_native()
    path = tmp_path / "bigquery-compatible.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"], type=pa.string()),
            "active": pa.array([True, None], type=pa.bool_()),
            "amount": pa.array(
                [Decimal("12.34"), Decimal("56.78")],
                type=pa.decimal128(10, 2),
            ),
            "event_date": pa.array(
                [dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    dt.datetime(2024, 1, 1, 1, 2, 3, 123456),
                    dt.datetime(2024, 1, 2, 1, 2, 3, 123456),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )
    pq.write_table(
        table,
        path,
        store_schema=False,
        compression="snappy",
        coerce_timestamps="us",
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert info["schema_elements"][4]["logical_type"] == "decimal"
    assert info["schema_elements"][5]["logical_type"] == "date"
    assert info["schema_elements"][6]["logical_type"] == "timestamp"
    assert any("unsupported compression" in blocker for blocker in info["native_reader_blockers"])

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [
        {
            "active": True,
            "amount": "12.34",
            "eventdate": dt.date(2024, 1, 1),
            "eventts": dt.datetime(2024, 1, 1, 1, 2, 3, 123456),
            "id": 1,
            "name": "a",
        },
        {
            "active": None,
            "amount": "56.78",
            "eventdate": dt.date(2024, 1, 2),
            "eventts": dt.datetime(2024, 1, 2, 1, 2, 3, 123456),
            "id": 2,
            "name": "b",
        },
    ]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_bigquery_export_like_nested_parquet_uses_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    """Verify BigQuery-export-like nested/repeated Parquet stays readable."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
    )

    require_native()
    path = tmp_path / "bigquery-export-like.parquet"
    table = pa.table(
        {
            "user_id": pa.array(["u1", "u2"], type=pa.string()),
            "event_date": pa.array(
                [dt.date(2024, 2, 1), dt.date(2024, 2, 2)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    dt.datetime(2024, 2, 1, 12, 0, 0, 123456),
                    dt.datetime(2024, 2, 2, 12, 0, 0, 123456),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "metrics": pa.array(
                [
                    {"score": Decimal("12.34"), "rank": 1},
                    {"score": Decimal("56.78"), "rank": 2},
                ],
                type=pa.struct(
                    [
                        pa.field("score", pa.decimal128(10, 2)),
                        pa.field("rank", pa.int64()),
                    ]
                ),
            ),
            "items": pa.array(
                [
                    [{"sku": "a", "quantity": 2}, {"sku": "b", "quantity": 1}],
                    [{"sku": "c", "quantity": 3}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("sku", pa.string()),
                            pa.field("quantity", pa.int64()),
                        ]
                    )
                ),
            ),
        }
    )
    pq.write_table(
        table,
        path,
        store_schema=False,
        compression="snappy",
        coerce_timestamps="us",
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert any(
        "file was not written by schema-sanitizer native parquet writer" in blocker
        or "nested or repeated column is not yet native materializable" in blocker
        or "unsupported compression" in blocker
        for blocker in info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [
        {
            "eventdate": dt.date(2024, 2, 1),
            "eventts": dt.datetime(2024, 2, 1, 12, 0, 0, 123456),
            "items": [{"quantity": 2, "sku": "a"}, {"quantity": 1, "sku": "b"}],
            "metrics": {"rank": 1, "score": "12.34"},
            "userid": "u1",
        },
        {
            "eventdate": dt.date(2024, 2, 2),
            "eventts": dt.datetime(2024, 2, 2, 12, 0, 0, 123456),
            "items": [{"quantity": 3, "sku": "c"}],
            "metrics": {"rank": 2, "score": "56.78"},
            "userid": "u2",
        },
    ]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_duckdb_written_parquet_uses_pyarrow_fallback(tmp_path: Path) -> None:
    """Verify DuckDB-written Parquet stays readable through the safe fallback."""
    duckdb = pytest.importorskip("duckdb")
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
    )

    require_native()
    path = tmp_path / "duckdb.parquet"
    with duckdb.connect() as connection:
        connection.execute(
            "COPY (SELECT 1::BIGINT AS a, 'x' AS b UNION ALL SELECT 2, 'y') "
            "TO ? (FORMAT PARQUET)",
            [str(path)],
        )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert "DuckDB" in info["created_by"]
    assert info["native_reader_ready"] == 0
    assert (
        "file was not written by schema-sanitizer native parquet writer"
        in info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert (
        "file was not written by schema-sanitizer native parquet writer" in diagnostics["blockers"]
    )


@_requires_pyarrow
def test_native_parquet_footer_info_reads_schema_sanitizer_file(tmp_path: Path) -> None:
    """Verify native footer parsing understands schema-sanitizer Parquet output."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    table = pa.table(
        {
            "a": pa.array(
                [
                    123456789012345678,
                    -333333333333333333,
                    987654321012345678,
                ],
                type=pa.int64(),
            ),
            "b": pa.array(
                [
                    "alpha-plain-value-with-enough-entropy-001",
                    "bravo-plain-value-with-enough-entropy-002-extra",
                    "charlie-plain-value-with-enough-entropy-003-extra-more",
                ],
                type=pa.string(),
            ),
        }
    )
    pq.write_table(table, src)

    ss.to_parquet(
        src,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(out)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    assert info["num_rows"] == 3
    assert info["row_group_count"] >= 1
    assert info["created_by"] == "schema-sanitizer native parquet writer"
    assert info["schema_elements"][0] == {
        "name": "schema",
        "num_children": 6,
    }
    assert info["schema_elements"][1]["name"] == "a"
    assert info["schema_elements"][1]["physical_type"] == 2
    assert info["schema_elements"][2]["name"] == "b"
    assert info["schema_elements"][2]["physical_type"] == 6
    assert info["schema_elements"][2]["converted_type"] == 0
    row_group = info["row_groups"][0]
    assert row_group["num_rows"] == 3
    assert row_group["total_byte_size"] > 0
    assert [column["path_in_schema"] for column in row_group["columns"][:2]] == [
        ["a"],
        ["b"],
    ]
    formats_by_path = {
        tuple(column["path_in_schema"]): column["native_arrow_format"]
        for column in row_group["columns"]
    }
    assert formats_by_path[("a",)] == "l"
    assert formats_by_path[("b",)] == "u"
    assert formats_by_path[("ingestion_timestamp",)] == "tsu:"
    assert all(column["codec"] == 0 for column in row_group["columns"])
    assert all(column["total_compressed_size"] > 0 for column in row_group["columns"])
    assert all(column["data_page_offset"] >= 4 for column in row_group["columns"])
    for column in row_group["columns"]:
        data_page_index = next(
            index for index, page in enumerate(column["pages"]) if page["is_dictionary_page"] == 0
        )
        data_page = column["pages"][data_page_index]
        expected_value_kind = column["native_read_value_buffer_kind"]
        expected_value_width = (
            8
            if expected_value_kind
            in {"fixed_width", "delta_binary_packed", "dictionary_fixed_width"}
            else 0
        )
        expected_arrow_buffers = (
            3 if expected_value_kind in {"plain_byte_array", "dictionary_byte_array"} else 2
        )
        expected_offsets_buffer = (
            1 if expected_value_kind in {"plain_byte_array", "dictionary_byte_array"} else 0
        )
        assert column["max_definition_level"] == 1
        assert column["max_repetition_level"] == 0
        assert column["column_index_decoded"] == 1
        assert column["offset_index_decoded"] == 1
        assert len(column["column_index_null_pages"]) == 1
        assert len(column["column_index_null_counts"]) == 1
        assert len(column["column_index_min_hex"]) == 1
        assert len(column["column_index_max_hex"]) == 1
        assert column["offset_index_locations"] == [
            {
                "offset": data_page["header_offset"],
                "compressed_page_size": (
                    data_page["header_size"] + data_page["compressed_page_size"]
                ),
                "first_row_index": 0,
            }
        ]
        assert column["native_read_plan_decoded"] == 1
        assert column["native_read_data_page_count"] == 1
        assert column["native_read_total_rows"] == data_page["num_values"]
        assert column["native_read_total_non_nulls"] == data_page["decoded_non_null_values"]
        assert column["native_read_total_nulls"] == data_page["decoded_null_values"]
        assert column["native_read_validity_bitmap_bytes"] == data_page["decoded_validity_bytes"]
        assert column["native_read_value_payload_bytes"] == data_page["decoded_value_bytes"]
        assert (
            column["native_read_materialized_value_bytes"] == data_page["materialized_value_bytes"]
        )
        assert (
            column["native_read_materialized_offset_bytes"]
            == data_page["materialized_offset_bytes"]
        )
        assert column["native_read_value_width_bytes"] == expected_value_width
        if expected_value_kind in {
            "rle_dictionary_indices",
            "dictionary_byte_array",
            "dictionary_fixed_width",
        }:
            assert column["native_read_dictionary_index_bit_width"] > 0
        else:
            assert column["native_read_dictionary_index_bit_width"] == 0
        assert column["native_read_value_buffer_kind"] == expected_value_kind
        assert column["native_read_arrow_length"] == column["native_read_total_rows"]
        assert column["native_read_arrow_null_count"] == column["native_read_total_nulls"]
        assert column["native_read_arrow_n_buffers"] == expected_arrow_buffers
        assert column["native_read_arrow_n_children"] == 0
        assert column["native_read_has_validity_buffer"] == (
            1 if column["native_read_total_nulls"] > 0 else 0
        )
        assert column["native_read_has_offsets_buffer"] == expected_offsets_buffer
        assert column["native_read_has_values_buffer"] == 1
        assert column["native_read_page_spans"] == [
            {
                "page_index": data_page_index,
                "first_row_index": 0,
                "row_count": data_page["num_values"],
                "non_null_count": data_page["decoded_non_null_values"],
                "null_count": data_page["decoded_null_values"],
                "value_encoding": data_page["value_encoding"],
                "payload_offset": data_page["compressed_payload_offset"],
                "payload_size": data_page["compressed_page_size"],
                "validity_bitmap_bytes": data_page["decoded_validity_bytes"],
                "value_payload_offset": data_page["value_payload_offset"],
                "value_payload_bytes": data_page["decoded_value_bytes"],
                "value_width_bytes": expected_value_width,
                "materialized_value_bytes": data_page["materialized_value_bytes"],
                "materialized_offset_bytes": data_page["materialized_offset_bytes"],
                "dictionary_index_bit_width": data_page["dictionary_index_bit_width"],
                "value_buffer_kind": expected_value_kind,
            }
        ]
        assert data_page["type"] == 0
        assert data_page["is_dictionary_page"] == 0
        assert data_page["num_values"] == 3
        assert data_page["compressed_page_size"] > 0
        assert data_page["decompressed_page_size"] == data_page["uncompressed_page_size"]
        assert data_page["payload_verified"] == 1
        assert data_page["levels_decoded"] == 1
        assert data_page["decoded_definition_levels"] == 3
        assert data_page["decoded_repetition_levels"] == 0
        assert data_page["value_payload_offset"] > 0
        assert data_page["validity_bitmap_decoded"] == 1
        assert data_page["decoded_validity_bytes"] == 1
        assert data_page["values_decoded"] == 1
        assert data_page["values_decode_skipped"] == 0
        assert data_page["decoded_value_bytes"] > 0
        assert data_page["materialized_value_bytes"] > 0
        if expected_value_kind in {"fixed_width", "delta_binary_packed"}:
            assert data_page["materialized_offset_bytes"] == 0
        elif expected_value_kind in {"plain_byte_array", "dictionary_byte_array"}:
            assert data_page["materialized_offset_bytes"] == (data_page["num_values"] + 1) * 4
        else:
            assert data_page["materialized_offset_bytes"] == 0
        assert data_page["definition_level_encoding"] == 3
    for column in row_group["columns"][:2]:
        assert column["pages"][0]["decoded_non_null_values"] == 3
        assert column["pages"][0]["decoded_null_values"] == 0
        assert column["pages"][0]["decoded_validity_hex_preview"] == "07"
    assert row_group["columns"][0]["pages"][0]["decoded_value_preview"] == [
        "123456789012345678",
        "-333333333333333333",
        "987654321012345678",
    ]
    assert row_group["columns"][1]["pages"][0]["decoded_value_preview"] == table["b"].to_pylist()


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_integer_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes simple top-level integer lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(
        pa.table({"scores": pa.array([[1, 2], [3]], type=pa.list_(pa.int64()))}),
        src,
    )

    ss.to_parquet(
        src,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(out)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(out, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().select(["scores"]).to_pylist() == [
        {"scores": [1, 2]},
        {"scores": [3]},
    ]
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_lists_across_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native list reconstruction stitches offsets across data pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_PAGE_BYTES", "96")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES", "1048576")
    path = tmp_path / "native-list-multi-page.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [[row, row + 1] for row in range(120)],
                type=pa.list_(pa.int64()),
            )
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
    column = info["row_groups"][0]["columns"][0]
    data_pages = [page for page in column["pages"] if page["is_dictionary_page"] == 0]
    assert len(data_pages) > 1
    assert column["repeated_level_offsets"][:4] == [0, 2, 4, 6]
    assert column["repeated_level_offsets"][-4:] == [234, 236, 238, 240]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_footer_info_captures_repeated_level_values(
    tmp_path: Path,
) -> None:
    """Verify repeated columns expose level streams needed for list offsets."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list.parquet"
    table = pa.table({"scores": pa.array([[1, 2], None, [], [3]], type=pa.list_(pa.int64()))})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["path_in_schema"] == ["scores", "list", "element"]
    assert column["max_definition_level"] == 3
    assert column["max_repetition_level"] == 1
    assert column["repeated_level_layout_decoded"] == 1
    assert column["repeated_level_row_count"] == 4
    assert column["repeated_level_null_count"] == 1
    assert column["repeated_level_element_count"] == 3
    assert column["repeated_level_non_null_value_count"] == 3
    assert column["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"] == "0d"
    page = column["pages"][0]
    assert page["decoded_definition_level_values"] == [3, 3, 0, 1, 3]
    assert page["decoded_repetition_level_values"] == [0, 1, 0, 0, 0]
    assert page["decoded_value_preview"] == ["1", "2", "3"]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_string_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes simple top-level string lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-string-list.parquet"
    table = pa.table(
        {"tags": pa.array([["a", "bb"], None, [], ["ccc"]], type=pa.list_(pa.string()))}
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
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "delta_length_byte_array"
    assert column["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"] == "0d"

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
@pytest.mark.parametrize(
    ("name", "array"),
    [
        ("string", pa.array([["only"]], type=pa.list_(pa.string()))),
        ("binary", pa.array([[b"only"]], type=pa.list_(pa.binary()))),
    ],
)
def test_native_parquet_stream_materializes_plain_byte_array_lists(
    tmp_path: Path,
    name: str,
    array: pa.Array,
) -> None:
    """Verify native reader materializes PLAIN byte-array list elements."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / f"native-plain-byte-array-{name}-list.parquet"
    table = pa.table({"tags": array})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "plain_byte_array"
    assert column["native_read_arrow_n_buffers"] == 3
    assert column["repeated_level_offsets"] == [0, 1]
    assert column["pages"][0]["value_encoding"] == 0

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_float_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes simple top-level fixed-width lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-float-list.parquet"
    table = pa.table(
        {
            "values": pa.array(
                [[1.25, 2.5], None, [], [3.75]],
                type=pa.list_(pa.float64()),
            )
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
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "fixed_width"
    assert column["native_read_value_width_bytes"] == 8
    assert column["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"] == "0d"

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_boolean_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes simple top-level boolean lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-boolean-list.parquet"
    table = pa.table(
        {
            "flags": pa.array(
                [[True, False], None, [], [True]],
                type=pa.list_(pa.bool_()),
            )
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
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "bit_packed_boolean"
    assert column["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"] == "0d"

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
@pytest.mark.parametrize(
    ("name", "array", "native_format", "expected_offsets"),
    [
        (
            "date32",
            pa.array(
                [[dt.date(2024, 1, 1)], None, [], [dt.date(2024, 1, 2)]],
                type=pa.list_(pa.date32()),
            ),
            "tdD",
            [0, 1, 1, 1, 2],
        ),
        (
            "timestamp_us",
            pa.array(
                [
                    [dt.datetime(2024, 1, 1, 1, 2, 3, 123456)],
                    None,
                    [],
                    [dt.datetime(2024, 1, 2, 1, 2, 3, 123456)],
                ],
                type=pa.list_(pa.timestamp("us")),
            ),
            "tsu:",
            [0, 1, 1, 1, 2],
        ),
        (
            "decimal128",
            pa.array(
                [[Decimal("12.34")], None, [], [Decimal("-0.10")]],
                type=pa.list_(pa.decimal128(10, 2)),
            ),
            "d:10,2,128",
            [0, 1, 1, 1, 2],
        ),
        (
            "fixed_size_binary",
            pa.array([[b"abcd"], None, [], [b"wxyz"]], type=pa.list_(pa.binary(4))),
            "w:4",
            [0, 1, 1, 1, 2],
        ),
        (
            "uint64",
            pa.array(
                [[1, 2**63], None, [], [2**64 - 1]],
                type=pa.list_(pa.uint64()),
            ),
            "L",
            [0, 2, 2, 2, 3],
        ),
    ],
)
def test_native_parquet_stream_materializes_logical_fixed_width_lists(
    tmp_path: Path,
    name: str,
    array: pa.Array,
    native_format: str,
    expected_offsets: list[int],
) -> None:
    """Verify native lists preserve fixed-width logical scalar element types."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / f"native-logical-{name}-list.parquet"
    table = pa.table({"items": array})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_arrow_format"] == native_format
    assert column["native_read_value_buffer_kind"] == "fixed_width"
    assert column["repeated_level_offsets"] == expected_offsets
    assert column["repeated_level_validity_hex_preview"] == "0d"

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
@pytest.mark.parametrize(
    ("name", "array"),
    [
        (
            "int",
            pa.array(
                [[1, None, 2], None, [], [None, 3]],
                type=pa.list_(pa.int64()),
            ),
        ),
        (
            "string",
            pa.array(
                [["a", None, "b"], None, [], [None, "c"]],
                type=pa.list_(pa.string()),
            ),
        ),
        (
            "bool",
            pa.array(
                [[True, None, False], None, [], [None, True]],
                type=pa.list_(pa.bool_()),
            ),
        ),
    ],
)
def test_native_parquet_stream_materializes_nullable_list_elements(
    tmp_path: Path,
    name: str,
    array: pa.Array,
) -> None:
    """Verify native list reconstruction preserves null child elements."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / f"native-nullable-{name}-list.parquet"
    table = pa.table({"items": array})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    column = info["row_groups"][0]["columns"][0]
    assert column["max_definition_level"] == 3
    assert column["native_read_total_nulls"] == 2
    assert column["repeated_level_offsets"] == [0, 3, 3, 3, 5]
    assert column["pages"][0]["decoded_definition_level_values"] == [
        3,
        2,
        3,
        0,
        1,
        2,
        3,
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_footer_info_plans_byte_stream_split_float_lists(
    tmp_path: Path,
) -> None:
    """Verify simple BYTE_STREAM_SPLIT float lists are planned natively."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    path = tmp_path / "pyarrow-byte-stream-split-list.parquet"
    table = pa.table(
        {
            "values": pa.array(
                [[1.25, 2.5], None, [], [3.75]],
                type=pa.list_(pa.float64()),
            )
        }
    )
    pq.write_table(
        table,
        path,
        compression="NONE",
        use_dictionary=False,
        use_byte_stream_split=True,
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 0
    assert info["native_reader_blockers"] == [
        "file was not written by schema-sanitizer native parquet writer"
    ]
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_plan_decoded"] == 1
    assert column["native_read_value_buffer_kind"] == "byte_stream_split"
    assert column["native_read_value_width_bytes"] == 8
    assert column["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"] == "0d"
    assert column["pages"][0]["value_encoding"] == 9


@_requires_pyarrow
def test_native_parquet_stream_materializes_dictionary_string_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes RLE dictionary string list elements."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-dict-string-list.parquet"
    table = pa.table(
        {"tags": pa.array([["same", "same"], None, [], ["same"]] * 200, type=pa.list_(pa.string()))}
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
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "dictionary_byte_array"
    assert column["repeated_level_offsets"][:5] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"].startswith("dd")

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_dictionary_integer_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes RLE dictionary fixed-width list elements."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-dict-integer-list.parquet"
    table = pa.table({"nums": pa.array([[7, 7], None, [], [7]] * 200, type=pa.list_(pa.int64()))})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "dictionary_fixed_width"
    assert column["native_read_value_width_bytes"] == 8
    assert column["repeated_level_offsets"][:5] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"].startswith("dd")

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_top_level_map_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level maps with scalar key/value leaves."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", 1), ("b", 2)], None, [], [("c", None)]],
                type=pa.map_(pa.string(), pa.int64()),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value"],
    ]
    assert columns[0]["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert columns[1]["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert columns[0]["native_read_total_nulls"] == 0
    assert columns[1]["native_read_total_nulls"] == 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_struct_scalar_leaves(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level list structs with scalar leaves."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [None, {"score": 1, "label": "a"}, {"score": None, "label": "b"}],
                    None,
                    [],
                    [{"score": 3, "label": None}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
                            pa.field("label", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "score"],
        ["items", "list", "element", "label"],
    ]
    assert columns[0]["repeated_level_offsets"] == [0, 3, 3, 3, 4]
    assert columns[1]["repeated_level_offsets"] == [0, 3, 3, 3, 4]
    assert columns[0]["native_read_total_nulls"] == 2
    assert columns[1]["native_read_total_nulls"] == 2

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_list_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level nested scalar lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-list.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[1, 2], [], None],
                    None,
                    [],
                    [[None, 3]],
                ],
                type=pa.list_(pa.list_(pa.int64())),
            )
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
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["path_in_schema"] == ["items", "list", "element", "list", "element"]
    assert column["repeated_level_offsets"] == [0, 3, 3, 3, 4]
    assert column["repeated_level_validity_hex_preview"] == "0d"
    assert column["nested_repeated_level_offsets"] == [0, 2, 2, 2, 4]
    assert column["nested_repeated_level_validity_hex_preview"] == "0b"
    assert column["native_read_arrow_length"] == 4
    assert column["native_read_arrow_null_count"] == 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_map_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level lists of scalar maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", 1), ("b", 2)], [], None],
                    None,
                    [],
                    [[("c", None)]],
                ],
                type=pa.list_(pa.map_(pa.string(), pa.int64())),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "key_value", "key"],
        ["items", "list", "element", "key_value", "value"],
    ]
    assert columns[0]["repeated_level_offsets"] == [0, 3, 3, 3, 4]
    assert columns[0]["nested_repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert columns[0]["nested_repeated_level_validity_hex_preview"] == "0b"
    assert columns[0]["native_read_total_nulls"] == 0
    assert columns[1]["native_read_total_nulls"] == 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_list_of_list_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level three-deep scalar lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-list-list.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[[1, 2], []], None],
                    [],
                    None,
                    [[[None, 3]]],
                ],
                type=pa.list_(pa.list_(pa.list_(pa.int64()))),
            )
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
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["path_in_schema"] == [
        "items",
        "list",
        "element",
        "list",
        "element",
        "list",
        "element",
    ]
    assert column["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_validity_hex_preview"] == "0b"
    assert column["nested_repeated_level_offsets"] == [0, 2, 2, 3]
    assert column["nested_repeated_level_validity_hex_preview"] == "05"
    assert column["deep_repeated_level_offsets"] == [0, 2, 2, 4]
    assert column["deep_repeated_level_validity_hex_preview"] == ""
    assert column["native_read_arrow_length"] == 4
    assert column["native_read_arrow_null_count"] == 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
@pytest.mark.parametrize(
    ("name", "array"),
    [
        (
            "list4-int",
            pa.array(
                [
                    [[[[1, 2], []], None], []],
                    None,
                    [],
                    [[[[None, 3]]]],
                ],
                type=pa.list_(pa.list_(pa.list_(pa.list_(pa.int64())))),
            ),
        ),
        (
            "list5-string",
            pa.array(
                [
                    [[[[["a"], []]]]],
                    None,
                    [],
                    [[[[[None, "b"]]]]],
                ],
                type=pa.list_(pa.list_(pa.list_(pa.list_(pa.list_(pa.string()))))),
            ),
        ),
    ],
)
def test_native_parquet_stream_materializes_arbitrary_depth_list_chains(
    tmp_path: Path,
    name: str,
    array: pa.Array,
) -> None:
    """Verify native reader materializes scalar list chains deeper than three."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / f"native-{name}.parquet"
    table = pa.table({"items": array})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level structs with map children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-map-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": 1, "b": 2}, "name": "x"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": None}, "name": "z"},
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), pa.int64())),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value"],
        ["rec", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_nested_struct_child(
    tmp_path: Path,
) -> None:
    """Verify native reader recursively materializes nested ordinary structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-nested-struct-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"inner": {"score": 1, "label": "x"}, "name": "a"},
                    None,
                    {"inner": None, "name": "b"},
                    {"inner": {"score": None, "label": None}, "name": None},
                ],
                type=pa.struct(
                    [
                        pa.field(
                            "inner",
                            pa.struct(
                                [
                                    pa.field("score", pa.int64()),
                                    pa.field("label", pa.string()),
                                ]
                            ),
                        ),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "inner", "score"],
        ["rec", "inner", "label"],
        ["rec", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level structs with list-valued maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-map-list-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": [1, 2], "b": []}, "name": "x"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": None, "d": [None, 3]}, "name": "z"},
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.int64()))),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "list", "element"],
        ["rec", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level structs with nested-list maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-map-list-chain-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": [[1, 2], []]}, "name": "x"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": None, "d": [[None, 3]]}, "name": "z"},
                ],
                type=pa.struct(
                    [
                        pa.field(
                            "attrs",
                            pa.map_(pa.string(), pa.list_(pa.list_(pa.int64()))),
                        ),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        [
            "rec",
            "attrs",
            "key_value",
            "value",
            "list",
            "element",
            "list",
            "element",
        ],
        ["rec", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level structs with struct-valued maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-map-struct-child.parquet"
    value_type = pa.struct(
        [
            pa.field("score", pa.int64()),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": {"score": 1, "label": "x"}, "b": None}, "name": "r"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": {"score": None, "label": "z"}}, "name": "s"},
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), value_type)),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "score"],
        ["rec", "attrs", "key_value", "value", "label"],
        ["rec", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes struct-owned maps with list struct fields."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-map-struct-list-child.parquet"
    value_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": {"ids": [1, 2], "label": "x"}, "b": None}, "name": "r"},
                    None,
                    {"attrs": None, "name": None},
                    {
                        "attrs": {
                            "c": {"ids": None, "label": "z"},
                            "d": {"ids": [None, 3], "label": None},
                        },
                        "name": "s",
                    },
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), value_type)),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "ids", "list", "element"],
        ["rec", "attrs", "key_value", "value", "label"],
        ["rec", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes struct-owned maps with nested-list struct fields."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-map-struct-list-chain-child.parquet"
    value_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.list_(pa.int64()))),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {
                        "attrs": {
                            "a": {"ids": [[1, 2], []], "label": "x"},
                            "b": None,
                        },
                        "name": "r",
                    },
                    None,
                    {"attrs": None, "name": None},
                    {
                        "attrs": {
                            "c": {"ids": None, "label": "z"},
                            "d": {"ids": [[None, 3]], "label": None},
                        },
                        "name": "s",
                    },
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), value_type)),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        [
            "rec",
            "attrs",
            "key_value",
            "value",
            "ids",
            "list",
            "element",
            "list",
            "element",
        ],
        ["rec", "attrs", "key_value", "value", "label"],
        ["rec", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_inner_list(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with scalar list children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-inner-list.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"ids": [1, 2], "name": "a"}, {"ids": [], "name": "b"}],
                    None,
                    [{"ids": None, "name": None}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("ids", pa.list_(pa.int64())),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "ids", "list", "element"],
        ["items", "list", "element", "name"],
    ]
    assert columns[0]["repeated_level_offsets"] == [0, 2, 2, 3]
    assert columns[0]["nested_repeated_level_offsets"] == [0, 2, 2, 2]
    assert columns[1]["repeated_level_offsets"] == [0, 2, 2, 3]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_inner_list_chain(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with nested list children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-inner-list-chain.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"ids": [[1, 2], []], "name": "a"}],
                    None,
                    [{"ids": None, "name": None}, {"ids": [[None, 3]], "name": "b"}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("ids", pa.list_(pa.list_(pa.int64()))),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "ids", "list", "element", "list", "element"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_nested_struct_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with ordinary struct children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-nested-struct-child.parquet"
    inner_type = pa.struct(
        [
            pa.field("score", pa.int64()),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"inner": {"score": 1, "label": "x"}, "name": "a"},
                        {"inner": None, "name": "b"},
                    ],
                    None,
                    [],
                    [{"inner": {"score": None, "label": None}, "name": None}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("inner", inner_type),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "inner", "score"],
        ["items", "list", "element", "inner", "label"],
        ["items", "list", "element", "name"],
    ]
    assert columns[0]["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert columns[1]["repeated_level_offsets"] == [0, 2, 2, 2, 3]
    assert columns[2]["repeated_level_offsets"] == [0, 2, 2, 2, 3]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_nested_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes nested struct list children in list structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-nested-struct-list-child.parquet"
    inner_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"inner": {"ids": [1, 2], "label": "x"}, "name": "a"},
                        {"inner": None, "name": "b"},
                    ],
                    None,
                    [{"inner": {"ids": [], "label": None}, "name": None}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("inner", inner_type),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "inner", "ids", "list", "element"],
        ["items", "list", "element", "inner", "label"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    out.validate(full=True)
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_nested_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes nested struct list-chain children in list structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-nested-struct-list-chain-child.parquet"
    inner_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.list_(pa.int64()))),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"inner": {"ids": [[1, 2], []], "label": "x"}, "name": "a"},
                        {"inner": None, "name": "b"},
                    ],
                    None,
                    [
                        {"inner": {"ids": None, "label": None}, "name": None},
                        {"inner": {"ids": [[None, 3]], "label": "z"}, "name": "c"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("inner", inner_type),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        [
            "items",
            "list",
            "element",
            "inner",
            "ids",
            "list",
            "element",
            "list",
            "element",
        ],
        ["items", "list", "element", "inner", "label"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    out.validate(full=True)
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with map children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-map-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": 1, "b": 2}, "name": "x"}, {"attrs": {}, "name": "y"}],
                    None,
                    [{"attrs": None, "name": None}, {"attrs": {"c": None}, "name": "z"}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), pa.int64())),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "attrs", "key_value", "key"],
        ["items", "list", "element", "attrs", "key_value", "value"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with list-valued map children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-map-list-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"attrs": {"a": [1, 2], "b": []}, "name": "x"},
                        {"attrs": {}, "name": "y"},
                    ],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {"attrs": {"c": None, "d": [None, 3]}, "name": "z"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.int64()))),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "attrs", "key_value", "key"],
        ["items", "list", "element", "attrs", "key_value", "value", "list", "element"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with nested-list-valued maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-map-list-chain-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"attrs": {"a": [[1, 2], []]}, "name": "x"},
                        {"attrs": {}, "name": "y"},
                    ],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {"attrs": {"c": None, "d": [[None, 3]]}, "name": "z"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field(
                                "attrs",
                                pa.map_(pa.string(), pa.list_(pa.list_(pa.int64()))),
                            ),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        [
            "items",
            "list",
            "element",
            "attrs",
            "key_value",
            "key",
        ],
        [
            "items",
            "list",
            "element",
            "attrs",
            "key_value",
            "value",
            "list",
            "element",
            "list",
            "element",
        ],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_struct_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with struct-valued maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-map-struct-child.parquet"
    value_type = pa.struct(
        [
            pa.field("score", pa.int64()),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": {"score": 1, "label": "x"}, "b": None}, "name": "r"}],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {"attrs": {"c": {"score": None, "label": "z"}}, "name": "s"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), value_type)),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "attrs", "key_value", "key"],
        ["items", "list", "element", "attrs", "key_value", "value", "score"],
        ["items", "list", "element", "attrs", "key_value", "value", "label"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with map struct list fields."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-map-struct-list-child.parquet"
    value_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": {"ids": [1, 2], "label": "x"}, "b": None}, "name": "r"}],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {
                            "attrs": {
                                "c": {"ids": None, "label": "z"},
                                "d": {"ids": [None, 3], "label": None},
                            },
                            "name": "s",
                        },
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), value_type)),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "attrs", "key_value", "key"],
        [
            "items",
            "list",
            "element",
            "attrs",
            "key_value",
            "value",
            "ids",
            "list",
            "element",
        ],
        ["items", "list", "element", "attrs", "key_value", "value", "label"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with map struct nested-list fields."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-struct-map-struct-list-chain-child.parquet"
    value_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.list_(pa.int64()))),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {
                            "attrs": {
                                "a": {"ids": [[1, 2], []], "label": "x"},
                                "b": None,
                            },
                            "name": "r",
                        }
                    ],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {
                            "attrs": {
                                "c": {"ids": None, "label": "z"},
                                "d": {"ids": [[None, 3]], "label": None},
                            },
                            "name": "s",
                        },
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), value_type)),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "attrs", "key_value", "key"],
        [
            "items",
            "list",
            "element",
            "attrs",
            "key_value",
            "value",
            "ids",
            "list",
            "element",
            "list",
            "element",
        ],
        ["items", "list", "element", "attrs", "key_value", "value", "label"],
        ["items", "list", "element", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are scalar lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-list-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", [1, 2]), ("b", [])], None, [("c", None)]],
                type=pa.map_(pa.string(), pa.list_(pa.int64())),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "list", "element"],
    ]
    assert columns[0]["repeated_level_offsets"] == [0, 2, 2, 3]
    assert columns[1]["repeated_level_offsets"] == [0, 2, 2, 3]
    assert columns[1]["nested_repeated_level_offsets"] == [0, 2, 2, 2]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are nested lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-list-chain-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", [[1, 2], []])], None, [("b", None), ("c", [[None, 3]])]],
                type=pa.map_(pa.string(), pa.list_(pa.list_(pa.int64()))),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "list", "element", "list", "element"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-struct-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [
                    [("a", {"score": 1, "name": "x"}), ("b", {"score": 2, "name": None})],
                    None,
                    [("c", None), ("d", {"score": None, "name": "z"})],
                ],
                type=pa.map_(
                    pa.string(),
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
                            pa.field("name", pa.string()),
                        ]
                    ),
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "score"],
        ["labels", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_nested_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader recursively materializes nested map value structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-nested-struct-values.parquet"
    inner_type = pa.struct(
        [
            pa.field("score", pa.int64()),
            pa.field("label", pa.string()),
        ]
    )
    value_type = pa.struct(
        [
            pa.field("inner", inner_type),
            pa.field("kind", pa.string()),
        ]
    )
    table = pa.table(
        {
            "attrs": pa.array(
                [
                    {"a": {"inner": {"score": 1, "label": "x"}, "kind": "ok"}, "b": None},
                    None,
                    {},
                    {"c": {"inner": None, "kind": "empty"}},
                    {"d": {"inner": {"score": None, "label": None}, "kind": None}},
                ],
                type=pa.map_(pa.string(), value_type),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["attrs", "key_value", "key"],
        ["attrs", "key_value", "value", "inner", "score"],
        ["attrs", "key_value", "value", "inner", "label"],
        ["attrs", "key_value", "value", "kind"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_nested_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values with nested struct lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-nested-struct-list-values.parquet"
    inner_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
            pa.field("label", pa.string()),
        ]
    )
    value_type = pa.struct(
        [
            pa.field("inner", inner_type),
            pa.field("kind", pa.string()),
        ]
    )
    table = pa.table(
        {
            "attrs": pa.array(
                [
                    {"a": {"inner": {"ids": [1, 2], "label": "x"}, "kind": "ok"}},
                    None,
                    {"b": {"inner": {"ids": [], "label": None}, "kind": None}},
                    {"c": {"inner": None, "kind": "z"}},
                    {"d": {"inner": {"ids": None, "label": "q"}, "kind": "r"}},
                ],
                type=pa.map_(pa.string(), value_type),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_nested_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes nested struct list children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-nested-struct-list-child.parquet"
    inner_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
            pa.field("label", pa.string()),
        ]
    )
    outer_type = pa.struct(
        [
            pa.field("inner", inner_type),
            pa.field("kind", pa.string()),
        ]
    )
    table = pa.table(
        {
            "outer": pa.array(
                [
                    {"inner": {"ids": [1, 2], "label": "x"}, "kind": "ok"},
                    None,
                    {"inner": {"ids": [], "label": None}, "kind": None},
                    {"inner": None, "kind": "z"},
                    {"inner": {"ids": None, "label": "q"}, "kind": "r"},
                ],
                type=outer_type,
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_nested_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes nested struct list-chain children."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-struct-nested-struct-list-chain-child.parquet"
    inner_type = pa.struct(
        [
            pa.field("groups", pa.list_(pa.list_(pa.int64()))),
            pa.field("label", pa.string()),
        ]
    )
    outer_type = pa.struct(
        [
            pa.field("inner", inner_type),
            pa.field("kind", pa.string()),
        ]
    )
    table = pa.table(
        {
            "outer": pa.array(
                [
                    {
                        "inner": {"groups": [[1, 2], [], None], "label": "x"},
                        "kind": "ok",
                    },
                    None,
                    {"inner": {"groups": [], "label": None}, "kind": None},
                    {"inner": None, "kind": "z"},
                    {"inner": {"groups": None, "label": "q"}, "kind": "r"},
                ],
                type=outer_type,
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_nested_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values with nested struct list chains."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-nested-struct-list-chain-values.parquet"
    inner_type = pa.struct(
        [
            pa.field("groups", pa.list_(pa.list_(pa.int64()))),
            pa.field("label", pa.string()),
        ]
    )
    value_type = pa.struct(
        [
            pa.field("inner", inner_type),
            pa.field("kind", pa.string()),
        ]
    )
    table = pa.table(
        {
            "attrs": pa.array(
                [
                    {
                        "a": {
                            "inner": {"groups": [[1, 2], [], None], "label": "x"},
                            "kind": "ok",
                        }
                    },
                    None,
                    {"b": {"inner": {"groups": [], "label": None}, "kind": None}},
                    {"c": {"inner": None, "kind": "z"}},
                    {"d": {"inner": {"groups": None, "label": "q"}, "kind": "r"}},
                ],
                type=pa.map_(pa.string(), value_type),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_nested_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values with nested struct lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-nested-struct-list-values.parquet"
    inner_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
            pa.field("label", pa.string()),
        ]
    )
    value_type = pa.struct(
        [
            pa.field("inner", inner_type),
            pa.field("kind", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"a": {"inner": {"ids": [1, 2], "label": "x"}, "kind": "ok"}},
                        {"b": {"inner": {"ids": [], "label": None}, "kind": None}},
                    ],
                    None,
                    [],
                    [
                        {"c": {"inner": None, "kind": "z"}},
                        {"d": {"inner": {"ids": None, "label": "q"}, "kind": "r"}},
                    ],
                ],
                type=pa.list_(pa.map_(pa.string(), value_type)),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are lists of structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-list-struct-values.parquet"
    element_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "m": pa.array(
                [
                    {
                        "a": [
                            {"x": 1, "ys": [10, 20]},
                            {"x": None, "ys": []},
                            None,
                        ]
                    },
                    None,
                    {"b": []},
                    {"c": None},
                    {"d": [{"x": 4, "ys": None}]},
                ],
                type=pa.map_(pa.string(), pa.list_(element_type)),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are lists of structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-list-struct-values.parquet"
    element_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "m": pa.array(
                [
                    [
                        {
                            "a": [
                                {"x": 1, "ys": [10, 20]},
                                {"x": None, "ys": []},
                                None,
                            ]
                        },
                        {"b": []},
                    ],
                    None,
                    [],
                    [{"c": None}, {"d": [{"x": 4, "ys": None}]}],
                ],
                type=pa.list_(pa.map_(pa.string(), pa.list_(element_type))),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose struct values contain lists."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-struct-list-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [
                    [("a", {"ids": [1, 2], "name": "x"}), ("b", {"ids": [], "name": None})],
                    None,
                    [
                        ("c", None),
                        ("d", {"ids": None, "name": "z"}),
                        ("e", {"ids": [None, 3], "name": "q"}),
                    ],
                ],
                type=pa.map_(
                    pa.string(),
                    pa.struct(
                        [
                            pa.field("ids", pa.list_(pa.int64())),
                            pa.field("name", pa.string()),
                        ]
                    ),
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "ids", "list", "element"],
        ["labels", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose struct values contain list chains."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-struct-list-chain-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [
                    [("a", {"ids": [[1, 2], []], "name": "x"})],
                    None,
                    [
                        ("c", None),
                        ("d", {"ids": None, "name": "z"}),
                        ("e", {"ids": [[None, 3]], "name": "q"}),
                    ],
                ],
                type=pa.map_(
                    pa.string(),
                    pa.struct(
                        [
                            pa.field("ids", pa.list_(pa.list_(pa.int64()))),
                            pa.field("name", pa.string()),
                        ]
                    ),
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "ids", "list", "element", "list", "element"],
        ["labels", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list maps whose values are structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-struct-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", {"score": 1, "name": "x"}), ("b", {"score": 2, "name": None})]],
                    None,
                    [None, [("c", None), ("d", {"score": None, "name": "z"})]],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct(
                            [
                                pa.field("score", pa.int64()),
                                pa.field("name", pa.string()),
                            ]
                        ),
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "key_value", "key"],
        ["items", "list", "element", "key_value", "value", "score"],
        ["items", "list", "element", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_nested_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader recursively materializes nested list-map value structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-nested-struct-values.parquet"
    inner_type = pa.struct(
        [
            pa.field("score", pa.int64()),
            pa.field("label", pa.string()),
        ]
    )
    value_type = pa.struct(
        [
            pa.field("inner", inner_type),
            pa.field("kind", pa.string()),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", {"inner": {"score": 1, "label": "x"}, "kind": "ok"})], None],
                    None,
                    [],
                    [[("b", None)], [("c", {"inner": None, "kind": "empty"})]],
                    [[("d", {"inner": {"score": None, "label": None}, "kind": None})]],
                ],
                type=pa.list_(pa.map_(pa.string(), value_type)),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "key_value", "key"],
        ["items", "list", "element", "key_value", "value", "inner", "score"],
        ["items", "list", "element", "key_value", "value", "inner", "label"],
        ["items", "list", "element", "key_value", "value", "kind"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list maps with list-bearing struct values."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-struct-list-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", {"ids": [1, 2], "name": "x"}), ("b", {"ids": [], "name": None})]],
                    None,
                    [None, [("c", None), ("d", {"ids": [None, 3], "name": "z"})]],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct(
                            [
                                pa.field("ids", pa.list_(pa.int64())),
                                pa.field("name", pa.string()),
                            ]
                        ),
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "key_value", "key"],
        ["items", "list", "element", "key_value", "value", "ids", "list", "element"],
        ["items", "list", "element", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list maps with nested-list struct values."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-struct-list-chain-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        [
                            ("a", {"ids": [[1, 2], []], "name": "x"}),
                            ("b", {"ids": [], "name": None}),
                        ]
                    ],
                    None,
                    [None, [("c", None), ("d", {"ids": [[None, 3]], "name": "z"})]],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct(
                            [
                                pa.field("ids", pa.list_(pa.list_(pa.int64()))),
                                pa.field("name", pa.string()),
                            ]
                        ),
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "element", "key_value", "key"],
        [
            "items",
            "list",
            "element",
            "key_value",
            "value",
            "ids",
            "list",
            "element",
            "list",
            "element",
        ],
        ["items", "list", "element", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level list-list-struct values."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-list-struct-values.parquet"
    struct_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[{"x": 1, "ys": [1, 2]}, {"x": 2, "ys": []}], []],
                    None,
                    [[None, {"x": 3, "ys": None}]],
                ],
                type=pa.list_(pa.list_(struct_type)),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_list_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level list-list-map values."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-list-map-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[{"a": 1}, {}, None], []],
                    None,
                    [[{"b": None, "c": 3}]],
                ],
                type=pa.list_(pa.list_(pa.map_(pa.string(), pa.int64()))),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_list_map_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-list-map values with struct values."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-list-map-struct-values.parquet"
    value_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[{"a": {"x": 1, "ys": [1]}, "b": None}, {}, None]],
                    None,
                    [[{"c": {"x": None, "ys": []}}]],
                ],
                type=pa.list_(pa.list_(pa.map_(pa.string(), value_type))),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_list_map_struct_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes nested map children inside map-value structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-list-map-struct-map-values.parquet"
    value_type = pa.struct(
        [
            pa.field("n", pa.int64()),
            pa.field("m", pa.map_(pa.string(), pa.int64())),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        [
                            {
                                "a": {"n": 1, "m": {"x": 2}},
                                "b": {"n": None, "m": None},
                            }
                        ]
                    ],
                    None,
                    [[{}]],
                ],
                type=pa.list_(pa.list_(pa.map_(pa.string(), value_type))),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are list-list-structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-list-list-struct-values.parquet"
    struct_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    {"a": [[{"x": 1, "ys": [1]}, None], []]},
                    None,
                    {"b": None, "c": [[{"x": 2, "ys": []}]]},
                ],
                type=pa.map_(pa.string(), pa.list_(pa.list_(struct_type))),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_list_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are list-list-maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-map-list-list-map-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    {"root": [[{"a": 1}], []]},
                    None,
                    {"empty": None, "other": [[{"b": None}]]},
                ],
                type=pa.map_(pa.string(), pa.list_(pa.list_(pa.map_(pa.string(), pa.int64())))),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are list-list-structs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-list-list-struct-values.parquet"
    struct_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"a": [[{"x": 1, "ys": [1]}, None]]}],
                    None,
                    [],
                    [{"b": []}],
                ],
                type=pa.list_(pa.map_(pa.string(), pa.list_(pa.list_(struct_type)))),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_list_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are list-list-maps."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "native-list-map-list-list-map-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"root": [[{"a": 1}], []]}],
                    None,
                    [],
                    [{"other": [[{"b": None}]]}],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.list_(pa.list_(pa.map_(pa.string(), pa.int64()))),
                    )
                ),
            )
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
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_deep_recursive_mixed_shapes(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes deeper generated recursive map/list shapes."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    map_type = pa.map_(pa.string(), pa.int64())
    map_list_map_type = pa.map_(pa.string(), pa.list_(map_type))
    inner_struct = pa.struct(
        [
            pa.field("m", map_type),
            pa.field("v", pa.int64()),
        ]
    )
    nested_map_struct = pa.struct(
        [
            pa.field("m", map_list_map_type),
            pa.field("v", pa.int64()),
        ]
    )
    list_list_struct_map = pa.list_(pa.list_(inner_struct))
    list_list_struct_map_list_map = pa.list_(pa.list_(nested_map_struct))
    map_list_list_struct_map = pa.map_(pa.string(), list_list_struct_map)
    cases = [
        (
            "struct-list-list-struct-map",
            pa.struct([pa.field("a", list_list_struct_map)]),
            [
                {"a": [[{"m": {"x": 1}, "v": 2}, None], []]},
                None,
                {"a": None},
            ],
        ),
        (
            "list-struct-list-list-struct-map",
            pa.list_(pa.struct([pa.field("a", list_list_struct_map)])),
            [
                [{"a": [[{"m": {"x": 1}, "v": 2}, None], []]}],
                None,
                [],
                [{"a": None}],
            ],
        ),
        (
            "map-struct-list-list-struct-map",
            pa.map_(pa.string(), pa.struct([pa.field("a", list_list_struct_map)])),
            [
                {"root": {"a": [[{"m": {"x": 1}, "v": 2}]]}},
                None,
                {"z": None},
            ],
        ),
        (
            "list-map-struct-list-list-struct-map",
            pa.list_(pa.map_(pa.string(), pa.struct([pa.field("a", list_list_struct_map)]))),
            [
                [{"root": {"a": [[{"m": {"x": 1}, "v": 2}]]}}],
                None,
                [],
                [{"z": None}],
            ],
        ),
        (
            "list-list-struct-map-list-map",
            list_list_struct_map_list_map,
            [
                [[{"m": {"a": [{"x": 1}, None], "b": []}, "v": 2}]],
                None,
                [[]],
            ],
        ),
        (
            "struct-map-list-list-struct-map",
            pa.struct([pa.field("k", map_list_list_struct_map)]),
            [
                {"k": {"root": [[{"m": {"x": 1}, "v": 2}]]}},
                None,
                {"k": None},
            ],
        ),
    ]

    for name, item_type, values in cases:
        path = tmp_path / f"native-{name}.parquet"
        table = pa.table({"items": pa.array(values, type=item_type)})
        write_parquet_native_first_stream(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
            path,
            feature="test",
            parquet_compression="uncompressed",
        )

        info = native_parquet_footer_info(path)

        assert info is not None
        assert info["native_reader_ready"] == 1
        assert info["native_reader_blockers"] == []
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()

        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_list_struct_projection_uses_native_reader(
    tmp_path: Path,
) -> None:
    """Verify projected list structs with scalar leaves use the native reader."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "complex-nested-list-projection.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "items": pa.array(
                [
                    [{"score": 1, "label": "a"}, {"score": 2, "label": "b"}],
                    [{"score": 3, "label": "c"}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
                            pa.field("label", pa.string()),
                        ]
                    )
                ),
            ),
        }
    )
    expected = table.select(["items"])
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["items"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_list_list_projection_uses_native_reader(
    tmp_path: Path,
) -> None:
    """Verify projected nested scalar lists use the native reader."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "nested-list-projection.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "items": pa.array(
                [
                    [[1, 2], [], None],
                    [[None, 3]],
                ],
                type=pa.list_(pa.list_(pa.int64())),
            ),
        }
    )
    expected = table.select(["items"])
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["items"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_required_struct_scalar_leaves(
    tmp_path: Path,
) -> None:
    """Verify native reader can materialize required structs with scalar leaves."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "required-struct.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("score", pa.float64(), nullable=False),
                    ]
                ),
                nullable=False,
            ),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {"id": 1, "profile": {"name": "a", "score": 1.5}},
            {"id": 2, "profile": {"name": None, "score": 2.5}},
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
    assert info["native_reader_blockers"] == []
    assert [column["path_in_schema"] for column in info["row_groups"][0]["columns"]] == [
        ["id"],
        ["profile", "name"],
        ["profile", "score"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_nullable_struct_scalar_leaves(
    tmp_path: Path,
) -> None:
    """Verify native reader can materialize nullable structs with scalar leaves."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "nullable-struct.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("score", pa.float64(), nullable=False),
                    ]
                ),
            ),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {"id": 1, "profile": {"name": "a", "score": 1.5}},
            {"id": 2, "profile": None},
            {"id": 3, "profile": {"name": None, "score": 3.5}},
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
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_projects_struct_scalar_leaves(
    tmp_path: Path,
) -> None:
    """Verify native struct projection keeps every leaf under the struct."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "projected-struct.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("score", pa.float64(), nullable=False),
                    ]
                ),
            ),
            pa.field("flag", pa.bool_()),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {"id": 1, "profile": {"name": "a", "score": 1.5}, "flag": True},
            {"id": 2, "profile": None, "flag": False},
            {"id": 3, "profile": {"name": None, "score": 3.5}, "flag": None},
        ],
        schema=schema,
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["profile", "id"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    expected = table.select(["profile", "id"])

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_projects_simple_list_with_unsupported_unprojected_column(
    tmp_path: Path,
) -> None:
    """Verify list projection can use native route despite unprojected blockers."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "projected-list.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("scores", pa.list_(pa.int64())),
            pa.field("items", pa.list_(pa.struct([pa.field("ids", pa.list_(pa.int64()))]))),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {"id": 1, "scores": [1, 2], "items": [{"ids": [10]}]},
            {"id": 2, "scores": None, "items": []},
            {"id": 3, "scores": [3], "items": None},
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
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["scores", "id"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    expected = table.select(["scores", "id"])

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_nested_native_parquet_reader_materializes_supported_nested_shapes(
    tmp_path: Path,
) -> None:
    """Verify supported nested native-written files materialize natively."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.native_file_output import write_parquet_native_first_stream

    require_native()
    path = tmp_path / "nested-native.parquet"
    table = pa.table(
        {
            "profile": pa.array(
                [{"name": "a"}, None, {"name": "b"}],
                type=pa.struct([pa.field("name", pa.string())]),
            ),
            "scores": pa.array([[1, 2], None, []], type=pa.list_(pa.int64())),
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
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_native_parquet_footer_info_decodes_plain_value_counts_with_nulls(
    tmp_path: Path,
) -> None:
    """Verify native PLAIN value validation counts only non-null values."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(
        pa.table(
            {
                "a": pa.array([1, None, 987654321012345678], type=pa.int64()),
                "b": pa.array(["x", None, "zz"], type=pa.string()),
            }
        ),
        src,
    )

    ss.to_parquet(
        src,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(out)

    assert info is not None
    int_column = info["row_groups"][0]["columns"][0]
    string_column = info["row_groups"][0]["columns"][1]
    int_page = int_column["pages"][0]
    string_page = string_column["pages"][0]
    assert int_column["native_read_arrow_length"] == 3
    assert int_column["native_read_arrow_null_count"] == 1
    assert int_column["native_read_arrow_n_buffers"] == 2
    assert int_column["native_read_arrow_n_children"] == 0
    assert int_column["native_read_has_validity_buffer"] == 1
    assert int_column["native_read_has_offsets_buffer"] == 0
    assert int_column["native_read_has_values_buffer"] == 1
    assert string_column["native_read_arrow_length"] == 3
    assert string_column["native_read_arrow_null_count"] == 1
    assert string_column["native_read_arrow_n_buffers"] == 3
    assert string_column["native_read_arrow_n_children"] == 0
    assert string_column["native_read_has_validity_buffer"] == 1
    assert string_column["native_read_has_offsets_buffer"] == 1
    assert string_column["native_read_has_values_buffer"] == 1
    assert int_page["decoded_non_null_values"] == 2
    assert int_page["decoded_null_values"] == 1
    assert int_page["decoded_value_bytes"] == 16
    assert int_page["materialized_value_bytes"] == 24
    assert int_page["materialized_offset_bytes"] == 0
    assert int_page["decoded_value_preview"] == ["1", "987654321012345678"]
    assert int_page["validity_bitmap_decoded"] == 1
    assert int_page["decoded_validity_bytes"] == 1
    assert int_page["decoded_validity_hex_preview"] == "05"
    assert string_page["decoded_non_null_values"] == 2
    assert string_page["decoded_null_values"] == 1
    assert string_page["decoded_value_bytes"] == 11
    assert string_page["materialized_value_bytes"] == 3
    assert string_page["materialized_offset_bytes"] == 16
    assert string_page["decoded_value_preview"] == ["x", "zz"]
    assert string_page["validity_bitmap_decoded"] == 1
    assert string_page["decoded_validity_bytes"] == 1
    assert string_page["decoded_validity_hex_preview"] == "05"


@_requires_pyarrow
def test_native_parquet_footer_info_decodes_delta_binary_packed_int_values(
    tmp_path: Path,
) -> None:
    """Verify native footer parsing validates DELTA_BINARY_PACKED integer pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(pa.table({"n": list(range(50)), "s": ["same"] * 50}), src)

    ss.to_parquet(src, out, input_format="parquet")
    info = native_parquet_footer_info(out)

    assert info is not None
    column = next(
        column for column in info["row_groups"][0]["columns"] if column["path_in_schema"] == ["n"]
    )
    page = column["pages"][0]
    if page["value_encoding"] != 5:
        pytest.skip("native writer did not choose DELTA_BINARY_PACKED on this platform")
    assert page["value_encoding"] == 5
    assert page["decoded_non_null_values"] == 50
    assert page["decoded_null_values"] == 0
    assert page["values_decoded"] == 1
    assert page["values_decode_skipped"] == 0
    assert page["decoded_value_bytes"] > 0
    assert page["materialized_value_bytes"] == 50 * 8
    assert page["materialized_offset_bytes"] == 0
    assert page["decoded_value_preview"] == [str(value) for value in range(8)]
    assert column["native_read_value_buffer_kind"] == "delta_binary_packed"
    assert column["native_read_arrow_n_buffers"] == 2

    factory = open_parquet_record_batch_stream_factory(out, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().column("n").to_pylist() == list(range(50))
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_footer_info_decodes_rle_dictionary_string_pages(
    tmp_path: Path,
) -> None:
    """Verify native footer parsing validates dictionary pages and indices."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(pa.table({"s": ["same"] * 500}), src)

    ss.to_parquet(
        src,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(out)

    assert info is not None
    column = next(
        column for column in info["row_groups"][0]["columns"] if column["path_in_schema"] == ["s"]
    )
    assert 8 in column["encodings"]
    assert column["native_read_value_buffer_kind"] == "dictionary_byte_array"
    assert column["native_read_arrow_n_buffers"] == 3
    assert column["native_read_has_offsets_buffer"] == 1
    dictionary_page = column["pages"][0]
    data_page = column["pages"][1]
    assert dictionary_page["is_dictionary_page"] == 1
    assert dictionary_page["values_decoded"] == 1
    assert dictionary_page["materialized_value_bytes"] == 4
    assert dictionary_page["materialized_offset_bytes"] == 8
    assert dictionary_page["decoded_value_preview"] == ["same"]
    assert data_page["value_encoding"] == 8
    assert data_page["decoded_non_null_values"] == 500
    assert data_page["values_decoded"] == 1
    assert data_page["values_decode_skipped"] == 0
    assert data_page["materialized_value_bytes"] == 500 * 4
    assert data_page["materialized_offset_bytes"] == (500 + 1) * 4
    assert data_page["dictionary_index_bit_width"] > 0
    assert data_page["decoded_value_preview"] == ["same"] * 8


@_requires_pyarrow
def test_native_parquet_footer_info_decodes_delta_length_byte_array_pages(
    tmp_path: Path,
) -> None:
    """Verify native footer parsing validates DELTA_LENGTH_BYTE_ARRAY pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    values = [f"x{value:04d}" for value in range(500)]
    pq.write_table(pa.table({"s": values}), src)

    ss.to_parquet(
        src,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(out)

    assert info is not None
    column = next(
        column for column in info["row_groups"][0]["columns"] if column["path_in_schema"] == ["s"]
    )
    page = column["pages"][0]
    assert page["value_encoding"] == 6
    assert page["decoded_non_null_values"] == 500
    assert page["values_decoded"] == 1
    assert page["values_decode_skipped"] == 0
    assert page["decoded_value_preview"] == values[:8]
    assert column["native_read_value_buffer_kind"] == "delta_length_byte_array"
    assert column["native_read_arrow_n_buffers"] == 3
    assert column["native_read_has_offsets_buffer"] == 1
    assert page["materialized_value_bytes"] == sum(len(value) for value in values)
    assert page["materialized_offset_bytes"] == (len(values) + 1) * 4

    factory = open_parquet_record_batch_stream_factory(out, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().column("s").to_pylist() == values
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_footer_info_decodes_byte_stream_split_float_pages(
    tmp_path: Path,
) -> None:
    """Verify native footer parsing validates BYTE_STREAM_SPLIT float pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
        native_parquet_footer_info,
        open_parquet_record_batch_stream_factory,
    )

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    values = [float(value % 7) for value in range(1000)]
    pq.write_table(pa.table({"f": pa.array(values, type=pa.float32())}), src)

    ss.to_parquet(
        src,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(out)

    assert info is not None
    column = next(
        column for column in info["row_groups"][0]["columns"] if column["path_in_schema"] == ["f"]
    )
    page = column["pages"][0]
    if page["value_encoding"] != 9:
        pytest.skip("native writer did not choose BYTE_STREAM_SPLIT on this platform")
    assert page["value_encoding"] == 9
    assert page["decoded_non_null_values"] == 1000
    assert column["native_read_value_buffer_kind"] == "byte_stream_split"
    assert column["native_read_value_width_bytes"] == 4
    assert column["native_read_arrow_n_buffers"] == 2
    assert page["values_decoded"] == 1
    assert page["values_decode_skipped"] == 0
    assert page["decoded_value_preview"] == [
        "0.000000",
        "1.000000",
        "2.000000",
        "3.000000",
        "4.000000",
        "5.000000",
        "6.000000",
        "0.000000",
    ]

    factory = open_parquet_record_batch_stream_factory(out, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().column("f").to_pylist() == values
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_read_parquet_file_uri_materializes_table(tmp_path: Path) -> None:
    """Verify read parquet accepts file URIs."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()

    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    result = read_test_parquet(path.as_uri())

    assert result.clean_data.to_pylist() == _sample_table().to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_native_parquet_file_uri_uses_native_route(tmp_path: Path) -> None:
    """Verify local file URIs share the native path-backed Parquet reader."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()

    src = tmp_path / "src.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(_sample_table(), src)
    ss.to_parquet(src, out, input_format="parquet", parquet_compression="uncompressed")

    result = read_test_parquet(out.as_uri())

    assert result.clean_data.select(["a", "b"]).to_pylist() == _sample_table().to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"


@_requires_pyarrow
def test_remote_parquet_uri_requires_staging() -> None:
    """Verify remote Parquet URIs are not treated as local direct sources."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        open_parquet_record_batch_stream_factory,
    )

    with pytest.raises(ValueError, match="URI inputs must be staged"):
        open_parquet_record_batch_stream_factory(
            "gs://bucket/data.parquet",
            source="uri",
            feature="test",
        )


@_requires_pyarrow
def test_parquet_file_uri_converter_writes_jsonl(tmp_path: Path) -> None:
    """Verify converters accept parquet file URIs."""
    require_native()

    path = tmp_path / "data.parquet"
    out = tmp_path / "out.jsonl"
    pq.write_table(_sample_table(), path)

    ss.to_jsonl(path.as_uri(), out, input_format="parquet")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [{k: v for k, v in row.items() if k not in generated} for row in rows] == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]


@_requires_pyarrow
def test_flat_parquet_converter_uses_direct_arrow_path(tmp_path: Path) -> None:
    """Verify flat Parquet file conversion bypasses the JSONL bridge."""
    require_native()
    path = tmp_path / "data.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(_sample_table(), path)

    ss.to_parquet(path, out, input_format="parquet")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert [{k: v for k, v in row.items() if k not in generated} for row in rows] == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]


@_requires_pyarrow
def test_flat_read_parquet_uses_direct_arrow_path(tmp_path: Path) -> None:
    """Verify flat Parquet reads bypass the JSONL bridge."""
    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == _sample_table().to_pylist()


@_requires_pyarrow
def test_parquet_directory_read_uses_direct_arrow_path(tmp_path: Path) -> None:
    """Verify Parquet directory reads bypass the JSONL bridge when schemas match."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
    )

    require_native()
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), folder / "a.parquet")
    pq.write_table(pa.table({"id": [3]}), folder / "b.parquet")

    result = ss.to_pyarrow(folder, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    assert [
        {k: v for k, v in row.items() if k not in generated}
        for row in result.clean_data.to_pylist()
    ] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"


@_requires_pyarrow
def test_parquet_directory_converter_uses_direct_arrow_path(tmp_path: Path) -> None:
    """Verify Parquet directory file conversion bypasses the JSONL bridge."""
    require_native()
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), folder / "a.parquet")
    pq.write_table(pa.table({"id": [3]}), folder / "b.parquet")
    out = tmp_path / "out.parquet"

    ss.to_parquet(folder, out, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert [{k: v for k, v in row.items() if k not in generated} for row in rows] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


@_requires_pyarrow
def test_parquet_directory_mismatched_schemas_use_native_child_arrow_path(tmp_path: Path) -> None:
    """Verify mixed Parquet child schemas bypass the JSONL bridge."""
    require_native()
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1]}), folder / "a.parquet")
    pq.write_table(pa.table({"name": ["two"]}), folder / "b.parquet")

    result = ss.to_pyarrow(folder, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = result.clean_data.to_pylist()
    assert [{key: value for key, value in row.items() if key not in generated} for row in rows] == [
        {"id": 1, "name": None},
        {"id": None, "name": "two"},
    ]
    assert [row["source_file"] for row in rows] == [
        str((folder / "a.parquet").resolve()),
        str((folder / "b.parquet").resolve()),
    ]


@_requires_pyarrow
def test_parquet_directory_mismatched_schema_converter_uses_native_child_arrow_path(
    tmp_path: Path,
) -> None:
    """Verify mixed-schema Parquet directory conversion bypasses the JSONL bridge."""
    require_native()
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1]}), folder / "a.parquet")
    pq.write_table(pa.table({"name": ["two"]}), folder / "b.parquet")
    out = tmp_path / "out.parquet"

    ss.to_parquet(folder, out, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert [{key: value for key, value in row.items() if key not in generated} for row in rows] == [
        {"id": 1, "name": None},
        {"id": None, "name": "two"},
    ]
    assert [row["source_file"] for row in rows] == [
        str((folder / "a.parquet").resolve()),
        str((folder / "b.parquet").resolve()),
    ]


@_requires_pyarrow
def test_direct_parquet_supports_nested_and_explicit_scalar_types() -> None:
    """Verify direct Parquet support is decided by the native schema checker."""
    from schema_sanitizer.adapters import pyarrow_parquet_direct as pyarrow_adapter

    schema = pa.schema(
        [
            pa.field("items", pa.list_(pa.struct([pa.field("score", pa.int64())]))),
            pa.field("binary_value", pa.binary()),
            pa.field("large_binary_value", pa.large_binary()),
            pa.field("uint64_value", pa.uint64()),
            pa.field("date64_value", pa.date64()),
            pa.field("decimal_value", pa.decimal128(10, 2)),
            pa.field(
                "dictionary_value",
                pa.dictionary(pa.int32(), pa.string()),
            ),
        ]
    )

    assert pyarrow_adapter.parquet_schema_supports_direct_native_ingest(
        schema,
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )


@_requires_pyarrow
def test_direct_parquet_schema_support_uses_native_payload_cache(monkeypatch) -> None:
    """Verify equivalent schemas avoid repeated native support calls."""
    from schema_sanitizer.adapters import pyarrow_parquet_direct as direct

    direct._DIRECT_SCHEMA_SUPPORT_CACHE = direct.SchemaSupportCache()
    calls = 0

    def fake_payload(schema):
        """Return one stable native logical-schema fingerprint."""
        nonlocal calls
        calls += 1
        assert schema.names == ["items"]
        return b"native-logical-schema"

    def fail_native_supported(_schema):
        """Fail if the native support call is needed after payload parsing."""
        raise AssertionError("native payload should decide supported schemas")

    monkeypatch.setattr(
        direct,
        "ARROW_SCHEMA_CONTRACT_PAYLOAD",
        SimpleNamespace(get=lambda: fake_payload),
    )
    monkeypatch.setattr(
        direct,
        "ARROW_DIRECT_SCHEMA_SUPPORTED",
        SimpleNamespace(get=lambda: fail_native_supported),
    )

    schema_one = pa.schema([pa.field("items", pa.list_(pa.struct([pa.field("id", pa.int64())])))])
    schema_two = pa.schema([pa.field("items", pa.list_(pa.struct([pa.field("id", pa.int64())])))])

    assert direct.parquet_schema_supports_direct_native_ingest(
        schema_one,
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )
    assert direct.parquet_schema_supports_direct_native_ingest(
        schema_two,
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )
    assert calls == 1


@_requires_pyarrow
def test_direct_parquet_schema_support_requires_native_checker(monkeypatch) -> None:
    """Verify direct Parquet support has no Python recursive fallback."""
    from schema_sanitizer.adapters import pyarrow_parquet_direct as direct

    direct._DIRECT_SCHEMA_SUPPORT_CACHE = direct.SchemaSupportCache()
    monkeypatch.setattr(
        direct,
        "ARROW_SCHEMA_CONTRACT_PAYLOAD",
        SimpleNamespace(get=lambda: None),
    )
    monkeypatch.setattr(
        direct,
        "ARROW_DIRECT_SCHEMA_SUPPORTED",
        SimpleNamespace(get=lambda: None),
    )

    assert not direct.parquet_schema_supports_direct_native_ingest(
        pa.schema([pa.field("value", pa.int64())]),
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )


@_requires_pyarrow
def test_direct_parquet_record_batch_reader_keeps_iterable_lazy() -> None:
    """Verify direct Parquet reader construction does not pre-load batches."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        record_batch_reader_from_iterable,
    )

    batch = pa.record_batch({"a": [1, 2, 3]})
    consumed = 0

    def batches():
        """Yield one batch and record when iteration starts."""
        nonlocal consumed
        consumed += 1
        yield batch

    reader = record_batch_reader_from_iterable(pa, batch.schema, batches())

    assert consumed == 0
    assert reader.read_next_batch().to_pylist() == batch.to_pylist()
    assert consumed == 1


@_requires_pyarrow
def test_nested_read_parquet_uses_direct_arrow_path(tmp_path: Path) -> None:
    """Verify nested Parquet reads bypass the JSONL bridge."""
    require_native()
    path = tmp_path / "nested.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "profile": pa.array(
                [{"name": "a"}, {"name": "b"}],
                type=pa.struct([pa.field("name", pa.string())]),
            ),
            "scores": pa.array([[1, 2], [3]], type=pa.list_(pa.int64())),
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.to_pylist() == table.to_pylist()


@_requires_pyarrow
def test_direct_parquet_normalizes_empty_lists_to_null(tmp_path: Path) -> None:
    """Verify typed Parquet columns remain while empty list values become null."""
    require_native()

    path = tmp_path / "empty-list.parquet"
    pq.write_table(
        pa.table({"items": pa.array([[], [1]], type=pa.list_(pa.int64()))}),
        path,
    )

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.schema.field("items").type == pa.list_(pa.int64())
    assert result.clean_data.to_pylist() == [{"items": None}, {"items": [1]}]


@_requires_pyarrow
def test_direct_parquet_scales_timestamp_units(tmp_path: Path) -> None:
    """Verify direct Parquet scales timestamp units to requested output precision."""
    require_native()
    path = tmp_path / "timestamps.parquet"
    values = [dt.datetime(2024, 1, 2, 3, 4, 5, 123456)]
    table = pa.table({"ts": pa.array(values, type=pa.timestamp("ns"))})
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.clean_data.schema.field("ts").type == pa.timestamp("us")
    assert result.clean_data.to_pylist() == [{"ts": values[0]}]


@_requires_pyarrow
def test_direct_parquet_binary_and_uint64_have_explicit_text_semantics(tmp_path: Path) -> None:
    """Verify direct Parquet handles binary and uint64 without JSONL fallback."""
    require_native()
    path = tmp_path / "scalars.parquet"
    table = pa.table(
        {
            "payload": pa.array([b"\xff"], type=pa.binary()),
            "u": pa.array([2**64 - 1], type=pa.uint64()),
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [{"payload": "/w==", "u": str(2**64 - 1)}]


@_requires_pyarrow
def test_direct_parquet_decimal_values_are_lossless_strings(tmp_path: Path) -> None:
    """Verify direct Parquet preserves decimal values as strings."""
    require_native()
    path = tmp_path / "decimal.parquet"
    table = pa.table(
        {
            "amount": pa.array(
                [Decimal("123.45"), Decimal("-0.10")],
                type=pa.decimal128(10, 2),
            )
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.clean_data.schema.field("amount").type == pa.string()
    assert result.clean_data.to_pylist() == [
        {"amount": "123.45"},
        {"amount": "-0.10"},
    ]


@_requires_pyarrow
def test_direct_parquet_map_and_fixed_size_list_use_arrow_path(tmp_path: Path) -> None:
    """Verify direct Parquet handles map and fixed-size list columns."""
    require_native()
    path = tmp_path / "map_fixed.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", 1), ("b", 2)]],
                type=pa.map_(pa.string(), pa.int64()),
            ),
            "vector": pa.array([[1, 2]], type=pa.list_(pa.int64(), 2)),
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.to_pylist() == [
        {
            "labels": [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
            "vector": [1, 2],
        }
    ]


@_requires_pyarrow
def test_direct_parquet_duration_values_are_lossless_strings(tmp_path: Path) -> None:
    """Verify direct Parquet handles duration values without JSONL fallback."""
    require_native()
    path = tmp_path / "duration.parquet"
    table = pa.table({"elapsed": pa.array([123, -5], type=pa.duration("us"))})
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.schema.field("elapsed").type == pa.string()
    assert result.clean_data.to_pylist() == [{"elapsed": "123us"}, {"elapsed": "-5us"}]


@_requires_pyarrow
def test_native_arrow_schema_contract_payload_supports_new_direct_shapes() -> None:
    """Verify native schema-contract encoding reuses the Arrow direct parser."""
    require_native()
    from schema_sanitizer.core_impl.native import _native
    from schema_sanitizer.core_impl.options_logical_schema import (
        _pyarrow_schema_from_logical_schema_payload,
    )

    schema = pa.schema(
        [
            pa.field("labels", pa.map_(pa.string(), pa.int64())),
            pa.field("vector", pa.list_(pa.int64(), 2)),
            pa.field("amount", pa.decimal128(10, 2)),
        ]
    )

    payload = _native.arrow_schema_contract_payload(schema)
    decoded = _pyarrow_schema_from_logical_schema_payload(payload)

    assert decoded == pa.schema(
        [
            pa.field(
                "labels",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("key", pa.string(), nullable=False),
                            pa.field("value", pa.int64()),
                        ]
                    )
                ),
            ),
            pa.field("vector", pa.list_(pa.int64())),
            pa.field("amount", pa.string()),
        ]
    )


@_requires_pyarrow
def test_parquet_threading_uses_memory_guard() -> None:
    """Verify Parquet direct threading is gated by configured memory."""
    from schema_sanitizer.adapters import pyarrow_parquet_common as pyarrow_adapter

    assert pyarrow_adapter.parquet_use_threads_from_memory_limit(None)
    assert pyarrow_adapter.parquet_use_threads_from_memory_limit(0)
    assert not pyarrow_adapter.parquet_use_threads_from_memory_limit(64 * 1024 * 1024)
    assert pyarrow_adapter.parquet_use_threads_from_memory_limit(256 * 1024 * 1024)


@_requires_pyarrow
def test_parquet_stream_result_drop_closes_reader(tmp_path: Path) -> None:
    """Verify parquet stream sink can be dropped without temporary files."""
    require_native()
    from schema_sanitizer.api_impl.context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    assert getattr(out, "_keepalive", None) is None

    del out
    gc.collect()


@_requires_pyarrow
def test_parquet_stream_survives_sink_result_drop(tmp_path: Path) -> None:
    """Verify parquet stream owns the native reader after stream access."""
    require_native()
    from schema_sanitizer.api_impl.context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    stream = out.stream
    assert stream is not None
    assert getattr(stream, "_keepalive", None) is None

    del out
    gc.collect()

    assert sum(batch.num_rows for batch in stream) == 3
    stream.close()


@_requires_pyarrow
def test_parquet_stream_drop_releases_reader(tmp_path: Path) -> None:
    """Verify parquet stream can be dropped without explicit close."""
    require_native()
    from schema_sanitizer.api_impl.context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    stream = out.stream
    assert stream is not None

    del out
    del stream
    gc.collect()


@_requires_pyarrow
def test_parquet_conversion_enforces_memory_limit_bytes(tmp_path: Path) -> None:
    """Verify parquet conversion enforces memory limit bytes."""
    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_parquet(path, batch_memory_limit_bytes=1)

    err = excinfo.value
    assert getattr(err, "code", None) == "E_RESOURCE_LIMIT"
    assert "memory_limit_bytes" in str(err)
    assert err.detail is not None
    assert err.detail["stage"] == "parquet_conversion"


@_requires_pyarrow
def test_arrow_ipc_inputs_are_not_public(tmp_path: Path) -> None:
    """Verify arrow ipc inputs are not public."""
    require_native()
    path = tmp_path / "data.feather"
    feather.write_feather(_sample_table(), path)

    with pytest.raises(Exception, match=r"requires extension"):
        read_test_parquet(path)
