"""Native scalar Parquet projection and encoding runtime tests."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from _support.parquet_runtime import pa, pq, sample_table
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native

import schema_sanitizer as ss


@_requires_pyarrow
def test_native_parquet_stream_respects_small_batch_size_with_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    """Verify native Parquet falls back when row-group batches exceed batch_size."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

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
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

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
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

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
def test_parquet_filter_stages_buffer_source_for_dataset_scan() -> None:
    """Verify buffer filters use an explicit staged dataset scanner."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )

    ds = pytest.importorskip("pyarrow.dataset")
    sink = pa.BufferOutputStream()
    pq.write_table(sample_table(pa), sink)

    factory = open_parquet_record_batch_stream_factory(
        sink.getvalue().to_pybytes(),
        source="text",
        feature="test",
        filters=ds.field("a") > 1,
    )
    try:
        reader = pa.RecordBatchReader.from_stream(factory)
        assert reader.read_all().to_pylist() == [
            {"a": 2, "b": "y"},
            {"a": 3, "b": "z"},
        ]
    finally:
        factory.close()


@_requires_pyarrow
def test_native_parquet_stream_materializes_plain_boolean_rows(
    tmp_path: Path,
) -> None:
    """Verify the native Parquet stream materializes PLAIN boolean pages."""
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
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
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
