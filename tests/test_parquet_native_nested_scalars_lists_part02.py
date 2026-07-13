"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import require_native

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")

# Split from test_parquet_native_nested_scalars_lists.py: test_native_parquet_stream_materializes_plain_byte_array_lists, test_native_parquet_stream_materializes_simple_float_lists, test_native_parquet_stream_materializes_simple_boolean_lists, ...


@_requires_pyarrow
@pytest.mark.parametrize("name", ["string", "binary"])
def test_native_parquet_stream_materializes_plain_byte_array_lists(
    tmp_path: Path,
    name: str,
) -> None:
    """Verify native reader materializes PLAIN byte-array list elements."""
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
    path = tmp_path / f"native-plain-byte-array-{name}-list.parquet"
    if name == "string":
        array = pa.array([["only"]], type=pa.list_(pa.string()))
    else:
        array = pa.array([[b"only"]], type=pa.list_(pa.binary()))
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
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 1]
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
    assert column["native_read_value_buffer_kind"] == "byte_stream_split"
    assert column["native_read_value_width_bytes"] == 8
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_boolean_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes simple top-level boolean lists."""
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
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
@pytest.mark.parametrize(
    ("name", "native_format", "expected_offsets"),
    [
        ("date32", "tdD", [0, 1, 1, 1, 2]),
        ("timestamp_us", "tsu:", [0, 1, 1, 1, 2]),
        ("decimal128", "d:10,2,128", [0, 1, 1, 1, 2]),
        ("fixed_size_binary", "w:4", [0, 1, 1, 1, 2]),
        ("uint64", "L", [0, 2, 2, 2, 3]),
    ],
)
def test_native_parquet_stream_materializes_logical_fixed_width_lists(
    tmp_path: Path,
    name: str,
    native_format: str,
    expected_offsets: list[int],
) -> None:
    """Verify native lists preserve fixed-width logical scalar element types."""
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
    path = tmp_path / f"native-logical-{name}-list.parquet"
    if name == "date32":
        array = pa.array(
            [[dt.date(2024, 1, 1)], None, [], [dt.date(2024, 1, 2)]],
            type=pa.list_(pa.date32()),
        )
    elif name == "timestamp_us":
        array = pa.array(
            [
                [dt.datetime(2024, 1, 1, 1, 2, 3, 123456)],
                None,
                [],
                [dt.datetime(2024, 1, 2, 1, 2, 3, 123456)],
            ],
            type=pa.list_(pa.timestamp("us")),
        )
    elif name == "decimal128":
        array = pa.array(
            [[Decimal("12.34")], None, [], [Decimal("-0.10")]],
            type=pa.list_(pa.decimal128(10, 2)),
        )
    elif name == "fixed_size_binary":
        array = pa.array([[b"abcd"], None, [], [b"wxyz"]], type=pa.list_(pa.binary(4)))
    else:
        array = pa.array(
            [[1, 2**63], None, [], [2**64 - 1]],
            type=pa.list_(pa.uint64()),
        )
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
    assert column["repeated_level_layouts"][0]["offsets"] == expected_offsets
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
@pytest.mark.parametrize("name", ["int", "string", "bool"])
def test_native_parquet_stream_materializes_nullable_list_elements(
    tmp_path: Path,
    name: str,
) -> None:
    """Verify native list reconstruction preserves null child elements."""
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
    path = tmp_path / f"native-nullable-{name}-list.parquet"
    if name == "int":
        array = pa.array(
            [[1, None, 2], None, [], [None, 3]],
            type=pa.list_(pa.int64()),
        )
    elif name == "string":
        array = pa.array(
            [["a", None, "b"], None, [], [None, "c"]],
            type=pa.list_(pa.string()),
        )
    else:
        array = pa.array(
            [[True, None, False], None, [], [None, True]],
            type=pa.list_(pa.bool_()),
        )
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
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 5]
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
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

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
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"
    assert column["pages"][0]["value_encoding"] == 9


@_requires_pyarrow
def test_native_parquet_stream_materializes_dictionary_string_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes RLE dictionary string list elements."""
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
    assert column["repeated_level_layouts"][0]["offsets"][:5] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"].startswith("dd")

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_dictionary_integer_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes RLE dictionary fixed-width list elements."""
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
    assert column["repeated_level_layouts"][0]["offsets"][:5] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"].startswith("dd")

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
