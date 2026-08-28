"""Optional PyArrow imports and fixtures shared by Parquet runtime tests."""

from __future__ import annotations

import datetime as dt
import inspect
import logging
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from conftest import read_test_parquet

import schema_sanitizer as ss

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None  # type: ignore[assignment]
    HAVE_PYARROW = False

requires_pyarrow = pytest.mark.skipif(
    not HAVE_PYARROW,
    reason="pyarrow not installed",
)
_requires_pyarrow = requires_pyarrow


def sample_table(pyarrow: Any) -> Any:
    """Return the canonical two-column Parquet runtime test table."""
    return pyarrow.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def write_read_native_parquet(
    table: Any,
    path: Path,
    *,
    feature: str = "test",
    parquet_compression: str = "uncompressed",
) -> dict[str, Any]:
    """Write and read one table through the certified native Parquet route."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature=feature,
        parquet_compression=parquet_compression,
    )
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature=feature)
    output = pa.RecordBatchReader.from_stream(factory).read_all()
    output.validate(full=True)
    assert output.schema.equals(table.schema)
    assert output.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    return info


def recursive_arrow_type(spec: object) -> object:
    """Build a PyArrow type from the recursive runtime-test grammar."""
    kind = spec[0]  # type: ignore[index]
    if kind == "int64":
        return pa.int64()
    if kind == "string":
        return pa.string()
    if kind == "bool":
        return pa.bool_()
    if kind == "float64":
        return pa.float64()
    if kind == "list":
        return pa.list_(recursive_arrow_type(spec[1]))  # type: ignore[index]
    if kind == "map":
        return pa.map_(pa.string(), recursive_arrow_type(spec[1]))  # type: ignore[index]
    if kind == "struct":
        return pa.struct(
            [
                pa.field(name, recursive_arrow_type(child))
                for name, child in spec[1]  # type: ignore[index]
            ]
        )
    raise AssertionError(kind)


@_requires_pyarrow
def test_native_parquet_footer_info_reads_schema_sanitizer_file(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    table = pa.table(
        {
            "a": pa.array(
                [123456789012345678, -333333333333333333, 987654321012345678], type=pa.int64()
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
    ss.to_parquet(src, out, input_format="parquet", parquet_compression="uncompressed")
    info = native_parquet_footer_info(out)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    assert info["num_rows"] == 3
    assert info["row_group_count"] >= 1
    assert info["created_by"] == "schema-sanitizer native parquet writer"
    assert info["schema_elements"][0] == {"name": "schema", "num_children": 6}
    assert info["schema_elements"][1]["name"] == "a"
    assert info["schema_elements"][1]["physical_type"] == 2
    assert info["schema_elements"][2]["name"] == "b"
    assert info["schema_elements"][2]["physical_type"] == 6
    assert info["schema_elements"][2]["converted_type"] == 0
    row_group = info["row_groups"][0]
    assert row_group["num_rows"] == 3
    assert row_group["total_byte_size"] > 0
    assert [column["path_in_schema"] for column in row_group["columns"][:2]] == [["a"], ["b"]]
    formats_by_path = {
        tuple(column["path_in_schema"]): column["native_arrow_format"]
        for column in row_group["columns"]
    }
    assert formats_by_path["a",] == "l"
    assert formats_by_path["b",] == "u"
    assert formats_by_path["ingestion_timestamp",] == "tsu:"
    assert all((column["codec"] == 0 for column in row_group["columns"]))
    assert all((column["total_compressed_size"] > 0 for column in row_group["columns"]))
    assert all((column["data_page_offset"] >= 4 for column in row_group["columns"]))
    for column in row_group["columns"]:
        data_page_index = next(
            (index for index, page in enumerate(column["pages"]) if page["is_dictionary_page"] == 0)
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
                "compressed_page_size": data_page["header_size"]
                + data_page["compressed_page_size"],
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
def test_native_parquet_stream_materializes_simple_integer_lists(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route

    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(pa.table({"scores": pa.array([[1, 2], [3]], type=pa.list_(pa.int64()))}), src)
    ss.to_parquet(src, out, input_format="parquet", parquet_compression="uncompressed")
    info = native_parquet_footer_info(out)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    factory = open_parquet_record_batch_stream_factory(out, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().select(["scores"]).to_pylist() == [{"scores": [1, 2]}, {"scores": [3]}]
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_lists_across_pages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "native-list-multi-page.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [list(range(row * 128, (row + 1) * 128)) for row in range(120)],
                type=pa.list_(pa.int64()),
            )
        }
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
        memory_limit_bytes=1024 * 1024,
    )
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    column = info["row_groups"][0]["columns"][0]
    data_pages = [page for page in column["pages"] if page["is_dictionary_page"] == 0]
    assert len(data_pages) > 1
    assert column["repeated_level_layouts"][0]["offsets"][:4] == [0, 128, 256, 384]
    assert column["repeated_level_layouts"][0]["offsets"][-4:] == [14976, 15104, 15232, 15360]
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_footer_info_captures_repeated_level_values(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
    assert column["path_in_schema"] == ["scores", "list", "item"]
    assert column["max_definition_level"] == 3
    assert column["max_repetition_level"] == 1
    assert column["repeated_level_layouts"] == [
        {
            "layout_index": 0,
            "decoded": 1,
            "row_count": 4,
            "null_count": 1,
            "element_count": 3,
            "non_null_value_count": 3,
            "offsets": [0, 2, 2, 2, 3],
            "validity_hex_preview": "0d",
        }
    ]
    assert column["repeated_level_layouts"][0]["decoded"] == 1
    assert column["repeated_level_layouts"][0]["row_count"] == 4
    assert column["repeated_level_layouts"][0]["null_count"] == 1
    assert column["repeated_level_layouts"][0]["element_count"] == 3
    assert column["repeated_level_layouts"][0]["non_null_value_count"] == 3
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"
    page = column["pages"][0]
    assert page["decoded_definition_level_values"] == [3, 3, 0, 1, 3]
    assert page["decoded_repetition_level_values"] == [0, 1, 0, 0, 0]
    assert page["decoded_value_preview"] == ["1", "2", "3"]
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_string_lists(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
@pytest.mark.parametrize("name", ["string", "binary"])
def test_native_parquet_stream_materializes_plain_byte_array_lists(
    tmp_path: Path, name: str
) -> None:
    path = tmp_path / f"native-plain-byte-array-{name}-list.parquet"
    if name == "string":
        array = pa.array([["only"]], type=pa.list_(pa.string()))
    else:
        array = pa.array([[b"only"]], type=pa.list_(pa.binary()))
    table = pa.table({"tags": array})
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "plain_byte_array"
    assert column["native_read_arrow_n_buffers"] == 3
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 1]
    assert column["pages"][0]["value_encoding"] == 0


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_float_lists(tmp_path: Path) -> None:
    path = tmp_path / "native-float-list.parquet"
    table = pa.table(
        {"values": pa.array([[1.25, 2.5], None, [], [3.75]], type=pa.list_(pa.float64()))}
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "byte_stream_split"
    assert column["native_read_value_width_bytes"] == 8
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"


@_requires_pyarrow
def test_native_parquet_stream_materializes_simple_boolean_lists(tmp_path: Path) -> None:
    path = tmp_path / "native-boolean-list.parquet"
    table = pa.table(
        {"flags": pa.array([[True, False], None, [], [True]], type=pa.list_(pa.bool_()))}
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "bit_packed_boolean"
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"


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
    tmp_path: Path, name: str, native_format: str, expected_offsets: list[int]
) -> None:
    path = tmp_path / f"native-logical-{name}-list.parquet"
    if name == "date32":
        array = pa.array(
            [[dt.date(2024, 1, 1)], None, [], [dt.date(2024, 1, 2)]], type=pa.list_(pa.date32())
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
            [[Decimal("12.34")], None, [], [Decimal("-0.10")]], type=pa.list_(pa.decimal128(10, 2))
        )
    elif name == "fixed_size_binary":
        array = pa.array([[b"abcd"], None, [], [b"wxyz"]], type=pa.list_(pa.binary(4)))
    else:
        array = pa.array([[1, 2**63], None, [], [2**64 - 1]], type=pa.list_(pa.uint64()))
    table = pa.table({"items": array})
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_arrow_format"] == native_format
    assert column["native_read_value_buffer_kind"] == "fixed_width"
    assert column["repeated_level_layouts"][0]["offsets"] == expected_offsets
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"


@_requires_pyarrow
@pytest.mark.parametrize("name", ["int", "string", "bool"])
def test_native_parquet_stream_materializes_nullable_list_elements(
    tmp_path: Path, name: str
) -> None:
    path = tmp_path / f"native-nullable-{name}-list.parquet"
    if name == "int":
        array = pa.array([[1, None, 2], None, [], [None, 3]], type=pa.list_(pa.int64()))
    elif name == "string":
        array = pa.array([["a", None, "b"], None, [], [None, "c"]], type=pa.list_(pa.string()))
    else:
        array = pa.array([[True, None, False], None, [], [None, True]], type=pa.list_(pa.bool_()))
    table = pa.table({"items": array})
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    column = info["row_groups"][0]["columns"][0]
    assert column["max_definition_level"] == 3
    assert column["native_read_total_nulls"] == 2
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 5]
    assert column["pages"][0]["decoded_definition_level_values"] == [3, 2, 3, 0, 1, 2, 3]


@_requires_pyarrow
def test_native_parquet_footer_info_plans_byte_stream_split_float_lists(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

    path = tmp_path / "pyarrow-byte-stream-split-list.parquet"
    table = pa.table(
        {"values": pa.array([[1.25, 2.5], None, [], [3.75]], type=pa.list_(pa.float64()))}
    )
    pq.write_table(
        table, path, compression="NONE", use_dictionary=False, use_byte_stream_split=True
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
def test_native_parquet_stream_materializes_dictionary_string_lists(tmp_path: Path) -> None:
    path = tmp_path / "native-dict-string-list.parquet"
    table = pa.table(
        {"tags": pa.array([["same", "same"], None, [], ["same"]] * 200, type=pa.list_(pa.string()))}
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "dictionary_byte_array"
    assert column["repeated_level_layouts"][0]["offsets"][:5] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"].startswith("dd")


@_requires_pyarrow
def test_native_parquet_stream_materializes_dictionary_integer_lists(tmp_path: Path) -> None:
    path = tmp_path / "native-dict-integer-list.parquet"
    table = pa.table({"nums": pa.array([[7, 7], None, [], [7]] * 200, type=pa.list_(pa.int64()))})
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["native_read_value_buffer_kind"] == "dictionary_fixed_width"
    assert column["native_read_value_width_bytes"] == 8
    assert column["repeated_level_layouts"][0]["offsets"][:5] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"].startswith("dd")


@_requires_pyarrow
def test_native_parquet_stream_materializes_fixed_size_binary(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "fixed-size-binary.parquet"
    plain_values = [
        None if index % 10 == 0 else index.to_bytes(4, "little") for index in range(600)
    ]
    table = pa.table(
        {
            "plain_token": pa.array(plain_values, type=pa.binary(4)),
            "dict_token": pa.array(
                [b"same", b"same", b"same", None, b"same"] * 120, type=pa.binary(4)
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
    assert columns["plain_token",]["physical_type"] == 7
    assert columns["plain_token",]["fixed_type_length"] == 4
    assert columns["plain_token",]["native_arrow_format"] == "w:4"
    assert columns["plain_token",]["native_read_value_buffer_kind"] == "fixed_width"
    assert columns["plain_token",]["native_read_value_width_bytes"] == 4
    assert columns["plain_token",]["native_read_arrow_n_buffers"] == 2
    assert columns["dict_token",]["physical_type"] == 7
    assert columns["dict_token",]["fixed_type_length"] == 4
    assert columns["dict_token",]["native_arrow_format"] == "w:4"
    assert columns["dict_token",]["native_read_value_buffer_kind"] == "dictionary_fixed_width"
    assert columns["dict_token",]["native_read_value_width_bytes"] == 4
    assert columns["dict_token",]["native_read_arrow_n_buffers"] == 2
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_preserves_required_scalar_nullability(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
                [Decimal("1.23"), Decimal("4.56"), Decimal("7.89")], type=pa.decimal128(10, 2)
            ),
            pa.array(
                [dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(2026, 1, 3)], type=pa.date32()
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
    assert all((column["max_definition_level"] == 0 for column in row_group["columns"]))
    assert all((column["native_read_total_nulls"] == 0 for column in row_group["columns"]))
    assert all((column["native_read_arrow_null_count"] == 0 for column in row_group["columns"]))
    assert all((column["native_read_has_validity_buffer"] == 0 for column in row_group["columns"]))
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_writer_rejects_null_in_required_field(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
def test_native_parquet_stream_reads_empty_file_schema(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "empty.parquet"
    table = pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())})
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
def test_native_parquet_stream_reads_empty_struct_file_schema(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "empty-struct.parquet"
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct(
                    [pa.field("name", pa.string()), pa.field("score", pa.float64(), nullable=False)]
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
def test_pyarrow_empty_row_group_parquet_falls_back_cleanly(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    path = tmp_path / "empty-row-group.parquet"
    table = pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())})
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
    assert all((not column["pages"] for column in info["row_groups"][0]["columns"]))
    assert info["native_reader_ready"] == 0
    assert any(("file was not written" in blocker for blocker in info["native_reader_blockers"]))
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
    assert any(("file was not written" in blocker for blocker in diagnostics["blockers"]))


@_requires_pyarrow
def test_native_parquet_stream_reads_empty_supported_list_file_schema(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "empty-list.parquet"
    table = pa.Table.from_pylist(
        [],
        schema=pa.schema(
            [
                pa.field("scores", pa.list_(pa.int64())),
                pa.field(
                    "items",
                    pa.list_(
                        pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
                    ),
                ),
                pa.field("nested_scores", pa.list_(pa.list_(pa.int64()))),
                pa.field("deep_scores", pa.list_(pa.list_(pa.list_(pa.int64())))),
                pa.field("very_deep_scores", pa.list_(pa.list_(pa.list_(pa.list_(pa.int64()))))),
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
def test_parquet_path_auto(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)
    result = read_test_parquet(path)
    assert result.clean_data.num_rows == 3
    assert result.clean_data.schema.names == ["a", "b"]


@_requires_pyarrow
def test_parquet_path_with_temporal_values(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

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
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array(
                    [dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC)],
                    type=pa.timestamp("us", tz="UTC"),
                )
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
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)
    result = read_test_parquet(path)
    assert result.clean_data.to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any(("file was not written" in blocker for blocker in diagnostics["blockers"]))


@_requires_pyarrow
def test_native_snappy_parquet_roundtrip_uses_native_reader(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    source = tmp_path / "native-snappy.jsonl"
    source.write_text(
        '{"id":1,"name":"alpha"}\n{"id":2,"name":"alpha"}\n{"id":3,"name":"beta"}\n',
        encoding="utf-8",
    )
    path = tmp_path / "native-snappy.parquet"
    ss.to_parquet(source, path, input_format="jsonl", parquet_compression="snappy")
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    assert {column["codec"] for column in info["row_groups"][0]["columns"]} == {1}
    for column in info["row_groups"][0]["columns"]:
        for page in column["pages"]:
            assert page["payload_verified"] == 1
            assert page["values_decoded"] == 1
    result = read_test_parquet(path)
    assert result.clean_data.select(["id", "name"]).to_pylist() == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "alpha"},
        {"id": 3, "name": "beta"},
    ]
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"


@_requires_pyarrow
def test_native_parquet_reader_logs_not_ready_fallback(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    path = tmp_path / "pyarrow.parquet"
    pq.write_table(sample_table(pa), path)
    caplog.set_level(logging.DEBUG, logger="schema_sanitizer.adapters.parquet.record_batch_factory")
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any(("file was not written" in blocker for blocker in diagnostics["blockers"]))
    assert "Native Parquet reader skipped; retrying input with PyArrow" in caplog.text


@_requires_pyarrow
def test_parquet_file_like_records_non_native_source_diagnostics() -> None:
    from io import BytesIO

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    data = BytesIO()
    pq.write_table(sample_table(pa), data)
    data.seek(0)
    factory = open_parquet_record_batch_stream_factory(data, source="stream", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_parquetfile_iter_batches"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is False
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "source_not_path"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_parquet_buffer_stages_then_falls_back_for_external_writer() -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    sink = pa.BufferOutputStream()
    pq.write_table(sample_table(pa), sink)
    data = sink.getvalue().to_pybytes()
    factory = open_parquet_record_batch_stream_factory(data, source="text", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert diagnostics["native_source_kind"] == "staged_text"
    assert diagnostics["blockers"]


@_requires_pyarrow
def test_parquet_buffer_projection_materializes_requested_columns() -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route

    table = pa.table({"a": [1, 2], "b": ["x", "y"], "c": [True, False]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    factory = open_parquet_record_batch_stream_factory(
        sink.getvalue().to_pybytes(), source="text", feature="test", columns=["b"]
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == [{"b": "x"}, {"b": "y"}]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"


@_requires_pyarrow
def test_native_parquet_stream_materializes_plain_fixed_width_rows(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
def test_native_parquet_stream_materializes_staged_buffer(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "native-buffer.parquet"
    table = pa.table({"a": [10, 20, None], "b": ["x", "y", None]})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    factory = open_parquet_record_batch_stream_factory(
        path.read_bytes(), source="text", feature="test"
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["native_source_kind"] == "staged_text"


@_requires_pyarrow
def test_native_parquet_stream_materializes_file_backed_stream(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "native-stream.parquet"
    table = pa.table({"a": [1, 2, 3]})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    with path.open("rb") as handle:
        factory = open_parquet_record_batch_stream_factory(handle, source="stream", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["native_source_kind"] == "stream_path"


@_requires_pyarrow
def test_native_parquet_stream_respects_small_batch_size_with_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
        path, source="path", feature="test", batch_size=2
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
    assert any(("requested batch_size is 2" in blocker for blocker in diagnostics["blockers"]))


@_requires_pyarrow
def test_native_parquet_stream_projection_uses_native_route(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
        path, source="path", feature="test", columns=["b"]
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
def test_parquet_filter_uses_dataset_scanner_instead_of_native_route(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
        path, source="path", feature="test", columns=["b"], filters=ds.field("a") > 1
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
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )

    ds = pytest.importorskip("pyarrow.dataset")
    sink = pa.BufferOutputStream()
    pq.write_table(sample_table(pa), sink)
    factory = open_parquet_record_batch_stream_factory(
        sink.getvalue().to_pybytes(), source="text", feature="test", filters=ds.field("a") > 1
    )
    try:
        reader = pa.RecordBatchReader.from_stream(factory)
        assert reader.read_all().to_pylist() == [{"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
    finally:
        factory.close()


@_requires_pyarrow
def test_native_parquet_stream_materializes_plain_boolean_rows(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
def test_native_parquet_stream_materializes_rle_dictionary_strings(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
    assert (
        info["row_groups"][0]["columns"][0]["native_read_value_buffer_kind"]
        == "dictionary_byte_array"
    )
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_rle_dictionary_fixed_width(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route

    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    values = [7] * 500
    table = pa.table({"n": pa.array(values, type=pa.int64())})
    pq.write_table(table, src)
    ss.to_parquet(src, out, input_format="parquet", parquet_compression="uncompressed")
    info = native_parquet_footer_info(out)
    assert info is not None
    column = next(
        (column for column in info["row_groups"][0]["columns"] if column["path_in_schema"] == ["n"])
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
def test_native_parquet_stream_materializes_integer_logical_widths(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "integer-logical-widths.parquet"
    table = pa.table(
        {
            "i8": pa.array([-5, 7, None, -1], type=pa.int8()),
            "u8": pa.array([250, 1, None, 2], type=pa.uint8()),
            "i16": pa.array([-300, 12, None, -2], type=pa.int16()),
            "u16": pa.array([65000, 2, None, 3], type=pa.uint16()),
            "u32": pa.array([4000000000, 7, None, 8], type=pa.uint32()),
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
    assert widths == {("i8",): 1, ("u8",): 1, ("i16",): 2, ("u16",): 2, ("u32",): 4, ("u64",): 8}
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_decimal_fixed_bytes(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "decimal-fixed-bytes.parquet"
    table = pa.table(
        {
            "plain_amount": pa.array(
                [Decimal("123.45"), Decimal("-0.10"), None, Decimal("999.99")],
                type=pa.decimal128(10, 2),
            ),
            "dict_amount": pa.array(
                [Decimal("1.00"), Decimal("2.00"), Decimal("1.00"), None], type=pa.decimal128(10, 2)
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
    assert columns["plain_amount",]["native_arrow_format"] == "d:10,2,128"
    assert columns["plain_amount",]["native_read_value_buffer_kind"] == "fixed_width"
    assert columns["plain_amount",]["native_read_value_width_bytes"] == 16
    assert columns["dict_amount",]["native_arrow_format"] == "d:10,2,128"
    assert columns["dict_amount",]["native_read_value_buffer_kind"] == "dictionary_fixed_width"
    assert columns["dict_amount",]["native_read_value_width_bytes"] == 16
    assert columns["big_amount",]["native_arrow_format"] == "d:40,4,256"
    assert columns["big_amount",]["native_read_value_buffer_kind"] == "fixed_width"
    assert columns["big_amount",]["native_read_value_width_bytes"] == 32
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_projects_empty_file_schema(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
        memory_limit_bytes=1024 * 1024,
    )
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    factory = open_parquet_record_batch_stream_factory(
        path, source="path", feature="test", columns=["scores", "id"]
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
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
        path, source="path", feature="test", columns=["id"]
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
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
def test_native_parquet_stream_reads_multiple_row_groups(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "multi-row-group.parquet"
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    batches = [
        pa.record_batch([pa.array([1, 2], type=pa.int64()), pa.array(["x", "y"])], schema=schema),
        pa.record_batch([pa.array([3], type=pa.int64()), pa.array(["z"])], schema=schema),
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
def test_native_parquet_stream_reads_list_columns_across_row_groups(tmp_path: Path) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "multi-row-group-list.parquet"
    schema = pa.schema([pa.field("items", pa.list_(pa.int64()))])
    batches = [
        pa.record_batch([pa.array([[1, 2], None], type=pa.list_(pa.int64()))], schema=schema),
        pa.record_batch([pa.array([[], [3, 4, 5]], type=pa.list_(pa.int64()))], schema=schema),
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
        row_group["columns"][0]["repeated_level_layouts"][0]["offsets"]
        for row_group in info["row_groups"]
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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "multi-page-null-spans.parquet"
    rows = 400
    table = pa.table(
        {
            "a": pa.array(
                [None if row % 7 == 0 else row * 1000003 for row in range(rows)], type=pa.int64()
            ),
            "b": pa.array(
                [None if row % 5 == 0 else f"value-{row:03d}-" + "x" * 512 for row in range(rows)],
                type=pa.string(),
            ),
        }
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
        memory_limit_bytes=1024 * 1024,
    )
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    row_group = info["row_groups"][0]
    assert row_group["num_rows"] == rows
    page_counts: list[int] = []
    for column in row_group["columns"]:
        data_pages = [page for page in column["pages"] if page["is_dictionary_page"] == 0]
        page_counts.append(len(data_pages))
        assert data_pages
        assert column["native_read_data_page_count"] == len(data_pages)
        assert len(column["native_read_page_spans"]) == len(data_pages)
        assert sum((span["row_count"] for span in column["native_read_page_spans"])) == rows
        assert sum((span["null_count"] for span in column["native_read_page_spans"])) > 0
        assert [span["first_row_index"] for span in column["native_read_page_spans"]] == [
            location["first_row_index"] for location in column["offset_index_locations"]
        ]
    assert max(page_counts) > 1
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


def _native_scalar_cases():
    excluded = {
        "test_native_parquet_stream_materializes_plain_byte_array_lists",
        "test_native_parquet_stream_materializes_logical_fixed_width_lists",
        "test_native_parquet_stream_materializes_nullable_list_elements",
    }
    cases = [
        (name.removeprefix("test_"), value)
        for name, value in globals().items()
        if name.startswith("test_") and name not in excluded and inspect.isfunction(value)
    ]
    plain = test_native_parquet_stream_materializes_plain_byte_array_lists
    cases.extend(
        (
            f"native_parquet_stream_materializes_plain_byte_array_lists[{name}]",
            partial(plain, name=name),
        )
        for name in ("string", "binary")
    )
    logical = test_native_parquet_stream_materializes_logical_fixed_width_lists
    logical_cases = (
        ("date32", "tdD", [0, 1, 1, 1, 2]),
        ("timestamp_us", "tsu:", [0, 1, 1, 1, 2]),
        ("decimal128", "d:10,2,128", [0, 1, 1, 1, 2]),
        ("fixed_size_binary", "w:4", [0, 1, 1, 1, 2]),
        ("uint64", "L", [0, 2, 2, 2, 3]),
    )
    cases.extend(
        (
            f"native_parquet_stream_materializes_logical_fixed_width_lists[{name}]",
            partial(
                logical,
                name=name,
                native_format=native_format,
                expected_offsets=expected_offsets,
            ),
        )
        for name, native_format, expected_offsets in logical_cases
    )
    nullable = test_native_parquet_stream_materializes_nullable_list_elements
    cases.extend(
        (
            f"native_parquet_stream_materializes_nullable_list_elements[{name}]",
            partial(nullable, name=name),
        )
        for name in ("int", "string", "bool")
    )
    return tuple(cases)


NATIVE_SCALAR_CASES = _native_scalar_cases()
