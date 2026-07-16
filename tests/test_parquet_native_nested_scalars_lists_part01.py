"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native
from parquet_runtime_shared import pa, pq
from parquet_runtime_shared import requires_pyarrow as _requires_pyarrow

import schema_sanitizer as ss

# Split from test_parquet_native_nested_scalars_lists.py: test_native_parquet_footer_info_reads_schema_sanitizer_file, test_native_parquet_stream_materializes_simple_integer_lists, test_native_parquet_stream_materializes_simple_lists_across_pages, ...


def test_native_parquet_footer_info_reads_schema_sanitizer_file(tmp_path: Path) -> None:
    """Verify native footer parsing understands schema-sanitizer Parquet output."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

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
    assert column["repeated_level_layouts"][0]["offsets"][-4:] == [
        14_976,
        15_104,
        15_232,
        15_360,
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_footer_info_captures_repeated_level_values(
    tmp_path: Path,
) -> None:
    """Verify repeated columns expose level streams needed for list offsets."""
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
def test_native_parquet_stream_materializes_simple_string_lists(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes simple top-level string lists."""
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
