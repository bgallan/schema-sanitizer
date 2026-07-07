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
def test_read_parquet_path_materializes_table(tmp_path: Path) -> None:
    """Verify read parquet path materializes table."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
        last_parquet_stream_factory_route,
    )

    require_native()

    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == _sample_table().to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"


@_requires_pyarrow
def test_native_parquet_stream_materializes_plain_fixed_width_rows(
    tmp_path: Path,
) -> None:
    """Verify the native Parquet stream materializes supported fixed-width pages."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import (
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
            8 if expected_value_kind in {"fixed_width", "delta_binary_packed"} else 0
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
        if expected_value_kind in {"rle_dictionary_indices", "dictionary_byte_array"}:
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
def test_native_parquet_footer_info_blocks_nested_native_reader_readiness(
    tmp_path: Path,
) -> None:
    """Verify native reader readiness stays conservative for nested files."""
    from schema_sanitizer.adapters.pyarrow_parquet_direct import native_parquet_footer_info

    require_native()
    src = tmp_path / "source.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(
        pa.table(
            {
                "profile": pa.array(
                    [{"name": "a"}, {"name": "b"}],
                    type=pa.struct([pa.field("name", pa.string())]),
                )
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
    assert info["native_reader_ready"] == 0
    assert any(
        "nested path is not materializable yet" in blocker
        for blocker in info["native_reader_blockers"]
    )


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
    require_native()

    path = tmp_path / "data.parquet"
    pq.write_table(_sample_table(), path)

    result = read_test_parquet(path.as_uri())

    assert result.clean_data.to_pylist() == _sample_table().to_pylist()


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
