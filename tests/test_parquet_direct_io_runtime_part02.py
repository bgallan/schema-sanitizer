"""Parquet API/runtime tests split by contract area."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import read_test_parquet, require_native
from parquet_runtime_support import sample_table

import schema_sanitizer as ss

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")

# Split from test_parquet_direct_io_runtime.py: test_native_parquet_footer_info_decodes_plain_value_counts_with_nulls, test_native_parquet_footer_info_decodes_delta_binary_packed_int_values, test_native_parquet_footer_info_decodes_rle_dictionary_string_pages, ...


@_requires_pyarrow
def test_native_parquet_footer_info_decodes_plain_value_counts_with_nulls(
    tmp_path: Path,
) -> None:
    """Verify native PLAIN value validation counts only non-null values."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

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
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

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
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    result = read_test_parquet(path.as_uri())

    assert result.clean_data.to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_native_parquet_file_uri_uses_native_route(tmp_path: Path) -> None:
    """Verify local file URIs share the native path-backed Parquet reader."""
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()

    src = tmp_path / "src.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(sample_table(pa), src)
    ss.to_parquet(src, out, input_format="parquet", parquet_compression="uncompressed")

    result = read_test_parquet(out.as_uri())

    assert result.clean_data.select(["a", "b"]).to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"


@_requires_pyarrow
def test_remote_parquet_uri_requires_staging() -> None:
    """Verify remote Parquet URIs are not treated as local direct sources."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
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
    pq.write_table(sample_table(pa), path)

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
    pq.write_table(sample_table(pa), path)

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
    pq.write_table(sample_table(pa), path)

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == sample_table(pa).to_pylist()


@_requires_pyarrow
def test_parquet_directory_read_uses_direct_arrow_path(tmp_path: Path) -> None:
    """Verify Parquet directory reads bypass the JSONL bridge when schemas match."""
    from schema_sanitizer.adapters.parquet.telemetry import (
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
