"""Parquet API/runtime tests split by contract area."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native
from parquet_runtime_shared import pa
from parquet_runtime_shared import requires_pyarrow as _requires_pyarrow

# Split from test_parquet_native_scalar_runtime.py: test_native_parquet_stream_projects_empty_file_schema, test_native_parquet_stream_projects_empty_file_past_unprojected_complex_repeated, test_native_parquet_footer_info_accepts_empty_list_struct_list_chain_readiness, ...


@_requires_pyarrow
def test_native_parquet_stream_projects_empty_file_schema(
    tmp_path: Path,
) -> None:
    """Verify native empty-file reads honor projected column order."""
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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native Parquet stream materializes split pages and null spans."""
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
    path = tmp_path / "multi-page-null-spans.parquet"
    rows = 400
    table = pa.table(
        {
            "a": pa.array(
                [None if row % 7 == 0 else row * 1000003 for row in range(rows)],
                type=pa.int64(),
            ),
            "b": pa.array(
                [
                    None if row % 5 == 0 else f"value-{row:03d}-" + ("x" * 512)
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
        assert sum(span["row_count"] for span in column["native_read_page_spans"]) == rows
        assert sum(span["null_count"] for span in column["native_read_page_spans"]) > 0
        assert [span["first_row_index"] for span in column["native_read_page_spans"]] == [
            location["first_row_index"] for location in column["offset_index_locations"]
        ]
    assert max(page_counts) > 1

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
