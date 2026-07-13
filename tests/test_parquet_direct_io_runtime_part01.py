"""Parquet API/runtime tests split by contract area."""

from __future__ import annotations

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

# Split from test_parquet_direct_io_runtime.py: test_list_list_projection_uses_native_reader, test_native_parquet_stream_materializes_required_struct_scalar_leaves, test_native_parquet_stream_materializes_nullable_struct_scalar_leaves, ...


@_requires_pyarrow
def test_list_list_projection_uses_native_reader(
    tmp_path: Path,
) -> None:
    """Verify projected nested scalar lists use the native reader."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

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
def test_native_parquet_stream_projection_skips_unprojected_page_planning(
    tmp_path: Path,
) -> None:
    """Verify projected native reads do not inspect unprojected page headers."""
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
    path = tmp_path / "projected-skip-unprojected-page-planning.parquet"
    table = pa.table(
        {
            "keep": pa.array([1, 2, 3], type=pa.int64()),
            "drop": pa.array(["alpha", "beta", "gamma"], type=pa.string()),
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
    drop_column = next(
        column
        for column in info["row_groups"][0]["columns"]
        if column["path_in_schema"] == ["drop"]
    )
    drop_page_header_offset = drop_column["pages"][0]["header_offset"]

    with path.open("r+b") as handle:
        handle.seek(drop_page_header_offset)
        handle.write(b"\xff")

    with pytest.raises(RuntimeError):
        native_parquet_footer_info(path)

    projected_info = native_parquet_footer_info(path, columns=["keep"])
    assert projected_info is not None
    assert projected_info["native_reader_ready"] == 1
    assert projected_info["native_reader_blockers"] == []
    assert [column["path_in_schema"] for column in projected_info["row_groups"][0]["columns"]] == [
        ["keep"]
    ]

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["keep"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.select(["keep"]).schema)
    assert out.to_pylist() == [{"keep": 1}, {"keep": 2}, {"keep": 3}]
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
