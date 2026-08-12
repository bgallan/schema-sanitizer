"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_runtime import pa
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_inner_list(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with scalar list children."""
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
        ["items", "list", "item", "ids", "list", "item"],
        ["items", "list", "item", "name"],
    ]
    assert columns[0]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 3]
    assert columns[0]["repeated_level_layouts"][1]["offsets"] == [0, 2, 2, 2]
    assert columns[1]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 3]

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
        ["items", "list", "item", "ids", "list", "item", "list", "item"],
        ["items", "list", "item", "name"],
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
        ["items", "list", "item", "inner", "score"],
        ["items", "list", "item", "inner", "label"],
        ["items", "list", "item", "name"],
    ]
    assert columns[0]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert columns[1]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert columns[2]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]

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
        ["items", "list", "item", "inner", "ids", "list", "item"],
        ["items", "list", "item", "inner", "label"],
        ["items", "list", "item", "name"],
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
            "item",
            "inner",
            "ids",
            "list",
            "item",
            "list",
            "item",
        ],
        ["items", "list", "item", "inner", "label"],
        ["items", "list", "item", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    out.validate(full=True)
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
