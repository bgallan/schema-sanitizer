"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native
from parquet_runtime_shared import pa
from parquet_runtime_shared import requires_pyarrow as _requires_pyarrow

# Split from test_parquet_native_nested_map_combinators.py: test_native_parquet_stream_materializes_struct_nested_struct_list_child, test_native_parquet_stream_materializes_struct_nested_struct_list_chain_child, test_native_parquet_stream_materializes_map_nested_struct_list_chain_values, ...


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_nested_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes nested struct list children."""
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
def test_native_parquet_stream_materializes_map_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are lists of structs."""
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
def test_native_parquet_stream_materializes_map_with_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose struct values contain lists."""
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
        ["labels", "key_value", "value", "ids", "list", "item"],
        ["labels", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
