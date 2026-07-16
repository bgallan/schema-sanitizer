"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native
from parquet_runtime_shared import pa
from parquet_runtime_shared import requires_pyarrow as _requires_pyarrow

# Split from test_parquet_native_nested_map_combinators.py: test_native_parquet_stream_materializes_map_with_list_values, test_native_parquet_stream_materializes_map_with_list_chain_values, test_native_parquet_stream_materializes_map_with_struct_values, ...


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are scalar lists."""
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
    path = tmp_path / "native-map-list-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", [1, 2]), ("b", [])], None, [("c", None)]],
                type=pa.map_(pa.string(), pa.list_(pa.int64())),
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
        ["labels", "key_value", "value", "list", "item"],
    ]
    assert columns[0]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 3]
    assert columns[1]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 3]
    assert columns[1]["repeated_level_layouts"][1]["offsets"] == [0, 2, 2, 2]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are nested lists."""
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
    path = tmp_path / "native-map-list-chain-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", [[1, 2], []])], None, [("b", None), ("c", [[None, 3]])]],
                type=pa.map_(pa.string(), pa.list_(pa.list_(pa.int64()))),
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
        ["labels", "key_value", "value", "list", "item", "list", "item"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are structs."""
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
    path = tmp_path / "native-map-struct-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [
                    [("a", {"score": 1, "name": "x"}), ("b", {"score": 2, "name": None})],
                    None,
                    [("c", None), ("d", {"score": None, "name": "z"})],
                ],
                type=pa.map_(
                    pa.string(),
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
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
        ["labels", "key_value", "value", "score"],
        ["labels", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_nested_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader recursively materializes nested map value structs."""
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
    path = tmp_path / "native-map-nested-struct-values.parquet"
    inner_type = pa.struct(
        [
            pa.field("score", pa.int64()),
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
                    {"a": {"inner": {"score": 1, "label": "x"}, "kind": "ok"}, "b": None},
                    None,
                    {},
                    {"c": {"inner": None, "kind": "empty"}},
                    {"d": {"inner": {"score": None, "label": None}, "kind": None}},
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
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["attrs", "key_value", "key"],
        ["attrs", "key_value", "value", "inner", "score"],
        ["attrs", "key_value", "value", "inner", "label"],
        ["attrs", "key_value", "value", "kind"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_nested_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values with nested struct lists."""
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
    path = tmp_path / "native-map-nested-struct-list-values.parquet"
    inner_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
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
                    {"a": {"inner": {"ids": [1, 2], "label": "x"}, "kind": "ok"}},
                    None,
                    {"b": {"inner": {"ids": [], "label": None}, "kind": None}},
                    {"c": {"inner": None, "kind": "z"}},
                    {"d": {"inner": {"ids": None, "label": "q"}, "kind": "r"}},
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
