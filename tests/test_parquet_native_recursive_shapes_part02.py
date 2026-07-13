"""Native Parquet recursive nested grammar runtime tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native

try:
    import pyarrow as pa

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")


# Split from test_parquet_native_recursive_shapes.py: test_native_parquet_stream_materializes_deeper_map_list_recursion, test_native_parquet_stream_materializes_map_list_list_struct_values, test_native_parquet_stream_materializes_map_list_list_map_values, ...


@_requires_pyarrow
def test_native_parquet_stream_materializes_deeper_map_list_recursion(
    tmp_path: Path,
) -> None:
    """Verify native reader handles deeper recursive map/list/struct combinations."""
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
    map_int_type = pa.map_(pa.string(), pa.int64())
    map_map_type = pa.map_(pa.string(), map_int_type)
    struct_map_type = pa.struct(
        [
            pa.field("m", map_map_type),
            pa.field("n", pa.int64()),
        ]
    )
    cases = [
        (
            "struct-map-map",
            pa.struct([pa.field("k", map_map_type)]),
            [
                {"k": [("a", [("x", 1)]), ("b", [])]},
                None,
                {"k": None},
            ],
        ),
        (
            "list-struct-map-map",
            pa.list_(pa.struct([pa.field("k", map_map_type)])),
            [
                [{"k": [("a", [("x", 1)])]}],
                None,
                [],
                [{"k": None}],
            ],
        ),
        (
            "map-list-map-map",
            pa.map_(pa.string(), pa.list_(map_map_type)),
            [
                [("a", [[("x", [("i", 1)])], []]), ("b", None)],
                None,
                [("c", [])],
            ],
        ),
        (
            "list-map-list-map-map",
            pa.list_(pa.map_(pa.string(), pa.list_(map_map_type))),
            [
                [[("a", [[("x", [("i", 1)])]]), ("b", [])]],
                None,
                [],
                [[("c", None)]],
            ],
        ),
        (
            "map-list-struct-map-map",
            pa.map_(pa.string(), pa.list_(struct_map_type)),
            [
                [
                    ("a", [{"m": [("x", [("i", 1)])], "n": 2}, None]),
                    ("b", []),
                ],
                None,
                [("c", None)],
            ],
        ),
        (
            "list-list-struct-map-map",
            pa.list_(pa.list_(struct_map_type)),
            [
                [[{"m": [("x", [("i", 1)])], "n": 2}, None]],
                None,
                [[]],
            ],
        ),
    ]

    for name, item_type, values in cases:
        path = tmp_path / f"native-{name}.parquet"
        try:
            items = pa.array(values, type=item_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise AssertionError(name) from exc
        table = pa.table({"items": items})
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

        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are list-list-structs."""
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
    path = tmp_path / "native-map-list-list-struct-values.parquet"
    struct_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    {"a": [[{"x": 1, "ys": [1]}, None], []]},
                    None,
                    {"b": None, "c": [[{"x": 2, "ys": []}]]},
                ],
                type=pa.map_(pa.string(), pa.list_(pa.list_(struct_type))),
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
def test_native_parquet_stream_materializes_map_list_list_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are list-list-maps."""
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
    path = tmp_path / "native-map-list-list-map-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    {"root": [[{"a": 1}], []]},
                    None,
                    {"empty": None, "other": [[{"b": None}]]},
                ],
                type=pa.map_(pa.string(), pa.list_(pa.list_(pa.map_(pa.string(), pa.int64())))),
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
def test_native_parquet_stream_materializes_list_map_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are list-list-structs."""
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
    path = tmp_path / "native-list-map-list-list-struct-values.parquet"
    struct_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("ys", pa.list_(pa.int64())),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"a": [[{"x": 1, "ys": [1]}, None]]}],
                    None,
                    [],
                    [{"b": []}],
                ],
                type=pa.list_(pa.map_(pa.string(), pa.list_(pa.list_(struct_type)))),
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
def test_native_parquet_stream_materializes_list_map_list_list_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are list-list-maps."""
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
    path = tmp_path / "native-list-map-list-list-map-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"root": [[{"a": 1}], []]}],
                    None,
                    [],
                    [{"other": [[{"b": None}]]}],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.list_(pa.list_(pa.map_(pa.string(), pa.int64()))),
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
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
