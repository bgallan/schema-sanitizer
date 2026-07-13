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


# Split from test_parquet_native_recursive_shapes.py: test_native_parquet_stream_materializes_deep_recursive_mixed_shapes, test_native_parquet_stream_materializes_required_and_optional_root_structs, test_native_parquet_stream_materializes_recursive_sibling_repeated_branches


@_requires_pyarrow
def test_native_parquet_stream_materializes_deep_recursive_mixed_shapes(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes deeper generated recursive map/list shapes."""
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
    map_type = pa.map_(pa.string(), pa.int64())
    map_list_map_type = pa.map_(pa.string(), pa.list_(map_type))
    inner_struct = pa.struct(
        [
            pa.field("m", map_type),
            pa.field("v", pa.int64()),
        ]
    )
    nested_map_struct = pa.struct(
        [
            pa.field("m", map_list_map_type),
            pa.field("v", pa.int64()),
        ]
    )
    list_list_struct_map = pa.list_(pa.list_(inner_struct))
    map_struct_list_list_struct_map_type = pa.map_(
        pa.string(), pa.struct([pa.field("a", list_list_struct_map)])
    )
    map_list_map_struct_type = pa.map_(
        pa.string(),
        pa.list_(
            pa.map_(
                pa.string(),
                pa.struct([pa.field("a", list_list_struct_map)]),
            )
        ),
    )
    list_list_struct_map_list_map = pa.list_(pa.list_(nested_map_struct))
    map_list_list_struct_map = pa.map_(pa.string(), list_list_struct_map)
    deep_list_map_struct = pa.struct(
        [
            pa.field(
                "payload",
                pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct(
                            [
                                pa.field(
                                    "scores",
                                    pa.list_(pa.map_(pa.string(), pa.int64())),
                                )
                            ]
                        ),
                    )
                ),
            ),
            pa.field("flag", pa.int64()),
        ]
    )
    list_map_list_list_deep_struct = pa.list_(
        pa.map_(pa.string(), pa.list_(pa.list_(deep_list_map_struct)))
    )
    cases = [
        (
            "struct-list-list-struct-map",
            pa.struct([pa.field("a", list_list_struct_map)]),
            [
                {"a": [[{"m": {"x": 1}, "v": 2}, None], []]},
                None,
                {"a": None},
            ],
        ),
        (
            "list-struct-list-list-struct-map",
            pa.list_(pa.struct([pa.field("a", list_list_struct_map)])),
            [
                [{"a": [[{"m": {"x": 1}, "v": 2}, None], []]}],
                None,
                [],
                [{"a": None}],
            ],
        ),
        (
            "map-struct-list-list-struct-map",
            map_struct_list_list_struct_map_type,
            [
                {"root": {"a": [[{"m": {"x": 1}, "v": 2}]]}},
                None,
                {"z": None},
            ],
        ),
        (
            "list-map-struct-list-list-struct-map",
            pa.list_(map_struct_list_list_struct_map_type),
            [
                [{"root": {"a": [[{"m": {"x": 1}, "v": 2}]]}}],
                None,
                [],
                [{"z": None}],
            ],
        ),
        (
            "list-list-struct-map-list-map",
            list_list_struct_map_list_map,
            [
                [[{"m": {"a": [{"x": 1}, None], "b": []}, "v": 2}]],
                None,
                [[]],
            ],
        ),
        (
            "struct-map-list-list-struct-map",
            pa.struct([pa.field("k", map_list_list_struct_map)]),
            [
                {"k": {"root": [[{"m": {"x": 1}, "v": 2}]]}},
                None,
                {"k": None},
            ],
        ),
        (
            "struct-map-list-map-struct-list-list-struct-map",
            pa.struct([pa.field("k", map_list_map_struct_type)]),
            [
                {
                    "k": {
                        "root": [
                            {
                                "nested": {
                                    "a": [[{"m": {"x": 1}, "v": 2}], []],
                                }
                            },
                            {},
                        ]
                    }
                },
                None,
                {"k": None},
            ],
        ),
        (
            "list-map-list-list-struct-list-map-struct-list-map",
            list_map_list_list_deep_struct,
            [
                [
                    {
                        "outer": [
                            [
                                {
                                    "payload": [
                                        {
                                            "p": {
                                                "scores": [{"s": 1}, {}],
                                            }
                                        }
                                    ],
                                    "flag": 7,
                                },
                                None,
                            ],
                            [],
                        ]
                    }
                ],
                None,
                [],
                [{"empty": None}],
            ],
        ),
    ]

    deepest_reported_repeated_layout_count = 0
    for name, item_type, values in cases:
        path = tmp_path / f"native-{name}.parquet"
        table = pa.table({"items": pa.array(values, type=item_type)})
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
        for column in info["row_groups"][0]["columns"]:
            max_repetition_level = column["max_repetition_level"]
            if max_repetition_level <= 0:
                continue
            layouts = column["repeated_level_layouts"]
            deepest_reported_repeated_layout_count = max(
                deepest_reported_repeated_layout_count, len(layouts)
            )
            assert len(layouts) == max_repetition_level, name
            assert [layout["layout_index"] for layout in layouts] == list(
                range(max_repetition_level)
            ), name
            assert all(layout["decoded"] == 1 for layout in layouts), name
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()

        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream"
    assert deepest_reported_repeated_layout_count > 3


@_requires_pyarrow
def test_native_parquet_stream_materializes_required_and_optional_root_structs(
    tmp_path: Path,
) -> None:
    """Verify root structs use the generic recursive struct materializer."""
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
    root_struct = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("labels", pa.list_(pa.string())),
            pa.field("meta", pa.struct([pa.field("score", pa.float64())])),
        ]
    )
    schema = pa.schema(
        [
            pa.field("required_root", root_struct, nullable=False),
            pa.field("optional_root", root_struct),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "required_root": {
                    "id": 1,
                    "labels": ["a", None],
                    "meta": {"score": 1.5},
                },
                "optional_root": None,
            },
            {
                "required_root": {
                    "id": None,
                    "labels": [],
                    "meta": None,
                },
                "optional_root": {
                    "id": 2,
                    "labels": None,
                    "meta": {"score": None},
                },
            },
        ],
        schema=schema,
    )
    path = tmp_path / "native-required-optional-root-structs.parquet"
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
def test_native_parquet_stream_materializes_recursive_sibling_repeated_branches(
    tmp_path: Path,
) -> None:
    """Verify sibling repeated subtrees keep independent recursive layout cursors."""
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
    path = tmp_path / "native-recursive-sibling-repeated-branches.parquet"
    left_value_type = pa.list_(
        pa.map_(
            pa.string(),
            pa.list_(
                pa.struct(
                    [
                        pa.field(
                            "meta",
                            pa.struct(
                                [
                                    pa.field("codes", pa.list_(pa.int64())),
                                    pa.field("tag", pa.string()),
                                ]
                            ),
                        ),
                        pa.field("x", pa.int64()),
                        pa.field("ys", pa.list_(pa.int64())),
                    ]
                )
            ),
        )
    )
    right_value_type = pa.map_(pa.string(), pa.list_(pa.list_(pa.map_(pa.string(), pa.int64()))))
    item_type = pa.struct(
        [
            pa.field("left", left_value_type),
            pa.field("plain", pa.int64()),
            pa.field("right", right_value_type),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    {
                        "left": [
                            {
                                "a": [
                                    {
                                        "meta": {"codes": [4, None], "tag": "a"},
                                        "x": 1,
                                        "ys": [1, None],
                                    },
                                    None,
                                    {"meta": None, "x": None, "ys": []},
                                ],
                            },
                            {},
                        ],
                        "plain": 10,
                        "right": {"r": [[{"z": 3}, {}], []], "empty": None},
                    },
                    None,
                    {"left": None, "plain": None, "right": {}},
                    {
                        "left": [{"b": [{"meta": {"codes": [], "tag": None}, "x": 2, "ys": None}]}],
                        "plain": 20,
                        "right": {"s": None},
                    },
                ],
                type=item_type,
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
    assert (
        max(
            len(column["repeated_level_layouts"])
            for column in info["row_groups"][0]["columns"]
            if column["max_repetition_level"] > 0
        )
        > 3
    )
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
