"""Collect named nested-Parquet round-trip cases for the parametrized native runner.

The cases exercise list, map, struct, null, and chained-child layouts through one shared runtime
harness.
"""

from __future__ import annotations

import inspect
from functools import partial
from pathlib import Path
from typing import Callable

import pytest

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct with map struct list chain child."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "native-list-struct-map-struct-list-chain-child.parquet"
    value_type = pa.struct(
        [pa.field("ids", pa.list_(pa.list_(pa.int64()))), pa.field("label", pa.string())]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": {"ids": [[1, 2], []], "label": "x"}, "b": None}, "name": "r"}],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {
                            "attrs": {
                                "c": {"ids": None, "label": "z"},
                                "d": {"ids": [[None, 3]], "label": None},
                            },
                            "name": "s",
                        },
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), value_type)),
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
        ["items", "list", "item", "attrs", "key_value", "key"],
        [
            "items",
            "list",
            "item",
            "attrs",
            "key_value",
            "value",
            "ids",
            "list",
            "item",
            "list",
            "item",
        ],
        ["items", "list", "item", "attrs", "key_value", "value", "label"],
        ["items", "list", "item", "name"],
    ]
    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_child(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list struct with map child."""
    path = tmp_path / "native-list-struct-map-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": 1, "b": 2}, "name": "x"}, {"attrs": {}, "name": "y"}],
                    None,
                    [{"attrs": None, "name": None}, {"attrs": {"c": None}, "name": "z"}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), pa.int64())),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "attrs", "key_value", "key"],
        ["items", "list", "item", "attrs", "key_value", "value"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_list_child(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list struct with map list child."""
    path = tmp_path / "native-list-struct-map-list-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": [1, 2], "b": []}, "name": "x"}, {"attrs": {}, "name": "y"}],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {"attrs": {"c": None, "d": [None, 3]}, "name": "z"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.int64()))),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "attrs", "key_value", "key"],
        ["items", "list", "item", "attrs", "key_value", "value", "list", "item"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct with map list chain child."""
    path = tmp_path / "native-list-struct-map-list-chain-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": [[1, 2], []]}, "name": "x"}, {"attrs": {}, "name": "y"}],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {"attrs": {"c": None, "d": [[None, 3]]}, "name": "z"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.list_(pa.int64())))),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "attrs", "key_value", "key"],
        ["items", "list", "item", "attrs", "key_value", "value", "list", "item", "list", "item"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_struct_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct with map struct child."""
    path = tmp_path / "native-list-struct-map-struct-child.parquet"
    value_type = pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": {"score": 1, "label": "x"}, "b": None}, "name": "r"}],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {"attrs": {"c": {"score": None, "label": "z"}}, "name": "s"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), value_type)),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "attrs", "key_value", "key"],
        ["items", "list", "item", "attrs", "key_value", "value", "score"],
        ["items", "list", "item", "attrs", "key_value", "value", "label"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct with map struct list child."""
    path = tmp_path / "native-list-struct-map-struct-list-child.parquet"
    value_type = pa.struct([pa.field("ids", pa.list_(pa.int64())), pa.field("label", pa.string())])
    table = pa.table(
        {
            "items": pa.array(
                [
                    [{"attrs": {"a": {"ids": [1, 2], "label": "x"}, "b": None}, "name": "r"}],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {
                            "attrs": {
                                "c": {"ids": None, "label": "z"},
                                "d": {"ids": [None, 3], "label": None},
                            },
                            "name": "s",
                        },
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("attrs", pa.map_(pa.string(), value_type)),
                            pa.field("name", pa.string()),
                        ]
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "attrs", "key_value", "key"],
        ["items", "list", "item", "attrs", "key_value", "value", "ids", "list", "item"],
        ["items", "list", "item", "attrs", "key_value", "value", "label"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_inner_list(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list struct with inner list."""
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
                        [pa.field("ids", pa.list_(pa.int64())), pa.field("name", pa.string())]
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
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


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_inner_list_chain(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct with inner list chain."""
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
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "ids", "list", "item", "list", "item"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_nested_struct_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct with nested struct child."""
    path = tmp_path / "native-list-struct-nested-struct-child.parquet"
    inner_type = pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
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
                    pa.struct([pa.field("inner", inner_type), pa.field("name", pa.string())])
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
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


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_nested_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct nested struct list child."""
    path = tmp_path / "native-list-struct-nested-struct-list-child.parquet"
    inner_type = pa.struct([pa.field("ids", pa.list_(pa.int64())), pa.field("label", pa.string())])
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
                    pa.struct([pa.field("inner", inner_type), pa.field("name", pa.string())])
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "inner", "ids", "list", "item"],
        ["items", "list", "item", "inner", "label"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_nested_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list struct nested struct list chain child."""
    path = tmp_path / "native-list-struct-nested-struct-list-chain-child.parquet"
    inner_type = pa.struct(
        [pa.field("ids", pa.list_(pa.list_(pa.int64()))), pa.field("label", pa.string())]
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
                    pa.struct([pa.field("inner", inner_type), pa.field("name", pa.string())])
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "inner", "ids", "list", "item", "list", "item"],
        ["items", "list", "item", "inner", "label"],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_nested_struct_list_child(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes struct nested struct list child."""
    path = tmp_path / "native-struct-nested-struct-list-child.parquet"
    inner_type = pa.struct([pa.field("ids", pa.list_(pa.int64())), pa.field("label", pa.string())])
    outer_type = pa.struct([pa.field("inner", inner_type), pa.field("kind", pa.string())])
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
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_nested_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes struct nested struct list chain child."""
    path = tmp_path / "native-struct-nested-struct-list-chain-child.parquet"
    inner_type = pa.struct(
        [pa.field("groups", pa.list_(pa.list_(pa.int64()))), pa.field("label", pa.string())]
    )
    outer_type = pa.struct([pa.field("inner", inner_type), pa.field("kind", pa.string())])
    table = pa.table(
        {
            "outer": pa.array(
                [
                    {"inner": {"groups": [[1, 2], [], None], "label": "x"}, "kind": "ok"},
                    None,
                    {"inner": {"groups": [], "label": None}, "kind": None},
                    {"inner": None, "kind": "z"},
                    {"inner": {"groups": None, "label": "q"}, "kind": "r"},
                ],
                type=outer_type,
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_nested_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes map nested struct list chain values."""
    path = tmp_path / "native-map-nested-struct-list-chain-values.parquet"
    inner_type = pa.struct(
        [pa.field("groups", pa.list_(pa.list_(pa.int64()))), pa.field("label", pa.string())]
    )
    value_type = pa.struct([pa.field("inner", inner_type), pa.field("kind", pa.string())])
    table = pa.table(
        {
            "attrs": pa.array(
                [
                    {"a": {"inner": {"groups": [[1, 2], [], None], "label": "x"}, "kind": "ok"}},
                    None,
                    {"b": {"inner": {"groups": [], "label": None}, "kind": None}},
                    {"c": {"inner": None, "kind": "z"}},
                    {"d": {"inner": {"groups": None, "label": "q"}, "kind": "r"}},
                ],
                type=pa.map_(pa.string(), value_type),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map list struct values."""
    path = tmp_path / "native-map-list-struct-values.parquet"
    element_type = pa.struct([pa.field("x", pa.int64()), pa.field("ys", pa.list_(pa.int64()))])
    table = pa.table(
        {
            "m": pa.array(
                [
                    {"a": [{"x": 1, "ys": [10, 20]}, {"x": None, "ys": []}, None]},
                    None,
                    {"b": []},
                    {"c": None},
                    {"d": [{"x": 4, "ys": None}]},
                ],
                type=pa.map_(pa.string(), pa.list_(element_type)),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_list_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map with struct list values."""
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
                        [pa.field("ids", pa.list_(pa.int64())), pa.field("name", pa.string())]
                    ),
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "ids", "list", "item"],
        ["labels", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_list_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map with list values."""
    path = tmp_path / "native-map-list-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", [1, 2]), ("b", [])], None, [("c", None)]],
                type=pa.map_(pa.string(), pa.list_(pa.int64())),
            )
        }
    )
    info = write_read_native_parquet(table, path)
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


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_list_chain_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map with list chain values."""
    path = tmp_path / "native-map-list-chain-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", [[1, 2], []])], None, [("b", None), ("c", [[None, 3]])]],
                type=pa.map_(pa.string(), pa.list_(pa.list_(pa.int64()))),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "list", "item", "list", "item"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map with struct values."""
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
                    pa.struct([pa.field("score", pa.int64()), pa.field("name", pa.string())]),
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "score"],
        ["labels", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_nested_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map with nested struct values."""
    path = tmp_path / "native-map-nested-struct-values.parquet"
    inner_type = pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
    value_type = pa.struct([pa.field("inner", inner_type), pa.field("kind", pa.string())])
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
    info = write_read_native_parquet(table, path)
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


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_nested_struct_list_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map nested struct list values."""
    path = tmp_path / "native-map-nested-struct-list-values.parquet"
    inner_type = pa.struct([pa.field("ids", pa.list_(pa.int64())), pa.field("label", pa.string())])
    value_type = pa.struct([pa.field("inner", inner_type), pa.field("kind", pa.string())])
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
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes map with struct list chain values."""
    path = tmp_path / "native-map-struct-list-chain-values.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [
                    [("a", {"ids": [[1, 2], []], "name": "x"})],
                    None,
                    [
                        ("c", None),
                        ("d", {"ids": None, "name": "z"}),
                        ("e", {"ids": [[None, 3]], "name": "q"}),
                    ],
                ],
                type=pa.map_(
                    pa.string(),
                    pa.struct(
                        [
                            pa.field("ids", pa.list_(pa.list_(pa.int64()))),
                            pa.field("name", pa.string()),
                        ]
                    ),
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "ids", "list", "item", "list", "item"],
        ["labels", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_nested_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list map nested struct list values."""
    path = tmp_path / "native-list-map-nested-struct-list-values.parquet"
    inner_type = pa.struct([pa.field("ids", pa.list_(pa.int64())), pa.field("label", pa.string())])
    value_type = pa.struct([pa.field("inner", inner_type), pa.field("kind", pa.string())])
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"a": {"inner": {"ids": [1, 2], "label": "x"}, "kind": "ok"}},
                        {"b": {"inner": {"ids": [], "label": None}, "kind": None}},
                    ],
                    None,
                    [],
                    [
                        {"c": {"inner": None, "kind": "z"}},
                        {"d": {"inner": {"ids": None, "label": "q"}, "kind": "r"}},
                    ],
                ],
                type=pa.list_(pa.map_(pa.string(), value_type)),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list map list struct values."""
    path = tmp_path / "native-list-map-list-struct-values.parquet"
    element_type = pa.struct([pa.field("x", pa.int64()), pa.field("ys", pa.list_(pa.int64()))])
    table = pa.table(
        {
            "m": pa.array(
                [
                    [{"a": [{"x": 1, "ys": [10, 20]}, {"x": None, "ys": []}, None]}, {"b": []}],
                    None,
                    [],
                    [{"c": None}, {"d": [{"x": 4, "ys": None}]}],
                ],
                type=pa.list_(pa.map_(pa.string(), pa.list_(element_type))),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list map with struct values."""
    path = tmp_path / "native-list-map-struct-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", {"score": 1, "name": "x"}), ("b", {"score": 2, "name": None})]],
                    None,
                    [None, [("c", None), ("d", {"score": None, "name": "z"})]],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct([pa.field("score", pa.int64()), pa.field("name", pa.string())]),
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "key_value", "key"],
        ["items", "list", "item", "key_value", "value", "score"],
        ["items", "list", "item", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_nested_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list map with nested struct values."""
    path = tmp_path / "native-list-map-nested-struct-values.parquet"
    inner_type = pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
    value_type = pa.struct([pa.field("inner", inner_type), pa.field("kind", pa.string())])
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", {"inner": {"score": 1, "label": "x"}, "kind": "ok"})], None],
                    None,
                    [],
                    [[("b", None)], [("c", {"inner": None, "kind": "empty"})]],
                    [[("d", {"inner": {"score": None, "label": None}, "kind": None})]],
                ],
                type=pa.list_(pa.map_(pa.string(), value_type)),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "key_value", "key"],
        ["items", "list", "item", "key_value", "value", "inner", "score"],
        ["items", "list", "item", "key_value", "value", "inner", "label"],
        ["items", "list", "item", "key_value", "value", "kind"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list map with struct list values."""
    path = tmp_path / "native-list-map-struct-list-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", {"ids": [1, 2], "name": "x"}), ("b", {"ids": [], "name": None})]],
                    None,
                    [None, [("c", None), ("d", {"ids": [None, 3], "name": "z"})]],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct(
                            [pa.field("ids", pa.list_(pa.int64())), pa.field("name", pa.string())]
                        ),
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "key_value", "key"],
        ["items", "list", "item", "key_value", "value", "ids", "list", "item"],
        ["items", "list", "item", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list map with struct list chain values."""
    path = tmp_path / "native-list-map-struct-list-chain-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", {"ids": [[1, 2], []], "name": "x"}), ("b", {"ids": [], "name": None})]],
                    None,
                    [None, [("c", None), ("d", {"ids": [[None, 3]], "name": "z"})]],
                ],
                type=pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct(
                            [
                                pa.field("ids", pa.list_(pa.list_(pa.int64()))),
                                pa.field("name", pa.string()),
                            ]
                        ),
                    )
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "key_value", "key"],
        ["items", "list", "item", "key_value", "value", "ids", "list", "item", "list", "item"],
        ["items", "list", "item", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_list_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list list struct values."""
    path = tmp_path / "native-list-list-struct-values.parquet"
    struct_type = pa.struct([pa.field("x", pa.int64()), pa.field("ys", pa.list_(pa.int64()))])
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[{"x": 1, "ys": [1, 2]}, {"x": 2, "ys": []}], []],
                    None,
                    [[None, {"x": 3, "ys": None}]],
                ],
                type=pa.list_(pa.list_(struct_type)),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_deep_list_chain_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes deep list chain struct values."""
    path = tmp_path / "native-deep-list-chain-struct-values.parquet"
    struct_type = pa.struct([pa.field("x", pa.int64()), pa.field("name", pa.string())])
    item_type = pa.list_(pa.list_(pa.list_(pa.list_(struct_type))))
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[[[{"x": 1, "name": "a"}, None], []]]],
                    None,
                    [[[[]]]],
                    [[[[{"x": None, "name": None}]]]],
                ],
                type=item_type,
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_deeper_map_list_recursion(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes deeper map list recursion."""
    map_int_type = pa.map_(pa.string(), pa.int64())
    map_map_type = pa.map_(pa.string(), map_int_type)
    struct_map_type = pa.struct([pa.field("m", map_map_type), pa.field("n", pa.int64())])
    cases = [
        (
            "struct-map-map",
            pa.struct([pa.field("k", map_map_type)]),
            [{"k": [("a", [("x", 1)]), ("b", [])]}, None, {"k": None}],
        ),
        (
            "list-struct-map-map",
            pa.list_(pa.struct([pa.field("k", map_map_type)])),
            [[{"k": [("a", [("x", 1)])]}], None, [], [{"k": None}]],
        ),
        (
            "map-list-map-map",
            pa.map_(pa.string(), pa.list_(map_map_type)),
            [[("a", [[("x", [("i", 1)])], []]), ("b", None)], None, [("c", [])]],
        ),
        (
            "list-map-list-map-map",
            pa.list_(pa.map_(pa.string(), pa.list_(map_map_type))),
            [[[("a", [[("x", [("i", 1)])]]), ("b", [])]], None, [], [[("c", None)]]],
        ),
        (
            "map-list-struct-map-map",
            pa.map_(pa.string(), pa.list_(struct_map_type)),
            [[("a", [{"m": [("x", [("i", 1)])], "n": 2}, None]), ("b", [])], None, [("c", None)]],
        ),
        (
            "list-list-struct-map-map",
            pa.list_(pa.list_(struct_map_type)),
            [[[{"m": [("x", [("i", 1)])], "n": 2}, None]], None, [[]]],
        ),
    ]
    for name, item_type, values in cases:
        path = tmp_path / f"native-{name}.parquet"
        try:
            items = pa.array(values, type=item_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise AssertionError(name) from exc
        table = pa.table({"items": items})
        info = write_read_native_parquet(table, path)
        assert info is not None
        assert info["native_reader_ready"] == 1
        assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_list_struct_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map list list struct values."""
    path = tmp_path / "native-map-list-list-struct-values.parquet"
    struct_type = pa.struct([pa.field("x", pa.int64()), pa.field("ys", pa.list_(pa.int64()))])
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
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_list_map_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes map list list map values."""
    path = tmp_path / "native-map-list-list-map-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [{"root": [[{"a": 1}], []]}, None, {"empty": None, "other": [[{"b": None}]]}],
                type=pa.map_(pa.string(), pa.list_(pa.list_(pa.map_(pa.string(), pa.int64())))),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list map list list struct values."""
    path = tmp_path / "native-list-map-list-list-struct-values.parquet"
    struct_type = pa.struct([pa.field("x", pa.int64()), pa.field("ys", pa.list_(pa.int64()))])
    table = pa.table(
        {
            "items": pa.array(
                [[{"a": [[{"x": 1, "ys": [1]}, None]]}], None, [], [{"b": []}]],
                type=pa.list_(pa.map_(pa.string(), pa.list_(pa.list_(struct_type)))),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_list_map_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list map list list map values."""
    path = tmp_path / "native-list-map-list-list-map-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [[{"root": [[{"a": 1}], []]}], None, [], [{"other": [[{"b": None}]]}]],
                type=pa.list_(
                    pa.map_(pa.string(), pa.list_(pa.list_(pa.map_(pa.string(), pa.int64()))))
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


MAP_CASE_IDS = (
    "list-list-map",
    "list-list-map-struct",
    "list-list-map-struct-map",
    "map-struct-map",
    "map-map",
    "map-map-map",
    "list-map-map",
)


def _recursive_map_case(case_id: str) -> tuple[object, list[object]]:
    """Build the recursive map case for the native nested corpus."""
    scalar_map = pa.map_(pa.string(), pa.int64())
    map_map = pa.map_(pa.string(), scalar_map)
    if case_id == "list-list-map":
        return (
            pa.list_(pa.list_(scalar_map)),
            [[[{"a": 1}, {}, None], []], None, [[{"b": None, "c": 3}]]],
        )
    if case_id == "list-list-map-struct":
        value_type = pa.struct([pa.field("x", pa.int64()), pa.field("ys", pa.list_(pa.int64()))])
        return (
            pa.list_(pa.list_(pa.map_(pa.string(), value_type))),
            [
                [[{"a": {"x": 1, "ys": [1]}, "b": None}, {}, None]],
                None,
                [[{"c": {"x": None, "ys": []}}]],
            ],
        )
    if case_id in {"list-list-map-struct-map", "map-struct-map"}:
        value_type = pa.struct([pa.field("n", pa.int64()), pa.field("m", scalar_map)])
        values = [
            {"a": {"n": 1, "m": {"x": 2}}, "b": {"n": None, "m": None}},
            None,
            {"c": {"n": 3, "m": {}}},
        ]
        if case_id == "map-struct-map":
            return (pa.map_(pa.string(), value_type), values)
        return (pa.list_(pa.list_(pa.map_(pa.string(), value_type))), [[[values[0]]], None, [[{}]]])
    if case_id == "map-map":
        return (map_map, [{"a": {"x": 1}, "b": {}}, None, {"c": None, "d": {"z": None}}])
    if case_id == "map-map-map":
        return (
            pa.map_(pa.string(), map_map),
            [{"a": {"x": {"i": 1}, "y": {}}, "b": None}, None, {"c": {"z": None}}],
        )
    assert case_id == "list-map-map"
    return (
        pa.list_(map_map),
        [[{"a": {"x": 1}, "b": {}}, None], None, [], [{"c": None, "d": {"z": None}}]],
    )


@_requires_pyarrow
@pytest.mark.parametrize("case_id", MAP_CASE_IDS, ids=MAP_CASE_IDS)
def test_native_parquet_stream_materializes_recursive_map_case(
    tmp_path: Path, case_id: str
) -> None:
    """Verify native Parquet stream materializes recursive map case."""
    item_type, values = _recursive_map_case(case_id)
    table = pa.table({"items": pa.array(values, type=item_type)})
    write_read_native_parquet(table, tmp_path / f"native-{case_id}-values.parquet")


@_requires_pyarrow
def test_native_parquet_stream_materializes_top_level_map_scalar_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes top level map scalar values."""
    path = tmp_path / "native-map.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", 1), ("b", 2)], None, [], [("c", None)]],
                type=pa.map_(pa.string(), pa.int64()),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value"],
    ]
    assert columns[0]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert columns[1]["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert columns[0]["native_read_total_nulls"] == 0
    assert columns[1]["native_read_total_nulls"] == 1


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_struct_scalar_leaves(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list of struct scalar leaves."""
    path = tmp_path / "native-list-struct.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [None, {"score": 1, "label": "a"}, {"score": None, "label": "b"}],
                    None,
                    [],
                    [{"score": 3, "label": None}],
                ],
                type=pa.list_(
                    pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "score"],
        ["items", "list", "item", "label"],
    ]
    assert columns[0]["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 4]
    assert columns[1]["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 4]
    assert columns[0]["native_read_total_nulls"] == 2
    assert columns[1]["native_read_total_nulls"] == 2


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_list_scalar_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list of list scalar values."""
    path = tmp_path / "native-list-list.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [[[1, 2], [], None], None, [], [[None, 3]]], type=pa.list_(pa.list_(pa.int64()))
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["path_in_schema"] == ["items", "list", "item", "list", "item"]
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 4]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0d"
    assert column["repeated_level_layouts"][1]["offsets"] == [0, 2, 2, 2, 4]
    assert column["repeated_level_layouts"][1]["validity_hex_preview"] == "0b"
    assert column["native_read_arrow_length"] == 4
    assert column["native_read_arrow_null_count"] == 1


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_map_scalar_values(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes list of map scalar values."""
    path = tmp_path / "native-list-map.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [[[("a", 1), ("b", 2)], [], None], None, [], [[("c", None)]]],
                type=pa.list_(pa.map_(pa.string(), pa.int64())),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["items", "list", "item", "key_value", "key"],
        ["items", "list", "item", "key_value", "value"],
    ]
    assert columns[0]["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 4]
    assert columns[0]["repeated_level_layouts"][1]["offsets"] == [0, 2, 2, 2, 3]
    assert columns[0]["repeated_level_layouts"][1]["validity_hex_preview"] == "0b"
    assert columns[0]["native_read_total_nulls"] == 0
    assert columns[1]["native_read_total_nulls"] == 1


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_list_of_list_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes list of list of list scalar values."""
    path = tmp_path / "native-list-list-list.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [[[[1, 2], []], None], [], None, [[[None, 3]]]],
                type=pa.list_(pa.list_(pa.list_(pa.int64()))),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["path_in_schema"] == ["items", "list", "item", "list", "item", "list", "item"]
    assert column["repeated_level_layouts"][0]["offsets"] == [0, 2, 2, 2, 3]
    assert column["repeated_level_layouts"][0]["validity_hex_preview"] == "0b"
    assert column["repeated_level_layouts"][1]["offsets"] == [0, 2, 2, 3]
    assert column["repeated_level_layouts"][1]["validity_hex_preview"] == "05"
    assert column["repeated_level_layouts"][2]["offsets"] == [0, 2, 2, 4]
    assert column["repeated_level_layouts"][2]["validity_hex_preview"] == ""
    assert column["native_read_arrow_length"] == 4
    assert column["native_read_arrow_null_count"] == 1


@_requires_pyarrow
@pytest.mark.parametrize("name", ["list4-int", "list5-string"])
def test_native_parquet_stream_materializes_arbitrary_depth_list_chains(
    tmp_path: Path, name: str
) -> None:
    """Verify native Parquet stream materializes arbitrary depth list chains."""
    path = tmp_path / f"native-{name}.parquet"
    if name == "list4-int":
        array = pa.array(
            [[[[[1, 2], []], None], []], None, [], [[[[None, 3]]]]],
            type=pa.list_(pa.list_(pa.list_(pa.list_(pa.int64())))),
        )
    else:
        array = pa.array(
            [[[[[["a"], []]]]], None, [], [[[[[None, "b"]]]]]],
            type=pa.list_(pa.list_(pa.list_(pa.list_(pa.list_(pa.string()))))),
        )
    table = pa.table({"items": array})
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_child(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes struct with map child."""
    path = tmp_path / "native-struct-map-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": 1, "b": 2}, "name": "x"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": None}, "name": "z"},
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), pa.int64())),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value"],
        ["rec", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes struct with map struct list chain child."""
    path = tmp_path / "native-struct-map-struct-list-chain-child.parquet"
    value_type = pa.struct(
        [pa.field("ids", pa.list_(pa.list_(pa.int64()))), pa.field("label", pa.string())]
    )
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": {"ids": [[1, 2], []], "label": "x"}, "b": None}, "name": "r"},
                    None,
                    {"attrs": None, "name": None},
                    {
                        "attrs": {
                            "c": {"ids": None, "label": "z"},
                            "d": {"ids": [[None, 3]], "label": None},
                        },
                        "name": "s",
                    },
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), value_type)),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "ids", "list", "item", "list", "item"],
        ["rec", "attrs", "key_value", "value", "label"],
        ["rec", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_nested_struct_child(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes struct with nested struct child."""
    path = tmp_path / "native-struct-nested-struct-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"inner": {"score": 1, "label": "x"}, "name": "a"},
                    None,
                    {"inner": None, "name": "b"},
                    {"inner": {"score": None, "label": None}, "name": None},
                ],
                type=pa.struct(
                    [
                        pa.field(
                            "inner",
                            pa.struct(
                                [pa.field("score", pa.int64()), pa.field("label", pa.string())]
                            ),
                        ),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "inner", "score"],
        ["rec", "inner", "label"],
        ["rec", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_list_child(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes struct with map list child."""
    path = tmp_path / "native-struct-map-list-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": [1, 2], "b": []}, "name": "x"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": None, "d": [None, 3]}, "name": "z"},
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.int64()))),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "list", "item"],
        ["rec", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes struct with map list chain child."""
    path = tmp_path / "native-struct-map-list-chain-child.parquet"
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": [[1, 2], []]}, "name": "x"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": None, "d": [[None, 3]]}, "name": "z"},
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.list_(pa.int64())))),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "list", "item", "list", "item"],
        ["rec", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_child(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes struct with map struct child."""
    path = tmp_path / "native-struct-map-struct-child.parquet"
    value_type = pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": {"score": 1, "label": "x"}, "b": None}, "name": "r"},
                    None,
                    {"attrs": None, "name": None},
                    {"attrs": {"c": {"score": None, "label": "z"}}, "name": "s"},
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), value_type)),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "score"],
        ["rec", "attrs", "key_value", "value", "label"],
        ["rec", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_list_child(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes struct with map struct list child."""
    path = tmp_path / "native-struct-map-struct-list-child.parquet"
    value_type = pa.struct([pa.field("ids", pa.list_(pa.int64())), pa.field("label", pa.string())])
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {"attrs": {"a": {"ids": [1, 2], "label": "x"}, "b": None}, "name": "r"},
                    None,
                    {"attrs": None, "name": None},
                    {
                        "attrs": {
                            "c": {"ids": None, "label": "z"},
                            "d": {"ids": [None, 3], "label": None},
                        },
                        "name": "s",
                    },
                ],
                type=pa.struct(
                    [
                        pa.field("attrs", pa.map_(pa.string(), value_type)),
                        pa.field("name", pa.string()),
                    ]
                ),
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    columns = info["row_groups"][0]["columns"]
    assert [column["path_in_schema"] for column in columns] == [
        ["rec", "attrs", "key_value", "key"],
        ["rec", "attrs", "key_value", "value", "ids", "list", "item"],
        ["rec", "attrs", "key_value", "value", "label"],
        ["rec", "name"],
    ]


NativeNestedCase = tuple[str, Callable[[Path], None]]


def _native_nested_cases() -> tuple[NativeNestedCase, ...]:
    """Return the named native nested cases consumed by the parametrized runner."""
    excluded = {
        "test_native_parquet_stream_materializes_recursive_map_case",
        "test_native_parquet_stream_materializes_arbitrary_depth_list_chains",
    }
    cases: list[NativeNestedCase] = [
        (name.removeprefix("test_"), value)
        for name, value in globals().items()
        if name.startswith("test_")
        and name not in excluded
        and inspect.isfunction(value)
        and len(inspect.signature(value).parameters) == 1
    ]
    cases.extend(
        (
            f"native_parquet_stream_materializes_recursive_map_case[{case_id}]",
            partial(test_native_parquet_stream_materializes_recursive_map_case, case_id=case_id),
        )
        for case_id in MAP_CASE_IDS
    )
    cases.extend(
        (
            f"native_parquet_stream_materializes_arbitrary_depth_list_chains[{name}]",
            partial(test_native_parquet_stream_materializes_arbitrary_depth_list_chains, name=name),
        )
        for name in ("list4-int", "list5-string")
    )
    return tuple(cases)


NATIVE_NESTED_CASES = _native_nested_cases()
