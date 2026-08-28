"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with map children."""

    require_native()
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
def test_native_parquet_stream_materializes_list_struct_with_map_list_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with list-valued map children."""

    require_native()
    path = tmp_path / "native-list-struct-map-list-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"attrs": {"a": [1, 2], "b": []}, "name": "x"},
                        {"attrs": {}, "name": "y"},
                    ],
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
    """Verify native reader materializes list structs with nested-list-valued maps."""

    require_native()
    path = tmp_path / "native-list-struct-map-list-chain-child.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        {"attrs": {"a": [[1, 2], []]}, "name": "x"},
                        {"attrs": {}, "name": "y"},
                    ],
                    None,
                    [
                        {"attrs": None, "name": None},
                        {"attrs": {"c": None, "d": [[None, 3]]}, "name": "z"},
                    ],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field(
                                "attrs",
                                pa.map_(pa.string(), pa.list_(pa.list_(pa.int64()))),
                            ),
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
        [
            "items",
            "list",
            "item",
            "attrs",
            "key_value",
            "key",
        ],
        [
            "items",
            "list",
            "item",
            "attrs",
            "key_value",
            "value",
            "list",
            "item",
            "list",
            "item",
        ],
        ["items", "list", "item", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_struct_with_map_struct_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list structs with struct-valued maps."""

    require_native()
    path = tmp_path / "native-list-struct-map-struct-child.parquet"
    value_type = pa.struct(
        [
            pa.field("score", pa.int64()),
            pa.field("label", pa.string()),
        ]
    )
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
    """Verify native reader materializes list structs with map struct list fields."""

    require_native()
    path = tmp_path / "native-list-struct-map-struct-list-child.parquet"
    value_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.int64())),
            pa.field("label", pa.string()),
        ]
    )
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
        ],
        ["items", "list", "item", "attrs", "key_value", "value", "label"],
        ["items", "list", "item", "name"],
    ]
