"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_nested_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values with nested struct lists."""

    require_native()
    path = tmp_path / "native-list-map-nested-struct-list-values.parquet"
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
def test_native_parquet_stream_materializes_list_map_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are lists of structs."""

    require_native()
    path = tmp_path / "native-list-map-list-struct-values.parquet"
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
                    [
                        {
                            "a": [
                                {"x": 1, "ys": [10, 20]},
                                {"x": None, "ys": []},
                                None,
                            ]
                        },
                        {"b": []},
                    ],
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
def test_native_parquet_stream_materializes_list_map_with_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list maps whose values are structs."""

    require_native()
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
                        pa.struct(
                            [
                                pa.field("score", pa.int64()),
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
        ["items", "list", "item", "key_value", "value", "score"],
        ["items", "list", "item", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_nested_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader recursively materializes nested list-map value structs."""

    require_native()
    path = tmp_path / "native-list-map-nested-struct-values.parquet"
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
    """Verify native reader materializes list maps with list-bearing struct values."""

    require_native()
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
                            [
                                pa.field("ids", pa.list_(pa.int64())),
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
        ["items", "list", "item", "key_value", "value", "ids", "list", "item"],
        ["items", "list", "item", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_with_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list maps with nested-list struct values."""

    require_native()
    path = tmp_path / "native-list-map-struct-list-chain-values.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [
                        [
                            ("a", {"ids": [[1, 2], []], "name": "x"}),
                            ("b", {"ids": [], "name": None}),
                        ]
                    ],
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
        [
            "items",
            "list",
            "item",
            "key_value",
            "value",
            "ids",
            "list",
            "item",
            "list",
            "item",
        ],
        ["items", "list", "item", "key_value", "value", "name"],
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level list-list-struct values."""

    require_native()
    path = tmp_path / "native-list-list-struct-values.parquet"
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
def test_native_parquet_stream_materializes_deep_list_chain_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes deep list chains with struct leaves."""

    require_native()
    path = tmp_path / "native-deep-list-chain-struct-values.parquet"
    struct_type = pa.struct(
        [
            pa.field("x", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )
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
