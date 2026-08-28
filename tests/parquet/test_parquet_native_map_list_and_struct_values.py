"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are scalar lists."""

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
def test_native_parquet_stream_materializes_map_with_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are nested lists."""

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
def test_native_parquet_stream_materializes_map_with_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose values are structs."""

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
def test_native_parquet_stream_materializes_map_with_nested_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader recursively materializes nested map value structs."""

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
def test_native_parquet_stream_materializes_map_nested_struct_list_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values with nested struct lists."""

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
    info = write_read_native_parquet(table, path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
