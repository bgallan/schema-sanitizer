"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_top_level_map_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level maps with scalar key/value leaves."""

    require_native()
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
def test_native_parquet_stream_materializes_list_of_struct_scalar_leaves(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level list structs with scalar leaves."""

    require_native()
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
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
                            pa.field("label", pa.string()),
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
        ["items", "list", "item", "score"],
        ["items", "list", "item", "label"],
    ]
    assert columns[0]["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 4]
    assert columns[1]["repeated_level_layouts"][0]["offsets"] == [0, 3, 3, 3, 4]
    assert columns[0]["native_read_total_nulls"] == 2
    assert columns[1]["native_read_total_nulls"] == 2


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_of_list_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level nested scalar lists."""

    require_native()
    path = tmp_path / "native-list-list.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[1, 2], [], None],
                    None,
                    [],
                    [[None, 3]],
                ],
                type=pa.list_(pa.list_(pa.int64())),
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
def test_native_parquet_stream_materializes_list_of_map_scalar_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level lists of scalar maps."""

    require_native()
    path = tmp_path / "native-list-map.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[("a", 1), ("b", 2)], [], None],
                    None,
                    [],
                    [[("c", None)]],
                ],
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
    """Verify native reader materializes top-level three-deep scalar lists."""

    require_native()
    path = tmp_path / "native-list-list-list.parquet"
    table = pa.table(
        {
            "items": pa.array(
                [
                    [[[1, 2], []], None],
                    [],
                    None,
                    [[[None, 3]]],
                ],
                type=pa.list_(pa.list_(pa.list_(pa.int64()))),
            )
        }
    )
    info = write_read_native_parquet(table, path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    column = info["row_groups"][0]["columns"][0]
    assert column["path_in_schema"] == [
        "items",
        "list",
        "item",
        "list",
        "item",
        "list",
        "item",
    ]
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
    tmp_path: Path,
    name: str,
) -> None:
    """Verify native reader materializes scalar list chains deeper than three."""

    require_native()
    path = tmp_path / f"native-{name}.parquet"
    if name == "list4-int":
        array = pa.array(
            [
                [[[[1, 2], []], None], []],
                None,
                [],
                [[[[None, 3]]]],
            ],
            type=pa.list_(pa.list_(pa.list_(pa.list_(pa.int64())))),
        )
    else:
        array = pa.array(
            [
                [[[[["a"], []]]]],
                None,
                [],
                [[[[[None, "b"]]]]],
            ],
            type=pa.list_(pa.list_(pa.list_(pa.list_(pa.list_(pa.string()))))),
        )
    table = pa.table({"items": array})
    info = write_read_native_parquet(table, path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes top-level structs with map children."""

    require_native()
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
