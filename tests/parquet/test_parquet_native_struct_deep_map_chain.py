"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes struct-owned maps with nested-list struct fields."""

    require_native()
    path = tmp_path / "native-struct-map-struct-list-chain-child.parquet"
    value_type = pa.struct(
        [
            pa.field("ids", pa.list_(pa.list_(pa.int64()))),
            pa.field("label", pa.string()),
        ]
    )
    table = pa.table(
        {
            "rec": pa.array(
                [
                    {
                        "attrs": {
                            "a": {"ids": [[1, 2], []], "label": "x"},
                            "b": None,
                        },
                        "name": "r",
                    },
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
        [
            "rec",
            "attrs",
            "key_value",
            "value",
            "ids",
            "list",
            "item",
            "list",
            "item",
        ],
        ["rec", "attrs", "key_value", "value", "label"],
        ["rec", "name"],
    ]
