"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose struct values contain list chains."""

    require_native()
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
