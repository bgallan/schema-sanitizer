"""Native Parquet recursive nested grammar runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_materializes_deeper_map_list_recursion(
    tmp_path: Path,
) -> None:
    """Verify native reader handles deeper recursive map/list/struct combinations."""

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
        info = write_read_native_parquet(table, path)

        assert info is not None
        assert info["native_reader_ready"] == 1
        assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are list-list-structs."""

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
    info = write_read_native_parquet(table, path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_list_list_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes map values that are list-list-maps."""

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
    info = write_read_native_parquet(table, path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_list_struct_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are list-list-structs."""

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
    info = write_read_native_parquet(table, path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_list_map_list_list_map_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes list-map values that are list-list-maps."""

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
    info = write_read_native_parquet(table, path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
