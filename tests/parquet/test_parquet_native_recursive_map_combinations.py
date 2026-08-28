"""Native Parquet recursive map materialization cases."""

from __future__ import annotations

from pathlib import Path

import pytest
from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native

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
    scalar_map = pa.map_(pa.string(), pa.int64())
    map_map = pa.map_(pa.string(), scalar_map)
    if case_id == "list-list-map":
        return pa.list_(pa.list_(scalar_map)), [
            [[{"a": 1}, {}, None], []],
            None,
            [[{"b": None, "c": 3}]],
        ]
    if case_id == "list-list-map-struct":
        value_type = pa.struct([pa.field("x", pa.int64()), pa.field("ys", pa.list_(pa.int64()))])
        return pa.list_(pa.list_(pa.map_(pa.string(), value_type))), [
            [[{"a": {"x": 1, "ys": [1]}, "b": None}, {}, None]],
            None,
            [[{"c": {"x": None, "ys": []}}]],
        ]
    if case_id in {"list-list-map-struct-map", "map-struct-map"}:
        value_type = pa.struct([pa.field("n", pa.int64()), pa.field("m", scalar_map)])
        values = [
            {"a": {"n": 1, "m": {"x": 2}}, "b": {"n": None, "m": None}},
            None,
            {"c": {"n": 3, "m": {}}},
        ]
        if case_id == "map-struct-map":
            return pa.map_(pa.string(), value_type), values
        return pa.list_(pa.list_(pa.map_(pa.string(), value_type))), [
            [[values[0]]],
            None,
            [[{}]],
        ]
    if case_id == "map-map":
        return map_map, [
            {"a": {"x": 1}, "b": {}},
            None,
            {"c": None, "d": {"z": None}},
        ]
    if case_id == "map-map-map":
        return pa.map_(pa.string(), map_map), [
            {"a": {"x": {"i": 1}, "y": {}}, "b": None},
            None,
            {"c": {"z": None}},
        ]
    assert case_id == "list-map-map"
    return pa.list_(map_map), [
        [{"a": {"x": 1}, "b": {}}, None],
        None,
        [],
        [{"c": None, "d": {"z": None}}],
    ]


@_requires_pyarrow
@pytest.mark.parametrize("case_id", MAP_CASE_IDS, ids=MAP_CASE_IDS)
def test_native_parquet_stream_materializes_recursive_map_case(
    tmp_path: Path,
    case_id: str,
) -> None:
    """Every declared recursive map/list/struct combination round-trips natively."""
    require_native()
    item_type, values = _recursive_map_case(case_id)
    table = pa.table({"items": pa.array(values, type=item_type)})

    write_read_native_parquet(table, tmp_path / f"native-{case_id}-values.parquet")
