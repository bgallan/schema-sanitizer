"""Native Parquet nested materialization runtime tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")

# Split from test_parquet_native_nested_scalars_lists.py: test_native_parquet_stream_materializes_struct_with_map_struct_list_chain_child


@_requires_pyarrow
def test_native_parquet_stream_materializes_struct_with_map_struct_list_chain_child(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes struct-owned maps with nested-list struct fields."""
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

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
