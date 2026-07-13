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

# Split from test_parquet_native_nested_map_combinators.py: test_native_parquet_stream_materializes_map_with_struct_list_chain_values


@_requires_pyarrow
def test_native_parquet_stream_materializes_map_with_struct_list_chain_values(
    tmp_path: Path,
) -> None:
    """Verify native reader materializes maps whose struct values contain list chains."""
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
        ["labels", "key_value", "key"],
        ["labels", "key_value", "value", "ids", "list", "item", "list", "item"],
        ["labels", "key_value", "value", "name"],
    ]

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(table.schema)
    assert out.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
