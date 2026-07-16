"""Native Parquet recursive nested grammar runtime tests."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native
from parquet_runtime_shared import pa
from parquet_runtime_shared import requires_pyarrow as _requires_pyarrow

# Split from test_parquet_native_recursive_projection_runtime.py: test_native_parquet_stream_projects_independent_recursive_roots_in_subsets, test_list_struct_projection_uses_native_reader


@_requires_pyarrow
def test_native_parquet_stream_projects_independent_recursive_roots_in_subsets(
    tmp_path: Path,
) -> None:
    """Verify recursive roots and leaf order stay independent across projections."""
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
    alpha_type = pa.list_(
        pa.struct(
            [
                pa.field("items", pa.list_(pa.map_(pa.string(), pa.int64()))),
                pa.field("note", pa.string()),
            ]
        )
    )
    beta_type = pa.struct(
        [
            pa.field("left", pa.map_(pa.string(), pa.list_(pa.float64()))),
            pa.field("right", pa.list_(pa.struct([pa.field("flag", pa.bool_())]))),
        ]
    )
    gamma_type = pa.map_(
        pa.string(),
        pa.list_(
            pa.struct(
                [
                    pa.field("labels", pa.list_(pa.string())),
                    pa.field("score", pa.int64()),
                ]
            )
        ),
    )
    schema = pa.schema(
        [
            pa.field("plain", pa.int64()),
            pa.field("alpha", alpha_type),
            pa.field("beta", beta_type),
            pa.field("gamma", gamma_type),
        ]
    )
    batches = [
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array(
                    [
                        [{"items": [[("a", 10)], []], "note": "a"}],
                        None,
                    ],
                    type=alpha_type,
                ),
                pa.array(
                    [
                        {
                            "left": [("x", [1.25, None])],
                            "right": [{"flag": True}, None],
                        },
                        None,
                    ],
                    type=beta_type,
                ),
                pa.array(
                    [
                        [("g", [{"labels": ["x", None], "score": 7}])],
                        [],
                    ],
                    type=gamma_type,
                ),
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.array([3], type=pa.int64()),
                pa.array([[{"items": None, "note": None}]], type=alpha_type),
                pa.array([{"left": [], "right": []}], type=beta_type),
                pa.array([None], type=gamma_type),
            ],
            schema=schema,
        ),
    ]
    full_table = pa.Table.from_batches(batches)
    path = tmp_path / "native-recursive-independent-root-subsets.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(schema, batches),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    assert info["row_group_count"] == 2

    for columns in (["gamma"], ["beta", "alpha"], ["gamma", "plain", "alpha"]):
        factory = open_parquet_record_batch_stream_factory(
            path,
            source="path",
            feature="test",
            columns=columns,
        )
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()
        expected = full_table.select(columns)

        assert out.schema.equals(expected.schema), columns
        assert out.to_pylist() == expected.to_pylist(), columns
        assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_list_struct_projection_uses_native_reader(
    tmp_path: Path,
) -> None:
    """Verify projected list structs with scalar leaves use the native reader."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "complex-nested-list-projection.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "items": pa.array(
                [
                    [{"score": 1, "label": "a"}, {"score": 2, "label": "b"}],
                    [{"score": 3, "label": "c"}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
                            pa.field("label", pa.string()),
                        ]
                    )
                ),
            ),
        }
    )
    expected = table.select(["items"])
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["items"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
