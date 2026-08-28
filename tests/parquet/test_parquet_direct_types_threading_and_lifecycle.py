"""Direct Parquet type, threading, and lifecycle runtime tests."""

from __future__ import annotations

import gc
from pathlib import Path

import pytest
from _support.parquet_runtime import feather, pa, pq, sample_table
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import read_test_parquet

import schema_sanitizer as ss


@_requires_pyarrow
def test_direct_parquet_map_and_fixed_size_list_use_arrow_path(
    tmp_path: Path, require_native: None
) -> None:
    path = tmp_path / "map_fixed.parquet"
    table = pa.table(
        {
            "labels": pa.array(
                [[("a", 1), ("b", 2)]],
                type=pa.map_(pa.string(), pa.int64()),
            ),
            "vector": pa.array([[1, 2]], type=pa.list_(pa.int64(), 2)),
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.to_pylist() == [
        {
            "labels": [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
            "vector": [1, 2],
        }
    ]


@_requires_pyarrow
def test_direct_parquet_duration_values_are_lossless_strings(
    tmp_path: Path, require_native: None
) -> None:
    path = tmp_path / "duration.parquet"
    table = pa.table({"elapsed": pa.array([123, -5], type=pa.duration("us"))})
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.schema.field("elapsed").type == pa.string()
    assert result.clean_data.to_pylist() == [{"elapsed": "123us"}, {"elapsed": "-5us"}]


@_requires_pyarrow
def test_native_arrow_schema_contract_payload_supports_new_direct_shapes(
    require_native: None,
) -> None:
    from schema_sanitizer.core_impl.logical_schema import pyarrow_schema_from_payload
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    schema = pa.schema(
        [
            pa.field("labels", pa.map_(pa.string(), pa.int64())),
            pa.field("vector", pa.list_(pa.int64(), 2)),
            pa.field("amount", pa.decimal128(10, 2)),
        ]
    )

    payload = _native.arrow_schema_contract_payload(schema)
    decoded = pyarrow_schema_from_payload(payload)

    assert decoded == pa.schema(
        [
            pa.field(
                "labels",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("key", pa.string(), nullable=False),
                            pa.field("value", pa.int64()),
                        ]
                    )
                ),
            ),
            pa.field("vector", pa.list_(pa.int64())),
            pa.field("amount", pa.string()),
        ]
    )


@_requires_pyarrow
def test_parquet_threading_uses_shared_execution_policy() -> None:
    from schema_sanitizer.adapters.parquet import memory as pyarrow_adapter

    assert not pyarrow_adapter.parquet_use_threads("single", None)
    assert not pyarrow_adapter.parquet_use_threads("single", 256 * 1024 * 1024)
    assert not pyarrow_adapter.parquet_use_threads("multi", 1)


@_requires_pyarrow
def test_parquet_stream_result_drop_closes_reader(tmp_path: Path, require_native: None) -> None:
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    assert getattr(out, "_keepalive", None) is not None

    del out
    gc.collect()


@_requires_pyarrow
def test_parquet_stream_survives_sink_result_drop(tmp_path: Path, require_native: None) -> None:
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    stream = out.stream
    assert stream is not None
    assert getattr(stream, "_keepalive", None) is not None

    del out
    gc.collect()

    assert sum(batch.num_rows for batch in stream) == 3
    stream.close()


@_requires_pyarrow
def test_parquet_stream_drop_releases_reader(tmp_path: Path, require_native: None) -> None:
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    stream = out.stream
    assert stream is not None

    del out
    del stream
    gc.collect()


@_requires_pyarrow
def test_parquet_conversion_enforces_memory_limit_bytes(
    tmp_path: Path, require_native: None
) -> None:
    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_parquet(path, memory_limit_bytes=1)

    err = excinfo.value
    assert getattr(err, "code", None) == "E_RESOURCE_LIMIT"
    assert "memory_limit_bytes" in str(err)
    assert err.detail is not None
    assert err.detail["stage"] == "parquet_conversion"


@_requires_pyarrow
def test_arrow_ipc_inputs_are_not_public(tmp_path: Path, require_native: None) -> None:
    path = tmp_path / "data.feather"
    feather.write_feather(sample_table(pa), path)

    with pytest.raises(Exception, match=r"requires extension"):
        read_test_parquet(path)
