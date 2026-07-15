"""Parquet API/runtime tests split by contract area."""

from __future__ import annotations

import gc
from pathlib import Path

import pytest
from conftest import read_test_parquet, require_native
from parquet_runtime_support import sample_table

import schema_sanitizer as ss

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")

# Split from test_parquet_direct_io_runtime.py: test_direct_parquet_map_and_fixed_size_list_use_arrow_path, test_direct_parquet_duration_values_are_lossless_strings, test_native_arrow_schema_contract_payload_supports_new_direct_shapes, ...


@_requires_pyarrow
def test_direct_parquet_map_and_fixed_size_list_use_arrow_path(tmp_path: Path) -> None:
    """Verify direct Parquet handles map and fixed-size list columns."""
    require_native()
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
def test_direct_parquet_duration_values_are_lossless_strings(tmp_path: Path) -> None:
    """Verify direct Parquet handles duration values without JSONL fallback."""
    require_native()
    path = tmp_path / "duration.parquet"
    table = pa.table({"elapsed": pa.array([123, -5], type=pa.duration("us"))})
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.schema.field("elapsed").type == pa.string()
    assert result.clean_data.to_pylist() == [{"elapsed": "123us"}, {"elapsed": "-5us"}]


@_requires_pyarrow
def test_native_arrow_schema_contract_payload_supports_new_direct_shapes() -> None:
    """Verify native schema-contract encoding reuses the Arrow direct parser."""
    require_native()
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
def test_parquet_threading_uses_memory_guard() -> None:
    """Verify Parquet direct threading is gated by configured memory."""
    from schema_sanitizer.adapters.parquet import memory as pyarrow_adapter

    assert pyarrow_adapter.parquet_use_threads_from_memory_limit(None)
    assert pyarrow_adapter.parquet_use_threads_from_memory_limit(0)
    assert not pyarrow_adapter.parquet_use_threads_from_memory_limit(64 * 1024 * 1024)
    assert pyarrow_adapter.parquet_use_threads_from_memory_limit(256 * 1024 * 1024)


@_requires_pyarrow
def test_parquet_stream_result_drop_closes_reader(tmp_path: Path) -> None:
    """Verify parquet stream sink can be dropped without temporary files."""
    require_native()
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    assert getattr(out, "_keepalive", None) is None

    del out
    gc.collect()


@_requires_pyarrow
def test_parquet_stream_survives_sink_result_drop(tmp_path: Path) -> None:
    """Verify parquet stream owns the native reader after stream access."""
    require_native()
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    out = ExecutionContext().to_sink(path, sink="stream", format="parquet")
    stream = out.stream
    assert stream is not None
    assert getattr(stream, "_keepalive", None) is None

    del out
    gc.collect()

    assert sum(batch.num_rows for batch in stream) == 3
    stream.close()


@_requires_pyarrow
def test_parquet_stream_drop_releases_reader(tmp_path: Path) -> None:
    """Verify parquet stream can be dropped without explicit close."""
    require_native()
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
def test_parquet_conversion_enforces_memory_limit_bytes(tmp_path: Path) -> None:
    """Verify parquet conversion enforces memory limit bytes."""
    require_native()
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
def test_arrow_ipc_inputs_are_not_public(tmp_path: Path) -> None:
    """Verify arrow ipc inputs are not public."""
    require_native()
    path = tmp_path / "data.feather"
    feather.write_feather(sample_table(pa), path)

    with pytest.raises(Exception, match=r"requires extension"):
        read_test_parquet(path)
