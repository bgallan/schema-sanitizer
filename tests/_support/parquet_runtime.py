"""Optional PyArrow imports and fixtures shared by Parquet runtime tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None  # type: ignore[assignment]
    HAVE_PYARROW = False

requires_pyarrow = pytest.mark.skipif(
    not HAVE_PYARROW,
    reason="pyarrow not installed",
)


def sample_table(pyarrow: Any) -> Any:
    """Return the canonical two-column Parquet runtime test table."""
    return pyarrow.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def write_read_native_parquet(
    table: Any,
    path: Path,
    *,
    feature: str = "test",
    parquet_compression: str = "uncompressed",
) -> dict[str, Any]:
    """Write and read one table through the certified native Parquet route."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature=feature,
        parquet_compression=parquet_compression,
    )
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature=feature)
    output = pa.RecordBatchReader.from_stream(factory).read_all()
    output.validate(full=True)
    assert output.schema.equals(table.schema)
    assert output.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    return info


def recursive_arrow_type(spec: object) -> object:
    """Build a PyArrow type from the recursive runtime-test grammar."""
    kind = spec[0]  # type: ignore[index]
    if kind == "int64":
        return pa.int64()
    if kind == "string":
        return pa.string()
    if kind == "bool":
        return pa.bool_()
    if kind == "float64":
        return pa.float64()
    if kind == "list":
        return pa.list_(recursive_arrow_type(spec[1]))  # type: ignore[index]
    if kind == "map":
        return pa.map_(pa.string(), recursive_arrow_type(spec[1]))  # type: ignore[index]
    if kind == "struct":
        return pa.struct(
            [
                pa.field(name, recursive_arrow_type(child))
                for name, child in spec[1]  # type: ignore[index]
            ]
        )
    raise AssertionError(kind)
