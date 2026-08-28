"""PyArrow CSV sink for record-batch streams.

It consumes Arrow batches as CSV, delegates nested rendering to the native path when
required, and reports final writer statistics.
"""

from __future__ import annotations

from typing import Any

from ...core_impl.atomic_output import atomic_local_output
from ...core_impl.dependencies import ensure_pyarrow
from ...core_impl.execution_policy import normalize_threading_mode
from ...core_impl.native_symbols import (
    CSV_NESTED_STREAM_WRAP,
    CSV_SCHEMA_SUPPORTED,
    CSV_STREAM_WRITE,
)
from ...core_impl.uris import local_output_path_or_reject_remote
from .file_metadata import prepare_file_output_metadata_stream
from .metadata_native import CapsuleArrowStream
from .metadata_specs import (
    AllRowColumns,
    FirstRowColumns,
    RowSpanColumns,
    TimestampColumns,
)


def native_csv_nested_reader(stream: Any, *, pa: Any, memory_limit_bytes: int | None = None) -> Any:
    """Return a reader that renders top-level nested columns as JSON strings."""
    capsule = CSV_NESTED_STREAM_WRAP(
        stream, -1 if memory_limit_bytes is None else memory_limit_bytes
    )
    return pa.RecordBatchReader.from_stream(CapsuleArrowStream(capsule))


def _schema_has_nested_columns(schema: Any, *, pa: Any) -> bool:
    """Return whether CSV output needs native nested-value rendering."""
    return any(pa.types.is_nested(field.type) for field in schema)


def _native_csv_schema_supported(schema: Any) -> bool:
    """Return whether the native CSV writer can serialize a schema."""
    try:
        return bool(CSV_SCHEMA_SUPPORTED(schema))
    except TypeError:
        return False


def _write_native_csv(
    stream: Any,
    metadata: Any,
    out_path: Any,
    *,
    has_metadata: bool,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> Any:
    """Write through the native CSV writer or fail before fallback materialization."""
    mode = normalize_threading_mode(threading_mode)
    native_write = CSV_STREAM_WRITE
    native_stream = metadata.reader if metadata.reader is not None else stream
    if has_metadata and metadata.reader is None:
        raise RuntimeError(
            "CSV output metadata columns require the native C++ metadata stream wrapper."
        )
    if not _native_csv_schema_supported(metadata.schema):
        raise RuntimeError("CSV output requires a schema supported by the native C++ CSV writer.")
    output_path = local_output_path_or_reject_remote(out_path, sink_name="CSV")
    with atomic_local_output(output_path) as staged_path:
        return (
            native_write(
                native_stream,
                staged_path,
                -1 if memory_limit_bytes is None else memory_limit_bytes,
                0 if mode == "single" else 1,
            )
            or True
        )


def write_csv_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: FirstRowColumns = None,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> Any:
    """Write an Arrow batch stream to CSV."""
    pa = ensure_pyarrow(feature=feature)
    if _schema_has_nested_columns(stream.schema, pa=pa):
        native_base = native_csv_nested_reader(stream, pa=pa, memory_limit_bytes=memory_limit_bytes)
        if native_base is None:
            raise RuntimeError("CSV nested columns require the native C++ CSV nested renderer.")
        stream = native_base
    metadata = prepare_file_output_metadata_stream(
        stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
        pa=pa,
    )

    try:
        stats = _write_native_csv(
            stream,
            metadata,
            out_path,
            has_metadata=metadata.has_metadata,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        return stats
    finally:
        metadata.close()
