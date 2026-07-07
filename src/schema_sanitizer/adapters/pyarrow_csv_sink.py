"""PyArrow CSV sink for record-batch streams."""

from __future__ import annotations

from typing import Any

from ..api_impl.file_output_metadata import (
    AllRowColumns,
    FirstRowColumns,
    RowSpanColumns,
    TimestampColumns,
    prepare_file_output_metadata_stream,
)
from ..core_impl.native_functions import CSV_SCHEMA_SUPPORTED, CSV_STREAM_WRITE
from ..core_impl.path_uris import local_path_or_reject_remote
from .pyarrow_common import ensure_pyarrow
from .pyarrow_csv_native import mark_csv_nested_route, native_csv_nested_reader
from .pyarrow_csv_values import nested_column_indices

_LAST_CSV_STREAM_ROUTE = "none"


def last_csv_stream_route() -> str:
    """Return the route used by the most recent CSV stream write."""
    return _LAST_CSV_STREAM_ROUTE


def mark_csv_stream_route(route: str) -> None:
    """Record the route used by a direct CSV writer."""
    global _LAST_CSV_STREAM_ROUTE
    _LAST_CSV_STREAM_ROUTE = route


def _native_csv_write_func() -> Any | None:
    """Return the native CSV writer when it is available."""
    return CSV_STREAM_WRITE.get()


def _native_csv_schema_supported(schema: Any) -> bool:
    """Return whether the native CSV writer can serialize a schema."""
    supported = CSV_SCHEMA_SUPPORTED.get()
    if supported is None:
        return False
    try:
        return bool(supported(schema))
    except TypeError:
        return False


def _local_output_path(out_path: Any) -> str:
    """Return a local output path or reject unstaged remote targets."""
    return local_path_or_reject_remote(
        out_path,
        remote_error="Remote outputs must be staged before CSV sink writing",
    )


def _write_native_csv(
    stream: Any,
    metadata: Any,
    out_path: Any,
    *,
    has_metadata: bool,
) -> Any:
    """Write through the native CSV writer or fail before fallback materialization."""
    native_write = _native_csv_write_func()
    if native_write is None:
        raise RuntimeError("CSV output requires the native C++ CSV stream writer.")
    native_stream = metadata.reader if metadata.reader is not None else stream
    if has_metadata and metadata.reader is None:
        raise RuntimeError(
            "CSV output metadata columns require the native C++ metadata stream wrapper."
        )
    if not _native_csv_schema_supported(metadata.schema):
        raise RuntimeError("CSV output requires a schema supported by the native C++ CSV writer.")
    return native_write(native_stream, _local_output_path(out_path)) or True


def write_csv_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: FirstRowColumns = None,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
) -> Any:
    """Write an Arrow batch stream to CSV."""
    global _LAST_CSV_STREAM_ROUTE
    _LAST_CSV_STREAM_ROUTE = "none"
    pa = ensure_pyarrow(feature=feature)
    base_nested_indices = nested_column_indices(stream.schema, pa=pa)
    if base_nested_indices:
        native_base = native_csv_nested_reader(stream, pa=pa)
        if native_base is None:
            raise RuntimeError("CSV nested columns require the native C++ CSV nested renderer.")
        stream = native_base
    else:
        mark_csv_nested_route("not_needed")
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
        )
        _LAST_CSV_STREAM_ROUTE = "native"
        return stats
    finally:
        metadata.close()
