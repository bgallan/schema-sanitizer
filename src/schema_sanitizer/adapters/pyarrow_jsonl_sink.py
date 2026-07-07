"""JSON Lines sink for PyArrow record-batch streams."""

from __future__ import annotations

import os
from typing import Any

from ..api_impl.file_output_metadata import (
    AllRowColumns,
    FirstRowColumns,
    RowSpanColumns,
    TimestampColumns,
    prepare_file_output_metadata_stream,
)
from ..core_impl.native_functions import (
    JSONL_SCHEMA_SUPPORTED,
    JSONL_STREAM_WRITE,
)
from ..core_impl.path_uris import local_path_or_reject_remote
from .pyarrow_common import ensure_pyarrow
from .pyarrow_schema_support import SchemaSupportCache

_JSONL_SCHEMA_SUPPORT_CACHE = SchemaSupportCache()
_LAST_JSONL_STREAM_ROUTE = "none"


def last_jsonl_stream_route() -> str:
    """Return the route used by the most recent JSONL stream write."""
    return _LAST_JSONL_STREAM_ROUTE


def mark_jsonl_stream_route(route: str) -> None:
    """Record the route used by a direct JSONL writer."""
    global _LAST_JSONL_STREAM_ROUTE
    _LAST_JSONL_STREAM_ROUTE = route


def _native_jsonl_schema_supported_func() -> Any | None:
    """Return the cached native JSONL schema-support checker."""
    return JSONL_SCHEMA_SUPPORTED.get()


def _schema_supports_native_jsonl(schema: Any, *, pa: Any) -> bool:
    """Return whether a schema can use the native JSONL writer."""
    del pa
    cached = _JSONL_SCHEMA_SUPPORT_CACHE.get_by_object(schema)
    if cached is not None:
        return cached

    cached = _JSONL_SCHEMA_SUPPORT_CACHE.get_by_text(schema)
    if cached is not None:
        return _JSONL_SCHEMA_SUPPORT_CACHE.set(schema, cached, include_text=False)

    schema_supported = _native_jsonl_schema_supported_func()
    if schema_supported is None:
        return _JSONL_SCHEMA_SUPPORT_CACHE.set(schema, False, include_text=True)

    try:
        supported = bool(schema_supported(schema))
    except TypeError:
        supported = False
    return _JSONL_SCHEMA_SUPPORT_CACHE.set(schema, supported, include_text=True)


def _native_jsonl_write_func() -> Any | None:
    """Return the native JSONL writer when it is available."""
    return JSONL_STREAM_WRITE.get()


def _local_output_path(out_path: Any) -> str:
    """Return a local output path or reject unstaged remote targets."""
    return local_path_or_reject_remote(
        os.fspath(out_path),
        remote_error="Remote outputs must be staged before JSONL sink writing",
    )


def _native_jsonl_stream_write(write: Any, stream: Any, out_path: Any, *, feature: str) -> Any:
    """Write JSONL natively to a local path."""
    del feature
    return write(stream, _local_output_path(out_path)) or True


def write_jsonl_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: FirstRowColumns = None,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
) -> Any:
    """Write an Arrow batch stream to JSON Lines."""
    global _LAST_JSONL_STREAM_ROUTE
    _LAST_JSONL_STREAM_ROUTE = "none"
    pa = ensure_pyarrow(feature=feature)
    metadata = prepare_file_output_metadata_stream(
        stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
        pa=pa,
    )
    try:
        native_jsonl_write = _native_jsonl_write_func()
        if native_jsonl_write is None:
            raise RuntimeError("JSONL output requires the native C++ JSONL stream writer.")
        if not _schema_supports_native_jsonl(metadata.schema, pa=pa):
            raise RuntimeError(
                "JSONL output requires a schema supported by the native C++ JSONL writer."
            )
        native_write_stream = metadata.reader if metadata.reader is not None else stream
        stats = _native_jsonl_stream_write(
            native_jsonl_write,
            native_write_stream,
            out_path,
            feature=feature,
        )
        _LAST_JSONL_STREAM_ROUTE = "native"
        return stats
    finally:
        metadata.close()
