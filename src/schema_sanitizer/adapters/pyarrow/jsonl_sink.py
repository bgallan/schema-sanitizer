"""JSON Lines sink for PyArrow record-batch streams."""

from __future__ import annotations

from typing import Any

from ...core_impl.dependencies import ensure_pyarrow
from ...core_impl.native_symbols import (
    JSONL_SCHEMA_SUPPORTED,
    JSONL_STREAM_WRITE,
)
from ...core_impl.uris import local_output_path_or_reject_remote
from .file_metadata import prepare_file_output_metadata_stream
from .metadata_specs import (
    AllRowColumns,
    FirstRowColumns,
    RowSpanColumns,
    TimestampColumns,
)
from .schema_decision_cache import SchemaDecisionCache

_JSONL_SCHEMA_SUPPORT_CACHE = SchemaDecisionCache()
_LAST_JSONL_STREAM_ROUTE = "none"


def last_jsonl_stream_route() -> str:
    """Return the route used by the most recent JSONL stream write."""
    return _LAST_JSONL_STREAM_ROUTE


def mark_jsonl_stream_route(route: str) -> None:
    """Record the route used by a direct JSONL writer."""
    global _LAST_JSONL_STREAM_ROUTE
    _LAST_JSONL_STREAM_ROUTE = route


def _schema_supports_native_jsonl(schema: Any, *, pa: Any) -> bool:
    """Return whether a schema can use the native JSONL writer."""
    del pa
    cached = _JSONL_SCHEMA_SUPPORT_CACHE.get_by_object(schema)
    if cached is not None:
        return cached

    cached = _JSONL_SCHEMA_SUPPORT_CACHE.get_by_text(schema)
    if cached is not None:
        return _JSONL_SCHEMA_SUPPORT_CACHE.set(schema, cached, include_text=False)

    try:
        supported = bool(JSONL_SCHEMA_SUPPORTED(schema))
    except TypeError:
        supported = False
    return _JSONL_SCHEMA_SUPPORT_CACHE.set(schema, supported, include_text=True)


def _native_jsonl_stream_write(
    write: Any,
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    memory_limit_bytes: int | None,
) -> Any:
    """Write JSONL natively to a local path."""
    del feature
    return (
        write(
            stream,
            local_output_path_or_reject_remote(out_path, sink_name="JSONL"),
            -1 if memory_limit_bytes is None else memory_limit_bytes,
        )
        or True
    )


def write_jsonl_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: FirstRowColumns = None,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
    memory_limit_bytes: int | None = None,
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
        native_jsonl_write = JSONL_STREAM_WRITE
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
            memory_limit_bytes=memory_limit_bytes,
        )
        _LAST_JSONL_STREAM_ROUTE = "native"
        return stats
    finally:
        metadata.close()
