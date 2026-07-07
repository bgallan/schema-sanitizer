"""Execution-context pooling and public pool facade exports."""

from __future__ import annotations

from typing import Any

from . import stream_file_writers as _stream_file_writers
from . import stream_writer_core as _stream_writer_core
from .context import ExecutionContext
from .ingest_runtime_selectors import (
    _Format,
    _resolve_source_and_format,
    _Source,
)
from .ingest_runtime_types import Result
from .native_ingest_plan import normalize_options
from .shared import Options, _maybe_enforce_memory_limit
from .source_plan import source_plan_from_data

_close_sink_output_full = _stream_writer_core.close_sink_output_full
_stream_from_sink_or_close = _stream_writer_core.stream_from_sink_or_close
_to_csv_stream = _stream_file_writers._to_csv_stream
_to_jsonl_stream = _stream_file_writers._to_jsonl_stream
_to_parquet_stream = _stream_file_writers._to_parquet_stream
_write_table_or_stream = _stream_writer_core.write_table_or_stream


class ExecutionContextPool:
    """Process-local cache for an :class:`ExecutionContext`."""

    def __init__(self):
        """Create an empty execution context cache."""
        self._ctx: ExecutionContext | None = None

    def get(self) -> ExecutionContext:
        """Return the cached execution context, creating it when needed."""
        ctx = self._ctx
        if ctx is None:
            ctx = ExecutionContext()
            self._ctx = ctx
        return ctx

    def close(self) -> None:
        """Discard the cached execution context."""
        self._ctx = None

    def __enter__(self) -> ExecutionContextPool:
        """Return the pool for context manager use."""
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        """Close the pool when leaving a context."""
        self.close()


_DEFAULT_POOL = ExecutionContextPool()


def default_pool() -> ExecutionContextPool:
    """Return the process-local default context pool."""
    return _DEFAULT_POOL


def to_table(
    data: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
) -> Result:
    """Materialize input as a table using the default context pool."""
    call_options = normalize_options(options)
    if source_plan_from_data(data) is not None:
        ctx = default_pool().get()
        return ctx.to_table(data, options=call_options, format=format, source=source)

    data, source, format = _resolve_source_and_format(
        data,
        format=format,
        source=source,
    )

    memory_limit_bytes = None
    if isinstance(call_options, Options):
        memory_limit_bytes = call_options.performance.memory_limit_bytes
    _maybe_enforce_memory_limit(data, format, memory_limit_bytes=memory_limit_bytes, source=source)

    ctx = default_pool().get()
    return ctx.to_table(data, options=call_options, format=format, source=source)


__all__ = [
    "ExecutionContextPool",
    "default_pool",
    "to_table",
]
