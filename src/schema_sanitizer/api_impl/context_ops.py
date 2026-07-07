"""Implements `schema_sanitizer.api_impl.context_ops`."""

from __future__ import annotations

from contextlib import suppress
from types import SimpleNamespace
from typing import Any

from .async_remote_io import stage_remote_single_file
from .ingest_input_prepare import prepare_native_text_data
from .ingest_lifecycle import (
    _close_keepalive_attr,
    _close_suppressing_errors,
)
from .ingest_runtime_binary import reject_unsupported_binary_direct_input
from .ingest_runtime_selectors import _Format, _resolve_source_and_format, _Source
from .ingest_runtime_types import ArrowCStream, Result, SinkResult
from .native_directory_errors import unsupported_native_directory_ingestion
from .parquet_direct import parquet_direct_sink_raw_or_none
from .shared import (
    Options,
    _call_core,
    _maybe_enforce_memory_limit,
    _translate_core_error,
    _unwrap_options,
)
from .source_plan import open_source_plan_sink_stream_or_none, source_plan_from_data
from .table_diagnostics import patch_table_diagnostics
from .table_output import TABLE_ADAPTER_FORMATS, convert_arrow_table_output


def _materialize_table_adapter_sink(
    ctx: Any,
    data: Any,
    *,
    sink: str,
    options: Any,
    format: _Format,
    source: _Source,
) -> Any:
    """Materialize a table-backed adapter sink."""
    result = ctx.to_table(data, options=options, format=format, source=source)
    table = result.clean_data
    if table is None:
        if sink == "duckdb":
            return None
        raise RuntimeError(f"{sink} sink: no table output")

    return convert_arrow_table_output(table, sink, feature=f"sink={sink!r}")


def execution_context_to_sink(
    self,
    data: Any,
    *,
    sink: str = "table",
    options: Any = None,
    format: _Format = "auto",
    source: _Source = "auto",
) -> Any:
    """Route an ExecutionContext input to a named sink.

    Keeps staged input files alive for stream-capable results and closes them before
    returning materialized results.
    """

    if not isinstance(sink, str):
        raise TypeError("sink must be a string")
    sink = sink.strip().lower()
    options = Options.from_dict(options) if isinstance(options, dict) else options

    # Built-in Python sinks (no user registry): pandas, polars, duckdb.
    if sink in TABLE_ADAPTER_FORMATS:
        return _materialize_table_adapter_sink(
            self, data, sink=sink, options=options, format=format, source=source
        )

    memory_limit_bytes = None
    input_text_encoding = "utf-8"
    if isinstance(options, Options):
        memory_limit_bytes = options.performance.memory_limit_bytes
        input_text_encoding = options.io.input_text_encoding
    prepared = _unwrap_options(options)

    source_plan = source_plan_from_data(data)
    if source_plan is not None and sink == "stream":
        field_name_policy = "lower_alpha"
        if isinstance(options, Options):
            field_name_policy = str(options.schema.field_name_policy)
        raw = open_source_plan_sink_stream_or_none(
            self._raw,
            source_plan,
            prepared,
            sink=sink,
            include_source_file=False,
            field_name_policy=field_name_policy,
            feature="source-plan stream sink",
        )
        if raw is not None:
            return SinkResult(raw)
        raise unsupported_native_directory_ingestion()

    data, src, fmt = _resolve_source_and_format(
        data,
        format=format,
        source=source,
    )

    _maybe_enforce_memory_limit(data, fmt, memory_limit_bytes=memory_limit_bytes, source=src)
    keepalive = None
    if src == "uri":
        staged = stage_remote_single_file(
            data,
            memory_limit_bytes=memory_limit_bytes,
        )
        data = staged.path
        src = "path"
        keepalive = staged

    if fmt == "parquet" and sink == "stream":
        raw = parquet_direct_sink_raw_or_none(
            self._raw,
            data,
            sink=sink,
            source=src,
            feature="parquet direct input",
            call_options=options,
            prepared=prepared,
        )
        if raw is not None:
            return SinkResult(raw)

    data, src, fmt = reject_unsupported_binary_direct_input(
        data,
        source=src,
        format=fmt,
        memory_limit_bytes=memory_limit_bytes,
    )
    try:
        native_data, src = prepare_native_text_data(
            data, src=src, fmt=fmt, input_text_encoding=input_text_encoding
        )
        if fmt == "python":
            raw = _call_core(self._raw.to_sink_python, sink, native_data, prepared)
        else:
            raw = _call_core(
                self._raw.to_sink_from_source,
                sink,
                fmt,
                src,
                native_data,
                prepared,
            )
    except Exception:
        if keepalive is not None:
            _close_suppressing_errors(keepalive)
        raise

    out = SinkResult(raw)
    if keepalive is not None:
        if hasattr(raw, "__arrow_c_stream__"):
            object.__setattr__(out, "_keepalive", keepalive)
        else:
            _close_suppressing_errors(keepalive)

    return out


def execution_context_to_table(
    self,
    data: Any,
    options: Any = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
) -> Result:
    """Materialize input through the stream sink as a table result."""
    source_rows = len(data) if format == "python" and isinstance(data, list) else None
    # Route table materialization through the stream sink. This keeps the native
    # surface area smaller while still returning a fully materialized PyArrow table.
    out = self.to_sink(
        data,
        sink="stream",
        options=options,
        format=format,
        source=source,
    )

    res = Result(
        SimpleNamespace(
            # Result materializes its default clean_data from this internal table.
            table=ArrowCStream(out.raw),
            # Diagnostics should always exist for native sinks.
            diagnostics=out.raw.diagnostics,
        )
    )
    ka = getattr(out, "_keepalive", None)
    if ka is not None:
        with suppress(Exception):
            object.__setattr__(res, "_keepalive", ka)
    try:
        # `to_table()` is materializing by contract. Force realization now so
        # any staged input file can be released before returning.
        try:
            table = res.clean_data
        except Exception as e:
            raise _translate_core_error(e) from e

        # Finalize diagnostics now, before closing native resources.
        #
        # The ABI3 runtime returns diagnostics as a JSON-backed snapshot taken
        # at sink creation time; patch table counters after materialization so
        # callers can reliably observe final row and batch counts.
        with suppress(Exception):
            patch_table_diagnostics(out.raw, res, table, source_rows=source_rows)

    finally:
        # If the stream sink allocates native resources, close as soon as we're done.
        _close_suppressing_errors(out.raw)

        if ka is not None:
            _close_keepalive_attr(res)
    return res
