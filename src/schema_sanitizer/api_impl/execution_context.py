"""High-level execution context, sink routing, and process-local pooling."""

from __future__ import annotations

import os
from collections.abc import Sized
from contextlib import suppress
from threading import Lock
from types import SimpleNamespace
from typing import Any

from ..core_impl.error_translation import (
    call_core,
    reader_error_context,
    translate_core_error,
)
from ..core_impl.execution import ExecutionContext as _CoreExecutionContext
from ..core_impl.resource_lifecycle import (
    _cleanup_with_note,
    _close_keepalive_attr,
    _close_suppressing_errors,
)
from ..input_impl.prepared import ChainedKeepalive
from ..input_impl.selection import (
    _Format,
    _Source,
    prepare_native_text_data,
    resolve_source_and_format,
    unsupported_native_directory_ingestion,
)
from ..input_impl.source_plan import (
    PATH_SOURCES,
    REMOTE_CHUNKS,
    SEQUENCE,
    NativeSourcePlan,
    _flatten_path_source_sequence_or_none,
    _mark_native_path_sources_route,
    _path_sources_for_native,
)
from ..options_impl.call_options import attach_operation_detected_at, unwrap_options
from ..options_impl.options import Options, memory_limit_bytes_or_none
from ..remote_impl.staging import stage_remote_single_file
from .ingest import normalize_options, reject_unsupported_binary_direct_input
from .input.memory_limits import enforce_materialized_input_limit
from .input_lifetime import (
    operation_context_for_source_plan,
    operation_input_keepalive,
    reserve_materialized_input,
)
from .output_diagnostics import patch_table_diagnostics
from .parquet.direct_routes import parquet_direct_sink_raw_or_none
from .results import (
    TABLE_ADAPTER_FORMATS,
    Result,
    SinkResult,
)
from .source_plan.attached import source_plan_from_data
from .source_plan.remote import RemotePathSourceChunkProvider, prefetched_remote_chunks
from .streams import ArrowCStream
from .table_adapter_sink import materialize_table_adapter_sink


def _open_source_plan_sink_stream_or_none(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    sink: str,
    include_source_file: bool,
) -> Any | None:
    """Open a plain stream sink from the canonical native source plan."""
    if plan.kind == PATH_SOURCES:
        raw = raw_context.to_sink_path_sources(
            sink,
            _path_sources_for_native(plan),
            call_options,
            include_source_file=include_source_file,
            first_row_columns={},
            timestamp_columns=(),
        )
        _mark_native_path_sources_route()
        return raw

    if plan.kind == REMOTE_CHUNKS:
        retained_chunks, remaining_start = prefetched_remote_chunks(plan.payload)
        provider = RemotePathSourceChunkProvider(
            retained_chunks=retained_chunks,
            remaining_manifest=plan.payload,
            remaining_start=remaining_start,
        )
        try:
            raw = raw_context.to_sink_path_source_chunk_provider(
                sink,
                provider,
                call_options,
                include_source_file=include_source_file,
                first_row_columns={},
                timestamp_columns=(),
            )
            _mark_native_path_sources_route()
            return raw
        except BaseException as exc:
            _cleanup_with_note(
                exc, provider, label="remote source provider cleanup failed", method="close_all"
            )
            raise

    if plan.kind == SEQUENCE:
        flattened = _flatten_path_source_sequence_or_none(plan)
        if flattened is not None:
            return _open_source_plan_sink_stream_or_none(
                raw_context,
                flattened,
                call_options,
                sink=sink,
                include_source_file=include_source_file,
            )
    return None


def execution_context_to_sink(
    context: Any,
    data: Any,
    *,
    sink: str = "table",
    options: Any = None,
    format: _Format = "auto",
    source: _Source = "auto",
) -> Any:
    """Route an execution-context input to a named sink."""
    if not isinstance(sink, str):
        raise TypeError("sink must be a string")
    sink = sink.strip().lower()
    options = Options.from_dict(options) if isinstance(options, dict) else options

    if sink in TABLE_ADAPTER_FORMATS:
        return materialize_table_adapter_sink(
            context, data, sink=sink, options=options, format=format, source=source
        )

    memory_limit_bytes = None
    input_text_encoding = "utf-8"
    threading_mode = "single"
    if isinstance(options, Options):
        memory_limit_bytes = memory_limit_bytes_or_none(options)
        input_text_encoding = options.io.input_text_encoding
        threading_mode = options.performance.threading_mode

    source_plan = source_plan_from_data(data)
    operation_context, owns_operation_context = operation_context_for_source_plan(
        source_plan,
        threading_mode=threading_mode,
        memory_limit_bytes=memory_limit_bytes,
    )
    options = attach_operation_detected_at(
        options,
        operation_context.detected_at,
        operation_context.memory_ledger,
    )
    prepared = unwrap_options(options)

    if source_plan is not None and sink == "stream":
        try:
            raw = _open_source_plan_sink_stream_or_none(
                context._raw,
                source_plan,
                prepared,
                sink=sink,
                include_source_file=False,
            )
        except BaseException as exc:
            if owns_operation_context:
                _cleanup_with_note(
                    exc,
                    operation_context,
                    label="operation-context cleanup also failed after direct sink error",
                )
            raise
        if raw is not None:
            output = SinkResult(raw)
            if owns_operation_context:
                object.__setattr__(output, "_keepalive", operation_context)
            return output
        error = unsupported_native_directory_ingestion()
        if owns_operation_context:
            _cleanup_with_note(
                error,
                operation_context,
                label="operation-context cleanup also failed after unsupported input",
            )
        raise error

    try:
        data, source_name, format_name = resolve_source_and_format(
            data,
            format=format,
            source=source,
        )
        enforce_materialized_input_limit(
            data,
            format_name,
            memory_limit_bytes=memory_limit_bytes,
            source=source_name,
        )
    except BaseException as exc:
        if owns_operation_context:
            _cleanup_with_note(
                exc,
                operation_context,
                label="operation-context cleanup also failed after input-limit error",
            )
        raise

    try:
        input_reservation = reserve_materialized_input(
            operation_context,
            data,
            format_name,
            source_name=source_name,
        )
    except BaseException as exc:
        if owns_operation_context:
            _cleanup_with_note(
                exc,
                operation_context,
                label="operation-context cleanup also failed after reservation error",
            )
        raise

    keepalive = operation_input_keepalive(
        operation_context,
        owns_operation_context=owns_operation_context,
        input_reservation=input_reservation,
    )
    if source_name == "uri":
        try:
            staged = stage_remote_single_file(
                data,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
                operation_context=operation_context,
            )
        except BaseException as exc:
            if input_reservation is not None:
                _cleanup_with_note(
                    exc,
                    input_reservation,
                    label="input-reservation cleanup also failed after remote staging",
                )
            if owns_operation_context:
                _cleanup_with_note(
                    exc,
                    operation_context,
                    label="operation-context cleanup also failed after remote staging",
                )
            raise
        data = staged.path
        source_name = "path"
        keepalive = ChainedKeepalive(keepalive, staged) if keepalive is not None else staged

    if format_name == "parquet" and sink == "stream":
        try:
            raw = parquet_direct_sink_raw_or_none(
                context._raw,
                data,
                sink=sink,
                source=source_name,
                feature="parquet direct input",
                call_options=options,
                prepared=prepared,
            )
        except BaseException as exc:
            if keepalive is not None:
                _cleanup_with_note(
                    exc,
                    keepalive,
                    label="input keepalive cleanup also failed after Parquet sink error",
                )
            raise
        if raw is not None:
            output = SinkResult(raw)
            if keepalive is not None:
                object.__setattr__(output, "_keepalive", keepalive)
            return output

    try:
        data, source_name, format_name = reject_unsupported_binary_direct_input(
            data,
            source=source_name,
            format=format_name,
            memory_limit_bytes=memory_limit_bytes,
        )
    except BaseException as exc:
        if keepalive is not None:
            _cleanup_with_note(
                exc,
                keepalive,
                label="input keepalive cleanup also failed after binary-input rejection",
            )
        raise
    try:
        native_data, source_name = prepare_native_text_data(
            data,
            source=source_name,
            format_name=format_name,
            input_text_encoding=input_text_encoding,
            memory_limit_bytes=memory_limit_bytes,
        )
        if format_name == "python":
            raw = call_core(
                context._raw.to_sink_python,
                sink,
                native_data,
                prepared,
                error_context=reader_error_context("python", source_name, native_data),
            )
        else:
            raw = call_core(
                context._raw.to_sink_from_source,
                sink,
                format_name,
                source_name,
                native_data,
                prepared,
                error_context=reader_error_context(format_name, source_name, native_data),
            )
    except BaseException as exc:
        if keepalive is not None:
            _cleanup_with_note(
                exc,
                keepalive,
                label="input keepalive cleanup also failed after native sink error",
            )
        raise

    output = SinkResult(raw)
    if keepalive is not None:
        if hasattr(raw, "__arrow_c_stream__"):
            object.__setattr__(output, "_keepalive", keepalive)
        else:
            _close_suppressing_errors(keepalive)
    return output


def execution_context_to_table(
    context: Any,
    data: Any,
    options: Any = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
) -> Result:
    """Materialize input through the stream sink as a table result."""
    source_rows = len(data) if format == "python" and isinstance(data, Sized) else None
    output = context.to_sink(
        data,
        sink="stream",
        options=options,
        format=format,
        source=source,
    )
    result = Result(
        SimpleNamespace(
            table=ArrowCStream(output.raw),
            diagnostics=output.raw.diagnostics,
        )
    )
    keepalive = getattr(output, "_keepalive", None)
    if keepalive is not None:
        with suppress(Exception):
            object.__setattr__(result, "_keepalive", keepalive)
    try:
        try:
            table = result.clean_data
        except Exception as error:
            raise translate_core_error(error) from error
        with suppress(Exception):
            patch_table_diagnostics(output.raw, result, table, source_rows=source_rows)
    finally:
        _close_suppressing_errors(output.raw)
        if keepalive is not None:
            _close_keepalive_attr(result)
    return result


class ExecutionContext:
    """Ingestion execution context."""

    def __init__(self):
        """Create a high-level execution context."""
        self._pid = os.getpid()
        self._raw = call_core(_CoreExecutionContext)

    def _ensure_owner_process(self) -> None:
        """Reject direct reuse of one native context after fork."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError("execution context cannot be reused after fork")

    def memory_stats(self) -> dict[str, Any]:
        """Return memory statistics from the native context."""
        self._ensure_owner_process()
        return call_core(self._raw.memory_stats)

    def performance_stats(self) -> dict[str, Any]:
        """Return telemetry for the most recent operation in this context."""
        self._ensure_owner_process()
        return call_core(self._raw.performance_stats)

    def to_sink(
        self,
        data: Any,
        *,
        sink: str = "table",
        options: Options | None = None,
        format: _Format = "auto",
        source: _Source = "auto",
    ) -> Any:
        """Route input data to a named sink."""
        self._ensure_owner_process()
        return execution_context_to_sink(
            self,
            data,
            sink=sink,
            options=options,
            format=format,
            source=source,
        )

    def to_table(
        self,
        data: Any,
        options: Options | None = None,
        *,
        format: _Format = "auto",
        source: _Source = "auto",
    ) -> Result:
        """Materialize input data as a table result."""
        self._ensure_owner_process()
        return execution_context_to_table(
            self,
            data,
            options=options,
            format=format,
            source=source,
        )


class ExecutionContextPool:
    """Process-local cache for an :class:`ExecutionContext`."""

    def __init__(self):
        """Create an empty execution context cache."""
        self._pid = os.getpid()
        self._lock = Lock()
        self._ctx: ExecutionContext | None = None

    def _ensure_process(self) -> None:
        """Replace inherited cache state before acquiring its old lock."""
        pid = os.getpid()
        if pid == self._pid:
            return
        self._pid = pid
        self._lock = Lock()
        self._ctx = None

    def get(self) -> ExecutionContext:
        """Return one lazily constructed process-local execution context."""
        self._ensure_process()
        with self._lock:
            context = self._ctx
            if context is None:
                context = ExecutionContext()
                self._ctx = context
            return context

    def close(self) -> None:
        """Discard the cached execution context in its owning process."""
        self._ensure_process()
        with self._lock:
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
        return (
            default_pool()
            .get()
            .to_table(
                data,
                options=call_options,
                format=format,
                source=source,
            )
        )

    data, source_name, format_name = resolve_source_and_format(
        data,
        format=format,
        source=source,
    )
    memory_limit_bytes = None
    if isinstance(call_options, Options):
        memory_limit_bytes = memory_limit_bytes_or_none(call_options)
    enforce_materialized_input_limit(
        data,
        format_name,
        memory_limit_bytes=memory_limit_bytes,
        source=source_name,
    )
    return (
        default_pool()
        .get()
        .to_table(
            data,
            options=call_options,
            format=format_name,
            source=source_name,
        )
    )
