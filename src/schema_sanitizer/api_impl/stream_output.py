"""Native-first stream-to-file execution."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

from ..core_impl.error_translation import translate_core_error
from ..core_impl.execution_policy import normalize_threading_mode
from ..core_impl.generated_metadata import TimestampColumns
from ..core_impl.native_results import SinkOutput as NativeSinkOutput
from ..core_impl.resource_lifecycle import _close_suppressing_errors
from ..input_impl.selection import _Format, _Source, resolve_source_and_format
from ..options_impl.call_options import unwrap_options
from ..options_impl.options import Options
from .file_conversion.writers import (
    try_write_raw_native_file_output,
    write_parquet_native_first_stream,
)
from .ingest import normalize_options
from .output_diagnostics import patch_file_output_diagnostics
from .parquet.direct_routes import parquet_direct_sink_raw_or_none
from .parquet.replay_stream import make_replayable_parquet_stream
from .results import Result
from .streams import Stream


def close_sink_output_or_stream(sink_out: Any, stream: Any = None) -> None:
    """Close a sink output or its fallback stream without surfacing cleanup errors."""
    target = sink_out if callable(getattr(sink_out, "close", None)) else stream
    _close_suppressing_errors(target)


def close_consumed_stream(stream: Any) -> None:
    """Close a stream after a writer has consumed its main Arrow stream."""
    close_main = getattr(stream, "close_main_stream", None)
    if callable(close_main):
        close_main()
    else:
        stream.close()


def diagnostics_only_result(raw: Any) -> Result:
    """Return a file-writer Result carrying diagnostics without a materialized table."""
    return Result(
        SimpleNamespace(diagnostics=getattr(raw, "diagnostics", None)),
        clean_data=None,
    )


def _stream_from_sink_or_close(sink_out: Any) -> Any:
    """Return the sink stream and close ownership when it is unavailable."""
    try:
        stream = sink_out.stream
        if stream is None:
            raise RuntimeError("streaming ingestion did not produce a stream")
    except Exception:
        close_sink_output_or_stream(sink_out)
        raise
    return stream


def _parquet_writer_kwargs(
    parquet_compression: str | None,
    parquet_gzip_level: int | None,
) -> dict[str, Any]:
    """Return only explicitly configured Parquet writer options."""
    if parquet_compression is None and parquet_gzip_level is None:
        return {}
    return {
        "parquet_compression": parquet_compression,
        "parquet_gzip_level": parquet_gzip_level,
    }


def _operation_limits(call_options: Options | None) -> tuple[int | None, str]:
    """Return the canonical memory limit and threading mode for file output."""
    if call_options is None:
        return None, "single"
    return (
        call_options.memory_limit_bytes,
        normalize_threading_mode(call_options.threading_mode),
    )


def _is_internal_native_stream(raw: Any) -> bool:
    """Return whether a stream is owned by the current native operation."""
    return isinstance(raw, NativeSinkOutput)


def _make_parquet_replay(raw: Any, *, feature: str, memory_limit_bytes: int | None = None) -> Any:
    """Create a replay only when fallback may need a fresh stream."""
    return make_replayable_parquet_stream(
        raw, feature=feature, memory_limit_bytes=memory_limit_bytes
    )


def write_raw_stream_to_file(
    raw: Any,
    out_path: Any,
    *,
    writer: Callable[..., Any],
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> Result:
    """Write an already-open native stream using the best available writer."""
    stream = None
    replay = None
    internal_parquet = writer is write_parquet_native_first_stream and _is_internal_native_stream(
        raw
    )
    try:
        raw_for_native = raw
        if writer is write_parquet_native_first_stream and not internal_parquet:
            replay = _make_parquet_replay(
                raw, feature=feature, memory_limit_bytes=memory_limit_bytes
            )
            raw_for_native = replay.reader()
        native_stats = try_write_raw_native_file_output(
            raw_for_native,
            out_path,
            writer=writer,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            parquet_retry_is_safe=not internal_parquet,
        )
        if native_stats:
            result = diagnostics_only_result(raw)
            patch_file_output_diagnostics(result, out_path, feature, native_stats=native_stats)
            return result

        if internal_parquet:
            replay = _make_parquet_replay(
                raw, feature=feature, memory_limit_bytes=memory_limit_bytes
            )
        stream = replay.reader() if replay is not None else Stream(raw)
        native_stats = writer(
            stream,
            out_path,
            feature=feature,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            **_parquet_writer_kwargs(parquet_compression, parquet_gzip_level),
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        close_consumed_stream(stream)
        result = diagnostics_only_result(raw)
        patch_file_output_diagnostics(result, out_path, feature, native_stats=native_stats)
        return result
    except Exception as exc:
        close_sink_output_or_stream(raw, stream)
        raise translate_core_error(exc) from exc
    finally:
        if replay is not None:
            replay.close()
        close_sink_output_or_stream(raw)


def write_table_or_stream(
    data: Any,
    out_path: Any,
    *,
    call_options: Options | None,
    format: _Format,
    source: _Source,
    write_stream: Callable[[Any, Any], Any],
    raw_writer: Callable[..., Any] | None = None,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    feature: str | None = None,
) -> Result:
    """Ingest input and write its stream without table materialization."""
    from .execution_context import default_pool

    sink_out = (
        default_pool()
        .get()
        .to_sink(
            data,
            sink="stream",
            options=call_options,
            format=format,
            source=source,
        )
    )
    memory_limit_bytes, threading_mode = _operation_limits(call_options)
    replay = None
    try:
        raw_for_native = sink_out.raw
        internal_parquet = raw_writer is write_parquet_native_first_stream
        native_stats = (
            try_write_raw_native_file_output(
                raw_for_native,
                out_path,
                writer=raw_writer,
                first_row_columns=first_row_columns,
                all_row_columns=all_row_columns,
                row_span_columns=row_span_columns,
                timestamp_columns=timestamp_columns,
                parquet_compression=parquet_compression,
                parquet_gzip_level=parquet_gzip_level,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
                parquet_retry_is_safe=not internal_parquet,
            )
            if raw_writer is not None
            else False
        )
        if native_stats:
            result = diagnostics_only_result(sink_out.raw)
            if feature is not None:
                patch_file_output_diagnostics(
                    result,
                    out_path,
                    feature,
                    native_stats=native_stats,
                )
            sink_out.close()
            return result

        if internal_parquet:
            replay = _make_parquet_replay(sink_out.raw, feature=feature or "Parquet file output")
        stream = replay.reader() if replay is not None else _stream_from_sink_or_close(sink_out)
        try:
            native_stats = write_stream(stream, out_path)
        except Exception as exc:
            close_sink_output_or_stream(sink_out, stream)
            raise translate_core_error(exc) from exc
        else:
            close_consumed_stream(stream)

        result = diagnostics_only_result(sink_out.raw)
        if feature is not None:
            patch_file_output_diagnostics(
                result,
                out_path,
                feature,
                native_stats=native_stats,
            )
        sink_out.close()
        return result
    finally:
        if replay is not None:
            replay.close()


def _try_write_direct_parquet_to_file(
    data: Any,
    out_path: Any,
    *,
    source: _Source,
    writer: Callable[..., Any],
    feature: str,
    call_options: Options | None,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    parquet_compression: str | None,
    parquet_gzip_level: int | None,
) -> Result | None:
    """Use the direct Parquet route when the input supports it."""
    from .execution_context import default_pool

    memory_limit_bytes, threading_mode = _operation_limits(call_options)
    direct_raw = parquet_direct_sink_raw_or_none(
        default_pool().get()._raw,
        data,
        sink="stream",
        source=source,
        feature=feature,
        call_options=call_options,
        prepared=unwrap_options(call_options),
    )
    if direct_raw is None:
        return None
    return write_raw_stream_to_file(
        direct_raw,
        out_path,
        writer=writer,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def write_with_file_output(
    data: Any,
    out_path: Any,
    *,
    options: Options | dict[str, Any] | None,
    format: _Format,
    source: _Source,
    writer: Callable[..., Any],
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Write an ingest stream to a file using native-first output."""
    call_options = normalize_options(options)
    memory_limit_bytes, threading_mode = _operation_limits(call_options)
    data, source, format = resolve_source_and_format(data, format=format, source=source)
    if format == "parquet":
        direct_result = _try_write_direct_parquet_to_file(
            data,
            out_path,
            source=source,
            writer=writer,
            feature=feature,
            call_options=call_options,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
        )
        if direct_result is not None:
            return direct_result

    writer_kwargs = _parquet_writer_kwargs(parquet_compression, parquet_gzip_level)
    return write_table_or_stream(
        data,
        out_path,
        call_options=call_options,
        format=format,
        source=source,
        write_stream=lambda stream, path: writer(
            stream,
            path,
            feature=feature,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            **writer_kwargs,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        ),
        raw_writer=writer,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
        feature=feature,
    )
