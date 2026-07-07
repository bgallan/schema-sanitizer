"""Shared lifecycle helpers for streaming file writers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .ingest_runtime_selectors import _Format, _resolve_source_and_format, _Source
from .ingest_runtime_types import Result, Stream
from .native_file_output import (
    make_replayable_parquet_stream,
    try_write_raw_native_file_output,
    write_parquet_native_first_stream,
)
from .native_ingest_plan import normalize_options
from .parquet_direct import parquet_direct_sink_raw_or_none
from .shared import Options, _unwrap_options
from .stream_writer_lifecycle import (
    close_consumed_stream,
    close_sink_output_or_stream,
    diagnostics_only_result,
)
from .table_diagnostics import patch_file_output_diagnostics


def close_sink_output_full(sink_out: Any, stream: Any = None) -> None:
    """Close a sink output or its fallback stream."""
    close_sink_output_or_stream(sink_out, stream)


def stream_from_sink_or_close(sink_out: Any) -> Any:
    """Return a sink stream, closing the sink if unavailable."""
    try:
        stream = sink_out.stream
        if stream is None:
            raise RuntimeError("streaming ingestion did not produce a stream")
    except Exception:
        close_sink_output_full(sink_out)
        raise
    return stream


def _parquet_writer_kwargs(
    parquet_compression: str | None,
    parquet_gzip_level: int | None,
) -> dict[str, Any]:
    """Return Parquet-only writer kwargs when explicitly configured."""
    if parquet_compression is None and parquet_gzip_level is None:
        return {}
    return {
        "parquet_compression": parquet_compression,
        "parquet_gzip_level": parquet_gzip_level,
    }


def write_raw_stream_to_file(
    raw: Any,
    out_path: Any,
    *,
    writer: Callable[..., None],
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Write a native raw stream to a file using the best available writer."""
    stream = None
    replay = None
    try:
        raw_for_native = raw
        if writer is write_parquet_native_first_stream:
            replay = make_replayable_parquet_stream(raw, feature=feature)
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
        )
        if native_stats:
            result = diagnostics_only_result(raw)
            patch_file_output_diagnostics(result, out_path, feature, native_stats=native_stats)
            return result
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
        )
        close_consumed_stream(stream)
        result = diagnostics_only_result(raw)
        patch_file_output_diagnostics(result, out_path, feature, native_stats=native_stats)
        return result
    except Exception:
        if stream is not None:
            close_sink_output_full(raw, stream)
        else:
            close_sink_output_full(raw)
        raise
    finally:
        if replay is not None:
            replay.close()
        close_sink_output_full(raw)


def try_write_direct_parquet_to_file(
    data: Any,
    out_path: Any,
    *,
    source: _Source,
    writer: Callable[..., None],
    feature: str,
    call_options: Options | None,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result | None:
    """Write direct Parquet output when the native Arrow path applies."""
    from .pool import default_pool

    direct_raw = parquet_direct_sink_raw_or_none(
        default_pool().get()._raw,
        data,
        sink="stream",
        source=source,
        feature=feature,
        call_options=call_options,
        prepared=_unwrap_options(call_options),
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
    )


def write_table_or_stream(
    data: Any,
    out_path: Any,
    *,
    options: Options | dict[str, Any] | None,
    format: _Format,
    source: _Source,
    write_stream: Callable[[Any, Any], None],
    raw_writer: Callable[..., None] | None = None,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    feature: str | None = None,
) -> Result:
    """Ingest input and write its stream without table materialization."""
    from .pool import default_pool

    call_options = normalize_options(options)
    ctx = default_pool().get()
    sink_out = ctx.to_sink(data, sink="stream", options=call_options, format=format, source=source)
    replay = None
    try:
        raw_for_native = sink_out.raw
        if raw_writer is write_parquet_native_first_stream:
            try:
                replay = make_replayable_parquet_stream(
                    sink_out.raw,
                    feature=feature or "Parquet file output",
                )
            except Exception:
                close_sink_output_full(sink_out)
                raise
            raw_for_native = replay.reader()
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
            )
            if raw_writer is not None
            else False
        )
        if native_stats:
            res = diagnostics_only_result(sink_out.raw)
            if feature is not None:
                patch_file_output_diagnostics(res, out_path, feature, native_stats=native_stats)
            sink_out.close()
            return res
        stream = replay.reader() if replay is not None else stream_from_sink_or_close(sink_out)

        try:
            native_stats = write_stream(stream, out_path)
        except Exception:
            close_sink_output_full(sink_out, stream)
            raise
        else:
            close_consumed_stream(stream)

        res = diagnostics_only_result(sink_out.raw)
        if feature is not None:
            patch_file_output_diagnostics(res, out_path, feature, native_stats=native_stats)
        sink_out.close()
        return res
    finally:
        if replay is not None:
            replay.close()


def write_with_file_output(
    data: Any,
    out_path: Any,
    *,
    options: Options | dict[str, Any] | None,
    format: _Format,
    source: _Source,
    writer: Callable[..., None],
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Write an ingest stream to a file using native-first output."""
    call_options = normalize_options(options)
    data, source, format = _resolve_source_and_format(data, format=format, source=source)
    if format == "parquet":
        direct_result = try_write_direct_parquet_to_file(
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
    return write_table_or_stream(
        data,
        out_path,
        options=call_options,
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
            **_parquet_writer_kwargs(parquet_compression, parquet_gzip_level),
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
