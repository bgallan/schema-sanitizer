"""Native-first stream and raw file writers grouped by output format.

It selects native stream, raw stream, or fallback writers for JSONL, CSV, and Parquet
while preserving primary errors and cleanup ownership.
"""

from __future__ import annotations

import logging
from typing import Any

from ...adapters.parquet.sink import write_parquet_stream as _write_parquet_stream
from ...adapters.pyarrow.csv_sink import write_csv_stream as _write_csv_stream
from ...adapters.pyarrow.jsonl_sink import write_jsonl_stream as _write_jsonl_stream
from ...core_impl.generated_metadata import TimestampColumns
from ..parquet.replay_stream import make_replayable_parquet_stream
from . import direct_writers as _native_output

_logger = logging.getLogger(__name__)


def _metadata_route(
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
) -> str:
    """Select the native metadata route required by the requested columns."""
    return (
        "native"
        if _native_output.has_metadata_columns(
            first_row_columns, all_row_columns, row_span_columns, timestamp_columns
        )
        else "none"
    )


def _write_record_native_first(
    stream: Any,
    out_path: Any,
    *,
    direct_writer: Any,
    stream_writer: Any,
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> _native_output.FileWriteOutcome:
    """Try direct native output, then wrap stream-writer statistics as a file outcome."""
    options = {
        "first_row_columns": first_row_columns,
        "all_row_columns": all_row_columns,
        "row_span_columns": row_span_columns,
        "timestamp_columns": timestamp_columns,
        "memory_limit_bytes": memory_limit_bytes,
        "threading_mode": threading_mode,
    }
    outcome = direct_writer(stream, out_path, **options)
    if outcome is not None:
        return outcome
    stats = stream_writer(stream, out_path, feature=feature, **options)
    return _native_output.FileWriteOutcome(
        stats,
        "native_stream",
        _metadata_route(first_row_columns, all_row_columns, row_span_columns, timestamp_columns),
    )


def write_jsonl_native_first_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> _native_output.FileWriteOutcome:
    """Write JSONL using direct native output or the PyArrow sink path."""
    return _write_record_native_first(
        stream,
        out_path,
        direct_writer=lambda source, path, **options: _native_output.try_write_jsonl_direct_native(
            source, path, feature=feature, **options
        ),
        stream_writer=_write_jsonl_stream,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def write_csv_native_first_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> _native_output.FileWriteOutcome:
    """Write CSV using direct native output or the PyArrow sink path."""
    return _write_record_native_first(
        stream,
        out_path,
        direct_writer=_native_output.try_write_csv_direct_native,
        stream_writer=_write_csv_stream,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def _log_native_parquet_fallback(exc: RuntimeError) -> None:
    """Log that native Parquet failed and PyArrow retry will be used."""
    del exc
    _logger.exception("Native Parquet writer failed; retrying Parquet output with PyArrow.")


def should_retry_native_parquet_failure(exc: RuntimeError) -> bool:
    """Return whether a native Parquet RuntimeError should fall back to PyArrow."""
    message = str(exc)
    return not any(
        marker in message
        for marker in (
            "native Parquet writer: invalid gzip level",
            "native Parquet writer: unsupported compression",
        )
    )


def write_parquet_native_first_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> _native_output.FileWriteOutcome:
    """Write Parquet using direct native output or the PyArrow sink path."""
    replay = make_replayable_parquet_stream(
        stream, feature=feature, memory_limit_bytes=memory_limit_bytes
    )
    try:
        try:
            native_written = _native_output.try_write_parquet_direct_native(
                replay.reader(),
                out_path,
                first_row_columns=first_row_columns,
                all_row_columns=all_row_columns,
                row_span_columns=row_span_columns,
                timestamp_columns=timestamp_columns,
                parquet_compression=parquet_compression,
                parquet_gzip_level=parquet_gzip_level,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )
        except RuntimeError as exc:
            if not should_retry_native_parquet_failure(exc):
                raise
            _log_native_parquet_fallback(exc)
            native_written = None
        if native_written:
            return native_written
        _write_parquet_stream(
            replay.reader(),
            out_path,
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
        return _native_output.FileWriteOutcome(
            None,
            "pyarrow",
            _metadata_route(
                first_row_columns, all_row_columns, row_span_columns, timestamp_columns
            ),
        )
    finally:
        replay.close()


def try_write_raw_native_file_output(
    raw: Any,
    out_path: Any,
    *,
    writer: Any,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
    parquet_retry_is_safe: bool = True,
) -> Any:
    """Write a raw native stream without PyArrow when supported."""
    if writer is write_jsonl_native_first_stream:
        return _native_output.try_write_jsonl_raw_direct_native(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
    if writer is write_csv_native_first_stream:
        return _native_output.try_write_csv_raw_direct_native(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
    if writer is not write_parquet_native_first_stream:
        return None
    try:
        written = _native_output.try_write_parquet_raw_direct_native(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
    except RuntimeError as exc:
        if not parquet_retry_is_safe:
            raise
        if not should_retry_native_parquet_failure(exc):
            raise
        _log_native_parquet_fallback(exc)
        return None
    return written
