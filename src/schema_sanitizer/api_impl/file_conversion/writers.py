"""Native-first stream and raw file writers grouped by output format."""

from __future__ import annotations

import logging
from typing import Any

from ...adapters.parquet.sink import write_parquet_stream as _write_parquet_stream
from ...adapters.pyarrow.csv_sink import write_csv_stream as _write_csv_stream
from ...adapters.pyarrow.jsonl_sink import write_jsonl_stream as _write_jsonl_stream
from ..parquet.replay_stream import make_replayable_parquet_stream
from . import direct_writers as _native_output

_LAST_PARQUET_STREAM_ROUTE = "none"
_logger = logging.getLogger(__name__)


def write_jsonl_native_first_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
) -> Any:
    """Write JSONL using direct native output or the PyArrow sink path."""
    stats = _native_output.try_write_jsonl_direct_native(
        stream,
        out_path,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
    )
    if stats:
        return stats
    return _write_jsonl_stream(
        stream,
        out_path,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
    )


def write_csv_native_first_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
) -> Any:
    """Write CSV using direct native output or the PyArrow sink path."""
    stats = _native_output.try_write_csv_direct_native(
        stream,
        out_path,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
    )
    if stats:
        return stats
    return _write_csv_stream(
        stream,
        out_path,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
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


def set_last_parquet_stream_route(route: str) -> None:
    """Record the route selected for the current Parquet write."""
    global _LAST_PARQUET_STREAM_ROUTE
    _LAST_PARQUET_STREAM_ROUTE = route


def last_parquet_stream_route() -> str:
    """Return how the most recent Parquet stream write was routed."""
    return _LAST_PARQUET_STREAM_ROUTE


def write_parquet_native_first_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> None:
    """Write Parquet using direct native output or the PyArrow sink path."""
    set_last_parquet_stream_route("none")
    replay = make_replayable_parquet_stream(stream, feature=feature)
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
            )
        except RuntimeError as exc:
            if not should_retry_native_parquet_failure(exc):
                raise
            _log_native_parquet_fallback(exc)
            native_written = False
        if native_written:
            set_last_parquet_stream_route("native")
            return
        set_last_parquet_stream_route("pyarrow")
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
    timestamp_columns: tuple[str, ...] = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
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
        )
    if writer is write_csv_native_first_stream:
        return _native_output.try_write_csv_raw_direct_native(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
        )
    if writer is not write_parquet_native_first_stream:
        return False
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
        )
    except RuntimeError as exc:
        if not should_retry_native_parquet_failure(exc):
            raise
        _log_native_parquet_fallback(exc)
        return False
    if written:
        set_last_parquet_stream_route("native")
        return True
    return False
