"""Native-first file output writers for Arrow streams."""

from __future__ import annotations

import logging
import os
import tempfile
from contextlib import suppress
from typing import Any

from ..adapters.pyarrow_common import ensure_pyarrow
from ..adapters.pyarrow_csv_sink import (
    write_csv_stream as _write_csv_stream,
)
from ..adapters.pyarrow_jsonl_sink import (
    write_jsonl_stream as _write_jsonl_stream,
)
from ..adapters.pyarrow_parquet_sink import (
    write_parquet_stream as _write_parquet_stream,
)
from . import direct_native_file_output as _direct_native_output

NATIVE_OUTPUT_FORMATS = frozenset({"csv", "jsonl", "parquet"})
_LAST_PARQUET_STREAM_ROUTE = "none"
_logger = logging.getLogger(__name__)


class _ReplayReader:
    """Keep replay-file resources alive while exposing an Arrow stream reader."""

    def __init__(self, reader: Any, keepalive: tuple[Any, ...] = ()):
        """Initialize the replay reader wrapper."""
        self._reader = reader
        self._keepalive = keepalive
        self.schema = reader.schema

    def __iter__(self) -> "_ReplayReader":
        """Return this reader as its own iterator."""
        return self

    def __next__(self) -> Any:
        """Return the next replayed record batch."""
        return next(self._reader)

    def read_next_batch(self) -> Any:
        """Return the next replayed record batch through PyArrow's reader API."""
        return self._reader.read_next_batch()

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export the replay reader through the Arrow C Stream protocol."""
        fn = self._reader.__arrow_c_stream__
        if requested_schema is not None:
            with suppress(TypeError):
                return fn(requested_schema)
        return fn()

    def close(self) -> None:
        """Close the wrapped reader and release keepalive references."""
        close = getattr(self._reader, "close", None)
        if callable(close):
            with suppress(Exception):
                close()
        self._keepalive = ()


class ReplayableArrowStream:
    """Spool a one-shot Arrow stream so native Parquet can fail over safely."""

    def __init__(self, stream: Any, *, feature: str):
        """Initialize a temporary Arrow IPC spool from a stream-like object."""
        self._pa = ensure_pyarrow(feature=feature)
        self._path: str | None = None
        self.schema: Any
        self._spool(stream)

    def _reader_or_batches_from_stream(self, stream: Any) -> Any:
        """Return a batch source that can be copied into the replay spool."""
        pa = self._pa
        if isinstance(stream, pa.Table):
            self.schema = stream.schema
            return stream.to_batches()
        if hasattr(stream, "schema"):
            self.schema = stream.schema
        if isinstance(stream, pa.RecordBatchReader):
            self.schema = stream.schema
            return stream
        if hasattr(stream, "__arrow_c_stream__"):
            reader = pa.RecordBatchReader.from_stream(stream)
            self.schema = reader.schema
            return reader
        if hasattr(stream, "__iter__") and hasattr(stream, "schema"):
            return stream
        raise TypeError("Parquet output requires a replayable Arrow stream for safe fallback.")

    def _spool(self, stream: Any) -> None:
        """Copy the source stream into a temporary Arrow IPC file."""
        source = self._reader_or_batches_from_stream(stream)
        fd, path = tempfile.mkstemp(
            prefix="schema-sanitizer-parquet-replay-",
            suffix=".arrow",
        )
        os.close(fd)
        self._path = path
        try:
            with self._pa.OSFile(path, "wb") as sink:
                with self._pa.ipc.new_file(sink, self.schema) as writer:
                    if hasattr(source, "read_next_batch"):
                        while True:
                            try:
                                batch = source.read_next_batch()
                            except StopIteration:
                                break
                            writer.write_batch(batch)
                    else:
                        for batch in source:
                            writer.write_batch(batch)
        except Exception:
            self.close()
            raise

    def reader(self) -> _ReplayReader:
        """Return a fresh reader over the replayed batches."""
        if self._path is None:
            raise RuntimeError("Replayable Parquet stream has been closed.")
        source = self._pa.memory_map(self._path, "r")
        file_reader = self._pa.ipc.open_file(source)
        batches = [file_reader.get_batch(i) for i in range(file_reader.num_record_batches)]
        reader = self._pa.RecordBatchReader.from_batches(file_reader.schema, batches)
        return _ReplayReader(reader, keepalive=(source, file_reader))

    def close(self) -> None:
        """Delete the temporary Arrow IPC replay file."""
        if self._path is not None:
            with suppress(OSError):
                os.unlink(self._path)
            self._path = None


def make_replayable_parquet_stream(stream: Any, *, feature: str) -> ReplayableArrowStream:
    """Return a replayable Arrow stream used by native Parquet safety fallback."""
    return ReplayableArrowStream(stream, feature=feature)


def _log_native_parquet_fallback(exc: RuntimeError) -> None:
    """Log that native Parquet failed and PyArrow retry will be used."""
    del exc
    _logger.exception("Native Parquet writer failed; retrying Parquet output with PyArrow.")


def _should_retry_native_parquet_failure(exc: RuntimeError) -> bool:
    """Return whether a native Parquet RuntimeError should fall back to PyArrow."""
    message = str(exc)
    return not any(
        marker in message
        for marker in (
            "native Parquet writer: invalid gzip level",
            "native Parquet writer: unsupported compression",
        )
    )


def last_parquet_stream_route() -> str:
    """Return how the most recent Parquet stream write was routed."""
    return _LAST_PARQUET_STREAM_ROUTE


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
    """Write a raw native stream without PyArrow when the file writer supports it."""
    if writer is write_jsonl_native_first_stream:
        return _direct_native_output.try_write_jsonl_raw_direct_native(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
        )
    if writer is write_csv_native_first_stream:
        return _direct_native_output.try_write_csv_raw_direct_native(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
        )
    if writer is write_parquet_native_first_stream:
        try:
            parquet_written = _direct_native_output.try_write_parquet_raw_direct_native(
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
            if not _should_retry_native_parquet_failure(exc):
                raise
            _log_native_parquet_fallback(exc)
            return False
        if parquet_written:
            global _LAST_PARQUET_STREAM_ROUTE
            _LAST_PARQUET_STREAM_ROUTE = "native"
            return True
        return False
    return False


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
    stats = _direct_native_output.try_write_jsonl_direct_native(
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
    stats = _direct_native_output.try_write_csv_direct_native(
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
    global _LAST_PARQUET_STREAM_ROUTE
    _LAST_PARQUET_STREAM_ROUTE = "none"
    replay = make_replayable_parquet_stream(stream, feature=feature)
    try:
        try:
            native_written = _direct_native_output.try_write_parquet_direct_native(
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
            if not _should_retry_native_parquet_failure(exc):
                raise
            _log_native_parquet_fallback(exc)
            native_written = False
        if native_written:
            _LAST_PARQUET_STREAM_ROUTE = "native"
            return
        _LAST_PARQUET_STREAM_ROUTE = "pyarrow"
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
