"""Replayable Arrow stream storage for safe Parquet fallback."""

from __future__ import annotations

import os
import tempfile
from contextlib import suppress
from typing import Any

from ...core_impl.dependencies import ensure_pyarrow


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
        export = self._reader.__arrow_c_stream__
        if requested_schema is not None:
            with suppress(TypeError):
                return export(requested_schema)
        return export()

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
                with self._pa.ipc.new_stream(sink, self.schema) as writer:
                    if hasattr(source, "read_next_batch"):
                        self._copy_reader(source, writer)
                    else:
                        for batch in source:
                            writer.write_batch(batch)
        except Exception:
            self.close()
            raise

    @staticmethod
    def _copy_reader(source: Any, writer: Any) -> None:
        """Copy every batch from a reader-like source into an IPC writer."""
        while True:
            try:
                batch = source.read_next_batch()
            except StopIteration:
                return
            writer.write_batch(batch)

    def reader(self) -> _ReplayReader:
        """Return a fresh reader over the replayed batches."""
        if self._path is None:
            raise RuntimeError("Replayable Parquet stream has been closed.")
        source = self._pa.memory_map(self._path, "r")
        reader = self._pa.ipc.open_stream(source)
        return _ReplayReader(reader, keepalive=(source,))

    def close(self) -> None:
        """Delete the temporary Arrow IPC replay file."""
        if self._path is not None:
            with suppress(OSError):
                os.unlink(self._path)
            self._path = None


def make_replayable_parquet_stream(stream: Any, *, feature: str) -> ReplayableArrowStream:
    """Return a replayable Arrow stream used by native Parquet safety fallback."""
    return ReplayableArrowStream(stream, feature=feature)
