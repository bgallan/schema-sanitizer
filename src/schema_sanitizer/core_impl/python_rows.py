"""Streaming JSONL adapter for Python row iterables."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from .byte_reader_base import BufferedSeekableByteReader
from .native_functions import PYTHON_ROWS_JSONL_BYTES


def _native_python_rows_jsonl_bytes_func() -> Any | None:
    """Return the cached native Python-row JSONL batch encoder when available."""
    return PYTHON_ROWS_JSONL_BYTES.get()


_LAST_PYTHON_ROWS_ROUTE = "none"


def last_python_rows_route() -> str:
    """Return the route used by the most recent Python rows read."""
    return _LAST_PYTHON_ROWS_ROUTE


class PythonRowsJsonlByteReader(BufferedSeekableByteReader):
    """Seekable byte reader that serializes Python rows as JSON Lines."""

    def __init__(self, rows: Iterable[Any]):
        """Store rows for replayable native ingestion."""
        if isinstance(rows, Sequence):
            self._rows: Sequence[Any] = rows
        else:
            # Native ingestion may perform multiple passes. A one-shot iterable
            # must be retained as rows, but avoids also retaining one large JSONL
            # string as the previous implementation did.
            self._rows = list(rows)
        self._index = 0
        self._native_batch = _native_python_rows_jsonl_bytes_func()
        if self._native_batch is None:
            raise RuntimeError(
                "Python row input requires the native python_rows_jsonl_bytes encoder."
            )
        super().__init__("PythonRowsJsonlByteReader", default_chunk_bytes=1024 * 1024)

    def _append_native_rows(self, target_bytes: int) -> bool:
        """Append a native-encoded batch of rows to the byte buffer."""
        global _LAST_PYTHON_ROWS_ROUTE
        if self._index >= len(self._rows):
            return False
        try:
            payload, next_index = self._native_batch(
                self._rows,
                self._index,
                max(1, target_bytes),
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            raise RuntimeError("Native Python row JSONL encoding failed") from exc
        if next_index <= self._index:
            raise RuntimeError("Native Python row JSONL encoder did not make progress")
        self._buffer.extend(payload)
        self._index = next_index
        _LAST_PYTHON_ROWS_ROUTE = "native_batch"
        return True

    def _append_next(self, target_bytes: int) -> bool:
        """Append the next native-encoded row batch."""
        return self._append_native_rows(target_bytes)

    def _reset_reader(self) -> None:
        """Reset the row stream to the beginning."""
        self._index = 0
