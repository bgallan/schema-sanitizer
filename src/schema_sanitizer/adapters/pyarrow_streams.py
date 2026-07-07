"""PyArrow stream materialization helpers."""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from .pyarrow_common import ensure_pyarrow


def table_from_stream_like(obj: Any, *, feature: str) -> Any:
    """Consume an Arrow stream-like object into a table."""
    pa = ensure_pyarrow(feature=feature)
    try:
        return pa.table(obj)
    finally:
        fn = getattr(obj, "close", None)
        if callable(fn):
            with suppress(Exception):
                fn()


def reader_from_stream_like(obj: Any, *, feature: str) -> Any:
    """Create a PyArrow record batch reader from a stream-like object."""
    pa = ensure_pyarrow(feature=feature)
    return pa.RecordBatchReader.from_stream(obj)


def is_record_batch_reader(obj: Any, *, feature: str) -> bool:
    """Return whether an object is a PyArrow record batch reader."""
    pa = ensure_pyarrow(feature=feature)
    return isinstance(obj, pa.RecordBatchReader)


__all__ = [
    "is_record_batch_reader",
    "reader_from_stream_like",
    "table_from_stream_like",
]
