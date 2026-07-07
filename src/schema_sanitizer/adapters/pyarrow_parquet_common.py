"""Shared helpers for PyArrow Parquet input adapters."""

from __future__ import annotations

from typing import Any

from .pyarrow_common import ensure_pyarrow

DEFAULT_PARQUET_BATCH_ROWS = 65536
ESTIMATED_ROW_BYTES = 4096
THREADED_PARQUET_MIN_MEMORY_BYTES = 256 * 1024 * 1024


def parquet_batch_size_from_memory_limit(memory_limit_bytes: int | None) -> int:
    """Return a conservative Parquet batch size for a memory limit."""
    if memory_limit_bytes is None or memory_limit_bytes <= 0:
        return DEFAULT_PARQUET_BATCH_ROWS
    return max(1, min(DEFAULT_PARQUET_BATCH_ROWS, memory_limit_bytes // ESTIMATED_ROW_BYTES))


def parquet_memory_limit_allows_direct_ingest(memory_limit_bytes: int | None) -> bool:
    """Return whether a memory limit leaves room for direct Arrow batches."""
    return (
        memory_limit_bytes is None
        or memory_limit_bytes <= 0
        or memory_limit_bytes >= ESTIMATED_ROW_BYTES
    )


def parquet_use_threads_from_memory_limit(memory_limit_bytes: int | None) -> bool:
    """Return whether Parquet decoding can safely use PyArrow worker threads."""
    return (
        memory_limit_bytes is None
        or memory_limit_bytes <= 0
        or memory_limit_bytes >= THREADED_PARQUET_MIN_MEMORY_BYTES
    )


def open_parquet_source(data: Any, *, source: str, feature: str, pa: Any) -> tuple[Any, Any | None]:
    """Open a Parquet source and return ``(source, owned_file)``."""
    if source == "path":
        import os

        return os.fspath(data), None
    if source == "uri":
        raise ValueError(f"{feature} URI inputs must be staged before Parquet decoding")
    if source == "text":
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"{feature} expects bytes for source='text', got {type(data)!r}")
        opened_file = pa.BufferReader(bytes(data))
        return opened_file, opened_file
    if source == "stream":
        seek = getattr(data, "seek", None)
        if not callable(seek):
            raise TypeError("Parquet stream inputs require seek(0)")
        seek(0)
        return data, None
    raise TypeError(f"Unsupported Parquet source: {source!r}")


__all__ = [
    "DEFAULT_PARQUET_BATCH_ROWS",
    "ESTIMATED_ROW_BYTES",
    "THREADED_PARQUET_MIN_MEMORY_BYTES",
    "ensure_pyarrow",
    "open_parquet_source",
    "parquet_batch_size_from_memory_limit",
    "parquet_memory_limit_allows_direct_ingest",
    "parquet_use_threads_from_memory_limit",
]
