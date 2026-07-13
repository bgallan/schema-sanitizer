"""Parquet output compression option normalization."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

_NATIVE_COMPRESSION_ENV = "SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION"
_NATIVE_GZIP_LEVEL_ENV = "SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL"


def normalize_parquet_compression(value: Any) -> str | None:
    """Validate public Parquet compression option values."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("Option 'parquet_compression' must be a string or None")
    compression = value.strip().lower()
    if compression not in {"gzip", "snappy", "uncompressed"}:
        raise ValueError("Option 'parquet_compression' must be 'gzip', 'snappy', or 'uncompressed'")
    return compression


def normalize_parquet_gzip_level(value: Any) -> int | None:
    """Normalize public Parquet gzip level option values."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("Option 'parquet_gzip_level' must be an int from 0 to 9 or None")
    if value < 0 or value > 9:
        raise ValueError("Option 'parquet_gzip_level' must be from 0 to 9")
    return value


def pyarrow_parquet_writer_options(
    *,
    parquet_compression: Any,
    parquet_gzip_level: Any,
) -> dict[str, Any]:
    """Return PyArrow ParquetWriter kwargs matching public compression options."""
    compression = normalize_parquet_compression(parquet_compression)
    gzip_level = normalize_parquet_gzip_level(parquet_gzip_level)
    if compression is None and gzip_level is None:
        return {}
    if compression is None:
        compression = "gzip"
    compression_name = {
        "gzip": "gzip",
        "snappy": "snappy",
        "uncompressed": "NONE",
    }[compression]
    out: dict[str, Any] = {
        "compression": compression_name,
    }
    if compression == "gzip" and gzip_level is not None:
        out["compression_level"] = gzip_level
    return out


@contextmanager
def native_parquet_compression_environment(
    *,
    parquet_compression: Any,
    parquet_gzip_level: Any,
):
    """Temporarily expose public compression settings to the native writer."""
    compression = normalize_parquet_compression(parquet_compression)
    gzip_level = normalize_parquet_gzip_level(parquet_gzip_level)
    if compression is None and gzip_level is None:
        yield
        return

    old_compression = os.environ.get(_NATIVE_COMPRESSION_ENV)
    old_gzip_level = os.environ.get(_NATIVE_GZIP_LEVEL_ENV)
    try:
        os.environ[_NATIVE_COMPRESSION_ENV] = compression or "gzip"
        if gzip_level is None:
            os.environ.pop(_NATIVE_GZIP_LEVEL_ENV, None)
        else:
            os.environ[_NATIVE_GZIP_LEVEL_ENV] = str(gzip_level)
        yield
    finally:
        if old_compression is None:
            os.environ.pop(_NATIVE_COMPRESSION_ENV, None)
        else:
            os.environ[_NATIVE_COMPRESSION_ENV] = old_compression
        if old_gzip_level is None:
            os.environ.pop(_NATIVE_GZIP_LEVEL_ENV, None)
        else:
            os.environ[_NATIVE_GZIP_LEVEL_ENV] = old_gzip_level


__all__ = [
    "native_parquet_compression_environment",
    "normalize_parquet_compression",
    "normalize_parquet_gzip_level",
    "pyarrow_parquet_writer_options",
]
