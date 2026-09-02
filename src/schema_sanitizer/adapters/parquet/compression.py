"""Parquet output compression option normalization.

It validates codec names and gzip levels, resolves defaults, and produces the exact
settings consumed by native and PyArrow writers.
"""

from __future__ import annotations

from typing import Any


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


def native_parquet_writer_options(
    *, parquet_compression: Any, parquet_gzip_level: Any
) -> tuple[str, int]:
    """Return normalized native writer arguments without global state."""
    compression = normalize_parquet_compression(parquet_compression) or "gzip"
    gzip_level = normalize_parquet_gzip_level(parquet_gzip_level)
    return compression, -1 if gzip_level is None else gzip_level


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
    out: dict[str, Any] = {"compression": compression_name}
    if compression == "gzip" and gzip_level is not None:
        out["compression_level"] = gzip_level
    return out


__all__ = [
    "native_parquet_writer_options",
    "normalize_parquet_compression",
    "normalize_parquet_gzip_level",
    "pyarrow_parquet_writer_options",
]
