"""Shared Parquet ingestion error helpers."""

from __future__ import annotations

from ...adapters.parquet.memory import ESTIMATED_ROW_BYTES
from ...errors import SchemaSanitizerResourceError


def unsupported_direct_parquet_ingestion() -> RuntimeError:
    """Return the public error for Parquet inputs that cannot use native Arrow."""
    return RuntimeError(
        "Parquet input requires the direct native Arrow path; this source or "
        "schema is not supported by direct Parquet ingestion."
    )


def direct_parquet_memory_limit_error(memory_limit_bytes: int) -> SchemaSanitizerResourceError:
    """Return the public resource error for a direct Parquet memory limit."""
    return SchemaSanitizerResourceError(
        "memory_limit_bytes limit exceeded during parquet_conversion: "
        f"{ESTIMATED_ROW_BYTES} bytes > {memory_limit_bytes} bytes",
        detail={
            "stage": "parquet_conversion",
            "limit_name": "memory_limit_bytes",
            "limit_bytes": memory_limit_bytes,
            "actual_bytes": ESTIMATED_ROW_BYTES,
        },
    )
