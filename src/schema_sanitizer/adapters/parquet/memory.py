"""Memory policy for Parquet batch decoding.

It derives safe decode batch sizes and direct-ingest eligibility from the operation
memory limit and estimated row width.
"""

from __future__ import annotations

from typing import Any

DEFAULT_PARQUET_BATCH_ROWS = 65536
ESTIMATED_ROW_BYTES = 4096


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


def parquet_use_threads(threading_mode: str, memory_limit_bytes: int | None) -> bool:
    """Return whether the shared policy permits PyArrow worker threads."""
    from ...core_impl.execution_policy import execution_policy

    return execution_policy(threading_mode, memory_limit_bytes).pyarrow_use_threads


def _native_parquet_max_row_group_rows(info: dict[str, Any] | None) -> int:
    """Return the largest row group size visible in native footer diagnostics."""
    info = info or {}
    max_rows = 0
    for row_group in info.get("row_groups") or []:
        try:
            max_rows = max(max_rows, int(row_group.get("num_rows") or 0))
        except (AttributeError, TypeError, ValueError):
            continue
    if max_rows <= 0:
        try:
            row_group_count = int(info.get("row_group_count") or 0)
            num_rows = int(info.get("num_rows") or 0)
        except (TypeError, ValueError):
            row_group_count = 0
            num_rows = 0
        if row_group_count == 1 and num_rows > 0:
            max_rows = num_rows
    return max_rows


def _native_parquet_batch_size_contract_issue(
    info: dict[str, Any] | None,
    batch_size: int | None,
) -> str | None:
    """Return the native batch-size blocker that runtime would use, if any."""
    if batch_size is None:
        return None
    try:
        requested_batch_size = int(batch_size)
    except (TypeError, ValueError):
        return f"invalid native reader batch_size: {batch_size!r}"
    if requested_batch_size <= 0:
        return None
    max_row_group_rows = _native_parquet_max_row_group_rows(info)
    if max_row_group_rows <= requested_batch_size:
        return None
    return (
        f"native reader row group has {max_row_group_rows} rows but requested "
        f"batch_size is {requested_batch_size}"
    )
