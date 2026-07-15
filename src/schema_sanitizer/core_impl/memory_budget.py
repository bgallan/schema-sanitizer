"""Derive every runtime resource budget from one per-operation memory limit."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MEMORY_LIMIT_BYTES = 512 * 1024 * 1024
MAX_MEMORY_LIMIT_BYTES = 64 * 1024 * 1024 * 1024
def normalize_memory_limit(memory_limit_bytes: int | None) -> int:
    """Return the effective positive per-operation memory limit.

    ``None`` selects the documented default. Values above the absolute native
    ceiling are rejected rather than silently weakening the safety contract.
    """
    if memory_limit_bytes is None:
        return DEFAULT_MEMORY_LIMIT_BYTES
    if isinstance(memory_limit_bytes, bool) or not isinstance(memory_limit_bytes, int):
        raise TypeError("Option 'memory_limit_bytes' must be an integer or None")
    if memory_limit_bytes <= 0:
        raise ValueError("Option 'memory_limit_bytes' must be > 0")
    if memory_limit_bytes > MAX_MEMORY_LIMIT_BYTES:
        raise ValueError(
            "Option 'memory_limit_bytes' exceeds the absolute 64 GiB safety ceiling"
        )
    return memory_limit_bytes



@dataclass(frozen=True, slots=True)
class MemoryBudget:
    """All internal limits deterministically derived from one memory budget."""

    total_bytes: int
    io_chunk_bytes: int
    batch_target_bytes: int
    coalesce_max_bytes: int
    metadata_bytes: int
    materialized_input_bytes: int
    replay_spool_bytes: int
    parquet_reader_buffer_bytes: int
    parquet_reader_rows: int
    parquet_row_group_bytes: int
    parquet_row_group_rows: int
    parquet_page_bytes: int
    parquet_footer_bytes: int
    async_concurrency: int
    async_prefetch_files: int
    async_retries: int
    async_timeout_seconds: float
    remote_chunk_prefetch: int
    source_discovery_concurrency: int

    @classmethod
    def from_limit(cls, memory_limit_bytes: int | None) -> "MemoryBudget":
        """Ask the native extension to derive all internal sub-budgets."""
        requested = (
            -1
            if memory_limit_bytes is None or memory_limit_bytes <= 0
            else normalize_memory_limit(memory_limit_bytes)
        )
        from .native_runtime import native_core

        values = native_core.memory_budget(requested)
        if not isinstance(values, tuple) or len(values) != 19:
            raise RuntimeError("native memory budget returned an invalid contract")
        return cls(*values)



def memory_budget(memory_limit_bytes: int | None) -> MemoryBudget:
    """Return the canonical derived budget for one public operation."""
    return MemoryBudget.from_limit(memory_limit_bytes)


__all__ = [
    "DEFAULT_MEMORY_LIMIT_BYTES",
    "MAX_MEMORY_LIMIT_BYTES",
    "MemoryBudget",
    "memory_budget",
    "normalize_memory_limit",
]
