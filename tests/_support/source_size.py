"""Shared source-size guardrails for the integral concurrency architecture.

Most production units remain capped at 2,500 lines. A small, explicit set of
cross-language authority ledgers is allowed a larger bound because splitting
those ledgers would duplicate lock/commit ownership. The exceptions are tight
per-file ceilings, so this remains a growth gate instead of a blanket increase.
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_PRODUCT_SOURCE_LINE_LIMIT = 2_500

INTEGRAL_AUTHORITY_LINE_LIMITS: dict[Path, int] = {
    Path("cpp/src/internal/runtime/operation_task_arena.cc"): 4_000,
    Path("src/schema_sanitizer/core_impl/memory_budget.py"): 3_100,
    Path("src/schema_sanitizer/core_impl/process_resources.py"): 7_200,
    Path("src/schema_sanitizer/core_impl/retry_scheduler.py"): 2_800,
}


def product_source_line_limit(relative_path: Path) -> int:
    """Return the explicit line ceiling for one repository-relative source."""
    return INTEGRAL_AUTHORITY_LINE_LIMITS.get(
        relative_path,
        DEFAULT_PRODUCT_SOURCE_LINE_LIMIT,
    )


def oversized_product_sources(lengths: dict[Path, int]) -> dict[Path, int]:
    """Return sources exceeding their default or authority-specific ceiling."""
    return {path: size for path, size in lengths.items() if size > product_source_line_limit(path)}
