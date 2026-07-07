"""Shared timing helpers for local ingestion benchmarks."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from route_details import stream_stats_suffix


def time_call(
    label: str,
    func: Callable[[], Any],
    rows: int,
    repeats: int,
    *,
    describe: Callable[[Any], str] | None = None,
) -> None:
    """Measure one benchmark function and print best rows-per-second."""
    best_elapsed = float("inf")
    result = None
    for _ in range(repeats):
        start = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - start
        best_elapsed = min(best_elapsed, elapsed)
    elapsed = best_elapsed
    rate = rows / elapsed if elapsed else float("inf")
    suffix = f" rows/s={rate:,.0f}"
    suffix += stream_stats_suffix(result)
    if describe is not None:
        detail = describe(result)
        if detail:
            suffix += f" {detail}"
    print(f"{label}: {elapsed:.3f}s{suffix}")
