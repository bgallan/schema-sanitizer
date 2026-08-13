"""Shared timing helpers for local ingestion benchmarks."""

from __future__ import annotations

import math
import statistics
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from benchmarks.ingestion.reporting import BenchmarkRecord, process_peak_rss_bytes
from benchmarks.ingestion.route_details import stream_stats_suffix

_RECORDS: list[BenchmarkRecord] = []
_DEFAULT_WARMUPS = 0


def set_default_warmups(value: int) -> None:
    """Configure warmups used by cases that do not override the value."""
    global _DEFAULT_WARMUPS
    _DEFAULT_WARMUPS = max(0, value)


def reset_records() -> None:
    """Clear records accumulated by prior benchmark runs."""
    _RECORDS.clear()


def records() -> list[BenchmarkRecord]:
    """Return a copy of accumulated benchmark records."""
    return list(_RECORDS)


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[index]


def _resolve_size(value: int | Path | Callable[[], int] | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, Path):
        return value.stat().st_size if value.exists() else 0
    if callable(value):
        return int(value())
    return int(value)


def time_call(
    label: str,
    func: Callable[[], Any],
    rows: int,
    repeats: int,
    *,
    warmups: int | None = None,
    input_bytes: int | Path | Callable[[], int] | None = None,
    output_bytes: int | Path | Callable[[], int] | None = None,
    describe: Callable[[Any], str] | None = None,
) -> BenchmarkRecord:
    """Run one case and report median/p95 throughput and resource metadata."""
    resolved_warmups = _DEFAULT_WARMUPS if warmups is None else max(0, warmups)
    for _ in range(resolved_warmups):
        func()

    elapsed_values: list[float] = []
    result = None
    for _ in range(max(1, repeats)):
        start = time.perf_counter()
        result = func()
        elapsed_values.append(time.perf_counter() - start)

    median_elapsed = statistics.median(elapsed_values)
    p95_elapsed = _p95(elapsed_values)
    row_rate = rows / median_elapsed if median_elapsed else float("inf")
    resolved_input_bytes = _resolve_size(input_bytes)
    byte_rate = (
        resolved_input_bytes / median_elapsed
        if resolved_input_bytes is not None and median_elapsed
        else None
    )
    resolved_output_bytes = _resolve_size(output_bytes)
    peak_rss = process_peak_rss_bytes()

    detail = ""
    if describe is not None:
        detail = describe(result)

    record = BenchmarkRecord(
        label=label,
        rows=rows,
        input_bytes=resolved_input_bytes,
        output_bytes=resolved_output_bytes,
        warmups=resolved_warmups,
        repeats=max(1, repeats),
        median_seconds=median_elapsed,
        p95_seconds=p95_elapsed,
        median_rows_per_second=row_rate,
        median_bytes_per_second=byte_rate,
        process_peak_rss_bytes=peak_rss,
        detail=detail,
    )
    _RECORDS.append(record)

    suffix = f" median={median_elapsed:.3f}s p95={p95_elapsed:.3f}s"
    suffix += f" rows/s={row_rate:,.0f}"
    if byte_rate is not None:
        suffix += f" bytes/s={byte_rate:,.0f}"
    if resolved_output_bytes is not None:
        suffix += f" output_bytes={resolved_output_bytes}"
    if peak_rss is not None:
        suffix += f" peak_rss={peak_rss}"
    suffix += stream_stats_suffix(result)
    if detail:
        suffix += f" {detail}"
    print(f"{label}:{suffix}")
    return record
