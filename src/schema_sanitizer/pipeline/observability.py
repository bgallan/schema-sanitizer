"""Reusable progress and summary helpers for partitioned pipelines."""

from __future__ import annotations

from typing import Any


def estimate_cpu_io_wall_time(
    wall_seconds: float,
    aggregate_cpu_seconds: float,
) -> tuple[float, float]:
    """Split wall time into bounded CPU and non-CPU estimates.

    Process CPU time is accumulated across threads and can exceed elapsed wall
    time. Cap it to the operation's wall interval before calculating the
    remainder so the two reported estimates are non-negative and additive.
    Concurrent I/O can overlap CPU work, so a zero remainder does not prove
    that no I/O occurred.
    """
    wall_seconds = max(float(wall_seconds), 0.0)
    aggregate_cpu_seconds = max(float(aggregate_cpu_seconds), 0.0)
    cpu_seconds = min(aggregate_cpu_seconds, wall_seconds)
    return cpu_seconds, wall_seconds - cpu_seconds


def cpu_io_wall_percentages(
    wall_seconds: float,
    aggregate_cpu_seconds: float,
) -> tuple[float, float]:
    """Return complementary CPU and estimated I/O percentages.

    CPU is rounded first and I/O is its complement so the displayed one-decimal
    percentages always add up to exactly 100.0%. A zero-duration interval is
    reported as 0% CPU and 100% I/O because no CPU time was observed.
    """
    wall_seconds = max(float(wall_seconds), 0.0)
    cpu_seconds, _ = estimate_cpu_io_wall_time(
        wall_seconds,
        aggregate_cpu_seconds,
    )
    if wall_seconds == 0.0:
        return 0.0, 100.0

    cpu_percent = round(cpu_seconds / wall_seconds * 100.0, 1)
    io_percent = round(100.0 - cpu_percent, 1)
    return cpu_percent, io_percent


def format_duration(seconds: float) -> str:
    """Format elapsed seconds compactly."""
    seconds = max(seconds, 0.0)
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {secs:.0f}s"

    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {secs:.0f}s"


def compact_uri(uri: str, *, max_len: int = 96) -> str:
    """Return a compact URI suitable for one-line logs."""
    if len(uri) <= max_len:
        return uri

    keep = max_len - 3
    left = keep // 3
    right = keep - left
    return f"{uri[:left]}...{uri[-right:]}"


def sample_items(items: list[Any], sample_size: int) -> list[Any]:
    """Return first/last sample without consuming the whole collection in logs."""
    if sample_size <= 0:
        return []
    if len(items) <= sample_size * 2:
        return items
    return items[:sample_size] + items[-sample_size:]


def compact_stats_for_log(stats: Any) -> str:
    """Extract useful scalar stats for compact one-line logging."""
    if not isinstance(stats, dict):
        return ""

    aliases = {
        "input_rows": "in",
        "rows_read": "read",
        "read_rows": "read",
        "source_rows": "src",
        "output_rows": "out",
        "rows_written": "written",
        "written_rows": "written",
        "null_rows": "null",
        "error_rows": "errors",
        "quarantined_rows": "quarantine",
        "bytes_read": "bytes_read",
        "bytes_written": "bytes_written",
    }
    parts: list[str] = []

    for key, alias in aliases.items():
        value = stats.get(key)
        if isinstance(value, (int, float, str, bool)) and value is not None:
            parts.append(f"{alias}={value}")

    if parts:
        return " ".join(parts)

    for key, value in stats.items():
        if len(parts) >= 6:
            break
        if isinstance(value, (int, float, str, bool)) and value is not None:
            parts.append(f"{key}={value}")

    return " ".join(parts)


def schema_drift_count(schema_drifts: Any) -> int | None:
    """Return a compact drift count if the shape is known."""
    if schema_drifts is None:
        return None
    if isinstance(schema_drifts, list):
        return len(schema_drifts)
    if isinstance(schema_drifts, dict):
        for key in ("drifts", "schema_drifts", "items", "changes"):
            value = schema_drifts.get(key)
            if isinstance(value, list):
                return len(value)
        return len(schema_drifts)
    return None
