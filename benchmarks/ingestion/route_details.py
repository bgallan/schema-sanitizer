"""Supported diagnostic details for local ingestion benchmark results.

It formats the input-plan, Parquet-input, and file-output routes owned by each public
operation result without consulting mutable process-global observations.
"""

from __future__ import annotations

from typing import Any


def join_route_details(*details: str | None) -> str:
    """Join non-empty benchmark route details."""
    return " ".join(detail for detail in details if detail)


def result_route_details(result: Any) -> str:
    """Return input and output routes carried by one public operation result."""
    stats = getattr(result, "stats", None)
    if not isinstance(stats, dict):
        return ""
    return join_route_details(
        *(
            f"{key}={value}"
            for key in (
                "input_source_route",
                "input_plan_route",
                "parquet_input_route",
                "parquet_input_fallback_reason",
                "file_output_route",
                "file_metadata_route",
            )
            if isinstance((value := stats.get(key)), str) and value
        )
    )


def stream_stats_suffix(result: Any) -> str:
    """Return common stats suffix details for a benchmark result."""
    stats = getattr(result, "stats", None)
    if stats is None:
        return ""
    output_rows = (
        stats.get("output_rows", stats.get("materialized_rows", "n/a"))
        if isinstance(stats, dict)
        else "n/a"
    )
    suffix = f" output_rows={output_rows}"
    direct_arrow = stats.get("direct_arrow_input") if isinstance(stats, dict) else None
    if direct_arrow:
        suffix += " direct_arrow=1"
    return suffix
