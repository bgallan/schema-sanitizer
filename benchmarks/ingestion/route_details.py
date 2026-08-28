"""Route-detail helpers for local ingestion benchmarks.

It extracts native-route, metadata, directory, and stream statistics into compact
human-readable annotations.
"""

from __future__ import annotations

from typing import Any


def jsonl_route_detail() -> str:
    """Return JSONL writer route details for benchmark output."""
    from schema_sanitizer.adapters.pyarrow.jsonl_sink import last_jsonl_stream_route

    route = last_jsonl_stream_route()
    return f"jsonl_route={route}"


def metadata_route_detail() -> str:
    """Return metadata injection route details for benchmark output."""
    from schema_sanitizer.adapters.pyarrow.file_metadata import last_metadata_route

    return f"metadata_route={last_metadata_route()}"


def csv_nested_route_detail() -> str:
    """Return CSV nested rendering route details for benchmark output."""
    from schema_sanitizer.adapters.pyarrow.csv_sink import last_csv_nested_route

    return f"csv_nested_route={last_csv_nested_route()}"


def parquet_direct_route_detail() -> str:
    """Return direct Parquet routing details for benchmark output."""
    from schema_sanitizer.api_impl.parquet.direct_routes import last_parquet_direct_route

    return f"parquet_direct_route={last_parquet_direct_route()}"


def native_directory_route_detail() -> str:
    """Return native directory routing details for benchmark output."""
    from schema_sanitizer.input_impl.source_plan import last_native_multisource_route

    return f"native_directory_route={last_native_multisource_route()}"


def sink_source_route_detail() -> str:
    """Return normal native sink source routing details for benchmark output."""
    from schema_sanitizer.core_impl.execution import last_sink_source_route

    return f"sink_source_route={last_sink_source_route()}"


def join_route_details(*details: str | None) -> str:
    """Join non-empty benchmark route details."""
    return " ".join(detail for detail in details if detail)


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
