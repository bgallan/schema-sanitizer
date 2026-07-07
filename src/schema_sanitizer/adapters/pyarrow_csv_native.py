"""Native Arrow C stream helpers for CSV output."""

from __future__ import annotations

from typing import Any

from ..core_impl.native_functions import CSV_NESTED_STREAM_WRAP
from .pyarrow_metadata_native import CapsuleArrowStream

_LAST_CSV_NESTED_ROUTE = "none"


def last_csv_nested_route() -> str:
    """Return the most recent CSV nested rendering route."""
    return _LAST_CSV_NESTED_ROUTE


def mark_csv_nested_route(route: str) -> None:
    """Record the most recent CSV nested rendering route."""
    global _LAST_CSV_NESTED_ROUTE
    _LAST_CSV_NESTED_ROUTE = route


def native_csv_nested_reader(stream: Any, *, pa: Any) -> Any | None:
    """Return a reader that renders top-level nested columns as JSON strings."""
    wrap = CSV_NESTED_STREAM_WRAP.get()
    if wrap is None:
        mark_csv_nested_route("unavailable")
        return None
    capsule = wrap(stream)
    mark_csv_nested_route("native")
    return pa.RecordBatchReader.from_stream(CapsuleArrowStream(capsule))
