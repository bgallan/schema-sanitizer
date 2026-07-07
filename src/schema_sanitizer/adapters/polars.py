"""Implements `schema_sanitizer.adapters.polars`."""

from __future__ import annotations

from typing import Any

from ._optional import ensure_optional_dependency


def from_arrow_table(table: Any, *, feature: str) -> Any:
    """Convert an Arrow table to a Polars DataFrame."""
    pl = ensure_optional_dependency("polars", extra="polars", feature=feature)
    return pl.from_arrow(table)
