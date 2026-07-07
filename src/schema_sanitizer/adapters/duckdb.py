"""Implements `schema_sanitizer.adapters.duckdb`."""

from __future__ import annotations

from typing import Any

from ._optional import ensure_optional_dependency


def from_arrow_table(table: Any, *, feature: str) -> Any:
    """Convert an Arrow table to a DuckDB relation."""
    duckdb = ensure_optional_dependency("duckdb", extra="duckdb", feature=feature)
    return duckdb.from_arrow(table)
