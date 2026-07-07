"""Implements `schema_sanitizer.adapters.pandas`."""

from __future__ import annotations

from typing import Any

from ._optional import ensure_optional_dependency


def from_arrow_table(table: Any, *, feature: str) -> Any:
    """Convert an Arrow table to a pandas DataFrame."""
    ensure_optional_dependency("pandas", extra="pandas", feature=feature)
    try:
        return table.to_pandas()
    except Exception as e:
        raise RuntimeError(
            f"{feature} could not convert the Arrow table to pandas DataFrame."
        ) from e
