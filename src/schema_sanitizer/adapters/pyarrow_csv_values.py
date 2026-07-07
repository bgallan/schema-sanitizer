"""Helpers for rendering Arrow batches into CSV-compatible values."""

from __future__ import annotations

from typing import Any

from .pyarrow_csv_native import (
    last_csv_nested_route as _last_csv_nested_route,
)


def last_csv_nested_route() -> str:
    """Return the most recent nested CSV rendering route."""
    return _last_csv_nested_route()


def nested_column_indices(schema: Any, *, pa: Any) -> set[int]:
    """Return column indices that need JSON stringification for CSV."""
    return {idx for idx, field in enumerate(schema) if pa.types.is_nested(field.type)}
