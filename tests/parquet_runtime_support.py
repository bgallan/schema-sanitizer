"""Shared fixtures for Parquet runtime tests."""

from __future__ import annotations

from typing import Any


def sample_table(pyarrow: Any) -> Any:
    """Return the canonical two-column Parquet runtime test table."""
    return pyarrow.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
