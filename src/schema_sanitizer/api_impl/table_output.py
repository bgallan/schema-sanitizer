"""Internal Arrow table output adapters."""

from __future__ import annotations

from typing import Any

TABLE_OUTPUT_FORMATS = frozenset({"pyarrow", "pandas", "polars", "duckdb"})
TABLE_ADAPTER_FORMATS = TABLE_OUTPUT_FORMATS - {"pyarrow"}
TABLE_OUTPUT_FORMAT_ERROR = "output_format must be 'pyarrow', 'pandas', 'polars', or 'duckdb'."


def normalize_table_output_format(output_format: str) -> str:
    """Normalize and validate a table output format."""
    if not isinstance(output_format, str):
        raise TypeError("output_format must be a string")
    target = output_format.strip().lower()
    if target not in TABLE_OUTPUT_FORMATS:
        raise ValueError(TABLE_OUTPUT_FORMAT_ERROR)
    return target


def convert_arrow_table_output(table: Any, target: str, *, feature: str) -> Any:
    """Convert a PyArrow table to an internal named output target."""

    if target == "pyarrow":
        return table

    if target == "pandas":
        from ..adapters import pandas as _pandas_adapter

        return _pandas_adapter.from_arrow_table(table, feature=feature)

    if target == "polars":
        from ..adapters import polars as _polars_adapter

        return _polars_adapter.from_arrow_table(table, feature=feature)

    if target == "duckdb":
        from ..adapters import duckdb as _duckdb_adapter

        return _duckdb_adapter.from_arrow_table(table, feature=feature)

    raise AssertionError(f"validated table output target was not handled: {target!r}")
