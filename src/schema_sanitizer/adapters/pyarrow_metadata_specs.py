"""Validation and schema helpers for PyArrow metadata columns."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

FirstRowColumns = Mapping[str, Any] | None
AllRowColumns = Mapping[str, Any] | None
RowSpanColumns = Mapping[str, list[tuple[int, str | None]]] | None
TimestampColumns = Sequence[str] | None


def validate_first_row_columns(first_row_columns: FirstRowColumns) -> dict[str, Any]:
    """Return validated first-row ETL metadata columns."""
    return _validate_scalar_columns(first_row_columns, "first-row")


def validate_all_row_columns(all_row_columns: AllRowColumns) -> dict[str, Any]:
    """Return validated all-row ETL metadata columns."""
    return _validate_scalar_columns(all_row_columns, "all-row")


def validate_row_span_columns(
    row_span_columns: RowSpanColumns,
) -> dict[str, list[tuple[int, str | None]]]:
    """Return validated row-span ETL metadata columns."""
    if row_span_columns is None:
        return {}
    if not isinstance(row_span_columns, Mapping):
        raise TypeError("row-span columns must be a mapping of column name to row spans")
    out: dict[str, list[tuple[int, str | None]]] = {}
    for name, spans in row_span_columns.items():
        if not isinstance(name, str) or not name:
            raise ValueError("row-span column names must be non-empty strings")
        normalized: list[tuple[int, str | None]] = []
        for row_count, value in spans:
            if not isinstance(row_count, int) or row_count < 0:
                raise ValueError("row-span row counts must be non-negative integers")
            if value is not None and not isinstance(value, str):
                raise TypeError("row-span ETL metadata values must be strings or None")
            if row_count:
                normalized.append((row_count, value))
        out[name] = normalized
    return out


def validate_timestamp_columns(timestamp_columns: TimestampColumns) -> tuple[str, ...]:
    """Return validated dynamic timestamp metadata column names."""
    if timestamp_columns is None:
        return ()
    if isinstance(timestamp_columns, str) or not isinstance(timestamp_columns, Sequence):
        raise TypeError("timestamp columns must be a sequence of column names")
    out = []
    seen = set()
    for name in timestamp_columns:
        if not isinstance(name, str) or not name:
            raise ValueError("timestamp column names must be non-empty strings")
        if name in seen:
            raise ValueError(f"generated metadata column {name!r} was provided twice")
        seen.add(name)
        out.append(name)
    return tuple(out)


def _validate_scalar_columns(columns: Mapping[str, Any] | None, label: str) -> dict[str, Any]:
    """Return validated scalar ETL metadata columns."""
    if columns is None:
        return {}
    if not isinstance(columns, Mapping):
        raise TypeError(f"{label} columns must be a mapping of column name to scalar value")
    out: dict[str, Any] = {}
    for name, value in columns.items():
        if not isinstance(name, str) or not name:
            raise ValueError(f"{label} column names must be non-empty strings")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"{label} ETL metadata values must be strings or None")
        out[name] = value
    return out


def reject_existing_metadata_columns(
    schema: Any,
    *metadata_columns: Mapping[str, Any],
    timestamp_columns: Sequence[str] = (),
) -> None:
    """Reject metadata names already present in the base schema."""
    metadata_names: set[str] = set()
    for columns in metadata_columns:
        duplicates = sorted(metadata_names & set(columns))
        if duplicates:
            raise ValueError(f"generated metadata column {duplicates[0]!r} was provided twice")
        metadata_names.update(columns)
    duplicates = sorted(metadata_names & set(timestamp_columns))
    if duplicates:
        raise ValueError(f"generated metadata column {duplicates[0]!r} was provided twice")
    metadata_names.update(timestamp_columns)
    duplicates = sorted(set(schema.names) & metadata_names)
    if duplicates:
        raise ValueError(
            f"generated metadata column {duplicates[0]!r} already exists in output schema"
        )
