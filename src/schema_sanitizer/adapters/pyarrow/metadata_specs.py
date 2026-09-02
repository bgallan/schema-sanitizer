"""Validation and schema helpers for PyArrow metadata columns.

It validates metadata names and values against the source schema, rejects collisions,
and constructs the resulting Arrow schema.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ...core_impl.generated_metadata import TimestampColumns, TimestampColumnValues

FirstRowColumns = Mapping[str, Any] | None
AllRowColumns = Mapping[str, Any] | None
RowSpanColumns = Mapping[str, list[tuple[int, str | None]]] | None


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


def validate_timestamp_columns(timestamp_columns: TimestampColumns) -> TimestampColumnValues:
    """Return validated dynamic names or fixed epoch-microsecond timestamps."""
    if timestamp_columns is None:
        return ()
    if isinstance(timestamp_columns, Mapping):
        out: dict[str, int] = {}
        for name, value in timestamp_columns.items():
            if not isinstance(name, str) or not name:
                raise ValueError("timestamp column names must be non-empty strings")
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError("fixed timestamp values must be integer epoch microseconds")
            if value < -(1 << 63) or value >= 1 << 63:
                raise OverflowError("fixed timestamp value is outside int64 range")
            out[name] = value
        return out
    if isinstance(timestamp_columns, str) or not isinstance(timestamp_columns, Sequence):
        raise TypeError(
            "timestamp columns must be a sequence of names or a mapping to epoch microseconds"
        )
    out_names = []
    seen = set()
    for name in timestamp_columns:
        if not isinstance(name, str) or not name:
            raise ValueError("timestamp column names must be non-empty strings")
        if name in seen:
            raise ValueError(f"generated metadata column {name!r} was provided twice")
        seen.add(name)
        out_names.append(name)
    return tuple(out_names)


def timestamp_column_names(timestamp_columns: TimestampColumnValues) -> tuple[str, ...]:
    """Return ordered timestamp column names for duplicate validation."""
    if isinstance(timestamp_columns, Mapping):
        return tuple(timestamp_columns)
    return timestamp_columns


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
    timestamp_columns: TimestampColumnValues = (),
) -> None:
    """Reject metadata names already present in the base schema."""
    metadata_names: set[str] = set()
    for columns in metadata_columns:
        duplicates = sorted(metadata_names & set(columns))
        if duplicates:
            raise ValueError(f"generated metadata column {duplicates[0]!r} was provided twice")
        metadata_names.update(columns)
    timestamp_names = timestamp_column_names(timestamp_columns)
    duplicates = sorted(metadata_names & set(timestamp_names))
    if duplicates:
        raise ValueError(f"generated metadata column {duplicates[0]!r} was provided twice")
    metadata_names.update(timestamp_names)
    duplicates = sorted(set(schema.names) & metadata_names)
    if duplicates:
        raise ValueError(
            f"generated metadata column {duplicates[0]!r} already exists in output schema"
        )
