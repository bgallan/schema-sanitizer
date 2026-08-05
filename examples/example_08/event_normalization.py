"""Vectorized Polars normalization for wide event columns."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from importlib import import_module
from typing import Any

_SCHEMA_REGISTRY_COLUMN = "schema_registry"
_SCHEMA_DRIFTS_COLUMN = "schema_drifts"


@dataclass(frozen=True, slots=True)
class EventColumn:
    """One parsed ``<integer><separator><event text>`` source column."""

    name: str
    event_id: int
    event_text: str


@dataclass(frozen=True, slots=True)
class EventNormalizationResult:
    """A normalized Polars frame and the source columns it consumed."""

    frame: Any
    event_columns: tuple[EventColumn, ...]


def parse_event_column(name: str, *, separator: str = "/") -> EventColumn | None:
    """Parse an event header, splitting only on the first separator."""
    if not isinstance(name, str):
        raise TypeError("column names must be strings")
    if not separator:
        raise ValueError("event separator must not be empty")
    raw_id, found, event_text = name.partition(separator)
    if not found or not raw_id.strip() or not event_text:
        return None
    try:
        event_id = int(raw_id.strip())
    except ValueError:
        return None
    return EventColumn(
        name=name,
        event_id=event_id,
        event_text=event_text,
    )


def detect_event_columns(
    column_names: Iterable[str],
    *,
    separator: str = "/",
) -> tuple[EventColumn, ...]:
    """Return event columns in deterministic input-column order."""
    detected: list[EventColumn] = []
    for name in column_names:
        parsed = parse_event_column(name, separator=separator)
        if parsed is not None:
            detected.append(parsed)
    return tuple(detected)


def normalize_event_columns(
    frame: Any,
    final_schema: Any,
    *,
    separator: str = "/",
    output_column: str = "event",
    omit_null_payloads: bool = False,
) -> EventNormalizationResult:
    """Replace wide event columns with one vectorized list-of-struct field.

    The returned frame contains every final non-metadata field exactly once.
    Registry columns are intentionally removed because
    ``finalize_analytical_output`` regenerates them from the normalized schema.
    """
    pl = import_module("polars")
    pa = import_module("pyarrow")
    if type(frame).__module__.split(".", 1)[0] != "polars":
        raise TypeError("frame must be a polars.DataFrame")
    if not isinstance(final_schema, pa.Schema):
        raise TypeError("final_schema must be a pyarrow.Schema")
    if output_column not in final_schema.names:
        raise ValueError(f"final schema has no event output column {output_column!r}")
    _validate_event_output_type(pa, final_schema.field(output_column).type)

    event_columns = detect_event_columns(frame.columns, separator=separator)
    source_event_names = {column.name for column in event_columns}
    final_data_names = [
        field.name
        for field in final_schema
        if field.name not in {_SCHEMA_REGISTRY_COLUMN, _SCHEMA_DRIFTS_COLUMN}
    ]
    passthrough_names = [name for name in final_data_names if name != output_column]
    missing = [
        name
        for name in passthrough_names
        if name not in frame.columns
        or (frame.height > 0 and frame.get_column(name).null_count() == frame.height)
    ]
    if missing:
        raise ValueError(f"wide CSV result is missing final scalar fields: {missing!r}")

    unexpected = [
        name
        for name in frame.columns
        if name not in passthrough_names
        and name not in source_event_names
        and name not in {_SCHEMA_REGISTRY_COLUMN, _SCHEMA_DRIFTS_COLUMN}
    ]
    if unexpected:
        raise ValueError(
            f"wide CSV result contains non-event columns outside the final schema: {unexpected!r}"
        )

    event_expr = _event_expression(
        pl,
        event_columns,
        output_column=output_column,
        omit_null_payloads=omit_null_payloads,
    )
    normalized = frame.with_columns(event_expr).select(final_data_names)
    return EventNormalizationResult(normalized, event_columns)


def normalize_event_columns_inferred(
    frame: Any,
    *,
    separator: str = "/",
    output_column: str = "event",
    omit_null_payloads: bool = True,
) -> EventNormalizationResult:
    """Normalize a sanitized wide frame without requiring a target schema.

    This local-validation variant retains every non-event data/provenance
    column. Intermediate registry metadata is removed because it describes the
    wide ingress shape, not the normalized analytical result.
    """
    pl = import_module("polars")
    if type(frame).__module__.split(".", 1)[0] != "polars":
        raise TypeError("frame must be a polars.DataFrame")

    event_columns = detect_event_columns(frame.columns, separator=separator)
    event_names = {column.name for column in event_columns}
    if output_column in frame.columns and output_column not in event_names:
        raise ValueError(f"input already contains output column {output_column!r}")
    passthrough = [
        name
        for name in frame.columns
        if name not in event_names and name not in {_SCHEMA_REGISTRY_COLUMN, _SCHEMA_DRIFTS_COLUMN}
    ]
    event_expr = _event_expression(
        pl,
        event_columns,
        output_column=output_column,
        omit_null_payloads=omit_null_payloads,
    )
    normalized = frame.with_columns(event_expr).select([*passthrough, output_column])
    return EventNormalizationResult(normalized, event_columns)


def _event_expression(
    pl: Any,
    columns: tuple[EventColumn, ...],
    *,
    output_column: str,
    omit_null_payloads: bool,
) -> Any:
    """Build one vectorized Polars expression for all event columns."""
    struct_type = pl.Struct(
        [
            pl.Field("event_id", pl.Int64),
            pl.Field("event_text", pl.String),
            pl.Field("payload", pl.String),
        ]
    )
    if not columns:
        return pl.lit([], dtype=pl.List(struct_type)).alias(output_column)

    items: list[Any] = []
    for column in columns:
        struct_expr = pl.struct(
            [
                pl.lit(column.event_id, dtype=pl.Int64).alias("event_id"),
                pl.lit(column.event_text, dtype=pl.String).alias("event_text"),
                pl.col(column.name).cast(pl.String, strict=False).alias("payload"),
            ]
        )
        if omit_null_payloads:
            struct_expr = pl.when(pl.col(column.name).is_not_null()).then(struct_expr)
        items.append(struct_expr)
    event = pl.concat_list(items)
    if omit_null_payloads:
        event = event.list.drop_nulls()
    return event.alias(output_column)


def _validate_event_output_type(pa: Any, data_type: Any) -> None:
    """Require the documented list-of-event-struct target shape."""
    if not (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)):
        raise ValueError("event output field must be list<struct<...>>")
    struct_type = data_type.value_type
    if not pa.types.is_struct(struct_type):
        raise ValueError("event output field must be list<struct<...>>")
    names = [field.name for field in struct_type]
    if names != ["event_id", "event_text", "payload"]:
        raise ValueError("event struct fields must be event_id, event_text, payload in that order")
    if not pa.types.is_int64(struct_type.field("event_id").type):
        raise ValueError("event_id must use int64")
    if not pa.types.is_string(struct_type.field("event_text").type):
        raise ValueError("event_text must use string")
    if not pa.types.is_string(struct_type.field("payload").type):
        raise ValueError("payload must use nullable string")


__all__ = [
    "EventColumn",
    "EventNormalizationResult",
    "detect_event_columns",
    "normalize_event_columns",
    "normalize_event_columns_inferred",
    "parse_event_column",
]
