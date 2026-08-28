"""Native Arrow C stream fast path for metadata column injection.

It checks whether metadata values are natively representable and builds an owned Arrow C
stream that injects them without Python materialization.
"""

from __future__ import annotations

from typing import Any

from ...core_impl.generated_metadata import TimestampColumnValues
from ...core_impl.native_symbols import METADATA_STREAM_WRAP


class CapsuleArrowStream:
    """Expose a native Arrow C stream capsule as a PyArrow-readable stream."""

    def __init__(self, capsule: Any):
        """Store a single-use Arrow C stream capsule."""
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Return the owned Arrow C stream capsule to PyArrow."""
        del requested_schema
        if self._capsule is None:
            raise RuntimeError("Arrow C stream capsule has already been consumed")
        capsule = self._capsule
        self._capsule = None
        return capsule


def metadata_values_are_native_supported(columns: dict[str, Any]) -> bool:
    """Return whether native metadata wrapping supports every value."""
    return all(value is None or isinstance(value, str) for value in columns.values())


def row_span_values_are_native_supported(
    columns: dict[str, list[tuple[int, str | None]]],
) -> bool:
    """Return whether native metadata wrapping supports every row-span value."""
    return all(
        isinstance(row_count, int) and row_count >= 0 and (value is None or isinstance(value, str))
        for spans in columns.values()
        for row_count, value in spans
    )


def native_metadata_reader(
    stream: Any,
    first_row_columns: dict[str, Any],
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumnValues = (),
    *,
    pa: Any,
) -> Any | None:
    """Return a native metadata-appending reader when the fast path applies."""
    all_row_columns = all_row_columns or {}
    row_span_columns = row_span_columns or {}
    if (
        not first_row_columns
        and not all_row_columns
        and not row_span_columns
        and not timestamp_columns
    ):
        return None
    if not metadata_values_are_native_supported(first_row_columns):
        return None
    if not metadata_values_are_native_supported(all_row_columns):
        return None
    if not row_span_values_are_native_supported(row_span_columns):
        return None
    if not all(isinstance(name, str) for name in timestamp_columns):
        return None
    capsule = METADATA_STREAM_WRAP(
        stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    return pa.RecordBatchReader.from_stream(CapsuleArrowStream(capsule))
