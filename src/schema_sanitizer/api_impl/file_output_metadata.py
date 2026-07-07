"""Shared metadata-column planning for streaming file output."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ..adapters.pyarrow_metadata_native import (
    metadata_values_are_native_supported,
    native_metadata_reader,
    row_span_values_are_native_supported,
)
from ..adapters.pyarrow_metadata_specs import (
    AllRowColumns,
    FirstRowColumns,
    RowSpanColumns,
    TimestampColumns,
    reject_existing_metadata_columns,
    validate_all_row_columns,
    validate_first_row_columns,
    validate_row_span_columns,
    validate_timestamp_columns,
)

_LAST_METADATA_ROUTE = "none"


def last_metadata_route() -> str:
    """Return the route used by the most recent metadata stream preparation."""
    return _LAST_METADATA_ROUTE


def mark_metadata_route(route: str) -> None:
    """Record the metadata route used by a file-output writer."""
    global _LAST_METADATA_ROUTE
    _LAST_METADATA_ROUTE = route


def has_metadata_columns(
    first_row_columns: FirstRowColumns,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
) -> bool:
    """Return whether output must append generated metadata columns."""
    return bool(first_row_columns or all_row_columns or row_span_columns or timestamp_columns)


def native_metadata_args_or_none(
    stream: Any | None,
    first_row_columns: FirstRowColumns,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
) -> (
    tuple[
        dict[str, Any],
        dict[str, Any],
        dict[str, list[tuple[int, str | None]]],
        tuple[str, ...],
    ]
    | None
):
    """Validate metadata and return arguments accepted by native output writers."""
    if not has_metadata_columns(
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    ):
        return None
    first_row_constants = validate_first_row_columns(first_row_columns)
    all_row_constants = validate_all_row_columns(all_row_columns)
    row_span_constants = validate_row_span_columns(row_span_columns)
    timestamp_constants = validate_timestamp_columns(timestamp_columns)
    if not metadata_values_are_native_supported(first_row_constants):
        return None
    if not metadata_values_are_native_supported(all_row_constants):
        return None
    if not row_span_values_are_native_supported(row_span_constants):
        return None
    if not all(isinstance(name, str) for name in timestamp_constants):
        return None
    if stream is not None:
        reject_existing_metadata_columns(
            stream.schema,
            first_row_constants,
            all_row_constants,
            row_span_constants,
            timestamp_columns=timestamp_constants,
        )
    return (
        first_row_constants,
        all_row_constants,
        row_span_constants,
        timestamp_constants,
    )


@dataclass(slots=True)
class PreparedFileOutputMetadataStream:
    """Prepared schema and batch source for metadata-augmented file output."""

    schema: Any
    batches: Any
    reader: Any | None
    has_metadata: bool

    def close(self) -> None:
        """Close the native metadata reader when one was created."""
        if self.reader is not None:
            with suppress(Exception):
                self.reader.close()


def prepare_file_output_metadata_stream(
    stream: Any,
    first_row_columns: FirstRowColumns,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
    *,
    pa: Any,
) -> PreparedFileOutputMetadataStream:
    """Return a stream plan with metadata columns appended by native code."""
    metadata_args = native_metadata_args_or_none(
        stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    mark_metadata_route("none")
    if metadata_args is None:
        if has_metadata_columns(
            first_row_columns,
            all_row_columns,
            row_span_columns,
            timestamp_columns,
        ):
            raise RuntimeError(
                "Metadata columns require the native C++ metadata stream wrapper; "
                "this metadata configuration is not supported by native metadata injection."
            )
        return PreparedFileOutputMetadataStream(
            schema=stream.schema,
            batches=stream,
            reader=None,
            has_metadata=False,
        )

    reader = native_metadata_reader(stream, *metadata_args, pa=pa)
    if reader is None:
        raise RuntimeError(
            "Metadata columns require the native C++ metadata stream wrapper; "
            "this metadata configuration is not supported by native metadata injection."
        )
    mark_metadata_route("native")
    return PreparedFileOutputMetadataStream(
        schema=reader.schema,
        batches=reader,
        reader=reader,
        has_metadata=True,
    )
