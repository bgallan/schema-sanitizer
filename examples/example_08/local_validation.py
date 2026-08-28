"""Local, infrastructure-free validation path for example 08.

It reads a local heterogeneous CSV directory, normalizes events, validates the
analytical schema, and writes Hive Parquet.
"""

from __future__ import annotations

from pathlib import Path

import schema_sanitizer as ss

try:
    from examples.example_08.event_normalization import (
        EventNormalizationResult,
        normalize_event_columns_inferred,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from event_normalization import (
        EventNormalizationResult,
        normalize_event_columns_inferred,
    )


def load_local_csv_directory_to_polars(
    source_directory: str | Path,
    *,
    event_separator: str = "/",
    event_column: str = "event",
    omit_null_payloads: bool = True,
    csv_delimiter: str = ",",
    csv_escape_char: str | None = "\\",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
) -> EventNormalizationResult:
    """Sanitize and normalize every CSV in one local directory into one frame."""
    converted = ss.to_polars(
        Path(source_directory),
        input_format="csv",
        input_mode="directory",
        schema_mode="additive",
        column_order="schema_contract_first",
        field_name_policy="preserve",
        csv_has_header=True,
        csv_delimiter=csv_delimiter,
        csv_escape_char=csv_escape_char,
        csv_header_mode="union",
        on_error="stop",
        multi_threading=multi_threading,
        memory_limit_bytes=memory_limit_bytes,
    )
    try:
        frame = converted.clean_data
    finally:
        converted.close()
    return normalize_event_columns_inferred(
        frame,
        separator=event_separator,
        output_column=event_column,
        omit_null_payloads=omit_null_payloads,
    )


__all__ = ["load_local_csv_directory_to_polars"]
