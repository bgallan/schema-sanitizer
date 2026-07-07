"""Public analytical conversion functions sharing the file-converter contract."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any

from .analytical_core import convert_analytical_with_options
from .file_api_helpers import _call_options_from_locals
from .file_api_option_sets import ANALYTICAL_HELPER_KEYS
from .ingest_runtime_types import Result


def _to_analytical(
    input_path: str | os.PathLike[str],
    *,
    target: str,
    input_format: str | None,
    input_mode: str,
    options: dict[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
) -> Result:
    """Invoke the shared analytical conversion core."""
    return convert_analytical_with_options(
        input_path,
        target=target,
        input_format=input_format,
        input_mode=input_mode,
        options=_call_options_from_locals(options, ANALYTICAL_HELPER_KEYS),
        schema_registry=schema_registry,
    )


def to_pyarrow(
    input_path: str | os.PathLike[str],
    *,
    input_format: str | None = None,
    input_mode: str = "single_file",
    schema_mode: str = "additive",
    column_order: str = "alphabetically",
    field_name_policy: str = "lower_alpha",
    timestamp_precision: str = "TIMESTAMP_MICROS",
    parse_integers: bool = False,
    parse_floats: bool = False,
    parse_float_decimal_separator: str = ".",
    parse_float_thousands_separator: str = ",",
    parse_iso_timestamps: bool = False,
    parse_iso_dates: bool = False,
    parse_iso_times: bool = False,
    true_tokens: Sequence[str] = (),
    false_tokens: Sequence[str] = (),
    custom_timestamp_patterns: Sequence[str] = (),
    custom_date_patterns: Sequence[str] = (),
    custom_time_patterns: Sequence[str] = (),
    arrow_max_depth: int = 32,
    parquet_max_depth: int = 15,
    scalar_object_key: str = "default_key",
    csv_has_header: bool = True,
    csv_delimiter: str = ",",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    batch_memory_limit_bytes: int | None = None,
    read_chunk_bytes: int = 1 << 20,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize file input into a PyArrow table."""
    options = locals()
    return _to_analytical(
        input_path,
        target="pyarrow",
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
    )


def to_pandas(
    input_path: str | os.PathLike[str],
    *,
    input_format: str | None = None,
    input_mode: str = "single_file",
    schema_mode: str = "additive",
    column_order: str = "alphabetically",
    field_name_policy: str = "lower_alpha",
    timestamp_precision: str = "TIMESTAMP_MICROS",
    parse_integers: bool = False,
    parse_floats: bool = False,
    parse_float_decimal_separator: str = ".",
    parse_float_thousands_separator: str = ",",
    parse_iso_timestamps: bool = False,
    parse_iso_dates: bool = False,
    parse_iso_times: bool = False,
    true_tokens: Sequence[str] = (),
    false_tokens: Sequence[str] = (),
    custom_timestamp_patterns: Sequence[str] = (),
    custom_date_patterns: Sequence[str] = (),
    custom_time_patterns: Sequence[str] = (),
    arrow_max_depth: int = 32,
    parquet_max_depth: int = 15,
    scalar_object_key: str = "default_key",
    csv_has_header: bool = True,
    csv_delimiter: str = ",",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    batch_memory_limit_bytes: int | None = None,
    read_chunk_bytes: int = 1 << 20,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize file input into a pandas DataFrame."""
    options = locals()
    return _to_analytical(
        input_path,
        target="pandas",
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
    )


def to_polars(
    input_path: str | os.PathLike[str],
    *,
    input_format: str | None = None,
    input_mode: str = "single_file",
    schema_mode: str = "additive",
    column_order: str = "alphabetically",
    field_name_policy: str = "lower_alpha",
    timestamp_precision: str = "TIMESTAMP_MICROS",
    parse_integers: bool = False,
    parse_floats: bool = False,
    parse_float_decimal_separator: str = ".",
    parse_float_thousands_separator: str = ",",
    parse_iso_timestamps: bool = False,
    parse_iso_dates: bool = False,
    parse_iso_times: bool = False,
    true_tokens: Sequence[str] = (),
    false_tokens: Sequence[str] = (),
    custom_timestamp_patterns: Sequence[str] = (),
    custom_date_patterns: Sequence[str] = (),
    custom_time_patterns: Sequence[str] = (),
    arrow_max_depth: int = 32,
    parquet_max_depth: int = 15,
    scalar_object_key: str = "default_key",
    csv_has_header: bool = True,
    csv_delimiter: str = ",",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    batch_memory_limit_bytes: int | None = None,
    read_chunk_bytes: int = 1 << 20,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize file input into a Polars DataFrame."""
    options = locals()
    return _to_analytical(
        input_path,
        target="polars",
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
    )


def to_duckdb(
    input_path: str | os.PathLike[str],
    *,
    input_format: str | None = None,
    input_mode: str = "single_file",
    schema_mode: str = "additive",
    column_order: str = "alphabetically",
    field_name_policy: str = "lower_alpha",
    timestamp_precision: str = "TIMESTAMP_MICROS",
    parse_integers: bool = False,
    parse_floats: bool = False,
    parse_float_decimal_separator: str = ".",
    parse_float_thousands_separator: str = ",",
    parse_iso_timestamps: bool = False,
    parse_iso_dates: bool = False,
    parse_iso_times: bool = False,
    true_tokens: Sequence[str] = (),
    false_tokens: Sequence[str] = (),
    custom_timestamp_patterns: Sequence[str] = (),
    custom_date_patterns: Sequence[str] = (),
    custom_time_patterns: Sequence[str] = (),
    arrow_max_depth: int = 32,
    parquet_max_depth: int = 15,
    scalar_object_key: str = "default_key",
    csv_has_header: bool = True,
    csv_delimiter: str = ",",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    batch_memory_limit_bytes: int | None = None,
    read_chunk_bytes: int = 1 << 20,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize file input into a DuckDB relation."""
    options = locals()
    return _to_analytical(
        input_path,
        target="duckdb",
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
    )
