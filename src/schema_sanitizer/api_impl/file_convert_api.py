"""Public file conversion API wrappers."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any

from .file_api_helpers import _call_options_from_locals
from .file_api_option_sets import CONVERTER_HELPER_KEYS, PARQUET_WRITER_OPTION_KEYS
from .file_convert_core import convert_file_with_options
from .ingest_runtime_types import Result
from .parquet_compression_options import (
    normalize_parquet_compression,
    normalize_parquet_gzip_level,
)


def _convert_public_file(
    input_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
    *,
    input_format: str | None,
    input_mode: str,
    options: dict[str, Any],
    writer: Any,
    schema_registry: Mapping[str, Any] | str | None,
    writer_options: dict[str, Any] | None = None,
) -> Result:
    """Invoke a public file converter with normalized call options."""
    writer_options = writer_options or {}
    if "parquet_compression" in writer_options:
        writer_options = dict(writer_options)
        writer_options["parquet_compression"] = normalize_parquet_compression(
            writer_options["parquet_compression"]
        )
        writer_options["parquet_gzip_level"] = normalize_parquet_gzip_level(
            writer_options.get("parquet_gzip_level")
        )
    return convert_file_with_options(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=_call_options_from_locals(
            options,
            CONVERTER_HELPER_KEYS | PARQUET_WRITER_OPTION_KEYS,
        ),
        writer=writer,
        schema_registry=schema_registry,
        writer_options=writer_options,
    )


def to_parquet(
    input_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
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
    parquet_compression: str | None = "gzip",
    parquet_gzip_level: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Stream-sanitize an input file to Parquet without materializing a table."""

    options = locals()
    from .registry_file_writers import _to_parquet_registry_stream

    return _convert_public_file(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        writer=_to_parquet_registry_stream,
        schema_registry=schema_registry,
        writer_options={
            "parquet_compression": parquet_compression,
            "parquet_gzip_level": parquet_gzip_level,
        },
    )


def to_jsonl(
    input_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
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
    """Stream-sanitize an input file to JSON Lines without materializing a table."""

    options = locals()
    from .registry_file_writers import _to_jsonl_registry_stream

    return _convert_public_file(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        writer=_to_jsonl_registry_stream,
        schema_registry=schema_registry,
    )


def to_csv(
    input_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
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
    """Stream-sanitize an input file to CSV without materializing a table."""

    options = locals()
    from .registry_file_writers import _to_csv_registry_stream

    return _convert_public_file(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        writer=_to_csv_registry_stream,
        schema_registry=schema_registry,
    )
