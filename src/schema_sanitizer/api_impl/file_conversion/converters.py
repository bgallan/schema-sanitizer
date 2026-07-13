"""Public streaming file-conversion orchestration and entry points."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ...adapters.parquet.compression import (
    normalize_parquet_compression,
    normalize_parquet_gzip_level,
)
from ...core_impl.generated_metadata import INGESTION_TIMESTAMP_COLUMN, SOURCE_FILE_COLUMN
from ...core_impl.probes import options_for_schema_probe
from ...core_impl.schema_registry import current_native_registry_state
from ...options_impl.call_options import (
    FILE_CONVERSION_HELPER_KEYS,
    call_options_from_locals,
    normalize_call_options_or_none,
)
from ...remote_impl.staging import (
    cleanup_output_target,
    finalize_output_target,
    prepare_output_target,
)
from ..input.preparation import prepare_public_input
from ..registry_output import (
    write_csv_registry_file,
    write_jsonl_registry_file,
    write_parquet_registry_file,
)
from ..results import Result
from .writers import (
    write_csv_native_first_stream,
    write_jsonl_native_first_stream,
    write_parquet_native_first_stream,
)


def try_convert_source_plan_with_options(
    prepared_input: Any,
    output_path: str | os.PathLike[str],
    *,
    source_plan_writer: Callable[..., None],
    feature: str,
    call_options: Any,
    schema_registry_json: str,
    schema_registry_native_state: Any = None,
    schema_mode: str,
    field_name_policy: str,
    writer_options: Mapping[str, Any] | None = None,
) -> Result | None:
    """Write a prepared source-plan input through the canonical native path."""
    from schema_sanitizer.input_impl.source_plan import PARQUET_ARROW_SOURCES

    from ...input_impl.selection import unsupported_native_directory_ingestion
    from ..execution_context import default_pool
    from ..parquet.errors import unsupported_direct_parquet_ingestion
    from ..source_plan.attached import source_plan_from_data
    from ..source_plan.registry import write_source_plan_registry_to_file

    plan = source_plan_from_data(prepared_input.data)
    if plan is None:
        return None
    resolved_writer_options = writer_options or {}

    plan_result = write_source_plan_registry_to_file(
        default_pool().get()._raw,
        plan,
        output_path,
        writer=source_plan_writer,
        feature=feature,
        call_options=call_options,
        first_row_columns={},
        timestamp_columns=(INGESTION_TIMESTAMP_COLUMN,),
        schema_registry_json=schema_registry_json,
        schema_mode=schema_mode,
        field_name_policy=field_name_policy,
        native_registry_state=schema_registry_native_state,
        parquet_compression=resolved_writer_options.get("parquet_compression"),
        parquet_gzip_level=resolved_writer_options.get("parquet_gzip_level"),
    )
    if plan_result is not None:
        return plan_result
    if plan.kind == PARQUET_ARROW_SOURCES:
        raise unsupported_direct_parquet_ingestion()
    raise unsupported_native_directory_ingestion()


def convert_file_with_options(
    input_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
    *,
    input_format: str | None,
    input_mode: str,
    options: dict[str, Any],
    writer: Callable[..., Result],
    source_plan_writer: Callable[..., None],
    feature: str,
    schema_registry: Mapping[str, Any] | str | None,
    schema_registry_native_state: Any = None,
    writer_options: Mapping[str, Any] | None = None,
) -> Result:
    """Normalize file conversion options and invoke a streaming writer."""
    from ...core_impl.schema_registry import _normalize_registry_json

    registry_json = _normalize_registry_json(schema_registry)
    resolved_writer_options = writer_options or {}
    if schema_registry_native_state is None:
        schema_registry_native_state = current_native_registry_state()
    schema_mode = str(options.get("schema_mode", "additive")).strip().lower()
    prepared_input = prepare_public_input(
        input_path,
        input_format=input_format,
        input_mode=input_mode,
        input_text_encoding=str(options.get("input_text_encoding", "utf-8")),
        xml_row_tag=options.get("xml_row_tag"),
        csv_delimiter=str(options.get("csv_delimiter", ",")),
        csv_has_header=bool(options.get("csv_has_header", True)),
        memory_limit_bytes=options.get("batch_memory_limit_bytes"),
    )
    all_row_columns = (
        {SOURCE_FILE_COLUMN: prepared_input.source_file}
        if prepared_input.source_file is not None
        else None
    )
    row_span_columns = (
        {SOURCE_FILE_COLUMN: prepared_input.source_file_spans}
        if prepared_input.source_file_spans is not None
        else None
    )
    try:
        if prepared_input.xml_row_tag is not None:
            options = dict(options)
            options["xml_row_tag"] = prepared_input.xml_row_tag
            options["input_text_encoding"] = "utf-8"
        options = call_options_from_locals(
            dict(options),
            FILE_CONVERSION_HELPER_KEYS,
        )
        call_options = normalize_call_options_or_none(**options_for_schema_probe(options))
        output_target = prepare_output_target(output_path)
        try:
            field_name_policy = str(options.get("field_name_policy", "lower_alpha"))
            result = try_convert_source_plan_with_options(
                prepared_input,
                output_target.local_path,
                source_plan_writer=source_plan_writer,
                feature=feature,
                call_options=call_options,
                schema_registry_json=registry_json,
                schema_registry_native_state=schema_registry_native_state,
                schema_mode=schema_mode,
                field_name_policy=field_name_policy,
                writer_options=resolved_writer_options,
            )
            if result is None:
                result = writer(
                    prepared_input.data,
                    output_target.local_path,
                    options=call_options,
                    format=prepared_input.format,
                    source=prepared_input.source,
                    schema_registry_json=registry_json,
                    schema_registry_native_state=schema_registry_native_state,
                    first_row_columns=None,
                    all_row_columns=all_row_columns,
                    row_span_columns=row_span_columns,
                    timestamp_columns=(INGESTION_TIMESTAMP_COLUMN,),
                    schema_mode=schema_mode,
                    field_name_policy=field_name_policy,
                    **resolved_writer_options,
                )
            finalize_output_target(output_target)
            return result
        except Exception:
            cleanup_output_target(output_target)
            raise
    finally:
        prepared_input.close()


def _convert_public_file(
    input_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
    *,
    input_format: str | None,
    input_mode: str,
    options: dict[str, Any],
    writer: Any,
    source_plan_writer: Any,
    feature: str,
    schema_registry: Mapping[str, Any] | str | None,
    writer_options: dict[str, Any] | None = None,
) -> Result:
    """Invoke one public file converter with canonical writer options."""
    normalized_writer_options = writer_options or {}
    if "parquet_compression" in normalized_writer_options:
        normalized_writer_options = dict(normalized_writer_options)
        normalized_writer_options["parquet_compression"] = normalize_parquet_compression(
            normalized_writer_options["parquet_compression"]
        )
        normalized_writer_options["parquet_gzip_level"] = normalize_parquet_gzip_level(
            normalized_writer_options.get("parquet_gzip_level")
        )
    return convert_file_with_options(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        writer=writer,
        source_plan_writer=source_plan_writer,
        feature=feature,
        schema_registry=schema_registry,
        writer_options=normalized_writer_options,
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
    return _convert_public_file(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        writer=write_jsonl_registry_file,
        source_plan_writer=write_jsonl_native_first_stream,
        feature="to_jsonl",
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
    return _convert_public_file(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        writer=write_csv_registry_file,
        source_plan_writer=write_csv_native_first_stream,
        feature="to_csv",
        schema_registry=schema_registry,
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
    return _convert_public_file(
        input_path,
        output_path,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        writer=write_parquet_registry_file,
        source_plan_writer=write_parquet_native_first_stream,
        feature="to_parquet",
        schema_registry=schema_registry,
        writer_options={
            "parquet_compression": parquet_compression,
            "parquet_gzip_level": parquet_gzip_level,
        },
    )
