"""Public and internal in-memory analytical conversion."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any

from schema_sanitizer.core_impl.error_translation import call_core
from schema_sanitizer.core_impl.generated_metadata import (
    INGESTION_TIMESTAMP_COLUMN,
    SOURCE_FILE_COLUMN,
)
from schema_sanitizer.input_impl.source_plan import PARQUET_ARROW_SOURCES

from ..core_impl.probes import options_for_schema_probe
from ..core_impl.schema_registry import _normalize_registry_json
from ..input_impl.prepared import PreparedPublicInput
from ..input_impl.selection import unsupported_native_directory_ingestion
from ..options_impl.call_options import (
    ANALYTICAL_HELPER_KEYS,
    call_options_from_locals,
    normalize_call_options_or_none,
    unwrap_options,
)
from .execution_context import default_pool
from .input.preparation import prepare_public_input
from .parquet.errors import unsupported_direct_parquet_ingestion
from .results import Result
from .source_plan.attached import source_plan_from_data
from .source_plan.registry import (
    OpenedSourcePlanRegistryStream,
    materialize_opened_registry_stream,
    open_source_plan_registry_stream,
)


def _open_single_source_registry_stream(
    raw_ctx: Any,
    *,
    prepared_input: PreparedPublicInput,
    call_options: Any,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
) -> OpenedSourcePlanRegistryStream:
    """Open a native registry stream with generated metadata already injected."""
    all_row_columns = (
        {SOURCE_FILE_COLUMN: prepared_input.source_file}
        if prepared_input.source_file is not None
        else {}
    )
    row_span_columns = (
        {SOURCE_FILE_COLUMN: prepared_input.source_file_spans}
        if prepared_input.source_file_spans is not None
        else {}
    )
    raw = call_core(
        raw_ctx.to_registry_sink_from_source,
        "stream",
        prepared_input.format,
        prepared_input.source,
        prepared_input.data,
        unwrap_options(call_options),
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        first_row_columns={},
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=(INGESTION_TIMESTAMP_COLUMN,),
    )
    return OpenedSourcePlanRegistryStream(
        stream=None,
        schema_registry_json=raw.schema_registry_json,
        schema_drifts_json=raw.schema_drifts_json,
        diagnostics=raw.diagnostics,
        native_registry_state=raw.native_registry_state,
        raw_stream=raw,
        close_items=[raw],
    )


def convert_analytical_with_options(
    input_path: str | os.PathLike[str],
    *,
    target: str,
    input_format: str | None,
    input_mode: str,
    options: dict[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
) -> Result:
    """Sanitize one public input into an in-memory analytical object."""
    registry_json = _normalize_registry_json(schema_registry)
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
    try:
        if prepared_input.xml_row_tag is not None:
            options = dict(options)
            options["xml_row_tag"] = prepared_input.xml_row_tag
            options["input_text_encoding"] = "utf-8"
        options = call_options_from_locals(options, ANALYTICAL_HELPER_KEYS)
        call_options = normalize_call_options_or_none(**options_for_schema_probe(options))
        raw_ctx = default_pool().get()._raw
        field_name_policy = str(options.get("field_name_policy", "lower_alpha"))
        source_plan = source_plan_from_data(prepared_input.data)
        if source_plan is not None:
            opened = open_source_plan_registry_stream(
                raw_ctx,
                source_plan,
                unwrap_options(call_options),
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                first_row_columns={},
                timestamp_columns=(INGESTION_TIMESTAMP_COLUMN,),
            )
            if opened is not None:
                return materialize_opened_registry_stream(opened, target=target)
            if source_plan.kind == PARQUET_ARROW_SOURCES:
                raise unsupported_direct_parquet_ingestion()
            raise unsupported_native_directory_ingestion()
        opened = _open_single_source_registry_stream(
            raw_ctx,
            prepared_input=prepared_input,
            call_options=call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )
        return materialize_opened_registry_stream(opened, target=target)
    finally:
        prepared_input.close()


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
    return convert_analytical_with_options(
        input_path,
        target="duckdb",
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
    return convert_analytical_with_options(
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
    return convert_analytical_with_options(
        input_path,
        target="polars",
        input_format=input_format,
        input_mode=input_mode,
        options=options,
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
    return convert_analytical_with_options(
        input_path,
        target="pyarrow",
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
    )
