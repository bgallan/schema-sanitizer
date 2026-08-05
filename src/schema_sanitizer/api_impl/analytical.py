"""Public and internal in-memory analytical conversion."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any, cast

from schema_sanitizer.core_impl.error_translation import call_core, reader_error_context
from schema_sanitizer.core_impl.execution_policy import (
    threading_mode_from_multi_threading,
)
from schema_sanitizer.core_impl.generated_metadata import (
    INGESTION_TIMESTAMP_COLUMN,
    SOURCE_FILE_COLUMN,
)
from schema_sanitizer.core_impl.memory_budget import normalize_memory_limit
from schema_sanitizer.input_impl.selection import _Source
from schema_sanitizer.input_impl.source_plan import PARQUET_ARROW_SOURCES
from schema_sanitizer.sources.models import PublicInput

from ..core_impl.probes import options_for_registry_operation
from ..core_impl.schema_registry import _normalize_registry_json
from ..input_impl.prepared import PreparedPublicInput
from ..input_impl.selection import unsupported_native_directory_ingestion
from ..options_impl.call_options import (
    ANALYTICAL_HELPER_KEYS,
    attach_operation_detected_at,
    call_options_from_locals,
    normalize_call_options_or_none,
    unwrap_options,
)
from ..options_impl.options import (
    CsvHeaderMode,
    memory_limit_bytes_or_none,
    require_implemented_csv_header_mode,
)
from .batch_streaming import lazy_stream_from_opened
from .execution_context import default_pool
from .input.directory_preparation import prepare_single_parquet_file
from .input.preparation import prepare_public_input
from .operation_context import OperationExecutionContext
from .parquet.direct_routes import parquet_direct_registry_sink_raw_or_none
from .parquet.errors import unsupported_direct_parquet_ingestion
from .results import Result
from .source_manifest_diagnostics import patch_source_manifest_diagnostics
from .source_plan.attached import source_plan_from_data
from .source_plan.registry import (
    OpenedSourcePlanRegistryStream,
    materialize_opened_registry_stream,
    open_source_plan_registry_stream,
)
from .streams import Stream


def _open_single_source_registry_stream(
    raw_ctx: Any,
    *,
    prepared_input: PreparedPublicInput,
    call_options: Any,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    ingestion_timestamp_micros: int,
) -> OpenedSourcePlanRegistryStream:
    """Open a native registry stream with generated metadata already injected."""
    if prepared_input.format == "parquet":
        raw = parquet_direct_registry_sink_raw_or_none(
            raw_ctx,
            prepared_input.data,
            source=cast(_Source, prepared_input.source),
            feature="analytical Parquet input",
            call_options=call_options,
            schema_registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )
        if raw is not None:
            return OpenedSourcePlanRegistryStream(
                stream=None,
                schema_registry_json=raw.schema_registry_json,
                schema_drifts_json=raw.schema_drifts_json,
                diagnostics=raw.diagnostics,
                native_registry_state=raw.native_registry_state,
                raw_stream=raw,
                close_items=[raw],
            )

        fallback = prepare_single_parquet_file(
            prepared_input.data,
            source_file=prepared_input.source_file or os.fspath(prepared_input.data),
            keepalive=None,
            memory_limit_bytes=(memory_limit_bytes_or_none(call_options)),
        )
        plan = source_plan_from_data(fallback.data)
        if plan is None:  # pragma: no cover - helper owns this invariant
            fallback.close()
            raise unsupported_direct_parquet_ingestion()
        opened = open_source_plan_registry_stream(
            raw_ctx,
            plan,
            unwrap_options(call_options),
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns={},
            timestamp_columns={INGESTION_TIMESTAMP_COLUMN: ingestion_timestamp_micros},
        )
        if opened is None:
            fallback.close()
            raise unsupported_direct_parquet_ingestion()
        opened.close_items.append(fallback)
        return opened

    if prepared_input.format == "python":
        raw = call_core(
            raw_ctx.to_registry_sink_python,
            "stream",
            prepared_input.data,
            unwrap_options(call_options),
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns={},
            all_row_columns={},
            row_span_columns={},
            timestamp_columns={INGESTION_TIMESTAMP_COLUMN: ingestion_timestamp_micros},
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
        timestamp_columns={INGESTION_TIMESTAMP_COLUMN: ingestion_timestamp_micros},
        error_context=reader_error_context(
            prepared_input.format, prepared_input.source, prepared_input.data
        ),
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
    input_path: PublicInput,
    *,
    target: str,
    input_format: str | None,
    input_mode: str,
    options: dict[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
) -> Result | Stream:
    """Sanitize one public input into an in-memory analytical object."""
    require_implemented_csv_header_mode(options.get("csv_header_mode", "exact"))
    registry_json = _normalize_registry_json(schema_registry)
    schema_mode = str(options.get("schema_mode", "additive")).strip().lower()
    threading_mode = threading_mode_from_multi_threading(options.get("multi_threading", False))
    memory_limit_bytes = normalize_memory_limit(options.get("memory_limit_bytes"))
    options = dict(options)
    options["memory_limit_bytes"] = memory_limit_bytes
    operation_context = OperationExecutionContext(
        threading_mode=threading_mode,
        memory_limit_bytes=memory_limit_bytes,
    )
    resources_transferred = False
    try:
        prepared_input = prepare_public_input(
            input_path,
            input_format=input_format,
            input_mode=input_mode,
            input_text_encoding=str(options.get("input_text_encoding", "utf-8")),
            xml_row_tag=options.get("xml_row_tag"),
            csv_delimiter=str(options.get("csv_delimiter", ",")),
            csv_has_header=bool(options.get("csv_has_header", True)),
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            operation_context=operation_context,
        )
    except BaseException:
        operation_context.close()
        raise
    try:
        if prepared_input.xml_row_tag is not None:
            options = dict(options)
            options["xml_row_tag"] = prepared_input.xml_row_tag
            options["input_text_encoding"] = "utf-8"
        options = call_options_from_locals(options, ANALYTICAL_HELPER_KEYS)
        call_options = normalize_call_options_or_none(
            **options_for_registry_operation(
                options,
                registry_json=registry_json,
                schema_mode=schema_mode,
            )
        )
        call_options = attach_operation_detected_at(
            call_options,
            operation_context.detected_at,
            operation_context.memory_ledger,
        )
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
                timestamp_columns={
                    INGESTION_TIMESTAMP_COLUMN: operation_context.ingestion_timestamp_micros
                },
            )
            if opened is not None:
                if target == "pyarrow_reader":
                    stream = lazy_stream_from_opened(opened, prepared_input, operation_context)
                    patch_source_manifest_diagnostics(stream, prepared_input.source_manifest)
                    resources_transferred = True
                    return stream
                result = materialize_opened_registry_stream(
                    opened, target=target, threading_mode=threading_mode
                )
                result.execution_policy = operation_context.policy.to_dict()
                patch_source_manifest_diagnostics(result, prepared_input.source_manifest)
                return result
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
            ingestion_timestamp_micros=operation_context.ingestion_timestamp_micros,
        )
        if target == "pyarrow_reader":
            stream = lazy_stream_from_opened(opened, prepared_input, operation_context)
            patch_source_manifest_diagnostics(stream, prepared_input.source_manifest)
            resources_transferred = True
            return stream
        result = materialize_opened_registry_stream(
            opened, target=target, threading_mode=threading_mode
        )
        result.execution_policy = operation_context.policy.to_dict()
        patch_source_manifest_diagnostics(result, prepared_input.source_manifest)
        return result
    finally:
        if not resources_transferred:
            prepared_input.close()
            operation_context.close()


def to_duckdb(
    input_path: PublicInput,
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
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize input into DuckDB; the returned relation is outside the memory budget."""
    options = locals()
    return cast(
        Result,
        convert_analytical_with_options(
            input_path,
            target="duckdb",
            input_format=input_format,
            input_mode=input_mode,
            options=options,
            schema_registry=schema_registry,
        ),
    )


def to_pandas(
    input_path: PublicInput,
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
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize input into pandas; the returned DataFrame is outside the memory budget."""
    options = locals()
    return cast(
        Result,
        convert_analytical_with_options(
            input_path,
            target="pandas",
            input_format=input_format,
            input_mode=input_mode,
            options=options,
            schema_registry=schema_registry,
        ),
    )


def to_polars(
    input_path: PublicInput,
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
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize input into Polars; the returned DataFrame is outside the memory budget."""
    options = locals()
    return cast(
        Result,
        convert_analytical_with_options(
            input_path,
            target="polars",
            input_format=input_format,
            input_mode=input_mode,
            options=options,
            schema_registry=schema_registry,
        ),
    )


def to_pyarrow(
    input_path: PublicInput,
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
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Sanitize input into PyArrow; the returned table is outside the memory budget."""
    options = locals()
    return cast(
        Result,
        convert_analytical_with_options(
            input_path,
            target="pyarrow",
            input_format=input_format,
            input_mode=input_mode,
            options=options,
            schema_registry=schema_registry,
        ),
    )
