"""Registry-backed file output routing and native stream ownership.

It routes registry streams to raw or metadata-aware file writers, handles direct native
paths, and returns diagnostics after authoritative cleanup.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from schema_sanitizer.core_impl.error_translation import call_core, reader_error_context
from schema_sanitizer.core_impl.execution_policy import normalize_threading_mode
from schema_sanitizer.core_impl.generated_metadata import (
    SCHEMA_DRIFTS_COLUMN,
    SCHEMA_REGISTRY_COLUMN,
    TimestampColumns,
)
from schema_sanitizer.input_impl.source_plan import PARQUET_ARROW_SOURCES

from ..input_impl.selection import (
    _Format,
    _Source,
    resolve_source_and_format,
    unsupported_native_directory_ingestion,
)
from ..options_impl.call_options import unwrap_options
from ..options_impl.options import Options, memory_limit_bytes_or_none
from .execution_context import default_pool
from .file_conversion.writers import (
    write_csv_native_first_stream,
    write_jsonl_native_first_stream,
    write_parquet_native_first_stream,
)
from .ingest import native_ingest_plan, normalize_options
from .input.memory_limits import enforce_materialized_input_limit
from .parquet.direct_routes import (
    parquet_direct_registry_sink,
    parquet_direct_stream_factory,
)
from .parquet.errors import unsupported_direct_parquet_ingestion
from .results import Result
from .source_plan.attached import source_plan_from_data
from .source_plan.registry import write_source_plan_registry_to_file
from .stream_output import write_raw_stream_to_file


def write_registry_raw_stream_to_file(
    raw: Any,
    out_path: Any,
    *,
    writer: Callable[..., Any],
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    metadata_already_in_stream: bool = False,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    schema_registry_json: str | None = None,
    schema_drifts_json: str | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> Result:
    """Write a registry-backed sink from an already-open native raw stream."""
    registry_json = (
        raw.schema_registry_json if schema_registry_json is None else schema_registry_json
    )
    drifts_json = raw.schema_drifts_json if schema_drifts_json is None else schema_drifts_json
    if metadata_already_in_stream:
        merged_first_row_columns = None
        all_row_columns = None
        row_span_columns = None
        timestamp_columns = ()
    else:
        merged_first_row_columns = dict(first_row_columns or {})
        merged_first_row_columns.update(
            {
                SCHEMA_REGISTRY_COLUMN: registry_json,
                SCHEMA_DRIFTS_COLUMN: drifts_json,
            }
        )
    result = write_raw_stream_to_file(
        raw,
        out_path,
        writer=writer,
        feature=feature,
        first_row_columns=merged_first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
    result.schema_registry_json = registry_json
    result.schema_drifts_json = drifts_json
    result.native_registry_state = raw.native_registry_state
    return result


def _try_write_direct_parquet_registry_to_file(
    data: Any,
    out_path: Any,
    *,
    source: _Source,
    writer: Callable[..., Any],
    feature: str,
    call_options: Options | None,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    parquet_compression: str | None,
    parquet_gzip_level: int | None,
) -> Result | None:
    """Write direct Parquet registry output when the native Arrow path applies."""
    memory_limit_bytes = memory_limit_bytes_or_none(call_options)
    threading_mode = (
        normalize_threading_mode(call_options.performance.threading_mode)
        if call_options is not None
        else "single"
    )
    direct_outcome = parquet_direct_registry_sink(
        default_pool().get()._raw,
        data,
        source=source,
        feature=feature,
        call_options=call_options,
        schema_registry_json=schema_registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
    )
    raw = direct_outcome.raw
    fallback_registry_json: str | None = None
    fallback_drifts_json: str | None = None
    if raw is None:
        if direct_outcome.route != "pyarrow_registry_unavailable":
            return None
        factory_outcome = parquet_direct_stream_factory(
            data,
            source=source,
            feature=feature,
            call_options=call_options,
        )
        raw = factory_outcome.raw
        if raw is None:
            return None
        fallback_registry_json = schema_registry_json
        fallback_drifts_json = "[]"
    return write_registry_raw_stream_to_file(
        raw,
        out_path,
        writer=writer,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
        schema_registry_json=fallback_registry_json,
        schema_drifts_json=fallback_drifts_json,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def _write_registry_file(
    data: Any,
    out_path: Any,
    *,
    options: Options | dict[str, Any] | None,
    format: _Format,
    source: _Source,
    writer: Callable[..., Any],
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any,
    parquet_compression: str | None,
    parquet_gzip_level: int | None,
) -> Result:
    """Ingest and write using native registry-backed schema preparation."""
    call_options = normalize_options(options)
    memory_limit_bytes = memory_limit_bytes_or_none(call_options)
    threading_mode = (
        normalize_threading_mode(call_options.performance.threading_mode)
        if call_options is not None
        else "single"
    )
    source_plan = source_plan_from_data(data)
    if source_plan is not None:
        plan_result = write_source_plan_registry_to_file(
            default_pool().get()._raw,
            source_plan,
            out_path,
            writer=writer,
            feature=feature,
            call_options=call_options,
            first_row_columns=first_row_columns or {},
            timestamp_columns=timestamp_columns,
            schema_registry_json=schema_registry_json,
            schema_mode=schema_mode,
            field_name_policy=field_name_policy,
            native_registry_state=schema_registry_native_state,
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
        )
        if plan_result is not None:
            return plan_result
        if source_plan.kind == PARQUET_ARROW_SOURCES:
            raise unsupported_direct_parquet_ingestion()
        raise unsupported_native_directory_ingestion()

    data, source, format = resolve_source_and_format(data, format=format, source=source)
    if format == "parquet":
        direct_result = _try_write_direct_parquet_registry_to_file(
            data,
            out_path,
            source=source,
            writer=writer,
            feature=feature,
            call_options=call_options,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            schema_registry_json=schema_registry_json,
            schema_mode=schema_mode,
            field_name_policy=field_name_policy,
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
        )
        if direct_result is not None:
            return direct_result

    if format == "python":
        enforce_materialized_input_limit(
            data,
            "python",
            memory_limit_bytes=memory_limit_bytes,
            source="python",
        )
        raw = call_core(
            default_pool().get()._raw.to_registry_sink_python,
            "stream",
            data,
            unwrap_options(call_options),
            registry_json=schema_registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns or {},
            all_row_columns=all_row_columns or {},
            row_span_columns=row_span_columns or {},
            timestamp_columns=timestamp_columns,
        )
        return write_registry_raw_stream_to_file(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            writer=writer,
            feature=feature,
            metadata_already_in_stream=True,
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )

    plan = native_ingest_plan(data, format=format, source=source, options=call_options)
    try:
        raw = call_core(
            default_pool().get()._raw.to_registry_sink_from_source,
            "stream",
            plan.format,
            plan.source,
            plan.data,
            unwrap_options(plan.call_options),
            registry_json=schema_registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns or {},
            all_row_columns=all_row_columns or {},
            row_span_columns=row_span_columns or {},
            timestamp_columns=timestamp_columns,
            error_context=reader_error_context(plan.format, plan.source, plan.data),
        )
        return write_registry_raw_stream_to_file(
            raw,
            out_path,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
            writer=writer,
            feature=feature,
            metadata_already_in_stream=True,
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
    finally:
        plan.close_keepalive()


def write_parquet_registry_file(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any = None,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Ingest and write Parquet using native registry-backed preparation."""
    return _write_registry_file(
        data,
        out_path,
        options=options,
        format=format,
        source=source,
        writer=write_parquet_native_first_stream,
        feature="to_parquet",
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        schema_registry_json=schema_registry_json,
        schema_mode=schema_mode,
        field_name_policy=field_name_policy,
        schema_registry_native_state=schema_registry_native_state,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
    )


def write_jsonl_registry_file(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any = None,
) -> Result:
    """Ingest and write JSONL using native registry-backed preparation."""
    return _write_registry_file(
        data,
        out_path,
        options=options,
        format=format,
        source=source,
        writer=write_jsonl_native_first_stream,
        feature="to_jsonl",
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        schema_registry_json=schema_registry_json,
        schema_mode=schema_mode,
        field_name_policy=field_name_policy,
        schema_registry_native_state=schema_registry_native_state,
        parquet_compression=None,
        parquet_gzip_level=None,
    )


def write_csv_registry_file(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: TimestampColumns = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any = None,
) -> Result:
    """Ingest and write CSV using native registry-backed preparation."""
    return _write_registry_file(
        data,
        out_path,
        options=options,
        format=format,
        source=source,
        writer=write_csv_native_first_stream,
        feature="to_csv",
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        schema_registry_json=schema_registry_json,
        schema_mode=schema_mode,
        field_name_policy=field_name_policy,
        schema_registry_native_state=schema_registry_native_state,
        parquet_compression=None,
        parquet_gzip_level=None,
    )
