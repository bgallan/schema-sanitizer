"""Registry-backed streaming file writer helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .ingest_runtime_selectors import (
    _Format,
    _resolve_source_and_format,
    _Source,
)
from .ingest_runtime_types import Result
from .native_directory_errors import unsupported_native_directory_ingestion
from .native_file_output import (
    write_csv_native_first_stream,
    write_jsonl_native_first_stream,
    write_parquet_native_first_stream,
)
from .native_ingest_plan import native_ingest_plan, normalize_options
from .parquet_errors import unsupported_direct_parquet_ingestion
from .pool import default_pool
from .registry_file_writer_helpers import (
    _try_write_direct_parquet_registry_to_file,
    _write_registry_raw_stream_to_file,
)
from .shared import Options, _call_core, _unwrap_options
from .source_plan import (
    PARQUET_ARROW_SOURCES,
    source_plan_from_data,
    write_source_plan_registry_to_file,
)


def _write_registry_to_file(
    data: Any,
    out_path: Any,
    *,
    options: Options | dict[str, Any] | None,
    format: _Format,
    source: _Source,
    writer: Callable[..., None],
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any = None,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Ingest and write using native registry-backed schema preparation."""
    call_options = normalize_options(options)
    source_plan = source_plan_from_data(data)
    if source_plan is not None:
        raw_ctx = default_pool().get()._raw
        plan_result = write_source_plan_registry_to_file(
            raw_ctx,
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

    data, source, format = _resolve_source_and_format(
        data,
        format=format,
        source=source,
    )
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

    plan = native_ingest_plan(
        data,
        format=format,
        source=source,
        options=call_options,
    )

    try:
        prepared = _unwrap_options(plan.call_options)
        raw_ctx = default_pool().get()._raw
        raw = _call_core(
            raw_ctx.to_registry_sink_from_source,
            "stream",
            plan.format,
            plan.source,
            plan.data,
            prepared,
            registry_json=schema_registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns or {},
            all_row_columns=all_row_columns or {},
            row_span_columns=row_span_columns or {},
            timestamp_columns=timestamp_columns,
        )
        return _write_registry_raw_stream_to_file(
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
        )
    finally:
        plan.close_keepalive()


def _to_parquet_registry_stream(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any = None,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Ingest and write Parquet using native registry-backed preparation."""
    return _write_registry_to_file(
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


def _to_jsonl_registry_stream(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any = None,
) -> Result:
    """Ingest and write JSONL using native registry-backed preparation."""
    return _write_registry_to_file(
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
    )


def _to_csv_registry_stream(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    schema_registry_native_state: Any = None,
) -> Result:
    """Ingest and write CSV using native registry-backed preparation."""
    return _write_registry_to_file(
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
    )


_to_parquet_registry_stream._source_plan_writer = write_parquet_native_first_stream  # type: ignore[attr-defined]
_to_parquet_registry_stream._source_plan_feature = "to_parquet"  # type: ignore[attr-defined]
_to_jsonl_registry_stream._source_plan_writer = write_jsonl_native_first_stream  # type: ignore[attr-defined]
_to_jsonl_registry_stream._source_plan_feature = "to_jsonl"  # type: ignore[attr-defined]
_to_csv_registry_stream._source_plan_writer = write_csv_native_first_stream  # type: ignore[attr-defined]
_to_csv_registry_stream._source_plan_feature = "to_csv"  # type: ignore[attr-defined]
