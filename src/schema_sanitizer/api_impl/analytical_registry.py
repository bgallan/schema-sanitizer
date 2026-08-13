"""Open one prepared analytical input as a native registry stream."""

from __future__ import annotations

import os
from typing import Any, cast

from ..core_impl.concurrency_stage_evidence import observe_successful_input_runtime_stage
from ..core_impl.error_translation import call_core, reader_error_context
from ..core_impl.generated_metadata import INGESTION_TIMESTAMP_COLUMN, SOURCE_FILE_COLUMN
from ..input_impl.prepared import PreparedPublicInput
from ..input_impl.selection import _Source
from ..options_impl.call_options import unwrap_options
from ..options_impl.options import memory_limit_bytes_or_none
from .input.directory_preparation import prepare_single_parquet_file
from .parquet.direct_routes import parquet_direct_registry_sink_raw_or_none
from .parquet.errors import unsupported_direct_parquet_ingestion
from .source_plan.attached import source_plan_from_data
from .source_plan.registry import (
    OpenedSourcePlanRegistryStream,
    open_source_plan_registry_stream,
)


def open_single_source_registry_stream(
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
            observe_successful_input_runtime_stage("parquet")
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
            memory_limit_bytes=memory_limit_bytes_or_none(call_options),
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
        observe_successful_input_runtime_stage("parquet")
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
        observe_successful_input_runtime_stage("python")
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
            prepared_input.format,
            prepared_input.source,
            prepared_input.data,
        ),
    )
    observe_successful_input_runtime_stage(prepared_input.public_format or prepared_input.format)
    return OpenedSourcePlanRegistryStream(
        stream=None,
        schema_registry_json=raw.schema_registry_json,
        schema_drifts_json=raw.schema_drifts_json,
        diagnostics=raw.diagnostics,
        native_registry_state=raw.native_registry_state,
        raw_stream=raw,
        close_items=[raw],
    )
