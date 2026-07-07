"""Registry-backed in-memory analytical conversion orchestration."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from ..options_impl.call_options import normalize_call_options_or_none
from .file_api_helpers import _call_options_from_locals
from .file_api_option_sets import ANALYTICAL_HELPER_KEYS
from .file_conversion_metadata import (
    INGESTION_TIMESTAMP_COLUMN,
    SOURCE_FILE_COLUMN,
)
from .file_convert_core import options_for_schema_probe
from .ingest_runtime_types import Result
from .native_directory_errors import unsupported_native_directory_ingestion
from .parquet_errors import unsupported_direct_parquet_ingestion
from .pool import default_pool
from .public_input import PreparedPublicInput, prepare_public_input
from .schema_registry import _normalize_registry_json
from .shared import _call_core, _unwrap_options
from .source_plan import (
    PARQUET_ARROW_SOURCES,
    OpenedSourcePlanRegistryStream,
    open_source_plan_registry_stream,
    source_plan_from_data,
)
from .source_plan_registry_output import materialize_opened_registry_stream


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
    raw = _call_core(
        raw_ctx.to_registry_sink_from_source,
        "stream",
        prepared_input.format,
        prepared_input.source,
        prepared_input.data,
        _unwrap_options(call_options),
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
        diagnostics=getattr(raw, "diagnostics", None),
        native_registry_state=getattr(raw, "native_registry_state", None),
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
        options = _call_options_from_locals(dict(options), ANALYTICAL_HELPER_KEYS)
        call_options = normalize_call_options_or_none(**options_for_schema_probe(options))
        raw_ctx = default_pool().get()._raw
        field_name_policy = str(options.get("field_name_policy", "lower_alpha"))
        source_plan = source_plan_from_data(prepared_input.data)
        if source_plan is not None:
            opened = open_source_plan_registry_stream(
                raw_ctx,
                source_plan,
                _unwrap_options(call_options),
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                first_row_columns={},
                timestamp_columns=(INGESTION_TIMESTAMP_COLUMN,),
                feature=f"to_{target}",
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
