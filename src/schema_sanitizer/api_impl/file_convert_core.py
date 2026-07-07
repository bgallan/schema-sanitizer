"""Shared internals for public file conversion wrappers."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from ..options_impl.call_options import normalize_call_options_or_none
from .async_remote_io import (
    cleanup_output_target,
    finalize_output_target,
    prepare_output_target,
)
from .file_api_helpers import _call_options_from_locals
from .file_api_option_sets import CONVERTER_HELPER_KEYS, PARQUET_WRITER_OPTION_KEYS
from .file_conversion_metadata import INGESTION_TIMESTAMP_COLUMN, SOURCE_FILE_COLUMN
from .ingest_runtime_types import Result
from .public_input import prepare_public_input

_SCHEMA_REGISTRY_NATIVE_STATE: ContextVar[Any | None] = ContextVar(
    "schema_sanitizer_schema_registry_native_state",
    default=None,
)


@contextmanager
def schema_registry_native_state_context(native_state: Any):
    """Temporarily seed file conversion with a native registry-state capsule."""
    token = _SCHEMA_REGISTRY_NATIVE_STATE.set(native_state)
    try:
        yield
    finally:
        _SCHEMA_REGISTRY_NATIVE_STATE.reset(token)


def options_for_schema_probe(options: dict[str, Any]) -> dict[str, Any]:
    """Return converter options that infer the current input schema."""
    out = dict(options)
    out["schema_contract"] = None
    out["schema_mode"] = "additive"
    return out


def _try_convert_source_plan_with_options(
    prepared_input: Any,
    output_path: str | os.PathLike[str],
    *,
    writer: Callable[..., Result],
    call_options: Any,
    schema_registry_json: str,
    schema_registry_native_state: Any = None,
    schema_mode: str,
    field_name_policy: str,
    writer_options: Mapping[str, Any] | None = None,
) -> Result | None:
    """Write a prepared source-plan input through the canonical native path."""
    source_plan_writer = getattr(writer, "_source_plan_writer", None)
    feature = getattr(writer, "_source_plan_feature", None)
    if source_plan_writer is None or not isinstance(feature, str):
        return None

    from . import source_plan as source_plan_module
    from .native_directory_errors import unsupported_native_directory_ingestion
    from .parquet_errors import unsupported_direct_parquet_ingestion
    from .pool import default_pool

    plan = source_plan_module.source_plan_from_data(prepared_input.data)
    if plan is None:
        return None

    plan_result = source_plan_module.write_source_plan_registry_to_file(
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
        parquet_compression=(writer_options or {}).get("parquet_compression"),
        parquet_gzip_level=(writer_options or {}).get("parquet_gzip_level"),
    )
    if plan_result is not None:
        return plan_result
    if plan.kind == source_plan_module.PARQUET_ARROW_SOURCES:
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
    schema_registry: Mapping[str, Any] | str | None,
    schema_registry_native_state: Any = None,
    writer_options: Mapping[str, Any] | None = None,
) -> Result:
    """Normalize file conversion options and invoke a streaming writer."""
    from .schema_registry import _normalize_registry_json

    registry_json = _normalize_registry_json(schema_registry)
    if schema_registry_native_state is None:
        schema_registry_native_state = _SCHEMA_REGISTRY_NATIVE_STATE.get()
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
        options = _call_options_from_locals(
            dict(options),
            CONVERTER_HELPER_KEYS | PARQUET_WRITER_OPTION_KEYS,
        )
        call_options = normalize_call_options_or_none(**options_for_schema_probe(options))
        output_target = prepare_output_target(output_path)
        try:
            field_name_policy = str(options.get("field_name_policy", "lower_alpha"))
            result = _try_convert_source_plan_with_options(
                prepared_input,
                output_target.local_path,
                writer=writer,
                call_options=call_options,
                schema_registry_json=registry_json,
                schema_registry_native_state=schema_registry_native_state,
                schema_mode=schema_mode,
                field_name_policy=field_name_policy,
                writer_options=writer_options,
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
                    **dict(writer_options or {}),
                )
            finalize_output_target(output_target)
            return result
        except Exception:
            cleanup_output_target(output_target)
            raise
    finally:
        prepared_input.close()
