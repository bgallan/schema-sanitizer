"""Public streaming file-conversion orchestration and entry points."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Mapping, Sequence
from time import perf_counter, process_time
from typing import Any

from ...adapters.parquet.compression import (
    normalize_parquet_compression,
    normalize_parquet_gzip_level,
)
from ...core_impl.concurrency_contracts import activate_runtime_concurrency_pair_admission
from ...core_impl.concurrency_route_evidence import (
    input_route_profile,
    output_file_route_profile,
)
from ...core_impl.concurrency_stage_evidence import (
    observe_successful_input_runtime_stage,
    observe_successful_output_runtime_stage,
)

# reset_runtime_concurrency_pair is performed by the pair admission scope.
from ...core_impl.execution_policy import threading_mode_from_multi_threading
from ...core_impl.generated_metadata import INGESTION_TIMESTAMP_COLUMN, SOURCE_FILE_COLUMN
from ...core_impl.memory_budget import memory_budget, normalize_memory_limit
from ...core_impl.probes import options_for_registry_operation
from ...core_impl.safe_errors import add_bounded_note
from ...core_impl.schema_registry import current_native_registry_state
from ...options_impl.call_options import (
    FILE_CONVERSION_HELPER_KEYS,
    attach_operation_detected_at,
    call_options_from_locals,
    normalize_call_options_or_none,
)
from ...options_impl.options import CsvHeaderMode, normalize_csv_header_mode
from ...remote_impl.staging import (
    cleanup_output_target,
    finalize_output_target,
    prepare_output_target,
)
from ...sources.models import PublicInput
from ..input.preparation import prepare_public_input
from ..operation_context import OperationExecutionContext
from ..partition_resources import take_borrowed_partition_resources
from ..registry_output import (
    write_csv_registry_file,
    write_jsonl_registry_file,
    write_parquet_registry_file,
)
from ..results import Result
from ..source_manifest_diagnostics import patch_source_manifest_diagnostics
from ..streams import patch_diagnostics_values
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
    ingestion_timestamp_micros: int,
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
        timestamp_columns={INGESTION_TIMESTAMP_COLUMN: ingestion_timestamp_micros},
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
    input_path: PublicInput,
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
    normalize_csv_header_mode(options.get("csv_header_mode", "exact"))
    from ...core_impl.schema_registry import _normalize_registry_json

    registry_json = _normalize_registry_json(schema_registry)
    resolved_writer_options = writer_options or {}
    if schema_registry_native_state is None:
        schema_registry_native_state = current_native_registry_state()
    schema_mode = str(options.get("schema_mode", "additive")).strip().lower()
    file_io_seconds = 0.0
    threading_mode = threading_mode_from_multi_threading(options.get("multi_threading", False))
    memory_limit_bytes = normalize_memory_limit(options.get("memory_limit_bytes"))
    options = dict(options)
    options["memory_limit_bytes"] = memory_limit_bytes
    borrowed = (
        take_borrowed_partition_resources(
            input_path,
            threading_mode=threading_mode,
            memory_limit_bytes=memory_limit_bytes,
        )
        if isinstance(input_path, (str, os.PathLike))
        else None
    )
    owns_operation_context = borrowed is None
    partition_resources = None if borrowed is None else borrowed[2]
    operation_context = (
        OperationExecutionContext(
            threading_mode=threading_mode,
            memory_limit_bytes=memory_limit_bytes,
        )
        if borrowed is None
        else borrowed[1]
    )
    input_started_at = perf_counter()
    try:
        prepared_input = (
            prepare_public_input(
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
            if borrowed is None
            else borrowed[0]
        )
    except BaseException:
        if owns_operation_context:
            operation_context.close()
        raise
    file_io_seconds += max(perf_counter() - input_started_at, 0.0)
    output_contract = feature[3:] if feature.startswith("to_") else feature
    pair_scope = None
    result: Result | None = None
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
        pair_scope = activate_runtime_concurrency_pair_admission(
            prepared_input.public_format or prepared_input.format,
            output_contract,
            memory_ledger=operation_context.memory_ledger,
            desired_payload_slots=max(1, operation_context.policy.effective_workers),
            payload_window_bytes=max(4096, memory_budget(memory_limit_bytes).io_chunk_bytes),
            execution_lease=operation_context.execution_lease,
            route_profiles=(
                input_route_profile(prepared_input),
                output_file_route_profile(output_path),
            ),
        )
        pair_scope.transfer_to_output()
        # Keep the pair identity alive through the complete writer/conversion
        # path. The structural bootstrap credit was retired at the handoff, so
        # only real downstream admissions count as payload evidence.
        if prepared_input.xml_row_tag is not None:
            options = dict(options)
            options["xml_row_tag"] = prepared_input.xml_row_tag
            options["input_text_encoding"] = "utf-8"
        output_started_at = perf_counter()
        output_target = prepare_output_target(
            output_path,
            memory_limit_bytes=options.get("memory_limit_bytes"),
            threading_mode=threading_mode,
            operation_context=operation_context,
        )
        file_io_seconds += max(perf_counter() - output_started_at, 0.0)
        try:
            if (
                partition_resources is not None
                and partition_resources.allow_early_lookahead
                and output_target.remote_uri is None
            ):
                partition_resources.trigger()
            conversion_cpu_started_at = process_time()
            options = call_options_from_locals(
                dict(options),
                FILE_CONVERSION_HELPER_KEYS,
            )
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
                ingestion_timestamp_micros=operation_context.ingestion_timestamp_micros,
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
                    timestamp_columns={
                        INGESTION_TIMESTAMP_COLUMN: operation_context.ingestion_timestamp_micros
                    },
                    schema_mode=schema_mode,
                    field_name_policy=field_name_policy,
                    **resolved_writer_options,
                )
            # Evidence is published at the actual combined decode/sink boundary,
            # not at the public wrapper return. A bypassed writer route therefore
            # cannot satisfy the release stage gate merely by returning success.
            observe_successful_input_runtime_stage(
                prepared_input.public_format or prepared_input.format
            )
            observe_successful_output_runtime_stage(output_contract)
            result.conversion_cpu_seconds = max(
                process_time() - conversion_cpu_started_at,
                0.0,
            )
            if partition_resources is not None and output_target.remote_uri is None:
                partition_resources.trigger()
            upload_started_at = perf_counter()
            if partition_resources is None:
                finalize_output_target(output_target)
            else:
                finalize_output_target(
                    output_target,
                    before_remote_upload=partition_resources.trigger,
                )
            file_io_seconds += max(perf_counter() - upload_started_at, 0.0)
            result.execution_policy = operation_context.policy.to_dict()
            patch_source_manifest_diagnostics(result, prepared_input.source_manifest)
            return result
        except Exception:
            cleanup_output_target(output_target)
            raise
    finally:
        primary = sys.exc_info()[1]
        cleanup_error: BaseException | None = None

        def record_cleanup_failure(label: str, exc: BaseException) -> None:
            nonlocal cleanup_error
            if primary is not None:
                add_bounded_note(primary, label, exc)
            elif cleanup_error is None:
                cleanup_error = exc
            else:
                add_bounded_note(cleanup_error, label, exc)

        cleanup_started_at = perf_counter()
        if pair_scope is not None:
            try:
                pair_scope.close()
                pair_scope = None
            except BaseException as exc:
                record_cleanup_failure("runtime-pair admission cleanup also failed", exc)
        try:
            prepared_input.close()
        except BaseException as exc:
            record_cleanup_failure("prepared input cleanup also failed", exc)
        try:
            operation_context.close()
        except BaseException as exc:
            record_cleanup_failure("operation context cleanup also failed", exc)
        file_io_seconds += max(perf_counter() - cleanup_started_at, 0.0)
        if result is not None:
            result.file_io_seconds = file_io_seconds
            if cleanup_error is None:
                diagnostics = getattr(getattr(result, "_raw", None), "diagnostics", None)
                patch_diagnostics_values(
                    diagnostics,
                    {"current_charged_memory_bytes": 0},
                )
        if primary is None and cleanup_error is not None:
            raise cleanup_error


def _convert_public_file(
    input_path: PublicInput,
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
    input_path: PublicInput,
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
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Stream-sanitize a file or Python row iterable to JSON Lines."""
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
    input_path: PublicInput,
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
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Stream-sanitize a file or Python row iterable to CSV."""
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
    input_path: PublicInput,
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
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    parquet_compression: str | None = "gzip",
    parquet_gzip_level: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Result:
    """Stream-sanitize a file or Python row iterable to Parquet."""
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
