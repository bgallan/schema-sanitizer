"""Public and internal in-memory analytical conversion."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any, cast

from schema_sanitizer.core_impl.concurrency_contracts import (
    activate_runtime_concurrency_pair_admission,
)
from schema_sanitizer.core_impl.concurrency_route_evidence import (
    analytical_output_route_profile,
    input_route_profile,
)
from schema_sanitizer.core_impl.concurrency_stage_evidence import (
    observe_successful_input_runtime_stage,
)

# reset_runtime_concurrency_pair is performed by the pair admission scope.
from schema_sanitizer.core_impl.execution_policy import (
    threading_mode_from_multi_threading,
)
from schema_sanitizer.core_impl.generated_metadata import INGESTION_TIMESTAMP_COLUMN
from schema_sanitizer.core_impl.memory_budget import memory_budget, normalize_memory_limit
from schema_sanitizer.input_impl.source_plan import PARQUET_ARROW_SOURCES
from schema_sanitizer.sources.models import PublicInput

from ..core_impl.probes import options_for_registry_operation
from ..core_impl.safe_errors import add_bounded_note
from ..core_impl.schema_registry import _normalize_registry_json
from ..input_impl.prepared import ChainedKeepalive, PreparedPublicInput
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
    require_implemented_csv_header_mode,
)
from .analytical_registry import open_single_source_registry_stream
from .batch_streaming import lazy_stream_from_opened
from .execution_context import default_pool
from .input.preparation import prepare_public_input
from .operation_context import OperationExecutionContext
from .parquet.errors import unsupported_direct_parquet_ingestion
from .results import Result, _OwnedDuckDBRelation
from .source_manifest_diagnostics import patch_source_manifest_diagnostics
from .source_plan.attached import source_plan_from_data
from .source_plan.registry import (
    materialize_opened_registry_stream,
    open_source_plan_registry_stream,
)
from .streams import Stream


def _retain_lazy_analytical_resources(
    result: Result,
    *,
    target: str,
    prepared_input: PreparedPublicInput,
    operation_context: OperationExecutionContext,
    pair_scope: Any,
) -> bool:
    """Keep source and operation authorities with a lazy analytical result."""
    if target != "duckdb":
        return False
    existing = getattr(result, "_keepalive", None)
    payload_owner = getattr(pair_scope, "payload_admission", None)
    if payload_owner is None:
        keepalive = (
            ChainedKeepalive(operation_context, prepared_input)
            if existing is None
            else ChainedKeepalive(operation_context, prepared_input, existing)
        )
    else:
        keepalive = (
            ChainedKeepalive(operation_context, prepared_input, payload_owner)
            if existing is None
            else ChainedKeepalive(
                operation_context,
                prepared_input,
                payload_owner,
                existing,
            )
        )
    clean_data = getattr(result, "_clean_data_cache", None)
    if isinstance(clean_data, _OwnedDuckDBRelation):
        # A caller may retain ``result.clean_data`` without retaining Result.
        # Publish the complete lazy operation on the relation's shared lifetime
        # so derived DuckDB relations inherit the same exact ownership chain.
        clean_data._attach_keepalive(keepalive)
        if existing is not None:
            result._keepalive = None
        result._sync_finalizer_capsule()
    else:
        # Compatibility path for test doubles and non-owned adapter results.
        result._keepalive = keepalive
        sync_finalizer = getattr(result, "_sync_finalizer_capsule", None)
        if callable(sync_finalizer):
            sync_finalizer()
    if (
        payload_owner is not None
        and getattr(pair_scope, "payload_admission", None) is payload_owner
    ):
        pair_scope.payload_admission = None
    return True


def _result_retains_lazy_analytical_resources(
    result: Result,
    prepared_input: PreparedPublicInput,
    operation_context: OperationExecutionContext,
) -> bool:
    """Recover a committed handoff after an asynchronous Python unwind."""
    clean_data = getattr(result, "_clean_data_cache", None)
    if isinstance(clean_data, _OwnedDuckDBRelation) and clean_data._retains_resources(
        prepared_input,
        operation_context,
    ):
        return True
    keepalive = getattr(result, "_keepalive", None)
    items = getattr(keepalive, "_items", None)
    if not isinstance(items, list):
        return False
    found_prepared = False
    found_context = False
    for item in items:
        if item is prepared_input:
            found_prepared = True
        elif item is operation_context:
            found_context = True
    return found_prepared and found_context


def _stream_retains_lazy_analytical_resources(
    stream: Stream,
    prepared_input: PreparedPublicInput,
    operation_context: OperationExecutionContext,
    payload_owner: Any,
) -> bool:
    """Confirm that one Stream owns the exact lazy operation handoff."""
    resources = getattr(stream, "_keepalive", None)
    retains = getattr(resources, "retains", None)
    return bool(callable(retains) and retains(prepared_input, operation_context, payload_owner))


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
    external_worker_envelope = (
        max(2, os.cpu_count() or 2)
        if target in {"pandas", "polars"} and threading_mode == "multi"
        else 0
    )
    exact_external_workers = external_worker_envelope if target == "polars" else 0
    operation_context = OperationExecutionContext(
        threading_mode=threading_mode,
        memory_limit_bytes=memory_limit_bytes,
        external_runtime_workers=external_worker_envelope,
        exact_external_runtime_workers=exact_external_workers,
    )
    resources_transferred = False
    pair_scope = None
    result: Result | None = None
    stream: Stream | None = None
    stream_handoff: list[Stream] = []
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
        pair_scope = activate_runtime_concurrency_pair_admission(
            prepared_input.public_format or prepared_input.format,
            target,
            memory_ledger=operation_context.memory_ledger,
            desired_payload_slots=max(1, operation_context.policy.effective_workers),
            payload_window_bytes=max(4096, memory_budget(memory_limit_bytes).io_chunk_bytes),
            execution_lease=operation_context.execution_lease,
            route_profiles=(
                input_route_profile(prepared_input),
                analytical_output_route_profile(target),
            ),
        )
        pair_scope.transfer_to_output()
        # Keep the pair identity alive through the complete writer/conversion
        # path. The structural bootstrap credit was retired at the handoff, so
        # only real downstream admissions count as pass51 payload evidence.
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
                observe_successful_input_runtime_stage(
                    prepared_input.public_format or prepared_input.format
                )
                if target == "pyarrow_reader":
                    payload_owner = getattr(pair_scope, "payload_admission", None)
                    stream = lazy_stream_from_opened(
                        opened,
                        prepared_input,
                        operation_context,
                        payload_owner=payload_owner,
                        handoff=stream_handoff,
                    )
                    if (
                        payload_owner is not None
                        and getattr(pair_scope, "payload_admission", None) is payload_owner
                    ):
                        pair_scope.payload_admission = None
                    patch_source_manifest_diagnostics(stream, prepared_input.source_manifest)
                    resources_transferred = True
                    return stream
                result = materialize_opened_registry_stream(
                    opened, target=target, threading_mode=threading_mode
                )
                result.execution_policy = operation_context.policy.to_dict()
                patch_source_manifest_diagnostics(result, prepared_input.source_manifest)
                resources_transferred = _retain_lazy_analytical_resources(
                    result,
                    target=target,
                    prepared_input=prepared_input,
                    operation_context=operation_context,
                    pair_scope=pair_scope,
                )
                return result
            if source_plan.kind == PARQUET_ARROW_SOURCES:
                raise unsupported_direct_parquet_ingestion()
            raise unsupported_native_directory_ingestion()
        opened = open_single_source_registry_stream(
            raw_ctx,
            prepared_input=prepared_input,
            call_options=call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            ingestion_timestamp_micros=operation_context.ingestion_timestamp_micros,
        )
        if target == "pyarrow_reader":
            payload_owner = getattr(pair_scope, "payload_admission", None)
            stream = lazy_stream_from_opened(
                opened,
                prepared_input,
                operation_context,
                payload_owner=payload_owner,
                handoff=stream_handoff,
            )
            if (
                payload_owner is not None
                and getattr(pair_scope, "payload_admission", None) is payload_owner
            ):
                pair_scope.payload_admission = None
            patch_source_manifest_diagnostics(stream, prepared_input.source_manifest)
            resources_transferred = True
            return stream
        result = materialize_opened_registry_stream(
            opened, target=target, threading_mode=threading_mode
        )
        result.execution_policy = operation_context.policy.to_dict()
        patch_source_manifest_diagnostics(result, prepared_input.source_manifest)
        resources_transferred = _retain_lazy_analytical_resources(
            result,
            target=target,
            prepared_input=prepared_input,
            operation_context=operation_context,
            pair_scope=pair_scope,
        )
        return result
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

        if (
            not resources_transferred
            and result is not None
            and _result_retains_lazy_analytical_resources(
                result,
                prepared_input,
                operation_context,
            )
        ):
            # The keepalive publication is the authority. This covers an async
            # exception after attachment but before the caller's boolean STORE.
            resources_transferred = True
            # The retained chain was assembled from this exact payload owner.
            # Clear the scope's mirror before closing it so an interrupt between
            # publication and the normal ownership STORE cannot double-close it.
            if pair_scope is not None:
                pair_scope.payload_admission = None
        stream_owner = stream
        if stream_owner is None and stream_handoff:
            stream_owner = stream_handoff[-1]
        if not resources_transferred and stream_owner is not None:
            payload_owner = (
                getattr(pair_scope, "payload_admission", None) if pair_scope is not None else None
            )
            if _stream_retains_lazy_analytical_resources(
                stream_owner,
                prepared_input,
                operation_context,
                payload_owner,
            ):
                resources_transferred = True
                if pair_scope is not None:
                    pair_scope.payload_admission = None
        if pair_scope is not None:
            try:
                pair_scope.close()
                pair_scope = None
            except BaseException as exc:
                record_cleanup_failure("runtime-pair admission cleanup also failed", exc)
        if target == "duckdb" and result is not None and not resources_transferred:
            try:
                result.close()
            except BaseException as exc:
                record_cleanup_failure("lazy analytical result rollback also failed", exc)
        if not resources_transferred:
            try:
                prepared_input.close()
            except BaseException as exc:
                record_cleanup_failure("prepared input cleanup also failed", exc)
            try:
                operation_context.close()
            except BaseException as exc:
                record_cleanup_failure("operation context cleanup also failed", exc)
        stream_handoff.clear()
        if primary is None and cleanup_error is not None:
            raise cleanup_error


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
