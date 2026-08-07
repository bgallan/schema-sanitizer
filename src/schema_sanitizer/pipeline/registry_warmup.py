"""Native schema-registry warm-up across partition plans."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from time import perf_counter, process_time
from typing import Any

from ..api_impl.input.preparation import prepare_public_input
from ..api_impl.operation_context import OperationExecutionContext
from ..api_impl.source_plan.preparation import source_plan_from_prepared_inputs
from ..api_impl.source_plan.probing import probe_prepared_source_plan_registry
from ..core_impl.execution import default_execution_context
from ..core_impl.execution_policy import threading_mode_from_multi_threading
from ..core_impl.memory_budget import normalize_memory_limit
from ..core_impl.probes import options_for_schema_probe
from ..core_impl.resource_lifecycle import _cleanup_with_note
from ..core_impl.schema_registry import _normalize_registry_json
from ..input_impl.directory_inputs import discovered_directory_input_context
from ..input_impl.prepared import PreparedPublicInput
from ..options_impl.call_options import (
    FILE_CONVERSION_HELPER_KEYS,
    attach_operation_detected_at,
    call_options_from_locals,
    normalize_call_options_or_none,
    unwrap_options,
)
from .observability import estimate_cpu_io_wall_time
from .types import PartitionRunPlan, SchemaRegistryState

SUPPORTED_WARM_UP_INPUT_FORMATS = frozenset(
    {"csv", "json", "json_array", "jsonl", "ndjson", "parquet", "xml"}
)
WarmUpProgressCallback = Callable[[int, int, PartitionRunPlan, float, float, float], None]
WarmUpSchemaDriftCallback = Callable[[int, int, PartitionRunPlan, str], None]

_LAST_WARM_UP_ROUTE = "none"


def last_warm_up_route() -> str:
    """Return the route used by the most recent schema warm-up inference."""
    return _LAST_WARM_UP_ROUTE


def prepare_schema_warm_up_input(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    csv_delimiter: str = ",",
    csv_has_header: bool = True,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
    _enable_parquet_native: bool = True,
    after_source_prepared: WarmUpProgressCallback | None = None,
    operation_context: OperationExecutionContext | None = None,
) -> PreparedPublicInput:
    """Prepare warm-up sources as one native source plan."""
    if input_format not in SUPPORTED_WARM_UP_INPUT_FORMATS:
        accepted = ", ".join(sorted(SUPPORTED_WARM_UP_INPUT_FORMATS))
        raise ValueError(
            f"Schema warm-up currently supports input_format={accepted}. "
            "Other formats need a format-specific multi-source reader."
        )
    if not plans:
        raise ValueError("Schema warm-up requires at least one source partition")

    prepared_inputs: list[PreparedPublicInput] = []
    source_plan: Any | None = None
    operation_context = (
        operation_context.fork()
        if operation_context is not None
        else OperationExecutionContext(
            threading_mode=threading_mode,
            memory_limit_bytes=memory_limit_bytes,
        )
    )
    owners_attached = False
    try:
        total = len(plans)
        for index, plan in enumerate(plans, start=1):
            wall_start = perf_counter()
            with discovered_directory_input_context(plan.source_uri, plan.discovered_input):
                prepared = prepare_public_input(
                    plan.source_uri,
                    input_format=input_format,
                    input_mode=input_mode,
                    input_text_encoding=input_text_encoding,
                    xml_row_tag=xml_row_tag,
                    csv_delimiter=csv_delimiter,
                    csv_has_header=csv_has_header,
                    memory_limit_bytes=memory_limit_bytes,
                    threading_mode=threading_mode,
                    operation_context=operation_context,
                )
            prepared_inputs.append(prepared)
            if after_source_prepared is not None:
                wall_seconds = plan.discovery_seconds + max(
                    perf_counter() - wall_start,
                    0.0,
                )
                after_source_prepared(
                    index,
                    total,
                    plan,
                    wall_seconds,
                    0.0,
                    wall_seconds,
                )

        if input_format != "parquet" or _enable_parquet_native:
            source_plan = source_plan_from_prepared_inputs(
                prepared_inputs,
                input_format=input_format,
                input_mode=input_mode,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
            )
        if source_plan is None:
            prepared_formats = sorted({prepared.format for prepared in prepared_inputs})
            if input_format == "parquet":
                raise ValueError(
                    "Parquet schema warm-up requires native Arrow-source probing; "
                    "ensure PyArrow is available and all Parquet sources have "
                    "compatible schemas."
                )
            raise ValueError(
                "Schema warm-up sources must be native path-source compatible; "
                "use UTF-8 text input and native-supported formats "
                f"(got prepared formats {prepared_formats})."
            )
        source_plan.close_items.append(operation_context)
        source_plan.close_items.extend(prepared_inputs)
        owners_attached = True
        return PreparedPublicInput(
            source_plan,
            source_plan.input_format,
            "source_plan",
            keepalive=source_plan,
            xml_row_tag=source_plan.xml_row_tag,
        )
    except BaseException as exc:
        if source_plan is not None:
            _cleanup_with_note(
                exc,
                source_plan,
                label="schema warm-up source-plan cleanup also failed",
            )
        if not owners_attached:
            for prepared in reversed(prepared_inputs):
                _cleanup_with_note(
                    exc,
                    prepared,
                    label="schema warm-up prepared-input cleanup also failed",
                )
            _cleanup_with_note(
                exc,
                operation_context,
                label="schema warm-up operation-context cleanup also failed",
            )
        raise


def _warm_up_call_options(
    options: Mapping[str, Any],
    *,
    operation_context: OperationExecutionContext,
) -> tuple[dict[str, Any], Any]:
    """Build additive probe options once for the warm-up workflow."""
    call_options_input = dict(options)
    call_options_input["schema_mode"] = "additive"
    call_options_input = call_options_from_locals(
        call_options_input,
        FILE_CONVERSION_HELPER_KEYS,
    )
    return (
        call_options_input,
        attach_operation_detected_at(
            normalize_call_options_or_none(**options_for_schema_probe(call_options_input)),
            operation_context.detected_at,
            operation_context.memory_ledger,
        ),
    )


def _probe_prepared_warm_up_input(
    prepared_input: PreparedPublicInput,
    *,
    call_options_input: Mapping[str, Any],
    call_options: Any,
    registry_json: str,
    native_registry_state: Any = None,
    field_name_policy: str,
    operation_context: OperationExecutionContext,
) -> Any:
    """Probe one prepared warm-up input while carrying registry state forward."""
    effective_call_options = call_options
    if prepared_input.xml_row_tag is not None:
        effective_input = dict(call_options_input)
        effective_input["xml_row_tag"] = prepared_input.xml_row_tag
        effective_input["input_text_encoding"] = "utf-8"
        effective_call_options = normalize_call_options_or_none(
            **options_for_schema_probe(effective_input)
        )
        effective_call_options = attach_operation_detected_at(
            effective_call_options,
            operation_context.detected_at,
            operation_context.memory_ledger,
        )
    if prepared_input.source != "source_plan":
        raise ValueError(f"Unsupported prepared schema warm-up source: {prepared_input.source!r}")
    probe = probe_prepared_source_plan_registry(
        default_execution_context(),
        prepared_input,
        unwrap_options(effective_call_options),
        registry_json=registry_json,
        native_registry_state=native_registry_state,
        field_name_policy=field_name_policy,
        schema_mode="additive",
    )
    if probe.raw is None:
        raise ValueError("Schema warm-up native probing is unavailable for these sources.")
    return probe


def _infer_partitioned_warm_up_state(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    options: Mapping[str, Any],
    registry_json: str,
    field_name_policy: str,
    call_options_input: Mapping[str, Any],
    call_options: Any,
    after_source_prepared: WarmUpProgressCallback | None,
    after_partition_warmed: WarmUpProgressCallback | None,
    after_schema_drifts: WarmUpSchemaDriftCallback | None,
    operation_context: OperationExecutionContext,
) -> SchemaRegistryState:
    """Probe partitions sequentially so each progress event has real CPU/I/O timing."""
    global _LAST_WARM_UP_ROUTE
    current_registry_json = registry_json
    current_native_registry_state: Any = None
    total = len(plans)

    for index, plan in enumerate(plans, start=1):
        partition_started_at = perf_counter()
        preparation_started_at = perf_counter()
        prepared_input = prepare_schema_warm_up_input(
            [plan],
            input_format=input_format,
            input_mode=input_mode,
            input_text_encoding=str(options.get("input_text_encoding", "utf-8")),
            xml_row_tag=options.get("xml_row_tag"),
            csv_delimiter=str(options.get("csv_delimiter", ",")),
            csv_has_header=bool(options.get("csv_has_header", True)),
            memory_limit_bytes=options.get("memory_limit_bytes"),
            threading_mode=threading_mode_from_multi_threading(
                options.get("multi_threading", False)
            ),
            operation_context=operation_context,
        )
        preparation_seconds = plan.discovery_seconds + max(
            perf_counter() - preparation_started_at,
            0.0,
        )
        probe_cpu_started_at = process_time()
        primary_error: BaseException | None = None
        try:
            if after_source_prepared is not None:
                after_source_prepared(
                    index,
                    total,
                    plan,
                    preparation_seconds,
                    0.0,
                    preparation_seconds,
                )

            probe = _probe_prepared_warm_up_input(
                prepared_input,
                call_options_input=call_options_input,
                call_options=call_options,
                registry_json=current_registry_json,
                native_registry_state=current_native_registry_state,
                field_name_policy=field_name_policy,
                operation_context=operation_context,
            )
            raw = probe.raw
            current_registry_json = raw.schema_registry_json or current_registry_json
            current_native_registry_state = raw.native_registry_state
            _LAST_WARM_UP_ROUTE = probe.route_name
            if after_schema_drifts is not None:
                after_schema_drifts(
                    index,
                    total,
                    plan,
                    raw.schema_drifts_json or "[]",
                )
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            probe_cpu_seconds = max(process_time() - probe_cpu_started_at, 0.0)
            if primary_error is None:
                prepared_input.close()
            else:
                _cleanup_with_note(
                    primary_error,
                    prepared_input,
                    label="schema warm-up partition cleanup also failed",
                )

        wall_seconds = plan.discovery_seconds + max(
            perf_counter() - partition_started_at,
            0.0,
        )
        cpu_seconds, io_wait_seconds = estimate_cpu_io_wall_time(
            wall_seconds,
            probe_cpu_seconds,
        )
        if after_partition_warmed is not None:
            after_partition_warmed(
                index,
                total,
                plan,
                wall_seconds,
                cpu_seconds,
                io_wait_seconds,
            )

    return SchemaRegistryState(
        schema_registry_json=current_registry_json,
        native_registry_state=current_native_registry_state,
    )


def infer_warm_up_schema_registry_state(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    options: Mapping[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
    field_name_policy: str,
    after_source_prepared: WarmUpProgressCallback | None = None,
    after_partition_warmed: WarmUpProgressCallback | None = None,
    after_schema_drifts: WarmUpSchemaDriftCallback | None = None,
) -> SchemaRegistryState:
    """Run additive schema warm-up and return JSON plus native registry state."""
    global _LAST_WARM_UP_ROUTE
    _LAST_WARM_UP_ROUTE = "none"
    if not plans:
        raise ValueError("Schema warm-up requires at least one source partition")
    options = dict(options)
    options["memory_limit_bytes"] = normalize_memory_limit(options.get("memory_limit_bytes"))
    threading_mode = threading_mode_from_multi_threading(options.get("multi_threading", False))
    operation_context = OperationExecutionContext(
        threading_mode=threading_mode,
        memory_limit_bytes=options["memory_limit_bytes"],
    )
    operation_error: BaseException | None = None
    try:
        call_options_input, call_options = _warm_up_call_options(
            options,
            operation_context=operation_context,
        )
        registry_json = _normalize_registry_json(schema_registry)
        if after_partition_warmed is not None or after_schema_drifts is not None:
            return _infer_partitioned_warm_up_state(
                plans,
                input_format=input_format,
                input_mode=input_mode,
                options=options,
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                call_options_input=call_options_input,
                call_options=call_options,
                after_source_prepared=after_source_prepared,
                after_partition_warmed=after_partition_warmed,
                after_schema_drifts=after_schema_drifts,
                operation_context=operation_context,
            )
        prepared_input = prepare_schema_warm_up_input(
            plans,
            input_format=input_format,
            input_mode=input_mode,
            input_text_encoding=str(options.get("input_text_encoding", "utf-8")),
            xml_row_tag=options.get("xml_row_tag"),
            csv_delimiter=str(options.get("csv_delimiter", ",")),
            csv_has_header=bool(options.get("csv_has_header", True)),
            memory_limit_bytes=options.get("memory_limit_bytes"),
            threading_mode=threading_mode,
            after_source_prepared=after_source_prepared,
            operation_context=operation_context,
        )
        prepared_error: BaseException | None = None
        try:
            probe = _probe_prepared_warm_up_input(
                prepared_input,
                call_options_input=call_options_input,
                call_options=call_options,
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                operation_context=operation_context,
            )
            raw = probe.raw
            _LAST_WARM_UP_ROUTE = probe.route_name
            return SchemaRegistryState(
                schema_registry_json=raw.schema_registry_json or "{}",
                native_registry_state=raw.native_registry_state,
            )
        except BaseException as exc:
            prepared_error = exc
            raise
        finally:
            if prepared_error is None:
                prepared_input.close()
            else:
                _cleanup_with_note(
                    prepared_error,
                    prepared_input,
                    label="schema warm-up prepared-input cleanup also failed",
                )
    except BaseException as exc:
        operation_error = exc
        raise
    finally:
        if operation_error is None:
            operation_context.close()
        else:
            _cleanup_with_note(
                operation_error,
                operation_context,
                label="schema warm-up operation-context cleanup also failed",
            )


def infer_warm_up_schema_registry_json(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    options: Mapping[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
    field_name_policy: str,
    after_source_prepared: WarmUpProgressCallback | None = None,
    after_partition_warmed: WarmUpProgressCallback | None = None,
    after_schema_drifts: WarmUpSchemaDriftCallback | None = None,
) -> str:
    """Run additive schema warm-up and return the canonical registry JSON."""
    return infer_warm_up_schema_registry_state(
        plans,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
        field_name_policy=field_name_policy,
        after_source_prepared=after_source_prepared,
        after_partition_warmed=after_partition_warmed,
        after_schema_drifts=after_schema_drifts,
    ).schema_registry_json


def infer_warm_up_schema_registry(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    options: Mapping[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
    field_name_policy: str,
    after_source_prepared: WarmUpProgressCallback | None = None,
    after_partition_warmed: WarmUpProgressCallback | None = None,
    after_schema_drifts: WarmUpSchemaDriftCallback | None = None,
) -> dict[str, Any]:
    """Run additive schema warm-up and return the updated registry."""
    registry_json = infer_warm_up_schema_registry_json(
        plans,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
        field_name_policy=field_name_policy,
        after_source_prepared=after_source_prepared,
        after_partition_warmed=after_partition_warmed,
        after_schema_drifts=after_schema_drifts,
    )
    return json.loads(registry_json or "{}")
