"""Registry probing for canonical source plans and prepared inputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from schema_sanitizer.input_impl.source_plan import (
    PARQUET_ARROW_SOURCES,
    PATH_SOURCES,
    REMOTE_CHUNKS,
    SEQUENCE,
    NativeSourcePlan,
    SourcePlanRegistryProbeResult,
    _flatten_path_source_sequence_or_none,
    _open_path_sources_auto_registry_stream,
    _path_sources_for_native,
)

from ...core_impl.resource_lifecycle import _close_suppressing_errors
from ...input_impl.prepared import NativeDirectorySourceManifest, PreparedPublicInput
from ..parquet.multisource import infer_parquet_multisource_registry
from .attached import source_plan_from_native_manifest
from .registry import append_schema_drifts
from .remote import probe_remote_registry


@dataclass(frozen=True, slots=True)
class RegistryProbeSummary:
    """Probe result used when a sequence combines multiple native probes."""

    schema_registry_json: str
    schema_drifts_json: str
    conversion_timestamp: str
    native_registry_state: Any = None


def _probe_sequence_registry(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any,
) -> RegistryProbeSummary:
    """Probe child plans in order while carrying registry state forward."""
    current_registry = registry_json
    current_state = native_registry_state
    conversion_timestamp = ""
    drifts: list[Any] = []
    for child in plan.payload:
        raw = probe_source_plan_registry(
            raw_context,
            child,
            call_options,
            registry_json=current_registry,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=current_state,
        )
        current_registry = raw.schema_registry_json
        current_state = raw.native_registry_state
        conversion_timestamp = raw.conversion_timestamp
        append_schema_drifts(drifts, raw.schema_drifts_json)
    return RegistryProbeSummary(
        schema_registry_json=current_registry,
        schema_drifts_json=json.dumps(drifts, separators=(",", ":")),
        conversion_timestamp=conversion_timestamp,
        native_registry_state=current_state,
    )


def probe_source_plan_registry(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Infer and merge registry state for a native source plan."""
    if plan.kind == PATH_SOURCES:
        kwargs = {
            "registry_json": registry_json,
            "field_name_policy": field_name_policy,
            "schema_mode": schema_mode,
        }
        if native_registry_state is not None:
            kwargs["native_registry_state"] = native_registry_state
        return raw_context.registry_probe_path_sources_best_effort(
            _path_sources_for_native(plan),
            call_options,
            **kwargs,
        )
    if plan.kind == REMOTE_CHUNKS:
        return probe_remote_registry(
            raw_context,
            plan.payload,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    if plan.kind == PARQUET_ARROW_SOURCES:
        return infer_parquet_multisource_registry(
            raw_context,
            plan.payload,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    if plan.kind == SEQUENCE:
        flattened = _flatten_path_source_sequence_or_none(plan)
        if flattened is not None:
            return probe_source_plan_registry(
                raw_context,
                flattened,
                call_options,
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                native_registry_state=native_registry_state,
            )
        return _probe_sequence_registry(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    raise ValueError(f"Unsupported native source plan kind: {plan.kind!r}")


def infer_native_multisource_registry(
    raw_context: Any,
    manifest: NativeDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Infer and merge one registry across all local manifest child files."""
    plan = source_plan_from_native_manifest(manifest)
    if plan is None:
        from ...input_impl.selection import unsupported_native_directory_ingestion

        raise unsupported_native_directory_ingestion()
    return probe_source_plan_registry(
        raw_context,
        plan,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        native_registry_state=native_registry_state,
    )


def _probe_path_plan_via_auto_registry_stream(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Probe a path plan through the same native stream used by normal runs."""
    raw = _open_path_sources_auto_registry_stream(
        raw_context,
        plan,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        first_row_columns={},
        timestamp_columns=(),
        skip_invalid_json_sources=True,
        native_registry_state=native_registry_state,
    )
    try:
        return SimpleNamespace(
            schema_registry_json=raw.schema_registry_json,
            schema_drifts_json=raw.schema_drifts_json,
            conversion_timestamp=raw.conversion_timestamp,
            field_names=(),
            native_registry_state=raw.native_registry_state,
        )
    finally:
        _close_suppressing_errors(raw)


def probe_prepared_source_plan_registry(
    raw_context: Any,
    prepared: PreparedPublicInput,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> SourcePlanRegistryProbeResult:
    """Infer and merge registry state from a prepared source-plan input."""
    if prepared.source != "source_plan":
        raise ValueError(f"Unsupported prepared source-plan input: {prepared.source!r}")
    plan = prepared.data
    if not isinstance(plan, NativeSourcePlan):
        raise TypeError("prepared source_plan input must contain a NativeSourcePlan")
    if plan.kind == PATH_SOURCES:
        raw = _probe_path_plan_via_auto_registry_stream(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    else:
        raw = probe_source_plan_registry(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    return SourcePlanRegistryProbeResult(raw=raw, route_name=plan.route_name)
