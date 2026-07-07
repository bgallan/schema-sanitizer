"""Reusable partition conversion loops."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from ..api_impl.file_api import to_parquet
from ..api_impl.file_api_helpers import _call_options_from_locals
from ..api_impl.file_api_option_sets import CONVERTER_HELPER_KEYS, PARQUET_WRITER_OPTION_KEYS
from ..api_impl.file_convert_core import schema_registry_native_state_context
from ..api_impl.public_input import discovered_directory_inputs
from ..api_impl.schema_registry import _normalize_registry_json, native_registry_state_from_json
from ..options_impl.call_options import normalize_call_options_or_none
from .types import PartitionRunPlan, PartitionRunResult, SchemaRegistryState

ToParquetKwargsFactory = Callable[[PartitionRunPlan], Mapping[str, Any]]
OutputSchemaReader = Callable[[str], Any]
AfterPartitionCallback = Callable[
    [int, int, PartitionRunResult, float, Any | None, bool],
    None,
]


@dataclass(frozen=True, init=False)
class PartitionPipelineResult:
    """Result for a completed partitioned conversion loop."""

    completed_runs: list[PartitionRunResult]
    final_schema_registry_json: str | None = None
    final_native_registry_state: Any | None = None
    _final_schema_registry_cache: dict[str, Any] | None = None

    def __init__(
        self,
        *,
        completed_runs: list[PartitionRunResult],
        final_schema_registry: Mapping[str, Any] | None = None,
        final_schema_registry_json: str | None = None,
        final_native_registry_state: Any | None = None,
    ) -> None:
        """Create a pipeline result with JSON registry state as the source of truth."""
        registry_json = (
            _normalize_registry_json(final_schema_registry)
            if final_schema_registry_json is None
            else _normalize_registry_json(final_schema_registry_json)
        )
        object.__setattr__(self, "completed_runs", completed_runs)
        object.__setattr__(self, "final_schema_registry_json", registry_json)
        object.__setattr__(self, "final_native_registry_state", final_native_registry_state)
        object.__setattr__(
            self,
            "_final_schema_registry_cache",
            dict(final_schema_registry) if final_schema_registry is not None else None,
        )

    @property
    def final_schema_registry(self) -> dict[str, Any]:
        """Return the parsed final registry, parsing JSON only when requested."""
        cached = self._final_schema_registry_cache
        if cached is None:
            cached = json.loads(self.final_schema_registry_json or "{}")
            object.__setattr__(self, "_final_schema_registry_cache", cached)
        return cached

    @property
    def final_schema_registry_state(self) -> SchemaRegistryState:
        """Return final registry JSON plus optional native state."""
        return SchemaRegistryState(
            schema_registry_json=self.final_schema_registry_json or "{}",
            native_registry_state=self.final_native_registry_state,
        )


def _compile_native_registry_state_for_kwargs(
    registry_json: str,
    kwargs: Mapping[str, Any],
) -> Any | None:
    """Compile durable registry JSON into native state for one converter option set."""
    options = _call_options_from_locals(
        dict(kwargs),
        CONVERTER_HELPER_KEYS | PARQUET_WRITER_OPTION_KEYS,
    )
    call_options = normalize_call_options_or_none(**options)
    try:
        return native_registry_state_from_json(
            registry_json,
            field_name_policy=str(kwargs.get("field_name_policy", "lower_alpha")),
            options=call_options,
        )
    except Exception:
        return None


def run_partitioned_to_parquet(
    plans: list[PartitionRunPlan],
    *,
    initial_schema_registry: dict[str, Any],
    to_parquet_kwargs: Mapping[str, Any] | ToParquetKwargsFactory,
    read_output_schema: OutputSchemaReader | None = None,
    after_partition: AfterPartitionCallback | None = None,
) -> PartitionPipelineResult:
    """Write every planned partition while carrying canonical registry JSON forward."""
    return run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json=_normalize_registry_json(initial_schema_registry),
        to_parquet_kwargs=to_parquet_kwargs,
        read_output_schema=read_output_schema,
        after_partition=after_partition,
    )


def run_partitioned_to_parquet_registry_json(
    plans: list[PartitionRunPlan],
    *,
    initial_schema_registry_json: str,
    to_parquet_kwargs: Mapping[str, Any] | ToParquetKwargsFactory,
    read_output_schema: OutputSchemaReader | None = None,
    after_partition: AfterPartitionCallback | None = None,
    initial_schema_registry_state: SchemaRegistryState | None = None,
) -> PartitionPipelineResult:
    """Write partitions while carrying the registry as canonical JSON."""
    if initial_schema_registry_state is None:
        current_schema_registry_json = _normalize_registry_json(initial_schema_registry_json)
        current_native_registry_state: Any | None = None
    else:
        current_schema_registry_json = initial_schema_registry_state.schema_registry_json
        current_native_registry_state = initial_schema_registry_state.native_registry_state
    completed_runs: list[PartitionRunResult] = []
    previous_output_schema: Any | None = None
    total = len(plans)

    for index, plan in enumerate(plans, start=1):
        kwargs = (
            dict(to_parquet_kwargs(plan))
            if callable(to_parquet_kwargs)
            else dict(to_parquet_kwargs)
        )
        if current_native_registry_state is None:
            current_native_registry_state = _compile_native_registry_state_for_kwargs(
                current_schema_registry_json,
                kwargs,
            )
        run_start = perf_counter()
        discovered_input = getattr(plan, "discovered_input", None)
        context = (
            discovered_directory_inputs({plan.source_uri: discovered_input})
            if discovered_input is not None
            else nullcontext()
        )
        state_context = (
            schema_registry_native_state_context(current_native_registry_state)
            if current_native_registry_state is not None
            else nullcontext()
        )
        with context:
            with state_context:
                result = to_parquet(
                    plan.source_uri,
                    plan.output_uri,
                    **kwargs,
                    schema_registry=current_schema_registry_json,
                )
        run_seconds = perf_counter() - run_start
        output_schema = read_output_schema(plan.output_uri) if read_output_schema else None
        schema_registry_json = getattr(result, "schema_registry_json", None)
        schema_drifts_json = getattr(result, "schema_drifts_json", None)
        if schema_registry_json is None:
            parsed_registry = getattr(result, "schema_registry", None)
            if parsed_registry is not None:
                schema_registry_json = _normalize_registry_json(parsed_registry)
        run_result = PartitionRunResult(
            plan=plan,
            output_schema=output_schema,
            stats=result.stats,
            schema_registry=None,
            schema_drifts=None,
            schema_registry_json=schema_registry_json,
            schema_drifts_json=schema_drifts_json,
            native_registry_state=getattr(result, "native_registry_state", None),
        )
        registry_updated = schema_registry_json is not None
        if registry_updated:
            current_schema_registry_json = schema_registry_json or current_schema_registry_json
            current_native_registry_state = getattr(result, "native_registry_state", None)
        completed_runs.append(run_result)
        if after_partition is not None:
            after_partition(
                index,
                total,
                run_result,
                run_seconds,
                previous_output_schema,
                registry_updated,
            )
        previous_output_schema = output_schema

    return PartitionPipelineResult(
        completed_runs=completed_runs,
        final_schema_registry_json=current_schema_registry_json,
        final_native_registry_state=current_native_registry_state,
    )


def run_partitioned_to_parquet_registry_state(
    plans: list[PartitionRunPlan],
    *,
    initial_schema_registry_state: SchemaRegistryState,
    to_parquet_kwargs: Mapping[str, Any] | ToParquetKwargsFactory,
    read_output_schema: OutputSchemaReader | None = None,
    after_partition: AfterPartitionCallback | None = None,
) -> PartitionPipelineResult:
    """Write partitions while carrying JSON plus native registry state."""
    return run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json=initial_schema_registry_state.schema_registry_json,
        initial_schema_registry_state=initial_schema_registry_state,
        to_parquet_kwargs=to_parquet_kwargs,
        read_output_schema=read_output_schema,
        after_partition=after_partition,
    )


def parse_final_schema_registry(result: PartitionPipelineResult) -> dict[str, Any]:
    """Return the final registry mapping for either runner result shape."""
    return result.final_schema_registry
