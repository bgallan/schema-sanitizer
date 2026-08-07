"""Runtime helpers for example 07 partitioned GCS-to-Parquet pipeline."""

from __future__ import annotations

import argparse
import copy
from collections.abc import Callable
from typing import Any

import schema_sanitizer as ss
from schema_sanitizer.integrations.bigquery.advanced import (
    log_schema_drift_from_namespace,
    new_schema_registry_from_namespace,
    registry_has_canonical_schema,
)
from schema_sanitizer.pipeline.advanced import (
    discover_existing_source_plans,
    infer_warm_up_schema_registry,
    infer_warm_up_schema_registry_json,
    infer_warm_up_schema_registry_state,
    read_parquet_schema,
)
from schema_sanitizer.pipeline.types import PartitionRunPlan as DateRunPlan
from schema_sanitizer.pipeline.types import PartitionRunResult as DateRunResult
from schema_sanitizer.pipeline.types import SchemaRegistryState

try:
    from examples.example_07.runtime_reporting import (
        LOGGER,
        _log_skipped_plan_summary,
        _warm_up_progress_logger,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from runtime_reporting import (
        LOGGER,
        _log_skipped_plan_summary,
        _warm_up_progress_logger,
    )


def _new_schema_registry() -> dict[str, Any]:
    """Return a fresh embedded schema-registry document."""
    return new_schema_registry_from_namespace(argparse.Namespace(field_name_policy="lower_snake"))


def _build_to_parquet_kwargs(
    args: argparse.Namespace,
    *,
    include_output_options: bool = True,
) -> dict[str, Any]:
    """Build kwargs for schema_sanitizer.to_parquet."""
    kwargs = {
        "input_format": args.input_format,
        "input_mode": args.input_mode,
        "schema_mode": "additive",
        "column_order": args.column_order,
        "field_name_policy": args.field_name_policy,
        "timestamp_precision": args.timestamp_precision,
        "parse_integers": args.parse_integers,
        "parse_floats": args.parse_floats,
        "parse_float_decimal_separator": args.parse_float_decimal_separator,
        "parse_float_thousands_separator": args.parse_float_thousands_separator,
        "parse_iso_timestamps": args.parse_iso_timestamps,
        "parse_iso_dates": args.parse_iso_dates,
        "parse_iso_times": args.parse_iso_times,
        "on_error": args.on_error,
        "memory_limit_bytes": args.memory_limit_bytes,
        "arrow_max_depth": args.arrow_max_depth,
        "parquet_max_depth": args.parquet_max_depth,
        "input_text_encoding": args.input_text_encoding,
    }
    if include_output_options:
        kwargs["parquet_compression"] = getattr(args, "parquet_compression", "gzip")
        kwargs["parquet_gzip_level"] = getattr(args, "parquet_gzip_level", None)
    return kwargs


def _schema_warm_up_plan_for_run(
    requested_warm_up_plan: list[DateRunPlan],
) -> list[DateRunPlan]:
    """Return explicitly requested sources to probe before materialization.

    Warm-up is opt-in. An additive run must not scan the current write range
    unless those sources were also selected through the warm-up date arguments.
    """
    plans = list(requested_warm_up_plan)

    unique: list[DateRunPlan] = []
    seen_sources: set[str] = set()
    for plan in plans:
        if plan.source_uri in seen_sources:
            continue
        seen_sources.add(plan.source_uri)
        unique.append(plan)
    return unique


def _schema_warm_up_requested(args: argparse.Namespace) -> bool:
    """Return whether the caller explicitly requested a warm-up date range."""
    return (
        getattr(args, "start_date_warm_up", None) is not None
        or getattr(args, "end_date_warm_up", None) is not None
    )


def _warm_up_schema_drift_collector(
    target: list[DateRunResult] | None,
) -> Callable[[int, int, DateRunPlan, str], None] | None:
    """Return a callback that retains partition-attributed warm-up drifts."""
    if target is None:
        return None

    def _collect(
        _index: int,
        _total: int,
        plan: DateRunPlan,
        schema_drifts_json: str,
    ) -> None:
        if not schema_drifts_json or schema_drifts_json.strip() == "[]":
            return
        target.append(
            DateRunResult(
                plan=plan,
                output_schema=None,
                stats={},
                schema_drifts_json=schema_drifts_json,
            )
        )

    return _collect


def _infer_warm_up_schema_registry(
    args: argparse.Namespace,
    warm_up_plan: list[DateRunPlan],
    initial_schema_registry: dict[str, Any],
    *,
    warm_up_drift_runs: list[DateRunResult] | None = None,
) -> dict[str, Any]:
    """Run additive schema warm-up and return the updated registry."""
    return infer_warm_up_schema_registry(
        warm_up_plan,
        input_format=args.input_format,
        input_mode=args.input_mode,
        options=_build_to_parquet_kwargs(args, include_output_options=False),
        schema_registry=initial_schema_registry,
        field_name_policy=args.field_name_policy,
        after_partition_warmed=_warm_up_progress_logger(len(warm_up_plan)),
        after_schema_drifts=_warm_up_schema_drift_collector(warm_up_drift_runs),
    )


def _infer_warm_up_schema_registry_json(
    args: argparse.Namespace,
    warm_up_plan: list[DateRunPlan],
    initial_schema_registry_json: str,
    *,
    warm_up_drift_runs: list[DateRunResult] | None = None,
) -> str:
    """Run additive schema warm-up and return the updated registry JSON."""
    return infer_warm_up_schema_registry_json(
        warm_up_plan,
        input_format=args.input_format,
        input_mode=args.input_mode,
        options=_build_to_parquet_kwargs(args, include_output_options=False),
        schema_registry=initial_schema_registry_json,
        field_name_policy=args.field_name_policy,
        after_partition_warmed=_warm_up_progress_logger(len(warm_up_plan)),
        after_schema_drifts=_warm_up_schema_drift_collector(warm_up_drift_runs),
    )


def _infer_warm_up_schema_registry_state(
    args: argparse.Namespace,
    warm_up_plan: list[DateRunPlan],
    initial_schema_registry_state: SchemaRegistryState,
    *,
    warm_up_drift_runs: list[DateRunResult] | None = None,
) -> SchemaRegistryState:
    """Run additive schema warm-up and return JSON plus native state."""
    return infer_warm_up_schema_registry_state(
        warm_up_plan,
        input_format=args.input_format,
        input_mode=args.input_mode,
        options=_build_to_parquet_kwargs(args, include_output_options=False),
        schema_registry=initial_schema_registry_state.schema_registry_json,
        field_name_policy=args.field_name_policy,
        after_partition_warmed=_warm_up_progress_logger(len(warm_up_plan)),
        after_schema_drifts=_warm_up_schema_drift_collector(warm_up_drift_runs),
    )


def _run_one_date(
    args: argparse.Namespace,
    plan: DateRunPlan,
    previous_schema_registry: dict[str, Any],
    previous_output_schema: Any | None,
    *,
    enable_parquet_schema_drift_logging: bool,
) -> DateRunResult:
    """Run one date using the previous accumulated schema registry."""
    run_args = copy.copy(args)
    run_args.source_jsonl_uri = plan.source_uri
    run_args.silver_parquet_uri = plan.output_uri

    if registry_has_canonical_schema(previous_schema_registry):
        LOGGER.debug(
            "Using previous embedded schema_registry canonical_schema for %s",
            plan.label,
        )
    else:
        LOGGER.debug(
            "No previous embedded schema_registry canonical_schema available for %s; "
            "this run will initialize it from the current source file",
            plan.label,
        )

    result = ss.to_parquet(
        run_args.source_jsonl_uri,
        run_args.silver_parquet_uri,
        **_build_to_parquet_kwargs(run_args),
        schema_registry=previous_schema_registry,
    )

    output_schema = read_parquet_schema(run_args.silver_parquet_uri)

    if enable_parquet_schema_drift_logging and previous_output_schema is not None:
        log_schema_drift_from_namespace(run_args, previous_output_schema, output_schema)

    return DateRunResult(
        plan=plan,
        output_schema=output_schema,
        stats=result.stats,
        schema_registry=result.schema_registry,
        schema_drifts=result.schema_drifts,
    )


def _filter_available_date_plans(
    run_plan: list[DateRunPlan],
    *,
    args: argparse.Namespace,
    skipped_log_sample_size: int,
) -> list[DateRunPlan]:
    """Filter range plans to source files/directories that contain input."""
    if not run_plan or all(plan.logical_date is None for plan in run_plan):
        return run_plan

    discovery = discover_existing_source_plans(
        run_plan,
        input_mode=args.input_mode,
        input_format=args.input_format,
        source_file_extension=args.source_file_extension,
        memory_limit_bytes=args.memory_limit_bytes,
    )

    LOGGER.info(
        "Source discovery found %d available partition(s) and %d empty/missing partition(s)",
        len(discovery.existing_plans),
        len(discovery.skipped_plans),
    )

    _log_skipped_plan_summary(
        discovery.skipped_plans,
        sample_size=skipped_log_sample_size,
    )

    if not discovery.existing_plans:
        raise FileNotFoundError(
            "No matching source files were found for the requested partition range. "
            "Check --start-date, --end-date, --source-jsonl-prefix/uri, "
            "--partition-granularity, --input-format, and --input-mode."
        )

    return discovery.existing_plans
