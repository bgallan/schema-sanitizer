"""Convert partitioned GCS inputs to silver Parquet and sync BigQuery."""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
from contextlib import contextmanager
from time import perf_counter
from typing import Any

import schema_sanitizer as ss
from schema_sanitizer.integrations.bigquery import (
    create_or_replace_external_bigquery_table_from_namespace,
    hive_partition_columns_from_namespace,
    log_schema_drift_from_namespace,
    new_schema_registry_from_namespace,
    partition_key_from_uri,
    prepare_existing_schema_registry_from_namespace,
    registry_has_canonical_schema,
    update_registry_sidecar_table_from_namespace,
    warn_if_output_uri_not_covered_by_external_source_uris,
)
from schema_sanitizer.integrations.bigquery import (
    normalize_external_format as _normalize_external_format,
)
from schema_sanitizer.integrations.bigquery import (
    parse_table_ref as _parse_table_ref,
)
from schema_sanitizer.pipeline import (
    build_hive_range_plan_from_namespace,
    build_warm_up_hive_range_plan_from_namespace,
    compact_stats_for_log,
    compact_uri,
    discover_existing_source_plans,
    format_duration,
    infer_warm_up_schema_registry,
    infer_warm_up_schema_registry_json,
    infer_warm_up_schema_registry_state,
    read_parquet_schema,
    run_partitioned_to_parquet_registry_state,
    sample_items,
    schema_drift_count,
)
from schema_sanitizer.pipeline.registry_bootstrap import last_warm_up_route
from schema_sanitizer.pipeline.types import (
    PartitionRunPlan as DateRunPlan,
)
from schema_sanitizer.pipeline.types import (
    PartitionRunResult as DateRunResult,
)
from schema_sanitizer.pipeline.types import SchemaRegistryState

try:
    from examples.example_07.cli import build_parser
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from cli import build_parser

build_date_run_plan = build_hive_range_plan_from_namespace
build_warm_up_date_run_plan = build_warm_up_hive_range_plan_from_namespace
create_or_replace_external_bigquery_table_from_schema = (
    create_or_replace_external_bigquery_table_from_namespace
)
log_schema_drift = log_schema_drift_from_namespace
prepare_existing_schema_registry = prepare_existing_schema_registry_from_namespace
update_registry_sidecar_table = update_registry_sidecar_table_from_namespace
warn_if_silver_uri_not_covered_by_external_source_uris = (
    warn_if_output_uri_not_covered_by_external_source_uris
)


LOGGER = logging.getLogger("gcs_input_to_silver_parquet")


def _read_int_env(name: str, default: int) -> int:
    """Read a positive integer from an environment variable."""
    raw = os.getenv(name)
    if raw is None:
        return default

    try:
        value = int(raw)
    except ValueError:
        LOGGER.warning("Ignoring invalid %s=%r. Using default %d.", name, raw, default)
        return default

    if value <= 0:
        LOGGER.warning("Ignoring non-positive %s=%r. Using default %d.", name, raw, default)
        return default

    return value


def _read_bool_env(name: str, default: bool = False) -> bool:
    """Read a boolean from an environment variable."""
    raw = os.getenv(name)
    if raw is None:
        return default

    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _format_duration(seconds: float) -> str:
    """Format elapsed seconds compactly."""
    return format_duration(seconds)


def _compact_uri(uri: str, *, max_len: int = 96) -> str:
    """Return a compact URI suitable for one-line logs."""
    return compact_uri(uri, max_len=max_len)


@contextmanager
def _timed_step(label: str):
    """Log elapsed time for a major blocking pipeline step."""
    start = perf_counter()
    LOGGER.info("Step start: %s", label)
    try:
        yield
    finally:
        elapsed = perf_counter() - start
        LOGGER.info("Step done: %s duration=%s", label, _format_duration(elapsed))


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
        "schema_mode": args.schema_mode,
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
        "batch_memory_limit_bytes": args.batch_memory_limit_bytes,
        "read_chunk_bytes": args.read_chunk_bytes,
        "arrow_max_depth": args.arrow_max_depth,
        "parquet_max_depth": args.parquet_max_depth,
        "input_text_encoding": args.input_text_encoding,
    }
    if include_output_options:
        kwargs["parquet_compression"] = getattr(args, "parquet_compression", "gzip")
        kwargs["parquet_gzip_level"] = getattr(args, "parquet_gzip_level", None)
    return kwargs


def _infer_warm_up_schema_registry(
    args: argparse.Namespace,
    warm_up_plan: list[DateRunPlan],
    initial_schema_registry: dict[str, Any],
) -> dict[str, Any]:
    """Run additive schema warm-up and return the updated registry."""
    start = perf_counter()
    LOGGER.info("Warm-up scan starting partitions=%d mode=additive", len(warm_up_plan))
    registry = infer_warm_up_schema_registry(
        warm_up_plan,
        input_format=args.input_format,
        input_mode=args.input_mode,
        options=_build_to_parquet_kwargs(args, include_output_options=False),
        schema_registry=initial_schema_registry,
        field_name_policy=args.field_name_policy,
        after_source_prepared=_warm_up_progress_logger(len(warm_up_plan)),
    )
    _log_warm_up_scan_finished(
        warm_up_plan=warm_up_plan,
        started_at=start,
        schema_registry_json=json.dumps(registry, ensure_ascii=False, separators=(",", ":")),
    )
    return registry


def _infer_warm_up_schema_registry_json(
    args: argparse.Namespace,
    warm_up_plan: list[DateRunPlan],
    initial_schema_registry_json: str,
) -> str:
    """Run additive schema warm-up and return the updated registry JSON."""
    start = perf_counter()
    LOGGER.info("Warm-up scan starting partitions=%d mode=additive", len(warm_up_plan))
    registry_json = infer_warm_up_schema_registry_json(
        warm_up_plan,
        input_format=args.input_format,
        input_mode=args.input_mode,
        options=_build_to_parquet_kwargs(args, include_output_options=False),
        schema_registry=initial_schema_registry_json,
        field_name_policy=args.field_name_policy,
        after_source_prepared=_warm_up_progress_logger(len(warm_up_plan)),
    )
    _log_warm_up_scan_finished(
        warm_up_plan=warm_up_plan,
        started_at=start,
        schema_registry_json=registry_json,
    )
    return registry_json


def _infer_warm_up_schema_registry_state(
    args: argparse.Namespace,
    warm_up_plan: list[DateRunPlan],
    initial_schema_registry_state: SchemaRegistryState,
) -> SchemaRegistryState:
    """Run additive schema warm-up and return JSON plus native state."""
    start = perf_counter()
    LOGGER.info("Warm-up scan starting partitions=%d mode=additive", len(warm_up_plan))
    state = infer_warm_up_schema_registry_state(
        warm_up_plan,
        input_format=args.input_format,
        input_mode=args.input_mode,
        options=_build_to_parquet_kwargs(args, include_output_options=False),
        schema_registry=initial_schema_registry_state.schema_registry_json,
        field_name_policy=args.field_name_policy,
        after_source_prepared=_warm_up_progress_logger(len(warm_up_plan)),
    )
    _log_warm_up_scan_finished(
        warm_up_plan=warm_up_plan,
        started_at=start,
        schema_registry_json=state.schema_registry_json,
    )
    return state


def _configure_logging(log_level: str) -> None:
    """Configure process logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _sample_items(items: list[Any], sample_size: int) -> list[Any]:
    """Return first/last sample without logging the whole collection."""
    return sample_items(items, sample_size)


def _log_run_plan_summary(
    title: str,
    run_plan: list[DateRunPlan],
    *,
    sample_size: int,
) -> None:
    """Log a compact summary of a potentially huge run plan."""
    LOGGER.info("%s partitions=%d", title, len(run_plan))

    if not run_plan:
        return

    sample = _sample_items(run_plan, sample_size)

    for plan in sample:
        LOGGER.info(
            "%s sample label=%s source=%s output=%s",
            title,
            plan.label,
            _compact_uri(plan.source_jsonl_uri),
            _compact_uri(plan.silver_parquet_uri),
        )

    omitted = len(run_plan) - len(sample)
    if omitted > 0:
        LOGGER.info("%s omitted %d middle run(s) from INFO logs", title, omitted)

    if LOGGER.isEnabledFor(logging.DEBUG):
        for plan in run_plan:
            LOGGER.debug(
                "%s full label=%s source=%s output=%s",
                title,
                plan.label,
                plan.source_jsonl_uri,
                plan.silver_parquet_uri,
            )


def _log_skipped_plan_summary(
    skipped_plans: list[DateRunPlan],
    *,
    sample_size: int,
) -> None:
    """Log missing source files compactly."""
    if not skipped_plans:
        return

    LOGGER.warning(
        "Skipping %d missing or empty source partition(s). Only a compact sample is shown.",
        len(skipped_plans),
    )

    sample = _sample_items(skipped_plans, sample_size)

    for plan in sample:
        LOGGER.warning(
            "Skipped sample label=%s source=%s",
            plan.label,
            _compact_uri(plan.source_jsonl_uri),
        )

    omitted = len(skipped_plans) - len(sample)
    if omitted > 0:
        LOGGER.warning("Skipped log omitted %d middle source partition(s)", omitted)

    if LOGGER.isEnabledFor(logging.DEBUG):
        for plan in skipped_plans:
            LOGGER.debug(
                "Skipped full label=%s source=%s",
                plan.label,
                plan.source_jsonl_uri,
            )


def _compact_stats_for_log(stats: Any) -> str:
    """Extract useful scalar stats for compact one-line logging."""
    return compact_stats_for_log(stats)


def _schema_drift_count(schema_drifts: Any) -> int | None:
    """Return a compact drift count if the shape is known."""
    return schema_drift_count(schema_drifts)


def _warm_up_progress_logger(total: int):
    """Return a compact warm-up source preparation progress logger."""
    start_time = perf_counter()

    def _log(index: int, _total: int, plan: DateRunPlan, source_seconds: float) -> None:
        elapsed = perf_counter() - start_time
        avg_seconds = elapsed / index if index else 0.0
        eta_seconds = max(total - index, 0) * avg_seconds
        percent = index * 100.0 / total if total else 100.0
        LOGGER.info(
            "Warm-up prepare %d/%d %.1f%% label=%s duration=%s avg=%s eta=%s source=%s",
            index,
            total,
            percent,
            plan.label,
            _format_duration(source_seconds),
            _format_duration(avg_seconds),
            _format_duration(eta_seconds),
            _compact_uri(plan.source_jsonl_uri),
        )

    return _log


def _log_warm_up_scan_finished(
    *,
    warm_up_plan: list[DateRunPlan],
    started_at: float,
    schema_registry_json: str,
) -> None:
    """Log one concise warm-up scan completion line."""
    registry = json.loads(schema_registry_json or "{}")
    LOGGER.info(
        "Warm-up scan finished partitions=%d duration=%s route=%s canonical_schema=%s",
        len(warm_up_plan),
        _format_duration(perf_counter() - started_at),
        last_warm_up_route(),
        registry_has_canonical_schema(registry),
    )


def _log_one_parquet_processed(
    *,
    index: int,
    total: int,
    plan: DateRunPlan,
    run_result: DateRunResult,
    run_seconds: float,
    pipeline_start_time: float,
    registry_updated: bool,
) -> None:
    """Emit one optimized INFO log line for one completed Parquet."""
    elapsed = perf_counter() - pipeline_start_time
    avg_seconds = elapsed / index if index else 0.0
    eta_seconds = max(total - index, 0) * avg_seconds
    percent = index * 100.0 / total if total else 100.0

    stats_text = _compact_stats_for_log(run_result.stats)
    stats_suffix = f" {stats_text}" if stats_text else ""

    drift_count = _schema_drift_count(run_result.schema_drifts)
    drift_suffix = f" drifts={drift_count}" if drift_count is not None else ""

    LOGGER.info(
        "Parquet %d/%d %.1f%% label=%s duration=%s avg=%s eta=%s registry_updated=%s%s%s output=%s",
        index,
        total,
        percent,
        plan.label,
        _format_duration(run_seconds),
        _format_duration(avg_seconds),
        _format_duration(eta_seconds),
        registry_updated,
        stats_suffix,
        drift_suffix,
        _compact_uri(run_result.plan.silver_parquet_uri),
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
    run_args.source_jsonl_uri = plan.source_jsonl_uri
    run_args.silver_parquet_uri = plan.silver_parquet_uri

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
        log_schema_drift(run_args, previous_output_schema, output_schema)

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


def _print_run_outputs_summary(
    completed_runs: list[DateRunResult],
    *,
    sample_size: int,
) -> None:
    """Print compact output file summary."""
    print("\nOutput files:")
    print(f"Completed output file(s): {len(completed_runs)}")

    sample = _sample_items(completed_runs, sample_size)

    for run_result in sample:
        print(f"{run_result.plan.label}: {run_result.plan.silver_parquet_uri}")

    omitted = len(completed_runs) - len(sample)
    if omitted > 0:
        print(f"... omitted {omitted} middle output file(s)")


def _print_stats_summary(
    completed_runs: list[DateRunResult],
    *,
    sample_size: int,
    print_all_details: bool,
) -> None:
    """Print compact stats summary."""
    print("\nStats by run:")

    if print_all_details:
        runs_to_print = completed_runs
    else:
        runs_to_print = _sample_items(completed_runs, sample_size)
        print(
            f"Showing {len(runs_to_print)} of {len(completed_runs)} run stat block(s). "
            "Set PIPELINE_PRINT_RUN_DETAILS=1 to print all."
        )

    for run_result in runs_to_print:
        print(f"\n{run_result.plan.label}:")
        print(json.dumps(run_result.stats, indent=2, sort_keys=True))

    omitted = len(completed_runs) - len(runs_to_print)
    if omitted > 0:
        print(f"\n... omitted {omitted} middle run stat block(s)")


def main() -> int:
    """Run the partitioned GCS input to silver Parquet pipeline."""
    args = build_parser().parse_args()
    _configure_logging(args.log_level)

    log_sample_size = _read_int_env("PIPELINE_LOG_SAMPLE_SIZE", 3)
    skipped_log_sample_size = _read_int_env("PIPELINE_SKIPPED_LOG_SAMPLE_SIZE", 5)
    print_run_details = _read_bool_env("PIPELINE_PRINT_RUN_DETAILS", False)
    enable_parquet_schema_drift_logging = _read_bool_env(
        "PIPELINE_LOG_PARQUET_SCHEMA_DRIFT",
        False,
    )

    external_format = _normalize_external_format(args.external_table_format)

    if external_format != "PARQUET":
        LOGGER.warning(
            "This script writes Parquet output files. "
            "The external table format is set to %s. Usually this should be PARQUET.",
            args.external_table_format,
        )

    table_ref = _parse_table_ref(args.target_table, default_project=args.bigquery_project)

    with _timed_step("building date run plan"):
        run_plan = build_date_run_plan(args)

    _log_run_plan_summary(
        "Planned",
        run_plan,
        sample_size=log_sample_size,
    )

    with _timed_step("source file discovery"):
        run_plan = _filter_available_date_plans(
            run_plan,
            args=args,
            skipped_log_sample_size=skipped_log_sample_size,
        )

    _log_run_plan_summary(
        "Selected",
        run_plan,
        sample_size=log_sample_size,
    )

    with _timed_step("building schema warm-up run plan"):
        warm_up_plan = build_warm_up_date_run_plan(args)

    if warm_up_plan:
        _log_run_plan_summary(
            "Planned schema warm-up",
            warm_up_plan,
            sample_size=log_sample_size,
        )
        with _timed_step("schema warm-up source file discovery"):
            warm_up_plan = _filter_available_date_plans(
                warm_up_plan,
                args=args,
                skipped_log_sample_size=skipped_log_sample_size,
            )
        _log_run_plan_summary(
            "Selected schema warm-up",
            warm_up_plan,
            sample_size=log_sample_size,
        )

    first_run_args = copy.copy(args)
    first_run_args.source_jsonl_uri = run_plan[0].source_jsonl_uri
    first_run_args.silver_parquet_uri = run_plan[0].silver_parquet_uri

    with _timed_step("checking silver URI coverage against external source URIs"):
        warn_if_silver_uri_not_covered_by_external_source_uris(first_run_args)

    with _timed_step("preparing existing schema_registry from BigQuery external table"):
        initial_schema_registry = prepare_existing_schema_registry(args, table_ref)

    if registry_has_canonical_schema(initial_schema_registry):
        LOGGER.info("Existing embedded schema_registry canonical schema is available")
    else:
        LOGGER.info("Existing embedded schema_registry canonical schema is not available")

    current_schema_registry_json = json.dumps(
        initial_schema_registry,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    current_schema_registry_state = SchemaRegistryState(
        schema_registry_json=current_schema_registry_json,
    )
    if warm_up_plan:
        with _timed_step(f"schema warm-up over {len(warm_up_plan)} selected source partition(s)"):
            current_schema_registry_state = _infer_warm_up_schema_registry_state(
                args,
                warm_up_plan,
                current_schema_registry_state,
            )
            current_schema_registry_json = current_schema_registry_state.schema_registry_json
        if registry_has_canonical_schema(json.loads(current_schema_registry_json or "{}")):
            LOGGER.info("Schema warm-up produced an embedded canonical schema")
        else:
            LOGGER.warning("Schema warm-up finished without an embedded canonical schema")

    total_runs = len(run_plan)
    parquet_start_time = perf_counter()

    def after_partition(
        index: int,
        total: int,
        run_result: DateRunResult,
        run_seconds: float,
        previous_output_schema: Any | None,
        registry_updated: bool,
    ) -> None:
        """Log one completed partition run."""
        if enable_parquet_schema_drift_logging and previous_output_schema is not None:
            log_schema_drift(args, previous_output_schema, run_result.output_schema)
        if not registry_updated:
            LOGGER.warning(
                "schema_sanitizer.to_parquet did not return an updated "
                "schema_registry for %s. Keeping the previous accumulated "
                "schema_registry for the next run.",
                run_result.plan.label,
            )
        _log_one_parquet_processed(
            index=index,
            total=total,
            plan=run_result.plan,
            run_result=run_result,
            run_seconds=run_seconds,
            pipeline_start_time=parquet_start_time,
            registry_updated=registry_updated,
        )

    with _timed_step(f"writing {total_runs} selected Parquet file(s)"):
        pipeline_result = run_partitioned_to_parquet_registry_state(
            run_plan,
            initial_schema_registry_state=current_schema_registry_state,
            to_parquet_kwargs=_build_to_parquet_kwargs(args),
            read_output_schema=read_parquet_schema,
            after_partition=after_partition,
        )

    completed_runs = pipeline_result.completed_runs

    final_schema = completed_runs[-1].output_schema

    with _timed_step("creating or replacing BigQuery external table from final schema"):
        create_or_replace_external_bigquery_table_from_schema(
            args,
            table_ref,
            final_schema,
        )

    if args.bigquery_registry_sidecar_table:
        with _timed_step("updating BigQuery registry sidecar table"):
            last_completed = completed_runs[-1]
            update_registry_sidecar_table(
                args,
                table_ref,
                last_ingested_partition=partition_key_from_uri(
                    last_completed.plan.silver_parquet_uri,
                    hive_partition_columns_from_namespace(args),
                ),
            )

    _print_run_outputs_summary(
        completed_runs,
        sample_size=log_sample_size,
    )

    print("\nBigQuery external table:")
    print(table_ref.display_name)

    _print_stats_summary(
        completed_runs,
        sample_size=log_sample_size,
        print_all_details=print_run_details,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
