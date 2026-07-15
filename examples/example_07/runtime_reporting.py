"""Logging and presentation helpers for example 07."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from time import perf_counter
from typing import Any

from schema_sanitizer.integrations.bigquery import registry_has_canonical_schema
from schema_sanitizer.pipeline import (
    compact_stats_for_log,
    compact_uri,
    format_duration,
    sample_items,
    schema_drift_count,
)
from schema_sanitizer.pipeline.registry_warmup import last_warm_up_route
from schema_sanitizer.pipeline.types import PartitionRunPlan as DateRunPlan
from schema_sanitizer.pipeline.types import PartitionRunResult as DateRunResult

LOGGER = logging.getLogger("gcs_input_to_silver_parquet")


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
            _compact_uri(plan.source_uri),
            _compact_uri(plan.output_uri),
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
                plan.source_uri,
                plan.output_uri,
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
            _compact_uri(plan.source_uri),
        )

    omitted = len(skipped_plans) - len(sample)
    if omitted > 0:
        LOGGER.warning("Skipped log omitted %d middle source partition(s)", omitted)

    if LOGGER.isEnabledFor(logging.DEBUG):
        for plan in skipped_plans:
            LOGGER.debug(
                "Skipped full label=%s source=%s",
                plan.label,
                plan.source_uri,
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
            _compact_uri(plan.source_uri),
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
        _compact_uri(run_result.plan.output_uri),
    )


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
        print(f"{run_result.plan.label}: {run_result.plan.output_uri}")

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
            "Use the script option that enables full run details to print all."
        )

    for run_result in runs_to_print:
        print(f"\n{run_result.plan.label}:")
        print(json.dumps(run_result.stats, indent=2, sort_keys=True))

    omitted = len(completed_runs) - len(runs_to_print)
    if omitted > 0:
        print(f"\n... omitted {omitted} middle run stat block(s)")
