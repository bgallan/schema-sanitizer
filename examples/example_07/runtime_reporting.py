"""Logging and presentation helpers for example 07."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

from schema_sanitizer.pipeline.advanced import (
    compact_uri,
    cpu_io_wall_percentages,
    format_duration,
    sample_items,
)
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


def _log_run_filesystem_prefixes(args: Any) -> None:
    """Log the unabridged source and target filesystem roots once per run."""
    source_prefix = args.source_jsonl_prefix or args.source_jsonl_uri
    target_prefix = args.silver_parquet_prefix or args.silver_parquet_uri
    LOGGER.info(
        "Run filesystems source_prefix=%s target_prefix=%s",
        source_prefix,
        target_prefix,
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


def _schema_drifts_for_run(run_result: DateRunResult) -> list[dict[str, Any]]:
    """Return one run's drift events from its parsed or JSON representation."""
    payload: Any = run_result.schema_drifts
    if payload is None and run_result.schema_drifts_json is not None:
        try:
            payload = json.loads(run_result.schema_drifts_json or "[]")
        except (TypeError, ValueError):
            LOGGER.warning(
                "Could not decode schema drift metadata partition=%s",
                run_result.plan.label,
            )
            return []

    if isinstance(payload, dict):
        for key in ("drifts", "schema_drifts", "items", "changes"):
            nested = payload.get(key)
            if isinstance(nested, list):
                payload = nested
                break
        else:
            payload = [payload]

    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _timing_suffix(
    wall_seconds: float,
    cpu_seconds: float | None,
) -> str:
    """Return complementary CPU and estimated I/O percentages."""
    if cpu_seconds is None:
        return ""
    cpu_percent, io_percent = cpu_io_wall_percentages(wall_seconds, cpu_seconds)
    return f" cpu={cpu_percent:.1f}% io={io_percent:.1f}%"


def _source_metrics(plan: DateRunPlan) -> tuple[int | None, int | None]:
    """Return discovered source count and bytes, with a local-file fallback."""
    if plan.source_file_count is not None or plan.source_bytes is not None:
        return plan.source_file_count, plan.source_bytes
    is_windows_path = (
        len(plan.source_uri) >= 2 and plan.source_uri[1] == ":" and plan.source_uri[0].isalpha()
    )
    parsed = urlparse(plan.source_uri)
    if not is_windows_path and parsed.scheme and parsed.scheme.lower() != "file":
        return None, None
    path = Path(
        url2pathname(unquote(parsed.path))
        if not is_windows_path and parsed.scheme.lower() == "file"
        else plan.source_uri
    )
    try:
        if path.is_file():
            return 1, path.stat().st_size
    except OSError:
        pass
    return None, None


def _source_metrics_suffix(plan: DateRunPlan) -> str:
    """Format source file count and aggregate decimal megabytes."""
    source_file_count, source_bytes = _source_metrics(plan)
    files = "unknown" if source_file_count is None else str(source_file_count)
    size_mb = "unknown" if source_bytes is None else f"{source_bytes / 1_000_000:.3f}"
    return f" source_files={files} source_size_mb={size_mb}"


def _warm_up_progress_logger(total: int):
    """Return the completed warm-up partition progress logger."""

    def _log(
        index: int,
        callback_total: int,
        plan: DateRunPlan,
        wall_seconds: float,
        cpu_seconds: float,
        _io_wait_seconds: float,
    ) -> None:
        progress_total = callback_total or total
        LOGGER.info(
            "run=warmup progress=%d/%d label=%s duration=%s%s%s",
            index,
            progress_total,
            plan.label,
            _format_duration(wall_seconds),
            _timing_suffix(wall_seconds, cpu_seconds),
            _source_metrics_suffix(plan),
        )

    return _log


def _log_one_parquet_processed(
    *,
    index: int,
    total: int,
    plan: DateRunPlan,
    run_result: DateRunResult,
    run_seconds: float,
) -> None:
    """Emit the progress log for one completed Parquet partition."""
    wall_seconds = run_result.wall_seconds
    if wall_seconds is None:
        wall_seconds = run_seconds
    timing_suffix = _timing_suffix(
        wall_seconds,
        run_result.cpu_seconds,
    )

    LOGGER.info(
        "run=parquet progress=%d/%d label=%s duration=%s%s%s",
        index,
        total,
        plan.label,
        _format_duration(wall_seconds),
        timing_suffix,
        _source_metrics_suffix(plan),
    )


def _log_schema_drift_summary(
    completed_runs: list[DateRunResult],
    *,
    schema_mode: str,
    warm_up_runs: list[DateRunResult] | None = None,
) -> None:
    """Log every schema change with the partition that triggered it."""
    warm_up_events = [
        ("warmup", run_result, drift)
        for run_result in warm_up_runs or []
        for drift in _schema_drifts_for_run(run_result)
    ]
    parquet_events = [
        ("parquet", run_result, drift)
        for run_result in completed_runs
        for drift in _schema_drifts_for_run(run_result)
    ]
    events = [*warm_up_events, *parquet_events]
    if warm_up_runs is None:
        LOGGER.info(
            "Schema drift summary mode=%s total=%d parquet=%d",
            schema_mode,
            len(events),
            len(parquet_events),
        )
    else:
        LOGGER.info(
            "Schema drift summary mode=%s total=%d warmup=%d parquet=%d",
            schema_mode,
            len(events),
            len(warm_up_events),
            len(parquet_events),
        )
    if not events:
        return

    labels = {
        "newly_added": "new_column_added",
        "new_version_generated": "new_column_version",
        "type_promoted": "column_type_promoted",
    }
    for run_type, run_result, drift in events:
        drift_type = str(drift.get("drift_type", "unknown"))
        change = labels.get(drift_type, drift_type)
        source_path = drift.get("source_path")
        output_name = drift.get("output_name")
        previous_schema = drift.get("previous_schema")
        new_schema = drift.get("new_schema")
        LOGGER.info(
            "Schema drift run=%s partition=%s change=%s source_path=%s "
            "output_column=%s schema=%s -> %s",
            run_type,
            run_result.plan.label,
            change,
            source_path,
            output_name,
            previous_schema,
            new_schema,
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
