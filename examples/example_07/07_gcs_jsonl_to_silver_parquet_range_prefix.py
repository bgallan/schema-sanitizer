"""Convert partitioned GCS inputs to silver Parquet and sync BigQuery."""

from __future__ import annotations

import copy
import json
from typing import Any

from schema_sanitizer.integrations.bigquery.advanced import (
    create_or_replace_external_bigquery_table_from_namespace,
    hive_partition_columns_from_namespace,
    log_schema_drift_from_namespace,
    partition_key_from_uri,
    prepare_existing_schema_registry_from_namespace,
    registry_has_canonical_schema,
    update_registry_sidecar_table_from_namespace,
    warn_if_output_uri_not_covered_by_external_source_uris,
)
from schema_sanitizer.integrations.bigquery.advanced import (
    normalize_external_format as _normalize_external_format,
)
from schema_sanitizer.integrations.bigquery.advanced import parse_table_ref as _parse_table_ref
from schema_sanitizer.pipeline.advanced import (
    build_hive_range_plan_from_namespace,
    build_warm_up_hive_range_plan_from_namespace,
    read_parquet_schema,
    run_partitioned_to_parquet_registry_state,
)
from schema_sanitizer.pipeline.types import PartitionRunResult as DateRunResult
from schema_sanitizer.pipeline.types import SchemaRegistryState

try:
    from examples.example_07.cli import build_parser
    from examples.example_07.runtime_reporting import (
        LOGGER,
        _configure_logging,
        _log_one_parquet_processed,
        _log_run_filesystem_prefixes,
        _log_run_plan_summary,
        _log_schema_drift_summary,
        _print_run_outputs_summary,
        _print_stats_summary,
        _timed_step,
    )
    from examples.example_07.runtime_support import (
        _build_to_parquet_kwargs,
        _filter_available_date_plans,
        _infer_warm_up_schema_registry_state,
        _schema_warm_up_plan_for_run,
        _schema_warm_up_requested,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from cli import build_parser
    from runtime_reporting import (
        LOGGER,
        _configure_logging,
        _log_one_parquet_processed,
        _log_run_filesystem_prefixes,
        _log_run_plan_summary,
        _log_schema_drift_summary,
        _print_run_outputs_summary,
        _print_stats_summary,
        _timed_step,
    )
    from runtime_support import (
        _build_to_parquet_kwargs,
        _filter_available_date_plans,
        _infer_warm_up_schema_registry_state,
        _schema_warm_up_plan_for_run,
        _schema_warm_up_requested,
    )


def main() -> int:
    """Run the partitioned GCS input to silver Parquet pipeline."""
    args = build_parser().parse_args()
    _configure_logging(args.log_level)
    _log_run_filesystem_prefixes(args)

    log_sample_size = 3
    skipped_log_sample_size = 5
    print_run_details = False
    enable_parquet_schema_drift_logging = False

    external_format = _normalize_external_format(args.external_table_format)

    if external_format != "PARQUET":
        LOGGER.warning(
            "This script writes Parquet output files. "
            "The external table format is set to %s. Usually this should be PARQUET.",
            args.external_table_format,
        )

    table_ref = _parse_table_ref(args.target_table, default_project=args.bigquery_project)

    with _timed_step("building date run plan"):
        run_plan = build_hive_range_plan_from_namespace(args)

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

    warm_up_plan = []
    if _schema_warm_up_requested(args):
        with _timed_step("building schema warm-up run plan"):
            warm_up_plan = build_warm_up_hive_range_plan_from_namespace(args)
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
    first_run_args.source_jsonl_uri = run_plan[0].source_uri
    first_run_args.silver_parquet_uri = run_plan[0].output_uri

    with _timed_step("checking silver URI coverage against external source URIs"):
        warn_if_output_uri_not_covered_by_external_source_uris(first_run_args)

    with _timed_step("preparing existing schema_registry from BigQuery external table"):
        initial_schema_registry = prepare_existing_schema_registry_from_namespace(args, table_ref)

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
    schema_warm_up_plan = _schema_warm_up_plan_for_run(warm_up_plan)
    warm_up_drift_runs: list[DateRunResult] | None = [] if schema_warm_up_plan else None
    if schema_warm_up_plan:
        with _timed_step(
            f"schema warm-up over {len(schema_warm_up_plan)} selected source partition(s)"
        ):
            current_schema_registry_state = _infer_warm_up_schema_registry_state(
                args,
                schema_warm_up_plan,
                current_schema_registry_state,
                warm_up_drift_runs=warm_up_drift_runs,
            )
            current_schema_registry_json = current_schema_registry_state.schema_registry_json
        if registry_has_canonical_schema(json.loads(current_schema_registry_json or "{}")):
            LOGGER.info("Schema warm-up produced an embedded canonical schema")
        else:
            LOGGER.warning("Schema warm-up finished without an embedded canonical schema")

    total_runs = len(run_plan)

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
            log_schema_drift_from_namespace(args, previous_output_schema, run_result.output_schema)
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
        create_or_replace_external_bigquery_table_from_namespace(
            args,
            table_ref,
            final_schema,
            reference_file_schema_uri=completed_runs[-1].plan.output_uri,
        )

    if args.bigquery_registry_sidecar_table:
        with _timed_step("updating BigQuery registry sidecar table"):
            last_completed = completed_runs[-1]
            update_registry_sidecar_table_from_namespace(
                args,
                table_ref,
                last_ingested_partition=partition_key_from_uri(
                    last_completed.plan.output_uri,
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
    _log_schema_drift_summary(
        completed_runs,
        schema_mode="additive",
        warm_up_runs=warm_up_drift_runs,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
