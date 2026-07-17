"""Reusable helpers for partitioned schema-sanitizer pipelines."""

from __future__ import annotations

from .hive import (
    HiveRangeConfig,
    build_hive_range_plan,
    build_hive_range_plan_from_namespace,
    build_warm_up_hive_range_plan_from_namespace,
    parse_hour,
    parse_iso_date,
)
from .observability import (
    compact_stats_for_log,
    compact_uri,
    cpu_io_wall_percentages,
    estimate_cpu_io_wall_time,
    format_duration,
    sample_items,
    schema_drift_count,
)
from .partition_execution import (
    PartitionPipelineResult,
    parse_final_schema_registry,
    run_partitioned_to_parquet,
    run_partitioned_to_parquet_registry_json,
    run_partitioned_to_parquet_registry_state,
)
from .registry_warmup import (
    infer_warm_up_schema_registry,
    infer_warm_up_schema_registry_json,
    infer_warm_up_schema_registry_state,
)
from .schemas import (
    SchemaDriftDiff,
    diff_arrow_schemas,
    diff_flat_schema_paths,
    flatten_arrow_schema_paths,
    read_parquet_schema,
)
from .source_discovery import (
    discover_existing_source_plans,
    discover_existing_source_plans_async,
)
from .types import PartitionRunPlan, PartitionRunResult, SchemaRegistryState, SourcePlanDiscovery

__all__ = [
    "HiveRangeConfig",
    "PartitionPipelineResult",
    "PartitionRunPlan",
    "PartitionRunResult",
    "SchemaDriftDiff",
    "SchemaRegistryState",
    "SourcePlanDiscovery",
    "build_hive_range_plan",
    "build_hive_range_plan_from_namespace",
    "build_warm_up_hive_range_plan_from_namespace",
    "compact_stats_for_log",
    "compact_uri",
    "cpu_io_wall_percentages",
    "diff_arrow_schemas",
    "diff_flat_schema_paths",
    "discover_existing_source_plans",
    "discover_existing_source_plans_async",
    "estimate_cpu_io_wall_time",
    "flatten_arrow_schema_paths",
    "format_duration",
    "infer_warm_up_schema_registry",
    "infer_warm_up_schema_registry_json",
    "infer_warm_up_schema_registry_state",
    "parse_final_schema_registry",
    "parse_hour",
    "parse_iso_date",
    "read_parquet_schema",
    "run_partitioned_to_parquet",
    "run_partitioned_to_parquet_registry_json",
    "run_partitioned_to_parquet_registry_state",
    "sample_items",
    "schema_drift_count",
]
