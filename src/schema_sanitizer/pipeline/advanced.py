"""Advanced pipeline primitives retained for custom orchestration.

This namespace re-exports Hive planning, source discovery, schema drift, observability, and
registry warm-up operations from their focused implementation modules. Discovery,
warm-up, and partition-execution entry points defer runtime-backed imports until called so
process managers may safely import DAGs before fork.
"""

# ruff: noqa: F401, F822 - re-exports and deferred names define this namespace

from __future__ import annotations

from collections.abc import Callable, Mapping
from importlib import import_module
from typing import TYPE_CHECKING, Any

from ..sources.models import SourceManifest
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
from .schemas import (
    SchemaDriftDiff,
    diff_arrow_schemas,
    diff_flat_schema_paths,
    flatten_arrow_schema_paths,
    read_parquet_schema,
)
from .types import PartitionRunPlan, PartitionRunResult, SchemaRegistryState, SourcePlanDiscovery

if TYPE_CHECKING:
    from .modified_time import (
        ModifiedTimeWindowPlan,
        UtcWindow,
        build_utc_daily_windows,
        plan_gcs_modified_time_windows,
        plan_gcs_modified_time_windows_async,
        plan_modified_time_windows_from_listing,
        select_remote_files_by_modified_time,
    )
    from .registry_warmup import (
        infer_warm_up_schema_registry,
        infer_warm_up_schema_registry_json,
    )
    from .source_discovery import discover_existing_source_plans_async


WarmUpProgressCallback = Callable[[int, int, PartitionRunPlan, float, float, float], None]
WarmUpSchemaDriftCallback = Callable[[int, int, PartitionRunPlan, str], None]


def discover_existing_source_plans(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> SourcePlanDiscovery:
    """Discover sources after the current process owns runtime initialization."""
    from .source_discovery import discover_existing_source_plans as implementation

    return implementation(
        plans,
        input_mode=input_mode,
        input_format=input_format,
        source_file_extension=source_file_extension,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
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
    """Warm a registry after the current process owns runtime initialization."""
    from .registry_warmup import infer_warm_up_schema_registry_state as implementation

    return implementation(
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


_LAZY: dict[str, tuple[str, str]] = {
    "ModifiedTimeWindowPlan": (".modified_time", "ModifiedTimeWindowPlan"),
    "UtcWindow": (".modified_time", "UtcWindow"),
    "build_utc_daily_windows": (".modified_time", "build_utc_daily_windows"),
    "discover_existing_source_plans_async": (
        ".source_discovery",
        "discover_existing_source_plans_async",
    ),
    "infer_warm_up_schema_registry": (
        ".registry_warmup",
        "infer_warm_up_schema_registry",
    ),
    "infer_warm_up_schema_registry_json": (
        ".registry_warmup",
        "infer_warm_up_schema_registry_json",
    ),
    "plan_gcs_modified_time_windows": (
        ".modified_time",
        "plan_gcs_modified_time_windows",
    ),
    "plan_gcs_modified_time_windows_async": (
        ".modified_time",
        "plan_gcs_modified_time_windows_async",
    ),
    "plan_modified_time_windows_from_listing": (
        ".modified_time",
        "plan_modified_time_windows_from_listing",
    ),
    "select_remote_files_by_modified_time": (
        ".modified_time",
        "select_remote_files_by_modified_time",
    ),
}


def __getattr__(name: str) -> Any:
    """Load advanced operations that are not part of the pre-fork facade."""
    spec = _LAZY.get(name)
    if spec is None:
        raise AttributeError(name)
    module_name, attribute = spec
    value = getattr(import_module(module_name, __package__), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Include deferred public names in interactive module discovery."""
    return sorted({*globals(), *__all__})


__all__ = [
    "HiveRangeConfig",
    "ModifiedTimeWindowPlan",
    "PartitionPipelineResult",
    "PartitionRunPlan",
    "PartitionRunResult",
    "SchemaDriftDiff",
    "SchemaRegistryState",
    "SourceManifest",
    "SourcePlanDiscovery",
    "UtcWindow",
    "build_hive_range_plan",
    "build_hive_range_plan_from_namespace",
    "build_utc_daily_windows",
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
    "plan_gcs_modified_time_windows",
    "plan_gcs_modified_time_windows_async",
    "plan_modified_time_windows_from_listing",
    "read_parquet_schema",
    "run_partitioned_to_parquet",
    "run_partitioned_to_parquet_registry_json",
    "run_partitioned_to_parquet_registry_state",
    "sample_items",
    "schema_drift_count",
    "select_remote_files_by_modified_time",
]
