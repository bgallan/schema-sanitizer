"""Small, configured facade for common partition-to-Parquet workflows."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from ..config import ParquetOptions, SanitizeOptions
from ..core_impl.execution_policy import threading_mode_from_multi_threading
from ..core_impl.uris import local_path_from_file_uri, location_kind
from .hive import HiveRangeConfig, build_hive_range_plan
from .modified_time import plan_gcs_modified_time_windows
from .partition_execution import (
    AfterPartitionCallback,
    OutputSchemaReader,
    PartitionPipelineResult,
    run_partitioned_to_parquet_registry_json,
)
from .source_discovery import discover_existing_source_plans
from .types import PartitionRunPlan, SchemaRegistryState


@dataclass(frozen=True, slots=True)
class ModifiedTimePartitions:
    """Daily UTC partitions selected from one immutable GCS listing."""

    start: date
    end: date
    suffixes: tuple[str, ...] = ("csv",)

    @classmethod
    def daily(
        cls,
        start: date,
        end: date,
        *,
        suffixes: tuple[str, ...] = ("csv",),
    ) -> ModifiedTimePartitions:
        """Create daily half-open UTC modification-time windows."""
        return cls(start, end, suffixes)


@dataclass(frozen=True, slots=True)
class HivePartitions:
    """Daily or hourly Hive partition paths derived from URI prefixes."""

    start: date
    end: date
    granularity: str = "daily"
    start_hour: int = 0
    end_hour: int = 23
    file_name_prefix: str | None = None
    source_file_extension: str | None = None
    output_file_extension: str = "parquet"

    @classmethod
    def daily(
        cls,
        start: date,
        end: date,
        *,
        file_name_prefix: str | None = None,
        source_file_extension: str | None = None,
    ) -> HivePartitions:
        """Create daily Hive path partitions."""
        return cls(
            start,
            end,
            file_name_prefix=file_name_prefix,
            source_file_extension=source_file_extension,
        )


Partitioning = ModifiedTimePartitions | HivePartitions


def _modified_output_uri(prefix_or_template: str, logical_date: date) -> str:
    """Render a date template or append one deterministic Parquet filename."""
    if "{" in prefix_or_template:
        return prefix_or_template.format(date=logical_date.isoformat())
    return f"{prefix_or_template.rstrip('/')}/{logical_date.isoformat()}.parquet"


def _prepare_local_output_parents(plans: list[PartitionRunPlan]) -> None:
    """Create missing parent directories for planned local file outputs."""
    for plan in plans:
        kind = location_kind(plan.output_uri)
        if kind not in {"path", "file"}:
            continue
        output_path = (
            local_path_from_file_uri(plan.output_uri) if kind == "file" else plan.output_uri
        )
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True, slots=True)
class ParquetPipeline:
    """Discover, sanitize, and publish an ordered partition range."""

    source: str
    output: str
    partitions: Partitioning
    options: SanitizeOptions = field(default_factory=SanitizeOptions)
    parquet: ParquetOptions = field(default_factory=ParquetOptions)
    initial_schema_registry: Mapping[str, Any] | str | None = None

    def plan(self) -> list[PartitionRunPlan]:
        """Build the immutable execution plan, including remote discovery."""
        if isinstance(self.partitions, ModifiedTimePartitions):
            resources = self.options.resources
            windows = plan_gcs_modified_time_windows(
                self.source,
                self.partitions.start,
                self.partitions.end,
                suffixes=self.partitions.suffixes,
                include_empty=False,
                memory_limit_bytes=resources.memory_limit_bytes,
                threading_mode=threading_mode_from_multi_threading(resources.multi_threading),
            )
            return [
                window.to_partition_run_plan(
                    _modified_output_uri(self.output, window.source_window.logical_date)
                )
                for window in windows
            ]

        plans = build_hive_range_plan(
            HiveRangeConfig(
                source_prefix=self.source,
                output_prefix=self.output,
                start_date=self.partitions.start,
                end_date=self.partitions.end,
                start_hour=self.partitions.start_hour,
                end_hour=self.partitions.end_hour,
                partition_granularity=self.partitions.granularity,
                input_format=self.options.input_format or "json_array",
                input_mode=self.options.input_mode,
                file_name_prefix=self.partitions.file_name_prefix,
                source_file_extension=self.partitions.source_file_extension,
                output_file_extension=self.partitions.output_file_extension,
            )
        )
        discovery = discover_existing_source_plans(
            plans,
            input_mode=self.options.input_mode,
            input_format=self.options.input_format or "json_array",
            source_file_extension=self.partitions.source_file_extension,
            memory_limit_bytes=self.options.resources.memory_limit_bytes,
            threading_mode=threading_mode_from_multi_threading(
                self.options.resources.multi_threading
            ),
        )
        return discovery.existing_plans

    def run(
        self,
        *,
        read_output_schema: OutputSchemaReader | None = None,
        after_partition: AfterPartitionCallback | None = None,
        result_retention: str = "full",
    ) -> PartitionPipelineResult:
        """Execute the plan while carrying one schema registry forward."""
        kwargs = self.options.to_kwargs()
        kwargs.update(
            parquet_compression=self.parquet.compression,
            parquet_gzip_level=self.parquet.gzip_level,
        )
        registry = self.initial_schema_registry
        state = SchemaRegistryState(dict(registry) if isinstance(registry, Mapping) else registry)
        plans = self.plan()
        _prepare_local_output_parents(plans)
        return run_partitioned_to_parquet_registry_json(
            plans,
            initial_schema_registry_json=state.schema_registry_json,
            initial_schema_registry_state=state,
            to_parquet_kwargs=kwargs,
            read_output_schema=read_output_schema,
            after_partition=after_partition,
            result_retention=result_retention,
        )


__all__ = ["HivePartitions", "ModifiedTimePartitions", "ParquetPipeline"]
