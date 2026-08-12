# Partitioned pipelines

`schema_sanitizer.pipeline` provides a configured high-level Parquet workflow
and an explicit advanced namespace for custom orchestration.

## Index

- [High-level Parquet pipeline](#high-level-parquet-pipeline)
  - [Hive partitions](#hive-partitions)
  - [Modification-time partitions](#modification-time-partitions)
- [Planning and execution](#planning-and-execution)
  - [Result retention](#result-retention)
- [Ordering and lookahead](#ordering-and-lookahead)
- [Advanced namespace](#advanced-namespace)
- [Examples](#examples)

## [High-level Parquet pipeline](#index)

`ParquetPipeline` discovers sources, writes partitions in order, and carries the
schema registry returned by each successful partition into the next.

### [Hive partitions](#index)

```python
from datetime import date

import schema_sanitizer as ss
from schema_sanitizer.pipeline import HivePartitions, ParquetPipeline

job = ParquetPipeline(
    source="gs://bronze/events",
    output="gs://silver/events",
    partitions=HivePartitions.daily(
        date(2026, 8, 1),
        date(2026, 8, 5),
        file_name_prefix="events",
    ),
    options=ss.SanitizeOptions(
        input_format="jsonl",
        parsing=ss.ParsingOptions(integers=True, iso_timestamps=True),
        resources=ss.ResourceOptions(multi_threading=True),
    ),
    initial_schema_registry=ss.new_schema_registry(),
)

result = job.run()
```

`HivePartitions` also accepts hourly bounds through `granularity="hourly"`,
`start_hour`, and `end_hour`. `source_file_extension` and
`output_file_extension` override derived filenames when necessary.

### [Modification-time partitions](#index)

Use this mode for a flat GCS prefix without Hive paths:

```python
from schema_sanitizer.pipeline import ModifiedTimePartitions, ParquetPipeline

job = ParquetPipeline(
    source="gs://raw-bucket/records",
    output="gs://silver-bucket/records",
    partitions=ModifiedTimePartitions.daily(
        date(2026, 8, 1),
        date(2026, 8, 5),
        suffixes=("csv",),
    ),
    options=ss.SanitizeOptions(
        input_format="csv",
        csv=ss.CsvOptions(header_mode="union"),
        resources=ss.ResourceOptions(multi_threading=True),
    ),
)

result = job.run()
```

The helper lists the prefix, freezes object generations, divides them into UTC
daily windows, skips empty days, and publishes one Parquet object per remaining
day. See [Modification-time CSV](flat-prefix-modified-time-csv.md).

## [Planning and execution](#index)

`job.plan()` returns ordered `PartitionRunPlan` values without writing outputs.
`job.run()` accepts optional `read_output_schema` and `after_partition`
callbacks, plus a `result_retention` policy, and returns
`PartitionPipelineResult`.

The supported high-level data models are:

| Type | Purpose |
|---|---|
| `PartitionRunPlan` | Immutable source/output selection for one ordinal. |
| `PartitionRunResult` | Registry, drift, schema, timing, and statistics for one completed ordinal. |
| `PartitionPipelineResult` | Ordered completed runs plus the final registry JSON and optional compiled native state. |
| `SchemaRegistryState` | Registry JSON with optional compiled native state. |

Local output parent directories are created before execution. Remote publication
uses the same staging and commit guarantees as direct file conversion.

### [Result retention](#index)

Partition history is independent from the registry carried between runs:

| Value | Retained history |
|---|---|
| `"full"` | Complete `PartitionRunResult` values, including plans, schemas, statistics, registry documents, and optional native state. This is the default. |
| `"metadata_only"` | Compact per-partition plan summaries and timings; manifests, schemas, statistics, registry documents, and native state are dropped from history. |
| `"streaming"` | No entries in `completed_runs`; consume each full run through `after_partition`. The final registry is still returned. |

Use `metadata_only` or `streaming` for long partition ranges so retained audit
history does not grow with source manifests or output schemas:

```python
result = job.run(
    result_retention="streaming",
    after_partition=record_completed_partition,
)
```

## [Ordering and lookahead](#index)

Registry reduction, callbacks, and publication remain strictly ordered.
Multi-threaded static pipelines may prepare one next source while the current
partition converts, but lookahead never mutates the next registry or publishes
its output. Callable per-partition options and single-threaded execution remain
sequential.

## [Advanced namespace](#index)

Import custom orchestration primitives from the definitive advanced namespace:

```python
from schema_sanitizer.pipeline import advanced

plans = advanced.build_hive_range_plan(config)
discovery = advanced.discover_existing_source_plans(
    plans,
    input_format="jsonl",
)
result = advanced.run_partitioned_to_parquet(
    discovery.existing_plans,
    initial_schema_registry={},
    to_parquet_kwargs={"input_format": "jsonl"},
)
```

The advanced surface is grouped as follows:

- Hive planning: `HiveRangeConfig`, `build_hive_range_plan`,
  `build_hive_range_plan_from_namespace`,
  `build_warm_up_hive_range_plan_from_namespace`, `parse_iso_date`, and
  `parse_hour`.
- Modification-time planning: `UtcWindow`, `ModifiedTimeWindowPlan`,
  `build_utc_daily_windows`, `select_remote_files_by_modified_time`,
  `plan_modified_time_windows_from_listing`,
  `plan_gcs_modified_time_windows`, and its async variant.
- Discovery: `SourceManifest`, `SourcePlanDiscovery`,
  `discover_existing_source_plans`, and its async variant.
- Execution: `run_partitioned_to_parquet`, registry-JSON/state variants,
  `parse_final_schema_registry`, and the four partition/registry result models.
- Warm-up: `infer_warm_up_schema_registry` and its JSON/state variants.
- Schema inspection: `read_parquet_schema`, `flatten_arrow_schema_paths`,
  `diff_flat_schema_paths`, `diff_arrow_schemas`, and `SchemaDriftDiff`.
- Observability: `compact_uri`, `compact_stats_for_log`, `sample_items`,
  `format_duration`, `estimate_cpu_io_wall_time`,
  `cpu_io_wall_percentages`, and `schema_drift_count`.

These names are not duplicated at the `schema_sanitizer.pipeline` root. This
keeps the common API small while giving custom pipelines one documented import
location.

## [Examples](#index)

- [Example 7](../../examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py)
  covers daily/hourly Hive planning, missing partitions, warm-up, BigQuery
  bootstrap, external-table creation, and sidecar updates.
- [Example 8](../../examples/example_08/08_gcs_csv_modified_window_to_polars_parquet.py)
  adds an application-specific Polars normalization between sanitization and
  Parquet publication.
