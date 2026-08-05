# Python API

The supported API is divided into a small top-level conversion surface and
explicit namespaces for sources, pipelines, and BigQuery. Implementation
packages ending in `_impl` are not public.

## Index

- [Functional conversion API](#functional-conversion-api)
- [Result](#result)
- [Streaming batches](#streaming-batches)
- [Reusable configuration](#reusable-configuration)
- [Source discovery API](#source-discovery-api)
- [Schema registry helpers](#schema-registry-helpers)
- [Cancellation and diagnostics](#cancellation-and-diagnostics)
- [Public errors](#public-errors)
- [Pipeline and BigQuery namespaces](#pipeline-and-bigquery-namespaces)

## [Functional conversion API](#index)

All converters accept the cleaning options in [Options](options.md).

| Function | Output |
|---|---|
| `iter_batches(...)` | Closeable iterator of `pyarrow.RecordBatch` values. |
| `to_pyarrow(...)` | `Result.clean_data` is a `pyarrow.Table`. |
| `to_pandas(...)` | `Result.clean_data` is a `pandas.DataFrame`. |
| `to_polars(...)` | `Result.clean_data` is a `polars.DataFrame`. |
| `to_duckdb(...)` | `Result.clean_data` is a DuckDB relation. |
| `to_csv(input_path, output_path, ...)` | Writes CSV. |
| `to_jsonl(input_path, output_path, ...)` | Writes JSON Lines. |
| `to_parquet(input_path, output_path, ...)` | Writes Parquet. |

`input_path` may be a local path, supported URI, `SourceManifest`, or Python
row iterable. File converters additionally require `output_path`.

## [Result](#index)

Every completed converter returns `schema_sanitizer.Result`.

| Property | Meaning |
|---|---|
| `clean_data` | Requested analytical object, or `None` for file output. |
| `stats` | Inference, materialization, reader, batching, and resource counters. |
| `execution_policy` | Requested mode and effective workers, queues, prefetch, and adapter settings. |
| `conversion_route` | Terminal handoff used for an analytical result. |
| `schema_registry` | Parsed durable schema state. |
| `schema_registry_json` | Serialized durable schema state. |
| `schema_drifts` | Parsed changes emitted by this operation. |
| `schema_drifts_json` | Serialized changes emitted by this operation. |

Registry and drift JSON are parsed lazily. Pass `result.schema_registry` or
`result.schema_registry_json` into the next conversion.

## [Streaming batches](#index)

`iter_batches` avoids constructing a complete table. Close it explicitly or use
it as a context manager:

```python
import schema_sanitizer as ss

with ss.iter_batches("raw/events.jsonl", input_format="jsonl") as batches:
    for batch in batches:
        consume(batch)
```

The stream owns conversion resources until closed. Batches retained by the
caller consume caller memory and are not charged after ownership transfers.

## [Reusable configuration](#index)

`Sanitizer` wraps the same functional converters with immutable nested options:

```python
sanitizer = ss.Sanitizer(
    ss.SanitizeOptions(
        input_format="csv",
        csv=ss.CsvOptions(delimiter=";", header_mode="union"),
        parsing=ss.ParsingOptions(floats=True),
        resources=ss.ResourceOptions(multi_threading=True),
    )
)

result = sanitizer.to_pyarrow("raw/csv/")
```

Available methods are `to_pyarrow`, `to_pandas`, `to_polars`, `to_duckdb`,
`to_csv`, `to_jsonl`, `to_parquet`, and `iter_batches`. Each accepts an optional
per-call `schema_registry`. `to_parquet` also accepts a `ParquetOptions` value.

The configuration classes are:

- `SanitizeOptions`: source, schema, naming, depth, encoding, error, CSV,
  parsing, and resource configuration;
- `CsvOptions`: header, delimiter, escape, and multi-file header policy;
- `ParsingOptions`: scalar and temporal string parsing;
- `ResourceOptions`: `multi_threading` and `memory_limit_bytes`;
- `ParquetOptions`: compression and optional GZIP level.

## [Source discovery API](#index)

`schema_sanitizer.sources` owns remote discovery models and operations:

| Name | Purpose |
|---|---|
| `RemoteFile` | Immutable metadata for one listed object. |
| `SourceManifest` | Ordered immutable GCS object-generation selection. |
| `list_objects(...)` | Deterministically list supported remote objects. |
| `discover(...)` | Build a GCS manifest, optionally filtered by modification time. |
| `publish_file_atomic(...)` | Publish one completed local file to a remote destination. |

See [Inputs and filesystems](inputs-and-filesystems.md) and
[Source manifests](source-manifests.md).

## [Schema registry helpers](#index)

`new_schema_registry(field_name_policy="lower_snake")` creates an empty registry
without depending on its JSON representation.

The following helpers bridge registries and application-defined analytical
transformations:

| Helper | Purpose |
|---|---|
| `arrow_schema_from_schema_registry(...)` | Recover the canonical PyArrow schema. |
| `schema_registry_from_arrow_schema(...)` | Create a registry from a PyArrow schema. |
| `project_ingress_scalar_schema(...)` | Select scalar fields suitable for wide CSV ingress. |
| `validate_analytical_result(...)` | Check exact field order, names, nullability, and types. |
| `finalize_analytical_output(...)` | Rebuild registry and drift metadata after a custom transformation. |

`AnalyticalValidationResult` and `FinalizedAnalyticalOutput` are the immutable
return models for these helpers. The complete workflow is in
[Final analytical schemas](analytical-schema-finalization.md).

## [Cancellation and diagnostics](#index)

Apply one deadline or external event to nested public operations:

```python
with ss.operation_cancellation(timeout_seconds=30) as token:
    result = ss.to_parquet(
        "raw/events.jsonl",
        "silver/events.parquet",
        input_format="jsonl",
    )
```

`OperationCancellationToken.cancel()` is thread-safe. Cancellation raises
`SchemaSanitizerCancelledError` and drains owned work before releasing staging
resources.

`process_operation_diagnostics()` returns bounded snapshots of live and recent
operations. Pass an operation ID to filter the result.

## [Public errors](#index)

All public failures derive from `SchemaSanitizerError`:

- `SchemaSanitizerInvalidArgumentError`;
- `SchemaSanitizerIntegrityError`;
- `SchemaSanitizerOutOfMemoryError`;
- `SchemaSanitizerResourceError`;
- `SchemaSanitizerCancelledError`;
- `SchemaSanitizerImportError`.

Use exception types and structured details instead of parsing message text.

## [Pipeline and BigQuery namespaces](#index)

The supported high-level pipeline classes are `HivePartitions`,
`ModifiedTimePartitions`, and `ParquetPipeline`. Data models for custom runners
and an explicit `pipeline.advanced` namespace are also public. See
[Pipelines](pipelines.md).

The curated BigQuery namespace covers table references, external-table DDL,
schema resolution, registry retrieval, and sidecar updates. Lower-level SQL and
CLI orchestration helpers live in `integrations.bigquery.advanced`. See
[BigQuery](bigquery.md).
