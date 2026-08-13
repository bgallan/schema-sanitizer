# Getting started

This guide goes from installation to a first schema-aware conversion. The
[Python API](../reference/python-api.md) and
[options reference](../reference/options.md) cover the full
surface.

## Index

- [Choose an extra](#choose-an-extra)
- [Read into an analytical library](#read-into-an-analytical-library)
- [Write a cleaned file](#write-a-cleaned-file)
- [Process several files](#process-several-files)
- [Reuse a configuration](#reuse-a-configuration)
- [Select concurrency and memory](#select-concurrency-and-memory)
- [Continue learning](#continue-learning)

## [Choose an extra](#index)

The core package has no mandatory Python dependencies. Install only the output
or provider adapters you need:

```bash
pip install "schema-sanitizer[pyarrow]"
pip install "schema-sanitizer[polars]"
pip install "schema-sanitizer[pandas]"
pip install "schema-sanitizer[duckdb]"
```

Cloud extras are independent:

```bash
pip install "schema-sanitizer[gcs]"
pip install "schema-sanitizer[s3]"
pip install "schema-sanitizer[azure]"
pip install "schema-sanitizer[bigquery]"
```

Use `cloud` for all three object stores or `all` for every adapter.

## [Read into an analytical library](#index)

Paths and URIs require an explicit `input_format`:

```python
import schema_sanitizer as ss

result = ss.to_polars(
    "raw/events.csv",
    input_format="csv",
    parse_integers=True,
    parse_iso_dates=True,
)

frame = result.clean_data
```

Replace `to_polars` with `to_pyarrow`, `to_pandas`, or `to_duckdb` to select a
different analytical result.

## [Write a cleaned file](#index)

Direct file output streams through the conversion and does not retain the
complete result:

```python
result = ss.to_parquet(
    "raw/events.csv",
    "silver/events.parquet",
    input_format="csv",
    parquet_compression="gzip",
)
```

`to_csv` and `to_jsonl` follow the same pattern. File-output
`result.clean_data` is `None`; registry, drift, statistics, and execution-policy
properties remain available.

## [Process several files](#index)

Directory mode reads matching direct children in deterministic filename order.
It does not recurse:

```python
result = ss.to_pyarrow(
    "raw/2026-08-05/",
    input_format="csv",
    input_mode="directory",
    csv_header_mode="union",
)
```

Use `csv_header_mode="union"` when files can add or reorder columns. The default
`exact` mode rejects header differences.

## [Reuse a configuration](#index)

```python
sanitizer = ss.Sanitizer(
    ss.SanitizeOptions(
        input_format="jsonl",
        parsing=ss.ParsingOptions(integers=True, iso_timestamps=True),
        resources=ss.ResourceOptions(multi_threading=True),
    )
)

first = sanitizer.to_parquet("raw/day-1.jsonl", "silver/day-1.parquet")
second = sanitizer.to_parquet(
    "raw/day-2.jsonl",
    "silver/day-2.parquet",
    schema_registry=first.schema_registry,
)
```

Configuration objects are immutable and safe to reuse. The registry is still
passed per call because it evolves with the data.

## [Select concurrency and memory](#index)

`multi_threading=False` is the deterministic inline reference mode.
`multi_threading=True` enables bounded concurrency derived from CPUs, available
memory, process pressure, and useful work.

`memory_limit_bytes=None` selects a safe share of current host or container
memory. Pass a positive integer to impose an explicit operation-wide budget:

```python
result = ss.to_parquet(
    "raw/large.jsonl",
    "silver/large.parquet",
    input_format="jsonl",
    multi_threading=True,
    memory_limit_bytes=512 * 1024 * 1024,
)
```

PyArrow, pandas, and Polars results are caller-owned and outside that budget
once returned. A lazy DuckDB relation keeps its governed upstream conversion
chain alive until the final related proxy closes. Prefer a file output or
`iter_batches` when the complete table may not fit in memory; see the
[result lifetime contract](../reference/python-api.md#result-lifetime-and-duckdb).

## [Continue learning](#index)

- [Options](../reference/options.md) lists every conversion parameter.
- [Inputs and filesystems](../reference/inputs-and-filesystems.md) covers Python, local, and
  cloud sources.
- [Schema and registry](../reference/schema-and-registry.md) explains inferred
  types and schema evolution.
- [Partitioned pipelines](partitioned-pipelines.md) covers recurring jobs.
- [Reader security limits](../operations/reader-security-limits.md) describes untrusted input
  handling.
