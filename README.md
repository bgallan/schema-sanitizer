# schema-sanitizer

`schema-sanitizer` turns inconsistent CSV, JSON, JSON Lines, XML, and Parquet
data into analytical tables or clean files with a stable schema.

Its native C++23 engine handles reading, inference, reconciliation, and
materialization. The Python API adds PyArrow, pandas, Polars, and DuckDB outputs,
file writers, cloud access, and partitioned pipelines.

> The project is alpha software. Its primary focus is Parquet pipelines and
> BigQuery external tables.

## Index

- [Install](#install)
- [First conversion](#first-conversion)
- [What it does](#what-it-does)
- [Two ways to use the API](#two-ways-to-use-the-api)
- [Schema and memory](#schema-and-memory)
- [Documentation](#documentation)
- [Development](#development)
- [License](#license)

## [Install](#index)

Install the package with the adapter you need:

```bash
pip install "schema-sanitizer[pyarrow]"
```

Extras are also available for `polars`, `pandas`, `duckdb`, `gcs`, `s3`,
`azure`, `bigquery`, `cloud`, and `all`.

## [First conversion](#index)

```python
import schema_sanitizer as ss

result = ss.to_pyarrow(
    "raw/events.jsonl",
    input_format="jsonl",
    parse_integers=True,
    parse_iso_timestamps=True,
)

table = result.clean_data
print(table.schema)
print(result.schema_drifts)
```

Write directly to Parquet without retaining a complete table in memory:

```python
ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.parquet",
    input_format="jsonl",
    multi_threading=True,
)
```

Every `to_*` conversion returns a `Result` containing the output, statistics,
execution policy, schema registry, and detected schema changes.

## [What it does](#index)

- Reads CSV, JSON, JSON arrays, JSONL, XML, Parquet, and Python
  dictionary iterables.
- Processes individual files, non-recursive directories, and remote objects.
- Produces PyArrow, pandas, Polars, DuckDB, CSV, JSONL, or Parquet.
- Carries a schema registry across runs and reports every schema change.
- Reconciles reordered or additive CSV headers.
- Uses one global memory limit and adapts concurrency to the machine.
- Supports local paths, GCS, S3, Azure Blob, and HTTP(S) files.
- Builds Parquet pipelines from Hive partitions or object modification times.
- Generates and maintains BigQuery external tables and schema sidecars.

## [Two ways to use the API](#index)

The `to_*` functions are convenient for one-off calls. Create a `Sanitizer` to
reuse one configuration:

```python
sanitizer = ss.Sanitizer(
    ss.SanitizeOptions(
        input_format="csv",
        csv=ss.CsvOptions(header_mode="union"),
        parsing=ss.ParsingOptions(iso_dates=True),
        resources=ss.ResourceOptions(
            multi_threading=True,
            memory_limit_bytes=512 * 1024 * 1024,
        ),
    )
)

frame = sanitizer.to_polars("raw/daily/").clean_data
```

## [Schema and memory](#index)

Pass one run's registry to the next to evolve the schema deterministically:

```python
first = ss.to_parquet(
    "raw/day-1.jsonl",
    "silver/day-1.parquet",
    input_format="jsonl",
)
second = ss.to_parquet(
    "raw/day-2.jsonl",
    "silver/day-2.parquet",
    input_format="jsonl",
    schema_registry=first.schema_registry,
)
```

`memory_limit_bytes` bounds resources owned by the conversion. Streaming readers
and writers can process files larger than that budget. A returned analytical
table or DataFrame becomes caller-owned and is outside the budget; direct file
output is the safe choice when a complete result may be too large.

## [Documentation](#index)

The [documentation guide](docs/README.md) organizes detailed material by task:

- [Getting started](docs/guides/getting-started.md)
- [Python API](docs/reference/python-api.md)
- [Configuration options](docs/reference/options.md)
- [Inputs and filesystems](docs/reference/inputs-and-filesystems.md)
- [Schemas and registries](docs/reference/schema-and-registry.md)
- [Partitioned pipelines](docs/guides/partitioned-pipelines.md)
- [BigQuery integration](docs/reference/bigquery.md)
- [Resources and concurrency](docs/operations/resources-and-concurrency.md)
- [Reader security](docs/operations/reader-security-limits.md)
- [Compatibility](docs/reference/compatibility.md)
- [CI/CD pipeline](docs/project/ci-cd.md)
- [Development and contribution](docs/project/development.md)

Complete programs live in [`examples/`](examples/).
[Example 7](examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py)
covers a Hive pipeline to Parquet and BigQuery.
[Example 8](examples/example_08/08_gcs_csv_modified_window_to_polars_parquet.py)
covers CSV under a flat GCS prefix, modification-time windows, a custom Polars
transformation, and UTC `year/month/day` Hive output from a chosen timestamp.

## [Development](#index)

```bash
python -m pip install -e ".[dev]"
pytest -q
pre-commit run --all-files
```

The first pre-commit run provisions its exact hash-verified tools below
`.work/pre-commit-tools`; it does not install them into the active environment.

See the [development guide](docs/project/development.md) for native builds, focused
tests, benchmarks, and CI.

## [License](#index)

Apache License 2.0. See [LICENSE](LICENSE).
