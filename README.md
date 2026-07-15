# schema-sanitizer

`schema-sanitizer` turns inconsistent CSV, JSON, JSON arrays, JSON Lines,
NDJSON, XML, and Parquet data into stable analytical tables or cleaned files.
A native C++23 engine performs schema inference, reconciliation, bounded
streaming, and Arrow C Data materialization; the Python API provides file,
dataframe, partitioned pipeline, and BigQuery integration helpers.

Version 0.3.7 is still alpha software, with particular focus on Parquet files
used by BigQuery external tables.

## Index

- [Documentation](#documentation)
- [Install](#install)
- [Quick start](#quick-start)
- [Python API](#python-api)
  - [Inputs and formats](#inputs-and-formats)
  - [Generated ETL columns](#generated-etl-columns)
- [Options](#options)
  - [Paths, selection, and schema](#paths-selection-and-schema)
  - [String scalar parsing](#string-scalar-parsing)
  - [Source parsing, errors, and resources](#source-parsing-errors-and-resources)
  - [Parquet output](#parquet-output)
- [Incremental schemas](#incremental-schemas)
- [Partition pipeline](#partition-pipeline)
- [BigQuery external tables](#bigquery-external-tables)
  - [Registry sidecar table](#registry-sidecar-table)
- [Local and cloud filesystems](#local-and-cloud-filesystems)
- [Development](#development)
- [License](#license)

## [Documentation](#index)

- This README is the installation, Python API, options, and pipeline guide.
- [HEURISTICS.md](HEURISTICS.md) explains inference, field sanitization,
  schema merging and versioning, the registry, drift records, and the BigQuery
  sidecar model.
- [RESPONSIBILITIES.md](RESPONSIBILITIES.md) maps the Python and C++ source
  layout for contributors.
- [COMPATIBILITY.md](COMPATIBILITY.md) defines supported runtimes and serialized-state guarantees.

## [Install](#index)

Install the core package plus the output adapter you need:

```bash
pip install 'schema-sanitizer[pyarrow]'
```

```bash
pip install 'schema-sanitizer[pandas]'
pip install 'schema-sanitizer[polars]'
pip install 'schema-sanitizer[duckdb]'
pip install 'schema-sanitizer[cloud]'
pip install 'schema-sanitizer[all]'
```

## [Quick start](#index)

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

Write the same cleaned data without retaining an output table in memory:

```python
ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.parquet",
    input_format="jsonl",
)
```

Every conversion returns a `schema_sanitizer.Result`.

| Property | Meaning |
|---|---|
| `clean_data` | The requested analytical object, or `None` for a file output. |
| `stats` | Inference, materialization, batching, depth, and error counters. |
| `schema_registry` / `schema_registry_json` | Updated durable schema state. |
| `schema_drifts` / `schema_drifts_json` | Drift events produced by this run. |

## [Python API](#index)

All public converters are named `to_*` and share the cleaning options described
below.

| Function | Result |
|---|---|
| `to_pyarrow(...)` | `Result.clean_data` is a `pyarrow.Table`. |
| `to_pandas(...)` | `Result.clean_data` is a `pandas.DataFrame`. |
| `to_polars(...)` | `Result.clean_data` is a `polars.DataFrame`. |
| `to_duckdb(...)` | `Result.clean_data` is a DuckDB relation. |
| `to_csv(input_path, output_path, ...)` | Writes CSV; `clean_data` is `None`. |
| `to_jsonl(input_path, output_path, ...)` | Writes JSON Lines; `clean_data` is `None`. |
| `to_parquet(input_path, output_path, ...)` | Writes Parquet; `clean_data` is `None`. |

`new_schema_registry()` creates an empty registry for a pipeline without
depending on the registry JSON structure:

```python
registry = ss.new_schema_registry()
```

### [Inputs and formats](#index)

`input_format` is mandatory. It is never inferred from the extension or file
contents, and `None` or `"auto"` is rejected.

| Value | Accepted extension | Source shape |
|---|---|---|
| `csv` | `.csv` | Delimited records. |
| `json` | `.json` | One complete JSON document treated as one row. |
| `json_array` | `.json` | A top-level array of row objects. |
| `jsonl` | `.jsonl` | One JSON object per line. |
| `ndjson` | `.ndjson` | One JSON object per line. |
| `xml` | `.xml` | One document, or streamed `xml_row_tag` elements. |
| `parquet` | `.parquet`, `.pq` | Parquet rows. |

`input_mode="single_file"` processes exactly one file. `input_mode="directory"`
processes matching direct children in deterministic filename order; it does not
recurse into subdirectories.

```python
result = ss.to_pandas(
    "raw/2026-07/",
    input_format="jsonl",
    input_mode="directory",
)
```

### [Generated ETL columns](#index)

Every analytical and file conversion adds four top-level columns. They always
occupy the end of the Arrow schema, physical output file, and generated
BigQuery external-table schema in this exact order, regardless of
`column_order`:

1. `schema_registry`
1. `schema_drifts`
1. `source_file`
1. `ingestion_timestamp`

`schema_registry` contains the updated canonical registry as JSON and
`schema_drifts` contains this run's drift events as JSON. They are populated on
the first output row and null on later rows. `source_file` and
`ingestion_timestamp` are populated on every row. `ingestion_timestamp` uses
Arrow/Parquet `TIMESTAMP_MICROS`.

Source fields that use one of these reserved root names are rejected rather
than allowed to replace the generated fields. See
[heuristics.md](heuristics.md#generated-etl-fields).

## [Options](#index)

This is the complete option set accepted by the seven public converters. File
converters additionally require `output_path`; `to_parquet` also accepts its
two output compression options.

### [Paths, selection, and schema](#index)

| Option | Default | Purpose |
|---|---:|---|
| `input_path` | required | Local path, `file://` URI, or supported remote URI. |
| `output_path` | required for file converters | Destination path or URI. |
| `input_format` | `None` (rejected) | `csv`, `json`, `json_array`, `jsonl`, `ndjson`, `xml`, or `parquet`. |
| `input_mode` | `"single_file"` | `single_file` or non-recursive `directory`. |
| `schema_mode` | `"additive"` | `additive` evolves a registry; `strict` rejects extra fields and requires a registry-derived schema. |
| `schema_registry` | `None` | Previous registry mapping or registry JSON. `None` starts a new registry. |
| `column_order` | `"alphabetically"` | `alphabetically`, or `schema_contract_first` to retain registered fields first and append new fields deterministically. Applies recursively to source fields only. |
| `field_name_policy` | `"lower_alpha"` | `lower_alpha`, `lower_snake`, or `preserve`. |
| `scalar_object_key` | `"default_key"` | Child field used when a scalar must coexist with an object/struct. |
| `arrow_max_depth` | `32` | Maximum expanded Arrow container depth before flattening deeper values to strings. |
| `parquet_max_depth` | `15` | Maximum Parquet/BigQuery RECORD depth; list wrappers do not add a RECORD level. |

### [String scalar parsing](#index)

These options affect strings such as CSV cells, XML text, and quoted JSON
values. JSON numbers and booleans are already typed by JSON syntax. Parsing is
opt-in, and unmatched strings remain strings.

| Option | Default | Purpose |
|---|---:|---|
| `parse_integers` | `False` | Parse integer-looking strings as `int64`. |
| `parse_floats` | `False` | Parse float-looking strings as `float64`. |
| `parse_float_decimal_separator` | `"."` | One ASCII punctuation character used as the decimal separator. |
| `parse_float_thousands_separator` | `","` | A distinct grouping separator; grouped sections must contain three digits. |
| `true_tokens` | `()` | Case-insensitive strings to parse as `True`. |
| `false_tokens` | `()` | Case-insensitive strings to parse as `False`; token sets may not overlap. |
| `parse_iso_timestamps` | `False` | Enable built-in ISO timestamp parsing. |
| `parse_iso_dates` | `False` | Enable built-in `YYYY-MM-DD` date parsing. |
| `parse_iso_times` | `False` | Enable built-in `HH:MM:SS` time parsing. |
| `custom_timestamp_patterns` | `()` | Regexes whose groups 1-6 are year through second; optional groups 7-8 are fraction and timezone. |
| `custom_date_patterns` | `()` | Regexes whose groups 1-3 are year, month, and day. |
| `custom_time_patterns` | `()` | Regexes whose groups 1-3 are hour, minute, and second. |
| `timestamp_precision` | `"TIMESTAMP_MICROS"` | `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, or `TIMESTAMP_NANOS`. |

String parsers first try the exact value, then retry after trimming surrounding
ASCII whitespace. A failed parse preserves the original string and whitespace.
When integers and floats coexist, inference promotes the field to `float64`.

```python
prices = ss.to_pyarrow(
    "prices.csv",
    input_format="csv",
    csv_delimiter=";",
    parse_floats=True,
    parse_float_decimal_separator=",",
    parse_float_thousands_separator=".",
).clean_data
```

### [Source parsing, errors, and resources](#index)

| Option | Default | Purpose |
|---|---:|---|
| `csv_has_header` | `True` | Treat the first CSV row as names; directory mode removes matching repeated headers. |
| `csv_delimiter` | `","` | One-character delimiter. |
| `input_text_encoding` | `"utf-8"` | `utf-8`, `utf-16`, `utf-16-le`, `utf-16-be`, or `iso8859-1`. Not used for Parquet. |
| `xml_row_tag` | `None` | Stream each matching direct XML element as a row; `None` treats the document as one row. |
| `on_error` | `"emit_null_row"` | `stop`, `skip_row`, or `emit_null_row`. |
| `memory_limit_bytes` | `None` | The only public memory/resource control. `None` selects 512 MiB. The native extension derives all chunk, batch, coalescing, metadata, spool, concurrency, Arrow, and Parquet sub-budgets from this value. |

`memory_limit_bytes` is local to one operation. It is validated before native
execution, cannot exceed the absolute 64 GiB safety ceiling, and never mutates
process-global state. There are no environment-variable overrides or secondary
public memory knobs. Two concurrent calls may therefore use different budgets
without interfering with each other. Schema-Sanitizer also contains no
environment-access hooks in its runtime, build files, examples, tests, or
project workflows; configuration is explicit or declarative. Provider SDKs may
still use their own standard credential discovery outside the library.

The native extension is the single source of truth for derived limits. Python
queries that native budget and uses the returned values for input chunks, replay
spooling, remote scheduling, metadata expansion, Arrow validation, coalescing,
and Parquet reading/writing. Internal structural ceilings such as maximum schema
depth, field cardinality, Arrow logical ranges, and row-group count remain
non-configurable and cannot be raised by callers. Scratch cleanup and hardened
allocation bookkeeping are always active. Best-effort overwriting cannot
guarantee physical erasure on copy-on-write filesystems, SSD wear-leveling, or
after data has been copied by a third-party Arrow consumer.

### [Parquet output](#index)

These options are accepted only by `to_parquet`:

| Option | Default | Purpose |
|---|---:|---|
| `parquet_compression` | `"gzip"` | `gzip`, `snappy`, or `uncompressed`. |
| `parquet_gzip_level` | `None` | Optional zlib level `0..9`; ignored for Snappy or uncompressed output. |

```python
ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.parquet",
    input_format="jsonl",
    parquet_compression="gzip",
    parquet_gzip_level=6,
)
```

Release wheels for Windows, Linux, and macOS build GZIP support from the same
pinned zlib source and expose the same native output matrix: `gzip`, `snappy`,
and `uncompressed`. Windows source builds default to that bundled static zlib,
so GZIP does not depend on vcpkg or a machine-wide zlib installation. Set
`-DSCHEMA_SANITIZER_ZLIB_PROVIDER=system` only when intentionally building against
a system package.

## [Incremental schemas](#index)

Pass one result's registry into the next conversion to preserve schema history:

```python
first = ss.to_parquet(
    "raw/2026-07-12/events.jsonl",
    "silver/2026-07-12/events.parquet",
    input_format="jsonl",
)

second = ss.to_parquet(
    "raw/2026-07-13/events.jsonl",
    "silver/2026-07-13/events.parquet",
    input_format="jsonl",
    schema_registry=first.schema_registry,
    schema_mode="additive",
)
```

Use `schema_mode="strict"` when a non-empty existing registry is mandatory and
unexpected fields should fail. Detailed compatibility, version-family, and
generation behavior is documented in [HEURISTICS.md](HEURISTICS.md).

## [Partition pipeline](#index)

`schema_sanitizer.pipeline` provides reusable single-writer building blocks for
Hive-style daily or hourly pipelines:

```python
from datetime import date

import schema_sanitizer as ss
from schema_sanitizer.pipeline import (
    HiveRangeConfig,
    build_hive_range_plan,
    discover_existing_source_plans,
    run_partitioned_to_parquet,
)

plans = build_hive_range_plan(
    HiveRangeConfig(
        source_prefix="gs://bronze/events",
        output_prefix="gs://silver/events",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 13),
        input_format="jsonl",
        input_mode="single_file",
        file_name_prefix="events",
    )
)

discovery = discover_existing_source_plans(plans, input_format="jsonl")
pipeline_result = run_partitioned_to_parquet(
    discovery.existing_plans,
    initial_schema_registry=ss.new_schema_registry(),
    to_parquet_kwargs={
        "input_format": "jsonl",
        "schema_mode": "additive",
        "parse_integers": True,
        "parse_iso_timestamps": True,
    },
)
```

The runner carries the registry returned by each successful partition into the
next one. `infer_warm_up_schema_registry*` can scan a separate range additively
before normal writes. Source discovery, warm-up, and writing support the same
local and remote paths as the public converters.

The complete production-shaped example is
[`examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py`](examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py).
It includes daily/hourly planning, directory inputs, missing-partition skips,
warm-up, BigQuery registry bootstrap, external-table creation, and sidecar
updates.

## [BigQuery external tables](#index)

`schema_sanitizer.integrations.bigquery` translates a final PyArrow schema into
explicit BigQuery external-table DDL. It removes fields supplied by Hive path
partitioning and preserves the physical root order, including the four ETL
columns at the end.

```python
from schema_sanitizer.integrations.bigquery import (
    BigQueryTableRef,
    ExternalTableSpec,
    external_table_ddl,
)

table_ref = BigQueryTableRef("my-project", "analytics", "events")
spec = ExternalTableSpec(
    source_uris=["gs://silver/events/*"],
    hive_uri_prefix="gs://silver/events",
    partition_columns=(
        ("year", "INT64"),
        ("month", "INT64"),
        ("date", "DATE"),
    ),
)

ddl, skipped_partition_fields = external_table_ddl(
    table_ref,
    final_arrow_schema,
    spec,
)
```

The integration also exposes ADBC-backed helpers to validate an existing
external table, retrieve its latest embedded `schema_registry`, and execute
create/replace DDL. BigQuery and Arrow ADBC connections are supplied by the
application; they are not hidden global clients.

### [Registry sidecar table](#index)

Scanning a large external table solely to find its latest registry can be
expensive. The optional sidecar is a native BigQuery table with one pointer per
external table:

```sql
external_table_name STRING NOT NULL
last_ingested_partition STRING NOT NULL
```

The partition value uses Hive key order, for example:

```text
year=2026/month=07/date=2026-07-13
year=2026/month=07/date=2026-07-13/hour=08
```

On bootstrap, `fetch_latest_schema_registry` can use that pointer to query one
partition. Missing, invalid, non-native, empty, or failed sidecar lookups fall
back to scanning the external table. After the Parquet outputs and external
table are successfully updated, `update_registry_sidecar_table` creates the
sidecar if needed and performs an idempotent `MERGE`.

The sidecar stores only the lookup pointer; the authoritative registry remains
the `schema_registry` value embedded in the output data. See
[heuristics.md](heuristics.md#bigquery-registry-sidecar).

## [Local and cloud filesystems](#index)

Local paths, `file://`, `gs://`/`gcs://`, `s3://`, common Azure Blob/ABFS URIs,
and single-file HTTP(S) sources are supported. Install cloud clients with:

```bash
pip install 'schema-sanitizer[cloud]'
```

Remote inputs are staged through provider-native async clients into replayable
local temporary files. File outputs are uploaded after conversion. Remote
directory listing is bounded, deterministic, and non-recursive; generic HTTP
directory listing is not supported.

Remote concurrency, file prefetch, retries, chunk lookahead, discovery workers,
and replay-spool capacity are derived automatically from the operation's
`memory_limit_bytes`. They are not separate API options and have no
environment-variable overrides. Absolute internal ceilings remain in place so
direct internal callers cannot create unbounded worker, queue, connection, or
staging state.

## [Development](#index)

Install development dependencies and compile the editable native extension:

```bash
python -m pip install -e '.[dev]'
```

Build the standalone CMake target:

```bash
cmake -S . -B build/dev -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/dev
```

Run checks:

```bash
python -m pytest -q
python -m ruff check .
python -m mypy
pre-commit run --all-files
```

Run the end-to-end benchmark suite with a small smoke workload or a focused
case:

```bash
python benchmarks/bench_ingest.py --rows 100 --width 4 --repeats 1
python benchmarks/bench_ingest.py --case jsonl --rows 100000 --repeats 3
```

For architecture and ownership, see [RESPONSIBILITIES.md](RESPONSIBILITIES.md).
For the production-readiness roadmap, see [todo.md](todo.md).

## [License](#index)

Apache License 2.0. See [LICENSE](LICENSE).
