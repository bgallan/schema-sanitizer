# schema-sanitizer

`schema-sanitizer` turns inconsistent CSV, JSON, JSON arrays, JSON Lines,
NDJSON, XML, and Parquet data into stable analytical tables or cleaned files.
A native C++23 engine performs schema inference, reconciliation, bounded
streaming, and Arrow C Data materialization; the Python API provides file,
dataframe, partitioned pipeline, and BigQuery integration helpers.

The project is still alpha software, with particular focus on Parquet files
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
- [Schema evolution](#schema-evolution)
- [Partition pipeline](#partition-pipeline)
- [Flat-prefix modified-time CSV ingestion](#flat-prefix-modified-time-csv-ingestion)
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
- [Flat-prefix CSV ingestion by modification time](docs/flat-prefix-modified-time-csv.md)
  documents UTC windows, immutable GCS generations, late arrivals, header union,
  publication safety, and analytical memory limits.
- [Concurrency and memory hardening](docs/concurrency-memory-hardening.md)
  documents cross-operation resident memory, filesystem reservations, remote
  ownership, cancellation, and bounded shutdown.

## [Install](#index)

Install the core package plus the output adapter you need:

```bash
pip install 'schema-sanitizer[pyarrow]'
```

```bash
pip install 'schema-sanitizer[pandas]'
pip install 'schema-sanitizer[polars]'
pip install 'schema-sanitizer[duckdb]'
pip install 'schema-sanitizer[gcs]'
pip install 'schema-sanitizer[s3]'
pip install 'schema-sanitizer[azure]'
pip install 'schema-sanitizer[bigquery]'
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
| `execution_policy` | Requested threading mode and the effective worker, queue, remote, and PyArrow limits used by the operation. |
| `conversion_route` | Terminal analytical handoff selected for PyArrow, pandas, Polars, or DuckDB results. |
| `schema_registry` / `schema_registry_json` | Updated durable schema state. |
| `schema_drifts` / `schema_drifts_json` | Drift events produced by this run with the operation-captured UTC timestamp. |

For repeated calls, configure the sanitizer once instead of repeating every
keyword:

```python
sanitizer = ss.Sanitizer(
    ss.SanitizeOptions(
        input_format="csv",
        csv=ss.CsvOptions(header_mode="union"),
        parsing=ss.ParsingOptions(iso_dates=True),
        resources=ss.ResourceOptions(multi_threading=True),
    )
)

frame = sanitizer.to_polars("raw/daily/").clean_data
```

The functional `to_*` API remains available for short, one-off calls.

## [Python API](#index)

All public converters are named `to_*` and share the cleaning options described
below.

| Function | Result |
|---|---|
| `iter_batches(...)` | Lazy, closeable iterator of `pyarrow.RecordBatch` objects. |
| `to_pyarrow(...)` | `Result.clean_data` is a `pyarrow.Table`. |
| `to_pandas(...)` | `Result.clean_data` is a `pandas.DataFrame`. |
| `to_polars(...)` | `Result.clean_data` is a `polars.DataFrame`. |
| `to_duckdb(...)` | `Result.clean_data` is a DuckDB relation. |
| `to_csv(input_path, output_path, ...)` | Writes CSV; `clean_data` is `None`. |
| `to_jsonl(input_path, output_path, ...)` | Writes JSON Lines; `clean_data` is `None`. |
| `to_parquet(input_path, output_path, ...)` | Writes Parquet; `clean_data` is `None`. |

`memory_limit_bytes` controls memory owned by the conversion. Files may be
larger than the budget because readers and writers stream them. Failed file
outputs never replace the destination with a partial result.

Analytical results are caller-owned and stay outside this budget. A large
PyArrow table, pandas or Polars DataFrame, or DuckDB relation can therefore
exhaust process memory. Use direct file outputs for bounded-memory completion.

See [reader memory accounting](docs/reader-memory-accounting.md) for the exact
ownership boundary and
[concurrency and memory hardening](docs/concurrency-memory-hardening.md) for
cross-operation guarantees.

`iter_batches(...)` avoids building one complete table. Close it explicitly or
use it as a context manager. Retained batches still consume caller memory:

```python
with ss.iter_batches("raw/events.jsonl", input_format="jsonl") as batches:
    for batch in batches:
        consume(batch)
```

`new_schema_registry()` creates an empty registry for a pipeline without
depending on the registry JSON structure:

```python
registry = ss.new_schema_registry()
```

### [Inputs and formats](#index)

`input_format` is mandatory for path and URI inputs. Pure-Python row iterables
are recognized directly when `input_format` is omitted; `input_format="python"`
may be supplied explicitly. File formats are never inferred from extensions or
contents, and `input_format="auto"` is rejected.

| Value | Accepted extension | Source shape |
|---|---|---|
| `csv` | `.csv` | Delimited records. |
| `json` | `.json` | One complete JSON document treated as one row. |
| `json_array` | `.json` | A top-level array of row objects. |
| `jsonl` | `.jsonl` | One JSON object per line. |
| `ndjson` | `.ndjson` | One JSON object per line. |
| `xml` | `.xml` | One document, or streamed `xml_row_tag` elements. |
| `parquet` | `.parquet`, `.pq` | Parquet rows. |
| `python` | none | A list, tuple, or one-shot iterable/generator of dictionaries. |

Python inputs use `input_mode="single_file"` because they represent one ordered
logical stream. They work with all seven public converters:

```python
rows = ({"id": index, "payload": f"row-{index}"} for index in range(100_000))
result = ss.to_parquet(
    rows,
    "clean.parquet",
    input_format="python",  # optional for a Python row iterable
    multi_threading=True,
    memory_limit_bytes=256 * 1024 * 1024,
)
```

Python iteration and dictionary inspection remain GIL-bound. The surrounding
work is handled as follows:

- Multi mode consumes up to 4,096 rows per ABI3 call, amortizing the Python
  boundary.
- Native inference, materialization, and output use the same bounded workers as
  file inputs.
- Native probes and writers release the caller's GIL while waiting on the
  operation arena.
- Reader and Python-output callbacks acquire the GIL only while running.
- Generators are not converted to lists; their replay spool stays within the
  operation memory budget.
- Single mode creates no helper thread and remains the deterministic reference
  path.

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

A `SourceManifest` freezes selected GCS object generations so a conversion
never relists the prefix or silently reads a newer object version:

```python
manifest = ss.sources.discover(
    "gs://raw-bucket/events",
    suffixes=("csv",),
    modified_between=(window_start, window_end),
)
result = ss.to_polars(
    manifest,
    input_format="csv",
)
```

The emitted `source_file` column remains the object URI. `Result.stats` also
contains `source_manifest_uri`, `source_object_count`, and deterministic
`source_objects` entries with both `uri` and `generation`. Reusing the same
manifest for inference and materialization therefore reuses the same immutable
object identities. Empty manifests are valid planning values but are rejected
as conversion inputs.

### [Generated ETL columns](#index)

Every analytical and file conversion adds four top-level columns. They always
occupy the end of the Arrow schema, physical output file, and generated
BigQuery external-table schema in this exact order, regardless of
`column_order`:

1. `schema_registry`
1. `schema_drifts`
1. `source_file`
1. `ingestion_timestamp`

- `schema_registry` contains the updated registry as JSON.
- `schema_drifts` contains drift events from the current run.
- Registry and drift values appear on the first row and are null afterwards.
- `source_file` and `ingestion_timestamp` appear on every row.
- One UTC microsecond timestamp is captured before work starts and reused for
  every batch.

Source fields that use one of these reserved root names are rejected rather
than allowed to replace the generated fields. See
[HEURISTICS.md](HEURISTICS.md#generated-etl-fields).

## [Options](#index)

This is the complete option set accepted by the seven public converters. File
converters additionally require `output_path`; `to_parquet` also accepts its
two output compression options.

### [Paths, selection, and schema](#index)

| Option | Default | Purpose |
|---|---:|---|
| `input_path` | required | Local path, `file://` URI, supported remote URI, or a list/tuple/iterable of dict rows. |
| `output_path` | required for file converters | Destination path or URI. |
| `input_format` | `None` | Required for files: `csv`, `json`, `json_array`, `jsonl`, `ndjson`, `xml`, or `parquet`; omit or use `python` for Python row iterables. |
| `input_mode` | `"single_file"` | `single_file` or non-recursive `directory`. |
| `schema_mode` | `"additive"` | With an explicit `schema_contract`, `strict` enforces that contract. In registry-backed conversions, `strict` requires an existing canonical registry while the registry continues to own promotions and version-family evolution. |
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
| `csv_escape_char` | `None` | Optional one-byte escape inside quoted fields. For exports that encode quotes as `\"`, pass `"\\"`. The strict default continues to accept RFC doubled quotes (`""`) only. |
| `csv_header_mode` | `"exact"` | Multi-source header policy. `exact` preserves current behavior; `union` builds immutable per-source projections, accepts reordered/additive headers, and null-fills missing fields. See `docs/csv-header-modes.md`. |
| `input_text_encoding` | `"utf-8"` | `utf-8`, `utf-16`, `utf-16-le`, `utf-16-be`, or `iso8859-1`. Not used for Parquet. |
| `xml_row_tag` | `None` | Stream each matching direct XML element as a row; `None` treats the document as one row. |
| `on_error` | `"emit_null_row"` | `stop`, `skip_row`, or `emit_null_row`. |
| `multi_threading` | `False` | `False` is the deterministic inline reference executor; `True` enables bounded concurrency derived from memory and CPUs. |
| `memory_limit_bytes` | `None` | The only public memory/resource control. `None` selects a safe share of currently available system/container memory. A positive integer creates one atomic operation ledger shared by Python discovery/staging resources and native readers, inference, materialization, writers, workers, Arrow, and Parquet paths. Stage-specific sub-budgets may reject earlier. See `docs/reader-memory-accounting.md` for the ownership boundary. |

#### Concurrency

| Behavior | `multi_threading=False` | `multi_threading=True` |
|---|---|---|
| Execution | Inline on the caller thread | Bounded native concurrency |
| Workers | One | Derived from CPUs and memory |
| Queues and prefetch | Sequential | Bounded by the execution policy |
| Remote clients | Blocking | Bounded asynchronous clients |

Multi mode can overlap remote I/O, inference, materialization, text encoding,
and Parquet preparation. All native stages share one operation arena instead
of creating independent pools.

There is no fixed worker ceiling. Wider machines can use more than 32 workers
when the available work and memory budget justify it. Small or constrained
operations may still run with one worker. Inspect `result.execution_policy` for
the effective values.

Concurrency preserves source order, diagnostics, output bytes, and failure
order. Local file outputs use a staging file and replace the destination only
after success.

#### Memory and resources

`memory_limit_bytes` applies to the complete conversion:

- `None` selects a safe share of available host or container memory.
- A positive integer sets an explicit operation budget.
- Readers, inference, queues, workers, staging metadata, and writers share that
  budget instead of receiving independent limits.
- Temporary-file contents use bounded filesystem permits.
- A conversion fails safely when it cannot make progress within its limits.

Input and output files may exceed the limit because file conversions stream
them. The final object returned by an analytical converter is outside the
budget and can still exhaust process memory.

See [reader memory accounting](docs/reader-memory-accounting.md) for ownership
details and [reader security limits](docs/reader-security-limits.md) for fixed
format ceilings.

#### Reader behavior

Readers fail closed on malformed UTF-8, invalid offsets, excessive nesting,
unsafe decoded sizes, and corrupt compressed data. Public exceptions expose
privacy-safe format, stage, offset, and limit metadata without echoing input
values.

CSV parsing is strict. Use `csv_escape_char` only for an explicitly known
dialect. `csv_header_mode="union"` reconciles reordered or additive headers
across multiple files; duplicate or colliding names remain errors.

The XML reader does not support `DOCTYPE`, custom or external entities,
XInclude, or network/filesystem resolution. It accepts comments, CDATA,
processing instructions, the five predefined entities, and valid numeric
character references.

JSON readers validate projected and unprojected values consistently. Native
Parquet input validates footer, metadata, page, row-group, decompression, and
checksum boundaries before allocation or decoding.

The reader trust model is documented in
[reader security limits](docs/reader-security-limits.md). Complexity and
performance gates live in
[reader complexity](docs/reader-complexity.md) and the benchmark suite.

#### Diagnostics

`Result.stats` reports aggregate resource information without field names or
input values. Useful entries include:

- current, peak, and limit memory bytes;
- decoded and compressed byte counts;
- reader record, node, and parser-depth totals; and
- cancellation counts and stable reason codes.

A completed or closed operation reports zero current charged memory. Peak
memory can differ between serial and parallel plans while logical results
remain equal.

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

## [Schema evolution](#index)

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

Use `schema_mode="strict"` when an existing registry is mandatory and
unexpected fields should fail. Detailed compatibility and schema evolution
rules are documented in [HEURISTICS.md](HEURISTICS.md).

## [Partition pipeline](#index)

`schema_sanitizer.pipeline` can expand and execute a complete daily or hourly
Parquet workflow from one immutable configuration:

```python
from datetime import date

import schema_sanitizer as ss
from schema_sanitizer.pipeline import HivePartitions, ParquetPipeline

job = ParquetPipeline(
    source="gs://bronze/events",
    output="gs://silver/events",
    partitions=HivePartitions.daily(
        date(2026, 7, 1),
        date(2026, 7, 13),
        file_name_prefix="events",
    ),
    options=ss.SanitizeOptions(
        input_format="jsonl",
        parsing=ss.ParsingOptions(integers=True, iso_timestamps=True),
        resources=ss.ResourceOptions(multi_threading=True),
    ),
    initial_schema_registry=ss.new_schema_registry(),
)

pipeline_result = job.run()
```

The job carries the registry returned by each successful partition into the
next one. Existing low-level planners, warm-up hooks, callbacks, and schema
utilities remain available under `schema_sanitizer.pipeline.advanced`.

Pipeline ordering stays deterministic:

- `multi` may prepare one next source while the current partition runs;
- registry changes, callbacks, and output commits remain ordered;
- `single` and callable per-partition options remain sequential; and
- constrained lookahead falls back to preparation at its normal ordinal.

The complete production-shaped example is
[`examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py`](examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py).
It includes daily/hourly planning, directory inputs, missing-partition skips,
warm-up, BigQuery registry bootstrap, external-table creation, and sidecar
updates.

## [Flat-prefix modified-time CSV ingestion](#index)

For CSV objects stored under a flat GCS prefix, modified-time planning can list
the prefix once, divide the immutable `(uri, generation)` snapshot into
half-open UTC days, reconcile each day with `csv_header_mode="union"`, and
publish one validated Parquet object per non-empty day. The default exact CSV
mode and all existing path, URI, directory, and partition inputs are unchanged.

Use `ModifiedTimePartitions.daily(...)` with `ParquetPipeline` when no custom
dataframe transformation is required. Example 8 keeps its analytical Polars
step because it performs application-specific question normalization.

Analytical converters still return a caller-owned dataframe/table outside the
operation memory ledger. Use direct file outputs for bounded-memory completion
when no custom dataframe transformation is required. Late objects created after
the one listing are not part of that run, so production pipelines should use a
defined lookback or rerun policy. See
[Flat-prefix CSV ingestion by modification time](docs/flat-prefix-modified-time-csv.md)
and the executable [example 8](examples/example_08/08_gcs_csv_modified_window_to_polars_parquet.py).

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
[HEURISTICS.md](HEURISTICS.md#bigquery-registry-sidecar).

## [Local and cloud filesystems](#index)

### Supported locations

| Location | Support |
|---|---|
| Local paths and `file://` | Files and directories |
| `gs://` and `gcs://` | Google Cloud Storage |
| `s3://` | Amazon S3 and compatible services |
| Azure Blob and ABFS URIs | Common Azure storage URI forms |
| HTTP(S) | Single files only |

Generic HTTP directory listing is not supported. Cloud directory listing is
deterministic, bounded, and non-recursive.

Install the optional provider clients with:

```bash
pip install 'schema-sanitizer[cloud]'
```

### Remote execution

Remote inputs are staged into replayable local temporary files. File outputs
are converted locally and uploaded only after conversion succeeds.

- Single mode uses blocking HTTP, GCS, S3, and Azure clients on the caller
  thread.
- Multi mode uses bounded provider-native asynchronous clients.
- Local-only and single-mode operations do not create an event-loop host.

In multi mode, one lazy event loop serves the complete operation: listing,
input staging, prefetch, and final upload. Compatible provider sessions and
connection pools are reused and close after submitted or cancelled work drains.

### Bounded staging

The following values are derived from `memory_limit_bytes` rather than exposed
as separate options:

- remote concurrency and discovery workers;
- file prefetch and packet lookahead;
- packet file counts and byte targets;
- retry counts;
- temporary-storage permits;
- replay-spool capacity;
- one-partition pipeline source lookahead.

Packet bytes are reserved before prefetch and corrected to the final staged
size. Reservations remain live until consumption, cancellation, or upload
completion. These limits bound workers, queues, connections, and staging state.

### Publication and retries

- S3 uses bounded multipart uploads. Parts may upload concurrently but commit in
  ordinal order.
- GCS uses resumable sessions and reconciles the committed offset.
- Azure uses block uploads with operation-derived concurrency.
- HTTP uses one ordered `PUT` as the portable fallback.

Failures drain active workers and abort multipart or resumable state before the
local spool lease is released.

HTTP `GET`, `HEAD`, and idempotent `PUT` operations use bounded transient
retries. Every `GET` attempt truncates its staging file; every `PUT` attempt
reopens the completed spool from byte zero.

Streamed HTTP uploads disable implicit connection replay to avoid resending an
already-consumed body as an empty file. `PUT` redirects are not followed: the
destination must return a final success response directly.

## [Development](#index)

Install development dependencies and build the editable native extension:

```bash
python -m pip install -e ".[dev]"
```

Run the complete checks:

```bash
pytest -q
pre-commit run --all-files
```

The suite is grouped by domain under [`tests/`](tests/README.md), so focused
runs stay simple:

```bash
pytest -q tests/io
pytest -q tests/concurrency
pytest -q tests/parquet
```

Build the standalone CMake target when working directly on C++ code:

```bash
cmake -S . -B build/dev -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/dev --parallel
```

CI builds ABI3 wheels for supported platforms and exercises the native core,
reader security, Parquet contracts, remote fault handling, sanitizers, and
packaging. The scripts in [`meta/ci/`](meta/ci/) are the source of truth for
those jobs.

Benchmarks live in [`benchmarks/`](benchmarks/). Start with a small smoke run
before increasing rows, width, workers, or memory:

```bash
python benchmarks/bench_ingest.py --rows 100 --width 4 --repeats 1
```

Security changes should be checked against
[reader security limits](docs/reader-security-limits.md). Development details
for concurrency and memory belong in
[docs/concurrency-memory-hardening.md](docs/concurrency-memory-hardening.md), not
in this introductory README.

## [License](#index)

Apache License 2.0. See [LICENSE](LICENSE).
