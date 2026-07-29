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
| `execution_policy` | Requested threading mode and the effective worker, queue, remote, and PyArrow limits used by the operation. |
| `conversion_route` | Terminal analytical handoff selected for PyArrow, pandas, Polars, or DuckDB results. |
| `schema_registry` / `schema_registry_json` | Updated durable schema state. |
| `schema_drifts` / `schema_drifts_json` | Drift events produced by this run with the operation-captured UTC timestamp. |

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

For file outputs, `memory_limit_bytes` is an operation-wide resident-memory
budget, independent of input or output file size. Input parsing, bounded
queues, transformation, writer buffers, and local or remote staging all share
the same resolved budget. Files larger than the budget are streamed; if the
operation cannot make progress within the budget, it fails without publishing
a partial replacement.

For analytical outputs (`to_pyarrow`, `to_pandas`, `to_polars`, and
`to_duckdb`), source processing still respects the operation budget, but the
final table, DataFrame, or relation returned in `Result.clean_data` is
deliberately outside it. That result may exhaust process memory when it is too
large. Use a file-output converter when bounded-memory completion is required.

`iter_batches(...)` avoids building that final table. It keeps the native
operation and its memory budget alive until the iterator is exhausted or
closed. Each yielded batch leaves the operation budget when ownership passes
to Python, so retaining every batch can still grow caller memory:

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
`ingestion_timestamp` are populated on every row. One UTC ingestion timestamp is
captured before the operation schedules source, transform, or sink work and is
reused for every emitted batch, so its value cannot depend on worker completion
order. `ingestion_timestamp` uses Arrow/Parquet `TIMESTAMP_MICROS`.

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
| `input_text_encoding` | `"utf-8"` | `utf-8`, `utf-16`, `utf-16-le`, `utf-16-be`, or `iso8859-1`. Not used for Parquet. |
| `xml_row_tag` | `None` | Stream each matching direct XML element as a row; `None` treats the document as one row. |
| `on_error` | `"emit_null_row"` | `stop`, `skip_row`, or `emit_null_row`. |
| `multi_threading` | `False` | `False` is the deterministic inline reference executor; `True` enables bounded concurrency derived from memory and CPUs. |
| `memory_limit_bytes` | `None` | The only public memory/resource control. `None` selects a safe share of currently available system/container memory. A positive integer sets a strict operation-wide budget. The native extension derives all chunk, batch, coalescing, metadata, spool, concurrency, Arrow, and Parquet sub-budgets from the resolved value. |

#### Execution modes

| Behavior | `multi_threading=False` | `multi_threading=True` |
|---|---|---|
| Execution | Inline on the caller thread | Bounded native concurrency |
| Worker count | One | Derived from CPUs and memory |
| Queues and prefetch | One item at a time | Bounded by the execution policy |
| PyArrow fallback | `use_threads=False` | Uses the derived policy |
| Remote clients | Blocking | Bounded asynchronous clients |

Single mode creates no Schema-Sanitizer thread pool, event-loop host, or child
process. Remote work also runs on the caller thread, so calling it from an
active `asyncio` loop blocks that loop.

Multi mode has no public worker-count option. It derives the effective width
from:

- `memory_limit_bytes`;
- CPUs available through host, affinity, and cgroup limits.

There is no fixed global worker ceiling. On machines wider than 32 CPUs, the
arena uses dynamically sized worker maps with hierarchical non-empty summaries.
It can continue growing while the operation memory budget provides both the
worker arena and a conservative native stack/runtime reserve.

When resources are constrained, multi mode may use only one worker. Inspect
`result.execution_policy` for the effective values and fallback reason.

#### Work performed concurrently

Multi mode can overlap:

- remote discovery, transfer, staging, and source prefetch;
- supported PyArrow operations;
- schema inference and native materialization;
- CSV and JSONL fragment encoding;
- Parquet column preparation and compression.

All native stages reuse one operation-wide task arena. They do not create
independent worker pools that could multiply CPU or memory use. Workers start
lazily, and small or inexpensive batches remain on the serial path.

Concurrent operations also share a process-wide CPU governor. An isolated
operation keeps the lock-free fast path; when operations overlap, native tasks
enter through cancelable FIFO admission so their combined active workers do
not exceed the CPU capacity visible through affinity and cgroups.

On Linux, wide arenas sample each worker's NUMA node. Idle workers first steal
compatible work from the same node, then fall back to unrestricted stealing so
cross-node placement never strands work.

Branch-heavy or irregular stages use conservative fractions of the arena.
Those fractions still grow on wider machines; they are not fixed 4-, 16-, or
32-worker ceilings. Stages with fewer independent work items use only the
workers that can perform useful work.

#### Ordering and failure safety

Concurrency does not change observable ordering:

- results, diagnostics, output bytes, and failures commit by source ordinal;
- bounded dispatch also bounds retained out-of-order results;
- text output is bounded by retained bytes as well as packet count;
- oversized rows are processed without allowing later rows to overtake them;
- stage-local cancellation stops failed work without invalidating unrelated
  arena users.

Local CSV, JSONL, and Parquet outputs are written to sibling staging files and
atomically replace the destination only after success. A failed conversion
therefore does not truncate an existing output.

#### Memory budget

`memory_limit_bytes` applies to one complete operation:

- `None` selects a safe share of currently available host or container memory;
- a positive integer sets an explicit budget;
- the resolved value is fixed once and shared by every stage;
- all files and substreams belonging to the call share one native pool.

On Linux, automatic sizing also respects the remaining cgroup allowance. It
reserves 12.5–25% for the system and untracked allocations, then applies a
64 GiB ceiling.

The budget covers Schema-Sanitizer-owned input chunks, queues, reorder windows,
materialization, writers, remote packets, and staging. These components cannot
each spend the full limit independently.

Concurrent public calls retain their own operation limit, while actual native
allocations also pass through one process-wide governor. FIFO admission leases
adapt to operation size and current contention. Files inside one directory
conversion still share one lease and pool instead of reserving the full budget
again.

The process ceiling is refreshed when operations start, so later calls observe
changes in available host or cgroup memory. A running operation keeps its fixed
public limit, while new aggregate allocations are held to the refreshed
process ceiling.

Input and output files may be larger than the budget because file conversions
stream them. If an operation cannot proceed safely, it fails before publishing
its staged output.

The final object returned by `to_pyarrow`, `to_pandas`, `to_polars`, or
`to_duckdb` is intentionally outside the budget. A very large analytical result
can therefore exhaust process memory. Use a file-output converter when bounded
memory is required.

#### Fixed safety limits

The native extension is the source of truth for all derived limits. There are
no environment-variable overrides or secondary public memory controls.

Structural ceilings such as schema depth, field cardinality, Arrow logical
ranges, and Parquet row-group count cannot be raised by callers. Scratch cleanup
and hardened allocation bookkeeping remain enabled.

Best-effort memory overwriting cannot guarantee physical erasure on
copy-on-write filesystems, SSDs with wear levelling, or after a third-party
Arrow consumer has copied the data.

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

With static conversion options, `multi` keeps at most one immutable source for
partition `N + 1` prepared while partition `N` converts or publishes.
The lookahead shares the operation memory permits and remote coordinator,
but each partition retains its own fixed run timestamp. Registry inference,
registry mutation, callbacks, and output commits remain strictly ordered.
Callable per-partition options and all `single` pipelines remain fully
sequential; capacity contention automatically falls back to preparation at
the partition's own ordinal.

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

Known or estimated packet bytes are reserved before prefetch. The reservation
is corrected to the exact staged size and retained until consumption or
cancellation. Final remote output keeps its exact reservation until upload
finishes.

Memory-derived windows and the available work count prevent direct callers from
creating unbounded workers, queues, connections, or staging state.

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

Install development dependencies and compile the editable native extension:

```bash
python -m pip install -e '.[dev]'
```

Build the standalone CMake target:

```bash
cmake -S . -B build/dev -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/dev
```

Run the focused executor probe and the complete ABI3 extension under GCC
ThreadSanitizer on Linux:

```bash
cmake -S . -B build/tsan -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DSCHEMA_SANITIZER_SANITIZER=tsan \
  -DSCHEMA_SANITIZER_ZLIB_PROVIDER=bundled \
  -DSCHEMA_SANITIZER_REQUIRE_ZLIB=ON \
  -DSCHEMA_SANITIZER_ENABLE_LTO=OFF
cmake --build build/tsan --parallel
g++ -std=c++17 -fsanitize=thread -fno-omit-frame-pointer \
  meta/ci/tsan_python_launcher.cc \
  $(python3-config --embed --cflags --ldflags) \
  -o python-tsan
meta/ci/run_tsan_extension_suite.sh \
  build/tsan ./python-tsan 1 \
  "$(python -c 'import site; print(site.getsitepackages()[0])')" \
  --verify-only
meta/ci/run_tsan_extension_suite.sh \
  build/tsan ./python-tsan 2 \
  "$(python -c 'import site; print(site.getsitepackages()[0])')" \
  tests/test_threading_golden_matrix.py
meta/ci/run_tsan_extension_suite.sh \
  build/tsan ./python-tsan 2 \
  "$(python -c 'import site; print(site.getsitepackages()[0])')" \
  tests/test_partition_lookahead.py
```

Invoke the second command separately for each threading domain listed by the
runner. CI deliberately uses one shell step per domain rather than chaining all
TSan interpreters inside one script invocation.

The general CI workflow runs real-loopback HTTP fault injection against the
ABI3 wheel on Linux, Windows, macOS x86-64, and macOS arm64. It covers:

- truncated downloads and publication disconnects;
- delayed cancellation and bounded retry exhaustion;
- metadata retries and fatal staging cleanup;
- SIGINT draining and abrupt interpreter shutdown.

Native parser fuzzing uses the production JSON, CSV, XML, and Parquet entry
points:

- Clang builds may use libFuzzer.
- GCC, MSVC, and AppleClang gates can use the deterministic standalone engine.
- Known crash inputs run before bounded mutation campaigns.
- Every campaign fixes its run count, seed, and maximum input length.
- Linux uses ASan/UBSan and TSan, Windows AMD64 uses MSVC ASan, and macOS uses
  AppleClang ASan/UBSan.

The same jobs repeat the sanitized ordinal-executor probe. Fuzzer settings are
development controls, not production API or environment configuration.

The dedicated CPython launcher loads the matching TSan runtime before extension
modules. Its checks are isolated deliberately:

- sanitizer options are compiled into the launcher;
- the runner verifies that the requested extension build was loaded;
- 64 native, fixed-clock public-path, and partition-lookahead differential tests
  run in fresh sanitizer-first interpreters;
- a success marker is written only after `pytest_sessionfinish`;
- a timeout before that marker remains a hard failure.

This isolation avoids cross-domain shutdown interactions with binary PyArrow.
CPython and PyArrow wheels are not TSan-instrumented, so the gate ignores races
owned entirely by those modules while retaining checks for the extension,
native core, and bundled zlib.

Ordinary Python also skips local TSan/ASan extensions unless the matching
runtime was linked before module loading.

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

Compare deterministic `single` and bounded `multi` execution with byte/logical
equivalence checks and a machine-readable report:

```bash
PYTHONPATH=src python benchmarks/bench_threading_modes.py \
  --rows 120000 --memory-mib 256 --warmups 1 --repeats 3 \
  --output threading-benchmark.json

# Focus only on native Parquet output and JSONL-to-Parquet pipelines.
PYTHONPATH=src python benchmarks/bench_threading_modes.py \
  --only parquet --parquet-compression snappy --rows 60000 \
  --memory-mib 256 --warmups 1 --repeats 3 \
  --output threading-benchmark-parquet.json
```

Measure complete-pipeline scaling against the same `multi` engine restricted by
process affinity to 1, 2, 4, and 8 CPUs. Every point runs in a fresh process and
logical output must match before timings are reported:

```bash
PYTHONPATH=src python benchmarks/bench_operation_arena_scaling.py \
  --workers 1,2,4,8 --rows 100000 --sources 8 \
  --warmups 1 --repeats 3 --output operation-arena-scaling.json
```

Use `--pipeline-shape scalar|nested` or
`--pipeline-format csv|jsonl|parquet` for a focused local run. The worker counts
are process-affinity inputs to the normal automatic policy, not a production API
option.

Profile the source, coordinator, Arrow, output, task-queue, and operation-memory
regions at each affinity with the operation-local telemetry harness:

```bash
PYTHONPATH=src python benchmarks/bench_concurrency_telemetry.py \
  --workers 1,2,4,8,16,32,64,128 --rows 20000 --columns 64 \
  --memory-mib 512 --warmups 1 --repeats 7 \
  --output concurrency-telemetry.json
```

Add `--hardware-counters` on Linux to wrap one isolated sample in `perf stat`.
Generic IPC and cache counters can distinguish compute/cache symptoms but do not
prove DRAM saturation. Supply `--dram-bandwidth-json` with same-host measured
and sustainable GiB/s values from PCM, uProf, or platform uncore counters before
the harness may report `dram_bandwidth_saturation`.

For the final 16/32 decision, run the resumable paired short+sustained suite. It
locks one CPU/NUMA plan, rejects unstable paired samples, and fingerprints the
host, command, and complete source revision before reusing results:

```bash
PYTHONPATH=src python benchmarks/bench_high_core_evidence.py \
  --workers 1,2,4,8,16,32,64,128 --columns 64 --memory-mib 2048 \
  --short-rows 20000 --sustained-rows 500000 \
  --warmups 1 --repeats 7 --numa-node 0 --resume \
  --short-dram-json short-dram.json \
  --sustained-dram-json sustained-dram.json \
  --output-dir high-core-evidence
```

Run the dimension matrix in fresh child processes. The `ci` profile is a small
cross-platform equivalence smoke; `standard` adds width, nesting, source count,
compression, and memory; `full` additionally exercises supported CPU-affinity
quotas:

```bash
PYTHONPATH=src python benchmarks/bench_threading_matrix.py \
  --profile standard --rows 60000 --warmups 1 --repeats 3 \
  --output threading-matrix.json
```

Measure complete remote pipelines against explicitly supplied local emulators.
The harness uploads deterministic sources, times remote-to-remote conversion,
downloads the outputs outside the timed region, and rejects logical Parquet
mismatches:

```bash
PYTHONPATH=src python benchmarks/bench_remote_providers.py \
  --s3-endpoint http://127.0.0.1:9000 \
  --gcs-endpoint http://127.0.0.1:4443 \
  --azure-connection-string 'UseDevelopmentStorage=true' \
  --rows 20000 --source-count 8 --warmups 1 --repeats 3
```

The native worker policy uses the smallest trustworthy capacity reported by the
host, process affinity, and Linux cgroup CPU quota. Therefore the CPU-quota
matrix validates the same automatic production policy without adding a public
worker-count knob.

Measure the bounded partition-source lookahead independently with a loopback
HTTP source whose latency is controlled by the harness:

```bash
PYTHONPATH=src python benchmarks/bench_partition_lookahead.py \
  --partitions 8 --rows-per-partition 50000 --delay-ms 75 \
  --memory-mib 256 --warmups 1 --repeats 3
```

The harness compares `single`, deliberately sequential `multi`, and static
`multi` with one-partition lookahead, and rejects any logical output
difference before reporting timings.

### Current concurrency model

Multi-threaded operations use one bounded native arena shared by inference,
materialization, Arrow handoff, and output. Worker counts are derived from CPU
affinity, cgroup capacity, and the public memory budget; there is no fixed
32-worker ceiling. Arenas up to 32 workers keep the compact bitset scheduler,
while wider arenas use summarized dynamic bitmaps and local-first NUMA stealing.
Worker admission reserves native runtime headroom that PMR allocations cannot
account for directly.

CSV and JSONL workers encode directly into operation-governed PMR buffers.
Their ordered window is bounded by bytes and packet count, so a slow early
packet cannot allow later fragments to consume an unbounded reorder window.
Each worker reuses a small private, budgeted block cache; first-touch placement
keeps that scratch local to its NUMA node. Actual-to-estimated expansion adjusts
later byte credits. A saturated row or high operation-memory pressure drains
parallel fragments and encodes serially. If a parallel allocation still fails,
the retained packet descriptors rebuild the unpublished window serially before
reporting that one packet cannot fit by itself.

Eligible fixed-width flat JSONL can use the complete arena for short,
moderate-cost schemas and a proportional half-arena policy for sustained work.
Variable-width, nested, ultra-wide, small, and memory-constrained inputs retain
conservative adaptive fractions. Those fractions continue scaling above 32
workers.

## [License](#index)

Apache License 2.0. See [LICENSE](LICENSE).
