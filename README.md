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
- [RESPONSIBILITIES.md](RESPONSIBILITIES.md) maps the Python and C++ source
  layout for contributors.
- [THREADING_TODO.md](THREADING_TODO.md) defines the deterministic single-thread
  and multi-thread architecture and its implementation checklist.

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

The Python object iteration and dictionary inspection remain GIL-bound. Multi
mode amortizes that boundary by consuming up to 4,096 rows per ABI3 call and
feeds the same bounded C++ inference, materialization, and output workers used
by file inputs. Native probes, source-to-sink execution, and CSV, JSONL, and
Parquet writers release the caller's GIL while waiting on the operation arena;
reader and Python-output callbacks acquire it only for the callback duration.
Generators are not converted to lists; their replay spool is bounded by the
single public memory budget. Single mode creates no helper thread and remains
the deterministic inline reference.

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
| `memory_limit_bytes` | `None` | The only public memory/resource control. `None` selects 512 MiB. The native extension derives all chunk, batch, coalescing, metadata, spool, concurrency, Arrow, and Parquet sub-budgets from this value. |

`multi_threading=False` forces every Schema-Sanitizer-owned worker count,
queue, reorder window, remote download/discovery window, and source prefetch
window to one. It does not create a project-owned thread pool, event-loop host,
or child process, and PyArrow fallback calls receive `use_threads=False`.
Remote discovery, DNS, metadata, download, and publication use blocking APIs on
the caller thread: stdlib HTTP, direct Botocore calls without a transfer
manager, the GCS JSON API with synchronous ADC, and the synchronous Azure Blob
SDK with `max_concurrency=1`. A remote call made from an active `asyncio` loop
therefore blocks that same caller thread instead of creating a helper thread.

`multi_threading=True` has no public worker-count setting. The immutable
execution policy derives an effective worker count, bounded queues, per-worker
arenas, and remote/PyArrow concurrency from `memory_limit_bytes`, available host
CPUs, and hard internal ceilings. A constrained multi run may safely fall back
to one effective worker; inspect `result.execution_policy` for the effective
values and fallback reason. The current implementation parallelizes bounded
remote discovery, transfer/staging, source prefetch, supported PyArrow
fallbacks, adaptive native inference, native materialization, native
CSV/JSONL fragment preparation, and native Parquet column preparation and
compression. One operation-wide native task arena owns the effective N-worker
budget for inference, materialization, and native sinks. Stages reuse those
physical workers instead of creating independent pools; narrow upstream and
output stages receive complementary stable lanes so they can overlap without
exceeding N active Schema-Sanitizer workers. Workers start lazily on first use,
so a cheap stage does not pay to construct idle helpers. Within a lane, idle
workers can steal compatible backlog from a busy physical queue; the thief uses
its own lane-relative parser/builder/compression slot, while ordinal commit keeps
rows, diagnostics, bytes, and failures deterministic. Each ordered stage owns a
local cancellation token layered over the shared arena token, so a sink or
materialization failure stops already-running work for that stage without
tearing down unrelated arena users. Arena packets are C++23
`std::move_only_function` tasks carrying a move-only completion lease: normal
execution disarms the lease after the executor's final locked access, while
queue destruction publishes abandonment. This prevents shutdown hangs and
executor use-after-free without one shared allocation per packet. The bounded
dispatch window is also the
result-retention bound, eliminating a redundant result-space wait and reducing
coordinator wakeups. Hardened allocation ownership remains enabled, but its
process and operation registries are address-sharded so concurrent workers do
not serialize every allocation/free pair on one mutex. Nested inference rows are converted in
worker-private parser state into compact preorder evidence packets. One ordered
reducer remains the sole owner of key interning, shape promotion, scalar
statistics, and diagnostics. Flat/scalar batches, small batches, and operations
whose worker pool is below the safe inference reserve stay on the serial
reference scanner, avoiding a pool-startup regression. Inference uses up to the operation's effective workers when plan complexity
and packet volume justify them, bounds evidence packets against worker arenas, and drains earlier
ordinals before scanning an oversized row inline. One lazy operation execution
context spans the complete public conversion:
initial listing, single-file or directory staging, probe/stream prefetch, and
final remote output upload share one event-loop host in `multi`. Compatible HTTP, S3, and Azure provider sessions are pooled for the complete
operation and close exactly once after submitted work drains. Provider creation
is single-flight per compatibility key while unrelated keys initialize
concurrently; coordinator startup and shutdown have bounded deadlines. Directory staging
also shares one global transfer semaphore on that operation host. Remote packets are bounded by both file count and
known bytes. A bounded probe prefix is reused by materialization instead of
being downloaded twice. Native materialization coalesces contiguous rows into
bounded packets, isolates oversized rows, prepares packets in worker-private
state, and commits rows through one ordered coordinator. CSV and JSONL output
use the same ordinal executor: workers encode immutable row ranges into private,
byte-accounted fragments while one writer owns the header, byte order,
statistics, earliest error, and final flush. The reorder window retains at most
one fragment per effective output worker. Stage-specific worker ceilings reuse
one shared budget-preserving policy helper; branch-heavy CSV encoding is capped
at four output workers when the host policy is wider, while upstream stages and
JSONL output can still use the complete operation arena. Local path outputs use sibling staging
files and atomic replacement, so a failed CSV, JSONL, native Parquet, or PyArrow
Parquet write cannot truncate a valid previous destination. Native Parquet
workers prepare independent leaf-column chunks and compression artifacts in
private memory; one coordinator assigns physical offsets, writes column chunks
and page indexes in schema order, and emits the footer/trailer once. Small,
narrow, low-memory, or cheap row groups remain on the serial writer. Remote
source packets and final remote-output spools additionally reserve bytes from
one operation-owned temporary-storage permit pool, so concurrent staging cannot
multiply disk usage beyond the derived spool ceiling.

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

Local paths, `file://`, `gs://`/`gcs://`, `s3://`, common Azure Blob/ABFS URIs,
and single-file HTTP(S) sources are supported. Install cloud clients with:

```bash
pip install 'schema-sanitizer[cloud]'
```

Remote inputs are staged into replayable local temporary files. `single` uses
strictly blocking same-thread HTTP/GCS/S3/Azure clients; `multi` uses bounded
provider-native async clients. File outputs are uploaded after conversion. In
`multi`, one lazy operation-owned event loop is reused from initial remote
listing through input staging and final output upload; compatible provider
sessions and connection pools live for that operation and close only after
cancellation has drained. Local-only and `single` calls do not create it. Remote
directory listing is bounded, deterministic, and non-recursive; generic HTTP
directory listing is not supported.

Remote concurrency, file prefetch, retries, packet lookahead, discovery workers,
one-partition pipeline source lookahead, packet file counts, packet byte targets,
temporary-storage permits, and replay-spool capacity are derived automatically
from the operation's `memory_limit_bytes`. They are not separate
API options and have no environment-variable overrides. Absolute internal
ceilings remain in place so direct internal callers cannot create unbounded
worker, queue, connection, or staging state. Known/estimated remote packet bytes
are reserved before prefetch; the reservation is resized to the exact staged
size and held until the packet is consumed or cancelled. Final remote output
holds an exact reservation through upload. Completed spools use bounded S3
multipart publication, GCS resumable sessions with committed-offset
reconciliation, and Azure SDK block uploads with operation-derived concurrency.
S3 parts may finish out of order but are completed in ordinal order; failures
drain workers and abort the multipart/session state before the local spool lease
is released. HTTP retains one ordered PUT as the portable fallback. Generic
HTTP GET, HEAD, and idempotent PUT operations use bounded transient retries
derived from `memory_limit_bytes`. Each GET attempt truncates its staging file,
and each PUT attempt reopens the completed spool from byte zero. Schema-Sanitizer
disables aiohttp's implicit connection replay for streamed PUT bodies because an
internal retry can otherwise resend an already-consumed file as an empty body.
Redirects are not followed for PUT publication; the destination must return a
final success status directly.

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

The general CI workflow also runs real-loopback HTTP provider fault injection
against the ABI3 wheel on Linux, Windows, macOS x86-64, and macOS arm64. It
covers truncated downloads, disconnects during publication, delayed
cancellation, bounded retry exhaustion, metadata retries, fatal staging cleanup,
SIGINT drain, and abrupt interpreter shutdown.

Native parser fuzzing shares the production JSON, CSV, XML, and Parquet entry
points. Clang builds may use libFuzzer; GCC, MSVC, and AppleClang platform gates
can use the deterministic standalone engine. Promoted crash inputs are replayed
first, followed by bounded mutation campaigns with explicit run count, seed, and
maximum input length. Linux executes those campaigns under ASan/UBSan and TSan;
Windows AMD64 executes them with MSVC ASan, and both macOS architectures use
AppleClang ASan/UBSan. The same platform jobs repeat the sanitized ordinal
executor probe. No fuzzer setting is a production API or environment-variable
configuration surface.

The dedicated CPython launcher links the matching TSan runtime before extension
modules load and compiles the sanitizer options into the executable, so the gate
does not depend on process-environment configuration. The runner verifies that
Python loaded the extension from the requested build directory and executes 64
native, fixed-clock public-path, and bounded partition-lookahead differential tests, with each domain in a
fresh sanitizer-first interpreter and CI shell step. A session-result marker is
written only after `pytest_sessionfinish`; the runner then allows normal teardown
a short grace period and terminates only a lingering non-instrumented interpreter
teardown. A timeout before the marker remains a hard failure. This avoids
cross-domain shutdown interactions with binary PyArrow while keeping failures
attributable to one stage. CPython and binary PyArrow wheels are not
TSan-instrumented, so the gate ignores races wholly owned by non-instrumented
modules while retaining checking for the extension, the native core, and the
bundled zlib used by Parquet GZIP output. Local development builds are also
filtered by their CMake sanitizer setting: ordinary Python skips TSan/ASan
extensions unless the matching runtime was linked before module loading.

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
  --workers 1,2,4,8,16,32 --rows 20000 --columns 64 \
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
  --workers 1,2,4,8,16,32 --columns 64 --memory-mib 512 \
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

Multi-threaded operations use one bounded native arena shared by inference, materialization, Arrow handoff, and output. Worker counts are derived from CPU affinity, cgroup capacity, and the public memory budget. Ordered commit, cancellation, backpressure, deterministic single-thread execution, and logical equivalence remain part of the common source-to-sink contract.

Eligible fixed-width flat JSONL uses the complete 32-worker arena for short moderate-cost schemas and a bounded half-arena policy for sustained work. Variable-width, nested, ultra-wide, small, and memory-constrained inputs retain conservative adaptive ceilings.

For architecture and ownership, see [RESPONSIBILITIES.md](RESPONSIBILITIES.md). For the active threading checklist and benchmark evidence, see [THREADING_TODO.md](THREADING_TODO.md).

## [License](#index)

Apache License 2.0. See [LICENSE](LICENSE).
