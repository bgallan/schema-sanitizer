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

For file outputs, `memory_limit_bytes` is the single public memory/resource
control, independent of input or output file size. Native parsing, inference,
materialization, writers, directory metadata, materialized input, remote control
responses, and transfer windows debit one operation-wide atomic ledger across
Python and C++. Temporary files use operation permits plus one process-wide
per-filesystem governor because their contents are not resident memory. Files
larger than the budget are streamed; if the operation cannot make progress
within its limits, it fails without
publishing a partial replacement. The ownership boundary is documented in
[Reader memory accounting](docs/reader-memory-accounting.md).

See also [concurrency and memory hardening](docs/concurrency-memory-hardening.md)
for cross-operation resident, disk, and remote-lifecycle guarantees.

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

A `SourceManifest` is a public immutable input for conversions that must consume
an already selected set of remote object versions without listing its prefix
again. Version one accepts GCS objects with a non-empty `generation`; downloads
request that exact generation with a matching precondition. The default
`input_mode` may be left unchanged because the manifest already defines the
multi-file selection:

```python
plans = ss.pipeline.plan_gcs_modified_time_windows(
    "gs://raw-bucket/events",
    start_date,
    end_date,
)
result = ss.to_polars(
    plans[0].source_manifest,
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
| `csv_escape_char` | `None` | Optional one-byte escape inside quoted fields. For exports that encode quotes as `\"`, pass `"\\"`. The strict default continues to accept RFC doubled quotes (`""`) only. |
| `csv_header_mode` | `"exact"` | Multi-source header policy. `exact` preserves current behavior; `union` builds immutable per-source projections, accepts reordered/additive headers, and null-fills missing fields. See `docs/csv-header-modes.md`. |
| `input_text_encoding` | `"utf-8"` | `utf-8`, `utf-16`, `utf-16-le`, `utf-16-be`, or `iso8859-1`. Not used for Parquet. |
| `xml_row_tag` | `None` | Stream each matching direct XML element as a row; `None` treats the document as one row. |
| `on_error` | `"emit_null_row"` | `stop`, `skip_row`, or `emit_null_row`. |
| `multi_threading` | `False` | `False` is the deterministic inline reference executor; `True` enables bounded concurrency derived from memory and CPUs. |
| `memory_limit_bytes` | `None` | The only public memory/resource control. `None` selects a safe share of currently available system/container memory. A positive integer creates one atomic operation ledger shared by Python discovery/staging resources and native readers, inference, materialization, writers, workers, Arrow, and Parquet paths. Stage-specific sub-budgets may reject earlier. See `docs/reader-memory-accounting.md` for the ownership boundary. |

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
- the resolved value is fixed once and derives every internal quota;
- all native files, substreams, stages, and workers belonging to the call share
  one governed native pool; and
- Python-owned materialized input, directory metadata, control responses, and
  transfer windows reserve from the same native atomic ledger before retention
  or I/O.

On Linux, automatic sizing also respects the remaining cgroup allowance. It
reserves 12.5–25% for the system and untracked allocations, then applies a
64 GiB ceiling.

The resident ledger covers Schema-Sanitizer-owned input chunks, conservative
leases for retained in-memory input, directory metadata, control bodies,
transfer windows, queues, reorder windows, inference, materialization, and
writers. These components cannot each spend the full limit independently.
Temporary-file contents are governed separately by operation permits plus a
process-wide per-filesystem reservation ceiling; interpreter, thread-stack,
opaque SDK-runtime, and post-transfer analytical-result memory is outside the
ledger as documented in `docs/reader-memory-accounting.md`.

Concurrent public calls retain their own operation limit, while every scalable
native allocation and Python-owned retained reservation also passes through one
exact process-wide resident pool. FIFO control-plane admission adapts to
operation size and contention. Files inside one directory conversion still
share one lease and pool instead of reserving the full budget again.

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

#### Reader resource diagnostics

Every completed conversion exposes privacy-safe resource counters in
`Result.stats`. The counters are aggregated across inference, materialization,
files, and workers without including field names or input values:

- `current_charged_memory_bytes`, `peak_charged_memory_bytes`, and
  `operation_memory_limit_bytes` describe the tracked operation pool;
- `parser_max_depth`, `decoded_bytes`, `reader_records`, and `reader_nodes`
  describe work observed by the active reader;
- `compressed_bytes`, `decompressed_bytes`, and `decompression_ratio` describe
  Parquet footer totals when available; and
- `cancellations` plus `cancellation_reason` report stable reason codes such as
  `consumer_close` or `interrupt`, never payload text.

The current charged value is zero after a completed or closed operation. Peak
charged memory can legitimately differ between serial and parallel plans; the
semantic counters remain plan-independent. A zero parser depth or node count
means that the selected frontend does not expose that structural metric, not
that input validation was skipped.

#### Reader hardening performance gate

`benchmarks/bench_reader_hardening_ab.py` compares two isolated Release trees
on valid CSV, JSONL, XML, and Parquet fixtures. The reviewed envelope in
`benchmarks/reader_hardening_performance_budget.json` is enforced against the
recorded matched-build evidence. The envelope treats the cost of strict UTF-8,
syntax, resource, and allocator validation as an explicit security tradeoff; a
future change that exceeds it requires a new benchmark, explanation, and review
rather than silently moving the baseline. The complementary
[`reader complexity contract`](docs/reader-complexity.md) defines the accepted
linear-work model and its cross-platform scaling smoke.

#### Fixed safety limits

The native extension is the source of truth for all derived limits. There are
no environment-variable overrides or secondary public memory controls.

Structural ceilings such as schema depth, field cardinality, Arrow logical
ranges, and Parquet row-group count cannot be raised by callers. Scratch cleanup
and hardened allocation bookkeeping remain enabled.

Best-effort memory overwriting cannot guarantee physical erasure on
copy-on-write filesystems, SSDs with wear levelling, or after a third-party
Arrow consumer has copied the data.

#### Reader strictness and XML subset

The complete trust model and immutable reader ceilings are documented in
[`SECURITY.md`](SECURITY.md) and
[`docs/reader-security-limits.md`](docs/reader-security-limits.md).

Native readers treat source bytes, offsets, lengths, nesting, and decoded sizes
as untrusted. Malformed records fail with a structured public exception. Existing exception
classes remain stable, while `exc.detail` carries privacy-safe fields such as
`format`, `source`, `stage`, `byte_offset`, structural indices, and applicable
limit/observed values when available; input payload values are not echoed. Safety
limits are checked before configurable projection limits such as
`arrow_max_depth`.

XML accepts well-formed UTF-8 in the supported XML 1.0 character repertoire.
Element and attribute names use the normal ASCII XML name characters; validated
non-ASCII UTF-8 bytes are also accepted in names. Comments, CDATA sections, and
processing instructions are supported. An XML declaration is accepted only at
the start of a document. The parser deliberately does not support `DOCTYPE`,
general `ENTITY` declarations, external entities, XInclude, or any other
network or filesystem resolution.

Only the five predefined XML entities and valid numeric character references
are decoded. Unknown, incomplete, malformed, surrogate, out-of-range, NUL, and
XML-forbidden character references are rejected in both element text and
attributes. Document and `xml_row_tag` modes share the same name, entity,
UTF-8, and markup validation.

XML has non-configurable parser safeguards of 512 nested elements, 1,000,000
nodes, 4,096 attributes on one element, 1,000,000 total attributes, and 512 MiB
of decoded text. These are denial-of-service ceilings, not promises that an
input of that size fits a smaller `memory_limit_bytes`; the operation-wide
budget always takes precedence. XML tree/model containers and all parallel XML
workers allocate from the same operation pool. Duplicate raw document bytes are
released once analytical execution no longer needs them, and streamed row trees
are released immediately after the ordered consumer commits each materialized
row rather than being retained until the frontend batch is destroyed.

CSV parsing is intentionally strict; implicit repair and lenient ingestion are
not supported. Quoted fields must terminate, doubled quotes
are decoded, quotes cannot appear inside an unquoted field, and only spaces,
tabs, a delimiter, or record end may follow a closing quote. Embedded newlines,
empty final fields, BOM handling, and configured single-byte delimiters remain
supported. CSV record and decoded-field allocations are bounded by the shared
operation budget, with a fixed ceiling of 65,536 cells per record.

Duplicate non-empty CSV header names and distinct names that collide after the
configured name-reconciliation policy are rejected before object
materialization. Every record is validated as UTF-8 even when a direct or
multi-threaded path would otherwise avoid decoding individual cells.

JSON document, JSON array, and JSON Lines readers share strict lexical
validation. Invalid UTF-8, escapes, surrogate pairs, numbers, trailing content,
and malformed values in projected-out fields are rejected on optimized,
deferred, and worker-authoritative paths. JSON nesting is capped at 512 and an
individual object is capped at 65,536 fields. These parser ceilings are fatal
operation errors rather than recoverable `skip_row` or `emit_null_row` events;
ordinary malformed JSON Lines rows still follow the selected `on_error` policy
with source offsets preserved.

Native Parquet input keeps immutable format ceilings while deriving stricter
effective footer, metadata, page, decompression, row-group, and reader-buffer
limits from `memory_limit_bytes`; the lower limit always wins. Footer size is
checked before the footer is read or decoded, and page/column ranges are
validated for negative, overflowing, backward, out-of-file, and footer-overlap
conditions before seeking, allocation, or decompression. Corrupt payloads for
compiled uncompressed, Snappy, and GZIP paths fail closed. Page CRC32 checksums
are validated when present before decompression or value decoding. Parallel
column decoders reserve scratch and estimated output from one shared atomic
operation coordinator rather than treating the limit as worker-local.

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

## [Flat-prefix modified-time CSV ingestion](#index)

For CSV objects stored under a flat GCS prefix, modified-time planning can list
the prefix once, divide the immutable `(uri, generation)` snapshot into
half-open UTC days, reconcile each day with `csv_header_mode="union"`, and
publish one validated Parquet object per non-empty day. The default exact CSV
mode and all existing path, URI, directory, and partition inputs are unchanged.

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

Known or estimated packet bytes are reserved before prefetch. The reservation
is corrected to the exact staged size and retained until consumption or
cancellation. Final remote output keeps its exact reservation until upload
finishes.

Memory-derived windows and the available work count prevent direct callers from
creating unbounded workers, queues, connections, or staging state.

Remote-directory sessions retain only the first file needed to classify the
provider and reuse each bounded chunk sequence directly. They do not duplicate
the complete manifest or allocate a second list of file references per chunk.
Process-wide endpoint throttling also uses a bounded registry that evicts only
closed-circuit idle state, and provider single-flight locks exist only while
creators or waiters are active.

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
  build/tsan ./python-tsan 2 \
  "$(python -c 'import site; print(site.getsitepackages()[0])')"
```

With no final test path, the runner checks the standalone executor and every
full-extension threading domain. Pass one test path as a final argument for a
focused local run.

The general CI workflow has a small set of responsibility-based lanes. Each
platform task builds its ABI3 wheel once and reuses it for the core-only import,
full suite, Parquet certificate, HTTP fault injection, threading benchmark, and
Python 3.11/3.14 ABI boundary checks. Release packaging is validated once after
all four platform wheels and the source distribution are available.

Real-loopback HTTP fault injection runs on Linux, Windows, macOS x86-64, and
macOS arm64. It covers:

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
account for directly. Remote submissions retain their process-wide admission and
staging owners until the underlying event-loop Task reaches its real terminal
`finally`; cancellation of a bridge Future is only a notification. Native arena
shutdown similarly destroys queued closures before detaching a task that ignores
cooperative stop, so one late task cannot retain the complete queued workload.

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

The process-wide retry scheduler treats callback ownership as one transaction. A
keyed replacement becomes visible only after it has been admitted; rejection
therefore leaves the old pending or ready callback intact. Item and retained-byte
charges follow work through pending, ready, and active states, and ready queues
are serviced round-robin by subsystem. Captures removed by cancellation,
replacement, or compaction are destroyed outside scheduler locks. Terminal
resource releases that have no caller able to retry are transferred to one
bounded release guardian rather than to subsystem-specific orphan threads.

Filesystem claims use bounded live-owner admission and process-instance
authority. A foreign-owned legacy root in a shared temporary directory is
replaced by an ownership-verified effective-UID namespace, while a securely
owned legacy root remains usable during rolling upgrades. A child created by
`fork()` may close its copied descriptor and return its logical FD lease, but it
cannot remove the parent's xattr or external claim.
External coordination sweeps count every directory entry against their work
budget and retain one governed `scandir` cursor, so unrelated files cannot turn a
bounded sweep into an unbounded scan. Claim publication rolls back a linked
record if the directory durability sync fails; interrupted private write and
delete records are recovered incrementally.

Remote host shutdown is accepted even during the interval between thread start
and event-loop `run_forever()`. Terminal callback work is drained while the host
thread is still alive, and recursive asyncio cancellation uses bounded backoff.
Explicit lifecycle methods still surface their first cleanup error to the caller;
only terminal paths with no remaining caller transfer the owner to the bounded
guardian.

### Pass38 lifecycle guarantees

Keyed retries are single-flight and cancellation is linearized against the exact
`CLAIMED -> RUNNING` transition. An active key can retain one coalesced
successor, while generation state is pruned after the last owner disappears.
Cleanup callbacks use subsystem-aware Deficit Round Robin, and failed worker
permits remain bounded, retryable owners rather than being truncated.

Crash-left claim publication aliases are recovered only when they match the
canonical record and inode. Claim publication syncs both the canonical link and
the subsequent removal of its private alias. Fork-child reset callbacks never
call inherited owners; the supported policy remains `spawn`, `forkserver`, or
`fork()+exec`. Native arena admission rejects oversized inline charges before
unsigned arithmetic and enforces reaper state/byte ceilings. Internal callers
may use `shutdown_concurrency_runtime()` for dependency-ordered bounded shutdown.

## [License](#index)

Apache License 2.0. See [LICENSE](LICENSE).

### Pass39 quiescence and immutable lease guarantees

Process-owned helper services now register in a weak generation-safe lifecycle
registry. The structured shutdown closes remote coordinators, async bridges, and
partition lookahead producers before the scheduler, janitor, cleanup dispatcher,
and release guardian, using one monotonic deadline. A service is not considered
quiescent merely because admission is closed: its own shutdown result also
accounts for live host threads, failed permit owners, pending callbacks, and
parked cleanup.

Thread and file-descriptor leases release through an internal lease-ID ledger;
the exposed amount is read-only and is not trusted for accounting. Retry keys
containing custom objects use stable identity rather than user hashing, cleanup
callbacks survive transient exceptions and close attempts, stale janitor scans
hold governed descriptors, and initialized runtime entry points reject use in a
post-`fork()` child until `exec()`.

`concurrency_debug_snapshot()` retains its original v1 schema.
`concurrency_runtime_debug_snapshot()` provides the additive integral v2 view of
scheduler, guardian, dispatcher, janitor, process governors, registered helpers,
and fork-poison state.

### Pass40 capability-ledger and terminal-shutdown guarantees

Thread and file-descriptor governors now accept a release only from the exact
capability-bearing lease recorded in their private ledger. Amount-only releases,
mutated authority fields, replayed leases, and cross-governor substitutions
cannot manufacture capacity. Bounded one-shot availability notifications run on
a governed notifier instead of synchronously on the thread returning capacity.

Retry keys are tagged by exact primitive type and bounded before publication;
custom objects use identity without invoking user hashing or equality under
scheduler locks. Cleanup work is physically separated into runnable, delayed,
dead-letter, and parked domains, while hostile exception formatting cannot kill
the dispatcher or release guardian.

Threaded services reserve registry authority before they start. Process shutdown
is terminal, phased, and single-flight, and includes remote/async/lookahead
hosts, janitor, dispatcher, retries, guardian, availability notifier, emergency
budgets, and the native joinable cleanup reaper under one monotonic deadline.
`concurrency_runtime_debug_snapshot()` now returns the additive integral v3
view; the compatibility v1 snapshot is unchanged.

## Pass41: teardown reserves and verified quiescence

Pass41 separates public admission from the resources needed to finish a clean
shutdown. New user work is rejected first, while a teardown-only thread and FD
reserve remains available to janitor, dispatcher, retry, guardian, notifier, and
remote cleanup. The reserve closes only after those consumers have either
released their owners or reported bounded terminal retention.

Availability callbacks are now transactionally delivered: a governor keeps a
one-shot subscription until the bounded notifier accepts it, and notifier start
failure schedules its own retry. Exact ledger release is the commit point for a
process-resource lease; diagnostic or notification failure after that commit
cannot make an already returned permit appear live again.

The quarantine namespace remains fixed by an open descriptor and production
operations are relative to that descriptor. The integral runtime snapshot is
version 4 and includes Python services plus native arenas, detached workers,
reaper queues, reservations, parking, and invariant underflows. Native arena
construction uses an RAII teardown-reservation guard, and both explicit and
`atexit` reaper shutdown are bounded.

## Pass42: sealed wakeups and terminal-owner integrity

Availability notification is now a closed internal protocol rather than a
callback executor. Governors publish sealed, generation-tagged wakeup events for
retry, cleanup, or janitor work; mutable function metadata cannot authorize code
for the privileged notifier. Delivery is acknowledged only after successful
execution, transient failures are retried within bounded state, and a stopped
notifier cannot be restarted by a late resource release.

Failed scheduler leases and quarantine-root handles transfer ownership one item
at a time and remain retained until release or guardian adoption is confirmed.
The guardian never tries to release its own bootstrap permits through itself.
Runtime registry and terminal-host state are bounded by circuit breakers, cleanup
fairness uses explicit subsystem tokens, and inherited post-fork graphs share one
bounded process capsule.

`concurrency_runtime_debug_snapshot()` advances additively to version 5. It
reports the sealed notifier lifecycle, registry saturation, unified fork
capsule, terminal remote hosts, and the sixteen-field native arena/reaper state.
Native reaper lanes reserve capacity before startup, use bounded teardown thread
permits, promote parked owners when capacity returns, and cannot report shutdown
success while arenas, detached workers, reservations, parking, or reaper workers
remain.
