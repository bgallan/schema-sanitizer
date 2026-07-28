# schema-sanitizer

`schema-sanitizer` turns inconsistent CSV, JSON, JSON arrays, JSON Lines,
NDJSON, XML, and Parquet data into stable analytical tables or cleaned files.
A native C++23 engine performs schema inference, reconciliation, bounded
streaming, and Arrow C Data materialization; the Python API provides file,
dataframe, partitioned pipeline, and BigQuery integration helpers.

Version 0.3.73 is still alpha software, with particular focus on Parquet files
used by BigQuery external tables.

The integral concurrency contract covers every supported input and output.

v120 compacts each queued task's validated lane bounds from two machine words to two bytes under the existing 32-worker ceiling. On the validated libstdc++ ABI the packet shrinks from 72 to 56 bytes, improving deque density for enqueue, local dequeue, and compatible stealing while all 56 input/output routes retain identical lane semantics.

v119 replaces the high-core stealing path's repeated visibility-shard geometry scan with fixed 2-4 physical-domain loads followed by the same initialized-worker mask. The 1-8-worker path remains unchanged, and all 56 input/output routes inherit the common arena optimization.

v118 normalizes each submission ticket exactly once per lane and reuses that origin for startup reservation, saturated placement, alternative selection, and helper selection. Overflow-sensitive advancement preserves the historical unsigned wrap semantics; all 56 input/output routes inherit the shared arena optimization.

v117 isolates each worker wake generation from the following queue control
block by aligning both sides of the boundary. The release/acquire wake protocol,
notifications, queue semantics, and public API are unchanged; all 56
input/output routes inherit the bounded common-arena layout.

v116 places the upstream, output, and all-lane submission cursors on separate
cache-line boundaries and isolates exact worker activity from producer ticket
traffic. Atomic operations and scheduling semantics are unchanged; all 56
input/output routes inherit the bounded common-arena layout.

v113 splits high-core queue visibility into cache-line-aligned groups of eight
physical workers. Empty-to-nonempty and final-drain transitions in disjoint
upstream/output lanes no longer exchange ownership of one operation-global
mask, while lane-local admission reads only the shards intersecting its allowed
workers. Arenas with 1-8 workers retain a single visibility shard. All 56
input/output routes inherit the common arena stage without changing ordering,
cancellation, stealing, backpressure, drain, or public APIs.

v112 removes redundant atomic snapshots from the initialized-worker park/wake
path. The one-shot startup flag is reloaded only while it can still be true,
local work discovered under the queue mutex no longer resamples the wake
generation, and the condition-variable predicate captures the generation that
satisfied a targeted wake instead of loading it again after return. All 56
input/output routes inherit the common arena stage without changing ordering,
cancellation, backpressure, drain, or public APIs.

v111 moves worker-active-streak telemetry from one operation-global atomic
read-modify-write to the existing cache-line-aligned shard owned by each
physical worker. Final telemetry sums the bounded snapshots under the unchanged
key, while live activity, peak activity, wake behavior, and scheduling semantics
remain exact. All 56 input/output routes inherit the common arena stage.

v110 adds a worker-local monotonic cache for the operation-wide peak-active
worker diagnostic. Each physical worker offers a newly observed active count to
the shared maximum only when it exceeds every count that worker has already
observed. Steady-state activity streaks therefore avoid a shared peak-counter
load while exact live activity, the exact global maximum, wake coalescing, and
all scheduling semantics remain unchanged. All 56 input/output routes inherit
the common arena stage.

v109 extends the existing single-writer completion-telemetry shards to every
multi-worker arena from 2 through 32 workers. Mid-core workers no longer
publish every task into shared operation-global atomics, and high-core workers
retain 32-task batching while flushing to private aligned shards rather than
shared counters. The strict inline path and all output semantics remain
unchanged; all 56 input/output routes inherit the common arena stage.

v108 shards task-submission telemetry by physical arena queue. Producers reuse
the queue mutex that already serializes admission, update plain per-kind totals,
and publish lock-free snapshots with atomic stores instead of contending on one
operation-global `fetch_add` and maximum-CAS cache line. Admission shards remain
separate from worker completion shards to avoid producer/worker false sharing.
All 56 input/output routes inherit the common arena stage when telemetry is
enabled; strict single-thread execution remains unchanged.

v105 tightens synchronization without changing queues, allocations, worker
selection, or cache layout. Internal `in_flight_` decisions already execute
under the executor mutex and now use relaxed snapshots, while the public
lock-free snapshot remains acquire/release. Arena slot claiming keeps the
acquire needed for prior-generation lifetime but drops an unnecessary release
half; the post-mutex ready-state revalidation is relaxed after the earlier
ready acquire. A completely unread `completed_count_` field and its writes are
removed. All 56 input/output routes inherit the common executor path.

v104 specializes high-core ordered admission bookkeeping. Above eight arena
workers, the executor mutex is already the sole writer authority for
`in_flight_`, so submission publishes the increment with a relaxed load plus a
release store instead of a locked atomic RMW. One-through-eight-worker paths
remain the v103 implementation. All 56 input/output routes inherit the stage
when configured for high-core execution.

v99 reuses one monotonic `initialized_mask` snapshot throughout arena
admission. Because initialized workers are necessarily started and admitted,
the snapshot selects idle workers, proves when no reservation is possible, and
skips a redundant startup check. Stale snapshots remain conservative. All 56
input/output routes inherit the reduction without new state or memory.

v98 compiles separate shutdown checks for low-core and parallel arena workers.
Two- and three-worker stages retain the historical operation-global check, while
four-through-thirty-two-worker loops use the worker-owned jthread stop token
already passed to every packet. This removes one shared acquire load per packet
without adding a runtime branch; all 56 input/output routes inherit the path.

v97 suppresses external-completion notifications during normal execution.
Each completion shard encodes its exact count and a shutdown-waiter bit in one
atomic value, so the existing completion RMW either precedes the drain snapshot
or observes the waiter and performs the wake. All 56 input/output routes avoid
futile `notify_all` calls while preserving abandonment and destruction safety.

v96 makes `started_mask` the sole worker-start authority. Once a stage with at
least four effective workers observes a release-published started bit, packet
admission bypasses the worker's `start_mutex` instead of rechecking an already
installed `std::jthread`. Exact started-worker diagnostics use bounded
`std::popcount`, removing the duplicate global startup counter. All 56
input/output routes inherit the startup and steady-state admission reduction.

v88 removes the preliminary executor-mutex acquisition from arena-backed
ordered consumption. Cancellation, fatal publication, and end-of-stream are
already published directly into each completion slot, while `take_mutex_`
owns the single-consumer ring cursor. The coordinator therefore waits on the
slot first and acquires the executor mutex only once, when it authoritatively
consumes the result or observes a terminal state. All 56 input/output routes
inherit the shorter commit path; the local worker-pool fallback remains
unchanged.

v87 assigns each admitted ordinal one bounded completion-ring slot and carries
that index through execution. Publication and canonical consumption reuse the
slot instead of repeatedly evaluating `ordinal % reorder_capacity`, so all 56
input/output routes remove runtime division from their ordered commit path
without changing packet layouts, dispatch limits, order, or memory ownership.

v86 compiles one immutable arena-submission plan per ordered stage. The plan
retains the normalized worker width, physical lane bounds, compatible-worker
mask, lane cursor, and alternative placement offset, so every packet avoids
recomputing stage geometry. All 56 input/output routes reuse the plan for input,
inference, materialization, Arrow handoff, and output tasks.

v85 removes the remaining operation-global stolen-task counter from the
stealing hot path. Each physical arena worker is the sole writer of its own
atomic publication slot, and performance telemetry uses the same worker-local
shard. Statistics sum those slots only when requested, so every one of the 56
input/output routes avoids a shared RMW whenever compatible workers rebalance a
skewed queue.

v84 publishes shared queue visibility only on empty-to-nonempty transitions
and publishes worker initialization only after the reserved worker acquires its
first real local task. The reservation state is cached across the task hot loop
and refreshed at park/wake boundaries, so all 56 input/output pairs avoid
steady-state writes to the global queue and initialization masks.

v83 samples worker wake generations only at real park/wake boundaries and
elides queue/run clock reads when an operation arena has no telemetry consumer.
The optimized worker loop is shared by all 56 input/output pairs; operations
that retain performance telemetry keep their exact timing metrics.

v82 replaces the operation-global scheduler wake epoch with cache-line-aligned
per-worker generations and suppresses notifications when a running target is
already guaranteed to drain the appended task. The shared arena therefore
reduces producer/consumer cache-line transfers for all 56 input/output pairs
without changing lanes, queues, workers, ordering, or memory controls.

v81 keeps a worker active while it drains an immediately available task
burst. The arena pays active/peak transitions once per busy streak instead of
once per packet, rechecks its local queue before parking, and exposes
`worker_active_streaks` for runtime verification.

v80 removes low-core task-telemetry cache contention for every supported
input/output pair through fixed worker-local shards. v79 gives wide
fixed-dominant CSV output an O(1)-plus-bounded-tail packet
planner and an adaptive 4/8/16 output-worker ceiling on high-core operation
arenas. The normal public metadata tail remains eligible, so all eight inputs
benefit when targeting CSV. Polars now preserves incoming Arrow batches with
`rechunk=False` when supported, while retaining a compatibility fallback.

v78 carries the parallel Arrow stream through output-specific analytical
handoffs. Polars consumes record batches directly, DuckDB binds a chunk-preserving
Arrow dataset, pandas receives the reader with its requested threading policy,
and PyArrow retains the shortest direct table route. Every one of the 56
input/output pairs records its exact terminal handoff and table boundary.

v77 gives every one of the 56 supported input/output pairs an explicit
source-to-sink concurrency contract. Python sequences and generators now share
one progressive replay stream: each Python row is encoded once, `seek(0)` no
longer drains a generator, and final native execution overlaps with continued
source production.

v76 makes pure-Python row iterables a first-class eighth input. Lists, tuples,
and one-shot generators of dictionaries now reach every analytical and native
file output through the same operation arena. Sequence rows are encoded in native
batches; generators are consumed directly by a bounded native iterator encoder,
so the GIL-bound object walk is amortized without adding a Python producer thread.

v75 removes the remaining duplicate full JSON syntax walk for worker-authoritative
`json` and `json_array` inputs. The coordinator now performs bounded structural
framing, while operation-arena workers retain the only authoritative parse; deep
or suspicious values fall back to the canonical scanner. It also fixes primitive
JSON values split exactly at an input-chunk boundary.

v74 also accelerates the remaining ordered CSV record-framing boundary with an
adaptive vector/scalar scanner, so wide and quote-heavy CSV inputs feed every
native output faster while preserving ordered framing, exact values, and the
single public memory budget. Small or indivisible workloads keep an adaptive
inline fallback when task creation would be slower.

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
- [CONCURRENCY_SCALING_V96.md](CONCURRENCY_SCALING_V96.md) makes the
  release-published worker-start mask authoritative, elides repeated startup
  mutex checks for sustained 4+ worker lanes, and removes the duplicate started
  counter across all 56 source-to-sink routes.
- [CONCURRENCY_SCALING_V83.md](CONCURRENCY_SCALING_V83.md) moves wake-epoch
  sampling to real park/wake boundaries and removes discarded clock reads from
  telemetry-free arena execution.
- [CONCURRENCY_SCALING_V82.md](CONCURRENCY_SCALING_V82.md) replaces the
  operation-global wake epoch with targeted per-worker generations and coalesces
  signals for workers that are already draining compatible work.
- [CONCURRENCY_SCALING_V81.md](CONCURRENCY_SCALING_V81.md) amortizes active and
  peak accounting over complete worker busy streaks and closes the pre-park
  local-queue race.
- [CONCURRENCY_SCALING_V79.md](CONCURRENCY_SCALING_V79.md) removes wide CSV
  row-by-column packet-planning work, enables adaptive 4/8/16 high-core output
  workers, and preserves Arrow batch chunks during Polars conversion.
- [CONCURRENCY_SCALING_V78.md](CONCURRENCY_SCALING_V78.md) extends the native
  Arrow stream into output-specific PyArrow, pandas, Polars, and DuckDB handoffs
  and records the exact terminal barrier for all 56 source-to-sink pairs.
- [CONCURRENCY_SCALING_V77.md](CONCURRENCY_SCALING_V77.md) removes Python-row
  re-encoding and replay-drain barriers, then publishes the complete 8 x 7
  source-to-sink concurrency matrix.
- [CONCURRENCY_SCALING_V76.md](CONCURRENCY_SCALING_V76.md) promotes pure-Python
  sequences and generators to a first-class concurrent input, batches iterator
  encoding in ABI3, and removes the duplicate Python `json.dump` preflight.
- [CONCURRENCY_SCALING_V75.md](CONCURRENCY_SCALING_V75.md) removes duplicate full
  JSON parsing from worker-authoritative arrays and fixes scalar chunk boundaries.
- [CONCURRENCY_SCALING_V74.md](CONCURRENCY_SCALING_V74.md) accelerates
  ordered quote-aware CSV framing with adaptive vector/scalar scanning and
  verifies that the faster input path benefits CSV, JSONL, and Parquet sinks.
- [CONCURRENCY_SCALING_V73.md](CONCURRENCY_SCALING_V73.md) overlaps independent
  native Parquet row groups while preserving ordered physical page indexes.
- [CONCURRENCY_SCALING_V72.md](CONCURRENCY_SCALING_V72.md) removes the
  coordinator/worker JSON array double parse, builds eligible flat arrays
  directly into worker-local Arrow packets, and propagates the public threading
  policy into pandas conversion.
- [CONCURRENCY_SCALING_V71.md](CONCURRENCY_SCALING_V71.md) makes concurrent
  participation explicit for every supported input and output and integrates
  CSV/XML frontend work into the common operation arena.
- [CONCURRENCY_SCALING_V70.md](CONCURRENCY_SCALING_V70.md) compacts generic
  evidence indices and selects trusted statistics reduction only at sufficient
  concurrency.
- [CONCURRENCY_SCALING_V69.md](CONCURRENCY_SCALING_V69.md) interns generic
  nested inference keys once per packet, shrinks evidence nodes, and caches the
  ordered reducer's global string identifiers.
- [CONCURRENCY_SCALING_V68.md](CONCURRENCY_SCALING_V68.md) renders binary,
  temporal, duration, and decimal CSV cells directly through shared canonical
  native formatters.
- [CONCURRENCY_SCALING_V67.md](CONCURRENCY_SCALING_V67.md) removes primitive
  CSV cells' JSON round trip and retunes packet sizing for stage interleaving.
- [CONCURRENCY_SCALING_V66.md](CONCURRENCY_SCALING_V66.md) groups only
  sustained fixed-cost native Parquet columns into bounded ordered worker ranges,
  reducing per-window coordination while variable and repeated columns retain
  dynamic v65 scheduling.
- [CONCURRENCY_SCALING_V65.md](CONCURRENCY_SCALING_V65.md) keeps private native
  Parquet worker file handles and decode scratch alive across bounded windows of
  one stream, releasing scratch at row-group boundaries.
- [CONCURRENCY_SCALING_V64.md](CONCURRENCY_SCALING_V64.md) audits every
  supported format and integrates native Parquet column decode into the shared
  operation arena with bounded scratch and ordered commit.
- [CONCURRENCY_SCALING_V63.md](CONCURRENCY_SCALING_V63.md) records
  cross-batch wide variable-width JSONL output admission without additional
  workers or reorder capacity.
- [CONCURRENCY_SCALING_V62.md](CONCURRENCY_SCALING_V62.md) records
  bounded reclamation of reorder-window bytes for wide variable-width JSONL
  packets, halving output task count without increasing the memory window.
- [CONCURRENCY_SCALING_V61.md](CONCURRENCY_SCALING_V61.md) records
  run-based JSON string escaping, copying ordinary UTF-8 spans in one append
  while preserving exact exceptional-byte encoding and bounded output memory.
- [CONCURRENCY_SCALING_V60.md](CONCURRENCY_SCALING_V60.md) records
  allocation-free pair-digit JSONL integer formatting, reducing dominant
  output-worker conversion cost without changing packet scheduling or memory.
- [CONCURRENCY_SCALING_V59.md](CONCURRENCY_SCALING_V59.md) records
  direct lexical scalar materialization for validation-certified JSONL rows,
  bypassing generic value parsing only when token and compiled type match.
- [CONCURRENCY_SCALING_V58.md](CONCURRENCY_SCALING_V58.md) records
  validation-certified positional JSONL materialization, removing repeated key
  decoding, hashing, field snapshots, and plan lookup from exact-order rows.
- [CONCURRENCY_SCALING_V57.md](CONCURRENCY_SCALING_V57.md) records the
  single-pass flat JSONL inference visitor, removing discarded key hashes,
  duplicate primitive scans, and unnecessary integer materialization.
- [CONCURRENCY_SCALING_V56.md](CONCURRENCY_SCALING_V56.md) records compact
  scalar-category dispatch in worker-private inference, replacing repeated
  predicates with one exact tag switch while preserving nested fallback.
- [CONCURRENCY_SCALING_V55.md](CONCURRENCY_SCALING_V55.md) records bounded
  wide flat JSONL inference aggregation, keeping 17-512 scalar fields on a
  tracked packet-local fast path instead of rebuilding generic evidence.
- [CONCURRENCY_SCALING_V54.md](CONCURRENCY_SCALING_V54.md) records bounded
  four-to-five-worker output progress and one-bypass FIFO fairness without
  changing the established high-core path.
- [CONCURRENCY_SCALING_V53.md](CONCURRENCY_SCALING_V53.md) records prepared
  constant-cost packet planning for non-null wide fixed JSONL batches, removing
  an O(rows x columns) coordinator pass without changing packet boundaries.
- [CONCURRENCY_SCALING_V52.md](CONCURRENCY_SCALING_V52.md) records shared
  zero-copy `RowRef` packet ranges, removing per-packet coordinator allocations
  and copies while preserving source ownership and bounded memory.
- [CONCURRENCY_SCALING_V51.md](CONCURRENCY_SCALING_V51.md) records use of the
  complete already-budgeted reorder window for high-core cross-batch dispatch,
  reducing coordinator starvation without increasing the memory model.
- [CONCURRENCY_SCALING_V50.md](CONCURRENCY_SCALING_V50.md) records high-core
  worker-start, empty-candidate, and wake fast paths that remove redundant
  coordinator locks, slot scans, and impossible notifications after eight
  workers.
- [CONCURRENCY_SCALING_V49.md](CONCURRENCY_SCALING_V49.md) records bounded
  worker-local telemetry batching above eight workers, preserving exact final
  counters while reducing shared cache-line updates.
- [CONCURRENCY_SCALING_V48.md](CONCURRENCY_SCALING_V48.md) records the high-core
  authoritative submit reservation, rejected pipeline-fusion experiments,
  paired scheduler evidence, and the required physical sixteen-CPU validation.
- [CONCURRENCY_SCALING_V47.md](CONCURRENCY_SCALING_V47.md) records distributed
  arena completion publication, rejected scheduler experiments, bounded slot
  ownership, local scaling evidence, and the required sixteen-CPU validation.
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
    threading_mode="multi",
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
[heuristics.md](heuristics.md#generated-etl-fields).

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
| `threading_mode` | `"single"` | `single` is the deterministic inline reference executor; `multi` enables bounded concurrency derived from memory and CPUs. |
| `memory_limit_bytes` | `None` | The only public memory/resource control. `None` selects 512 MiB. The native extension derives all chunk, batch, coalescing, metadata, spool, concurrency, Arrow, and Parquet sub-budgets from this value. |

`threading_mode="single"` forces every Schema-Sanitizer-owned worker count,
queue, reorder window, remote download/discovery window, and source prefetch
window to one. It does not create a project-owned thread pool, event-loop host,
or child process, and PyArrow fallback calls receive `use_threads=False`.
Remote discovery, DNS, metadata, download, and publication use blocking APIs on
the caller thread: stdlib HTTP, direct Botocore calls without a transfer
manager, the GCS JSON API with synchronous ADC, and the synchronous Azure Blob
SDK with `max_concurrency=1`. A remote call made from an active `asyncio` loop
therefore blocks that same caller thread instead of creating a helper thread.

`threading_mode="multi"` has no public worker-count setting. The immutable
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
[heuristics.md](heuristics.md#bigquery-registry-sidecar).

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
the harness may report `dram_bandwidth_saturation`. See
`CONCURRENCY_SCALING_V33.md` for the report contract and sidecar format.

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

See `CONCURRENCY_SCALING_V35.md` for stability thresholds, profile resumption,
and the cross-profile decision contract. `CONCURRENCY_SCALING_V36.md` documents
the lazy high-width worker state and in-place JSON emission.
`CONCURRENCY_SCALING_V37.md` makes the dedicated JSONL fan-out reachable from
the public API, aligns single/multi plan-ordered semantics and logical batch
diagnostics, and adds cost-balanced critical-path-first group submission.
`CONCURRENCY_SCALING_V38.md` replaces per-worker/group projected builders with
at most sixteen stable packet-slot/group materializers and ensures low-memory
multi fallback does not activate parallel-only JSONL row representations.
`CONCURRENCY_SCALING_V39.md` moves sustained wide JSONL parse/materialization
into bounded row packets while retaining column fan-out for microloads.
`CONCURRENCY_SCALING_V40.md` adds an immutable eight-byte top-level field index
so workers reuse source-ordered validation instead of reparsing each root
object, with row-atomic memory fallback and exact single/multi error precedence.
`CONCURRENCY_SCALING_V41.md` moves validation and token capture into bounded
packets on the shared operation arena. `CONCURRENCY_SCALING_V42.md` emits
deferred raw `RowRef` values once and delays chunk-segment ownership state until
a JSONL record actually crosses an input chunk. `CONCURRENCY_SCALING_V43.md`
keeps the v42 output policy through eight workers, then gives eligible
fixed-cost wide JSONL output up to the high half of a sixteen-worker arena.
`CONCURRENCY_SCALING_V44.md` keeps that high-core lane stable from its
first packet, removing work-item admission sampling and the four-to-eight
executor drain/recreation barrier while leaving the one-through-eight-worker
path unchanged.
`CONCURRENCY_SCALING_V45.md` removes local high-core head-of-line blocking:
high-half workers may select the earliest dedicated output-lane task ahead of
broad upstream backlog, while a compile-time specialization preserves the exact
FIFO hot path through eight workers.
`CONCURRENCY_SCALING_V46.md` extends that bounded preference to compatible
remote stealing without a queue scan. `CONCURRENCY_SCALING_V47.md` removes the
executor-wide result mutex from normal arena worker completion: each active
ordinal publishes through its own bounded slot while strict source-order
consumption and the single-thread inline path remain unchanged.
`CONCURRENCY_SCALING_V48.md` removes the duplicated coordinator-mutex
acquisition from each arena submission above eight workers: high-core packets
perform one authoritative bounded reservation, while one-through-eight workers
keep the v47 path. `CONCURRENCY_SCALING_V49.md` batches completed-task telemetry
inside each high-core worker and publishes fixed-size aggregates, removing most
per-task writes to shared cache lines while preserving exact post-drain totals.
`CONCURRENCY_SCALING_V50.md` removes the repeated worker-start lock after safe
publication, turns empty idle-worker searches into O(1) negative lookups, and
avoids high-core epoch writes or notifications when the target is already
running and no compatible helper is idle.
`CONCURRENCY_SCALING_V51.md` removes the `effective_workers + 2` high-core
submission cap and uses the reorder capacity already reserved by the execution
policy, keeping more workers fed under skew without increasing bounded memory.

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

On the reference Linux runs documented in `THREADING_TODO.md`, compact parallel
inference improved a nested JSONL probe by about 1.4x-1.9x once the batch reached
roughly 8,000 rows, while flat scalar inference stayed on the serial scanner and
remained near parity. Native text-output preparation improved by roughly
2x-6x. In the final 60,000-row repeated-median matrix, isolated nested inference
improved by 1.96x, text-fragment preparation by 2.00x-3.69x, complete nested
JSONL pipelines by 1.40x-1.46x, and scalar pipelines by 1.41x-1.42x. Tiny
workloads can still be slower because pool startup and ordered coordination
dominate. The final focused 30,000-row Snappy run measured 2.32x for isolated
16-column Parquet output, 1.34x for a nested JSONL-to-Parquet pipeline, 1.33x
for the scalar pipeline, and 0.91x for narrow four-column output that the
adaptive policy keeps serial. A separate
120,000-row, 16-column matrix measured 1.64x uncompressed, 1.53x Snappy, and
2.57x GZIP speedups. v65 additionally keeps the native Parquet reader's
private worker file handles alive across bounded windows of the same stream.
v66 then groups eligible fixed-cost columns into one ordered range task per
worker and window. On the 400,000-row, 32-column, 13-window fixture, v66 measured
650.316 ms (1 worker), 412.342 ms (2), 308.799 ms (4), and 258.860 ms (5):
1.58x, 2.11x, and 2.51x speedups. Against v65, paired medians improved 10.38%,
17.34%, and 12.88% at 2/4/5 workers while RSS remained effectively unchanged.
v67 removes the native CSV scalar/string JSON round trip and replaces JSON-object
packet estimates with conservative CSV cell bounds. On the 15,000-row,
96-column JSONL-to-CSV fixture it reduced output tasks from 116 to 86 and paired
wall medians by 3.90%, 1.60%, and 0.88% at 2/4/5 CPU affinity. The v67-only
curve measured 917.226, 370.268, 217.173, and 211.435 ms at 1/2/4/5 CPUs, so the
available-host curve remained monotonic. v68 extends the direct CSV path to
binary, temporal, decimal, and duration Arrow scalars, reusing the authoritative
native formatters without constructing and decoding temporary JSON strings. On
the 20,000-row, 64-temporal-column fixture, paired wall medians improved by
4.65% at 2 CPUs and 14.51% at 5 CPUs; repeated-operation 4-CPU process medians
retained a positive 11.29% signal despite higher host variance. The v68-only
curve measured 833.077, 341.542, 241.164, and 236.188 ms at 1/2/4/5 CPUs.
v69 compacts generic nested-inference keys into one packet-local byte table,
shrinking each evidence node from 56 to 24 bytes and resolving each distinct
key into the global interner only once per packet. On the 24,000-row nested
fixture, preparation-only paired medians improved by 11.55% at two CPUs and
10.10% at four while tracked peaks fell by 58.44% and 59.94%. The complete
v69 curve on the four-CPU cgroup measured 5,483.968, 3,195.261, and 1,770.876
ms for 1/2/4 CPU execution, or 1.00x/1.72x/3.10x.
v70 further compacts generic preorder evidence from 24 to 16 bytes per node
and from 24 to 12 bytes per row. It keeps complete shape validation, retains
the validated statistics traversal below four workers, and selects a
compile-time trusted statistics specialization at four or more workers. Long
ABBA inference blocks improved by 1.76% at two CPUs and 3.80% at four while
tracked peaks fell by 28.86% and 23.91%. The v70 complete curve measured
1,028.569, 486.993, and 297.489 ms for 1/2/4 CPU execution, or
1.00x/2.11x/3.46x.
v79 reduces wide fixed-dominant CSV packet planning to a schema-derived base
plus a bounded variable tail and raises the eligible output ceiling from four
to eight workers at 16 CPUs and sixteen at 32 CPUs. On the available four-worker
cgroup, a 30,000-row by 48-column JSONL-to-CSV fixture measured
3,373.583/552.311 ms for single/multi, or 6.11x, with identical logical hashes.
The isolated planner A/B was neutral within noise at four workers; the high-core
ceiling remains the physical 8/16/32-CPU validation frontier. Polars conversion
now requests `rechunk=False` and falls back only when that keyword is unsupported.
v76 adds pure-Python row iterables to that coverage matrix. One-shot generators
are consumed by a native iterator encoder in batches of at most 4,096 rows,
replacing one Python/ABI transition per row and feeding the existing JSONL,
inference, materialization, and output stages. Against the previous one-row
iterator route on 50,000 rows by 24 columns, paired wall medians improved by
37.18% at two CPUs and 43.24% at four; the v76 1/2/4-CPU curve measured
1,280.833/676.611/534.856 ms, or 1.00x/1.89x/2.39x. Sequences also account their
encoded source bytes during the first native pass, removing an 802 ms duplicate
Python `json.dump` preflight in the same fixture.
v71 makes concurrency coverage explicit for every supported input and output.
CSV now decodes already-framed records in ordered upstream tasks, while
`xml_row_tag` parses sufficiently large independent elements in the shared
operation arena. Quote-aware CSV framing, XML tag framing, one-row JSON/XML
parsers, and tiny workloads remain serial when partitioning would be slower.
On the available four-CPU cgroup, the wide quoted CSV probe measured 1,860.479,
1,727.688, and 1,680.334 ms at 1/2/4 CPUs; large row-tag XML measured 841.951,
672.784, and 529.832 ms. A machine-readable coverage matrix and telemetry tests
now fail if a public format loses its concurrent stage.
v72 strengthens JSON and JSON-array input: complete top-level values are framed
serially but parsed authoritatively only once in operation-arena workers. Flat
scalar packets of at least 64 rows build Arrow columns directly. Against v71,
24,000-row by 48-column arrays improved paired wall medians by 8.81% through
`json_array` and 9.46% through top-level-array `json`, while tracked peaks fell
by 55.66% and 55.36%. Their v72 1/2/4-CPU curves were
1,032.033/699.426/588.782 ms and 1,038.937/726.548/571.042 ms. One-row JSON
documents retain the one-task fallback. pandas output now also receives the
policy explicitly through `to_pandas(use_threads=False/True)`.

v86 moves invariant arena admission geometry out of the per-packet path. Each
`OrderedExecutor` prepares one `TaskArenaSubmissionPlan` containing its worker
width, lane begin/end, compatible-worker mask, cursor, and alternative queue
offset, then reuses it for every task. The legacy direct arena API still builds
the same plan on demand. In a paired 20,000-task arena-completion A/B pinned to
four CPUs, medians improved from 54.613 to 53.865 ms at two workers (**1.37%**,
6/10 wins) and from 68.835 to 64.824 ms at four workers (**5.83%**, 7/10 wins).
These measurements isolate cheap ordered packets; heavier parsing or compression
reduces the end-to-end percentage.

v85 moves stolen-task accounting from one operation-global atomic to the
physical worker that performed the steal. Because that worker is the only
writer, both arena diagnostics and performance telemetry publish with relaxed
load/store operations and aggregate only when statistics are read. In a
controlled A/B forcing exactly 20,000 steals, two workers were neutral within
noise (4.715 versus 4.768 ms), while four workers improved from 9.652 to
9.116 ms, or **5.55%**, winning 8/10 paired runs. These are isolated scheduler
figures rather than universal end-to-end gains.

v84 makes queue visibility and worker initialization transition-driven inside
the shared `OperationTaskArena`. A worker queue publishes `nonempty_mask` only
on its 0-to-1 depth transition, and a reserved worker publishes
`initialized_mask` only after acquiring its first real local packet. The
reservation flag is cached by the owning worker and refreshed only at startup
and park/wake boundaries. This removes repeated operation-global RMWs from
every input, validation, materialization, Arrow handoff, and output task while
preserving lanes, stealing, targeted wakes, exact counters, and bounded memory.
On the available host, an isolated 200,000-task A/B measured median scheduler
reductions of 6.58% at two workers and 2.92% at four workers; these are scheduler
figures rather than universal end-to-end speedups.

These are workload-specific measurements, not a universal crossover guarantee.

For architecture and ownership, see [RESPONSIBILITIES.md](RESPONSIBILITIES.md).
For the deterministic threading roadmap, see [THREADING_TODO.md](THREADING_TODO.md).

## [License](#index)

Apache License 2.0. See [LICENSE](LICENSE).

v94 makes queue visibility publication transition-exact: failed local probes
and stale remote victim checks no longer repeat an operation-global empty-mask
RMW. The existing visibility ordering is preserved while queue mutexes remain the authoritative synchronization boundary. All 56 input/output combinations,
including pure-Python input and analytical outputs, inherit the common arena
stage.

v95 makes successful-steal accounting single-store end to end. Each physical
worker and its optional telemetry shard increment a plain writer-local counter
and publish one relaxed atomic snapshot, removing the redundant atomic load
without changing stealing, diagnostics, or the 56-pair concurrency contract.

v100 makes `ExternalTaskLease` use its owner pointer as the single ownership
sentinel. Moves and successful completion now clear one pointer instead of two,
while abandonment, cancellation, worker-sharded drain, and all 56 input/output
contracts remain exact. The isolated ownership benchmark improved 4.89% in
paired median and the Release ABI3 module became 4,096 bytes smaller.

v101 makes the external-task abandonment callback a compile-time lease policy
and removes the duplicate completion-shard capture. Each arena-backed ordered
closure carries 16 fewer bytes than v100 while preserving exactly-once
completion/abandonment and all 56 input/output contracts. The isolated lease
benchmark improved 2.38% in paired median with 16/21 wins, and the identically
built ABI3 module became another 944 bytes smaller.

v102 removes the final `void*` type erasure from external-task leases. The owner and abandonment member are compile-time typed while the 16-byte lease, exactly-once drain, and all 56 input/output contracts remain unchanged.

v103 combines arena cancellation and fatality into independent bits of one monotonic terminal word. Every arena-backed ordered result now takes one acquire snapshot instead of two global loads while preserving cancellation precedence, exact drain and all 56 input/output contracts.

v114 isolates each worker's `running` publication on its own cache line instead
of sharing ownership with queue, submission, and steal snapshots. The bounded
layout change applies in the shared `OperationTaskArena`, so all 56 input/output
combinations inherit it without format-specific scheduling paths.

v115 replaces full-lane modulo scans in arena admission and idle-helper
selection with exact round-robin traversal of only the set candidate bits. A
fully eligible lane retains a direct preferred-worker fast path. The change is
implemented in the shared `OperationTaskArena`, so all 56 input/output
combinations inherit it without format-specific scheduling paths.

v116 separates the operation-wide upstream, output, and all-lane ticket cursors
from one another and from exact worker activity accounting. The internal
cache-line layout change keeps existing atomic operations and public behavior
unchanged while benefiting all 56 input/output combinations.

v117 keeps each worker `wake_epoch` on an independent cache line by aligning
the immediately following queue control block as well as the epoch itself. The
layout-only change preserves targeted wakes, park predicates, helper
notifications, queue ordering, and all 56 input/output contracts.

v118 replaces repeated lane-origin division inside one arena submission with a single normalization plus bounded exact advancement. Non-power-of-two lanes and size_t wraparound retain identical round-robin behavior across all 56 input/output contracts.

v119 removes repeated `countr_zero`-driven visibility-domain discovery from high-core stealing. The arena now loads its immutable 2-4 physical queue-visibility shards directly and applies the same initialized-worker mask afterwards, preserving exact eligibility across all 56 input/output contracts.

v120 compacts each queued task's validated lane bounds from two machine words to two bytes under the existing 32-worker ceiling. On the validated libstdc++ ABI the packet shrinks from 72 to 56 bytes, improving deque density for enqueue, local dequeue, and compatible stealing while all 56 input/output routes retain identical lane semantics.

The post-v120 high-core policy makes the 32-worker frontier reachable for
eligible fixed-width flat JSONL. Short moderate-cost schemas may use the
complete operation arena; sustained stages grow geometrically from half of a
16-worker arena to half of a 32-worker arena. JSONL output uses the matching
high half. Variable-width and ultra-wide schemas retain their conservative
ceilings. On the fixed-affinity 20,000-row, 64-column matrix, 16→32 improved
Arrow-stream throughput by 23.0% and complete JSONL-to-JSONL throughput by
20.8%; the 50,000-row sustained matrix improved by 36.3% and 33.2%,
respectively.
