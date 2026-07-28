# Sanitization and schema heuristics

This document describes how `schema-sanitizer` turns observations into a
stable schema, how that schema changes across runs, and how the embedded
registry and BigQuery sidecar participate in an incremental pipeline. For the
public API and option reference, start with [README.md](README.md).

## Processing model

A conversion follows the same logical stages for analytical and file outputs:

1. Select and validate the source format and input mode.
1. Scan source shapes and scalar evidence with bounded buffers.
1. Infer a logical schema and sanitize field names recursively.
1. Reconcile the inferred schema with the previous registry.
1. Apply the requested schema mode and recursive column order.
1. Materialize rows against that final schema.
1. Append the four generated ETL fields.
1. Return the updated registry, drift events, and diagnostics.

The inference pass considers all selected input rows; it is not a sampling
algorithm. Directory and partition workflows keep memory bounded by streaming
or staging sources incrementally rather than by reducing the inference set.

## Field-name sanitization

Field names are sanitized at the root and inside every struct, including
structs nested in lists.

### `lower_alpha`

This is the default policy. ASCII letters are lowercased and every other byte
is removed:

```text
Customer ID  -> customerid
price_$      -> price
123          -> field
```

If nothing remains, the base name is `field`.

### `lower_snake`

ASCII letters are lowercased. Digits and underscores are retained. Other
characters become underscores, adjacent/trailing underscores are collapsed or
removed, and a leading digit is prefixed with `field_`:

```text
Customer ID  -> customer_id
price-USD    -> price_usd
123_code     -> field_123_code
```

### `preserve`

The source spelling is retained. This is useful when an external schema owns
the names, but it also leaves responsibility for downstream naming constraints
with the caller.

### Sibling collisions

Two source names can sanitize to the same base. Under `lower_alpha` and
`lower_snake`, colliding siblings receive a deterministic hash-derived suffix.
The assignment is based on source names rather than encounter order, so the
same sibling set produces stable output names. If `preserve` is selected, names
are not rewritten.

`scalar_object_key` is treated as an intentional schema key and remains stable
when it is introduced for scalar/struct reconciliation.

The names `schema_registry`, `schema_drifts`, `source_file`, and
`ingestion_timestamp` are reserved at the root. A source schema containing one
of those names cannot also receive generated metadata and the conversion is
rejected with a generated-column collision error.

## Scalar inference

The native logical scalar types are null, Boolean, signed 64-bit integer,
64-bit float, UTF-8 string, timestamp, date, and time.

Native JSON numbers and booleans carry their JSON type. CSV cells, XML text,
and quoted JSON values begin as strings and are coerced only by explicitly
enabled parsers. String parsing priority is:

1. configured Boolean tokens;
1. timestamp patterns;
1. date patterns;
1. time patterns;
1. integer parsing;
1. float parsing;
1. otherwise string.

Each parser tries the source value exactly, then retries once after removing
surrounding ASCII spaces, tabs, line breaks, form feeds, and vertical tabs. A
failed parse leaves the original value unchanged. Exact configured tokens or
patterns therefore take precedence over the trimmed retry.

### Mixed scalar types

Inference combines evidence within one field as follows:

| Observations | Inferred type |
|---|---|
| Only one non-null scalar kind | That kind. |
| Integers and floats | `float64`. |
| Any other mixture of scalar kinds | UTF-8 string. |
| Only null values | No type evidence; the field is omitted unless a source schema or registry supplies its type. |

Across registry-backed runs, an existing float accepts an integer-only batch.
An existing integer is promoted to float when float evidence appears. Durable
integer/float version families are normalized so an obsolete separate numeric
variant is not retained merely because different batches observed integer and
float values.

Boolean token matching is case-insensitive. True and false sets must not
overlap. Float grouping is strict: after the first group, every grouped section
must contain exactly three digits, and decimal and grouping separators must be
different ASCII punctuation characters.

### Temporal values

Built-in ISO timestamp, date, and time recognition is disabled by default.
Custom patterns operate independently from the corresponding `parse_iso_*`
flag. Timestamps are inferred internally and then materialized using
`timestamp_precision`; microseconds are the default because they are compatible
with BigQuery.

Invalid calendar or clock values do not become temporal values. If no other
enabled scalar parser accepts them, they remain strings.

## Nulls, empty containers, and missing fields

Objects infer as structs and arrays infer as lists. Struct fields are nullable
because a field may be absent from another row.

Inference treats an explicit null, an empty array, and an empty object as
having no type evidence. Missing fields likewise provide no evidence. The
effect depends on whether any other source of type information exists:

| Observations for one field | Inferred-schema behavior | Materialized value |
|---|---|---|
| Only `null` | Field is omitted. | No output column. |
| Only `[]` | Field is omitted. | No output column. |
| Only `{}` | Field is omitted. | No output column. |
| Missing from every row | Field is absent. | No output column. |
| Null/empty observations plus a non-empty value | The non-empty value determines the field type. | Null and empty-container observations become null, except the typed nested cases below. |
| Existing registry or explicit schema | The established type is retained. | A root null, `[]`, or `{}` becomes null without changing the schema. |

This policy is recursive. An object such as `{"wrapper":{"child":null}}`
and a list such as `{"items":[null]}` provide no usable descendant type
evidence. If those are the only observations, `wrapper` and `items` are both
omitted. The same applies to descendants containing only empty objects or
empty lists.

Once another row or a schema establishes the type, values are handled against
that type:

- a scalar null materializes as null;
- an empty list materializes as null, not `[]`;
- an empty object materializes as null, not an all-null struct;
- `[null]` for an established list materializes as a list containing a null
  element because the list itself is not empty;
- `{"child":null}` for an established struct materializes as a non-null struct
  whose `child` is null because the object itself is not empty.

A typed input schema is evidence even when its data pages contain only nulls.
Consequently, a typed all-null Parquet or Arrow column is retained with its
declared type. A registry-backed strict conversion behaves the same way: the
registered field remains, nullish input does not create drift, and the schema
generation does not change. Unknown null or empty-container fields in strict
input behave like absent fields and do not trigger the extra-field error.

For schema-less inputs, this prevents the first empty or all-null partition
from guessing `string`, `list<string>`, or an empty struct. A later partition
with real evidence receives the original sanitized field name rather than an
unnecessary versioned name.

### Scalar and list reconciliation

If a path is observed as a list, a scalar at that same path is treated as one
list element. This keeps a repeated field repeated while accepting a singleton
source representation. The `scalar_wrappings` diagnostic counts these cases.

Typed lists of scalars and lists of structs are supported. Lists of structs may
contain their own repeated fields. A direct `list<list<T>>` shape falls back to
`list<string>`; this boundary avoids an unsupported repeated-list contract
while preserving nested lists that are fields of a struct element.

### Scalar and struct reconciliation

If a path is observed as a struct and another row supplies a scalar, the scalar
is placed below `scalar_object_key`, whose default is `default_key`:

```text
5  -> {"default_key": 5}
```

The same rule is used recursively during registry merging. If the existing
`default_key` child can evolve compatibly, it is reused; otherwise normal field
versioning applies at the relevant path.

The reverse case is deliberately conservative: an existing scalar does not
absorb an incoming struct. If no compatible historical member exists, the
struct becomes a new version in the field family.

## Depth limits and flattening

Arrow depth counts both structs and list wrappers. Parquet/BigQuery RECORD
depth counts structs but not list wrappers. Before expanding a nested value,
the engine checks its complete descendant shape against both limits.

When either `arrow_max_depth` or `parquet_max_depth` would be exceeded, the
nested value is serialized into a string-compatible field. `_flattened` is
appended before the selected field-name policy is applied, so `a_flattened`
remains so under `lower_snake` and becomes `aflattened` under `lower_alpha`.
The `flattened_fields` diagnostic records the event. This is deterministic for
a given value shape and pair of limits.

## Schema merging

Registry merging works recursively through root fields, struct children, and
list element types.

Compatible changes reuse the current field:

- null is absorbed by any established scalar type;
- an all-null field can adopt later non-null evidence;
- integer and float merge to float;
- struct fields merge recursively;
- list elements merge recursively;
- an incoming scalar can be wrapped into an established struct;
- historical exact shapes are reused during reprocessing.

Other scalar changes, scalar-to-struct changes, and incompatible element
changes are not silently cast. They are represented by another member of the
same version family.

### Version families

The original sanitized name is the family base. Incompatible shapes receive a
monotonic suffix:

```text
amount
amount_v2_string
amount_v3_struct
tags_v2_integer_array
```

Semantic suffixes include `null`, `boolean`, `integer`, `float`, `string`,
`timestamp`, `date`, `time`, `struct`, and `<type>_array`.

When new input arrives, the registry first looks for an exact historical shape
in the family, newest to oldest. This makes past-date reprocessing route to the
original compatible column rather than clone it. If there is no exact match,
the newest recursively compatible member is evolved. Only when no family
member is compatible is the next version number created.

Each versioned output field is nullable: rows for other family members carry a
null in that column.

### Schema modes

`schema_mode="additive"` is the normal incremental mode. It preserves the
registered contract, recursively adds compatible fields, promotes numeric
types where safe, and creates versions where necessary.

`schema_mode="strict"` requires a non-empty registry-derived schema. An
observed field absent from the contract is rejected. The contract supplies the
materialized schema; strict mode is appropriate after warm-up or whenever the
producer is expected to remain inside a known schema.

A partition pipeline may optimistically write against a warmed strict schema
and retry a partition additively when that partition reveals genuine drift.
That orchestration policy lives above the converter; the converter itself
honors the requested mode for each call.

## Column order

Ordering is applied recursively after schema reconciliation.

- `alphabetically` sorts source fields by sanitized output name at every
  struct level. Adding a field can therefore change positions.
- `schema_contract_first` keeps registered fields in their registered order,
  then appends new fields alphabetically. With no base contract, encounter
  order is retained for that initial struct.

The four generated ETL fields are not source fields. They are appended after
ordering and always remain at the root tail in their canonical order. This
same root order is used in Arrow tables, CSV/JSONL/Parquet materialization, and
BigQuery external-table DDL.

## The schema registry

The registry is durable JSON returned in both parsed and serialized form. Its
top-level document contains:

| Key | Meaning |
|---|---|
| `field_name_policy` | Naming policy used to build the registry. |
| `registry_version` | Registry document format version; currently `1`. |
| `schema_generation` | Monotonic count of registry-changing runs. |
| `canonical_schema` | Full logical schema used to materialize output. |
| `variants` | Source-path families and their historical output versions. |

Each entry under `variants` contains a `versions` list. A version records its
`output_name`, serialized logical `schema`, and whether it is the current
preferred member. For multiple shapes at one path, list is preferred over
struct and struct over scalar when choosing the current compatibility target;
historical members remain available for exact-shape routing.

`schema_generation` increments only when the merge emits drift. Reprocessing
input already covered by the registry leaves the generation unchanged.

Pass the registry mapping or JSON from one result into the next call. JSON is
the durable interchange format; the optional native compiled state held by
pipeline helpers is a process-local optimization and is not a replacement for
the JSON document.

## Drift events

`schema_drifts` is a JSON list for the current conversion. Each event contains:

| Key | Meaning |
|---|---|
| `detected_at` | Detection timestamp. |
| `source_path` | Logical source path; `[]` denotes a list element. |
| `output_name` | Output field affected by the change. |
| `drift_type` | `newly_added`, `type_promoted`, or `new_version_generated`. |
| `previous_schema` | Previous logical type, or null for a new path. |
| `new_schema` | Resulting logical type. |

Nested drift is reported at the narrowest affected path. The registry remains
the canonical state; drift records are the per-run audit trail explaining how
that state changed.

## Generated ETL fields

All converters append exactly these top-level fields, in this order:

| Position from tail | Field | Population |
|---:|---|---|
| 4 | `schema_registry` | Updated registry JSON on the first output row; null afterward. |
| 3 | `schema_drifts` | Current drift JSON on the first output row; null afterward. |
| 2 | `source_file` | Exact producing local/cloud source on every row. |
| 1 | `ingestion_timestamp` | UTC conversion timestamp on every row, materialized as microseconds. |

For directory inputs, `source_file` identifies the specific child that produced
the row. Storing registry and drift payloads only once avoids repeating large
JSON documents while still making every output file self-describing.

The canonical order is enforced independently at metadata injection, schema
finalization, physical Parquet output, and BigQuery schema translation. Hive
partition fields are declared separately by BigQuery and do not interrupt this
physical field order.

## Registry probing and warm-up

A registry probe runs inference and reconciliation without producing the final
clean dataset. Pipeline warm-up combines all selected source plans into one
logical additive inference input and returns a registry state.

Warm-up properties:

- it always merges additively, even if normal writes use strict mode;
- it scans every selected source rather than sampling;
- it can start from an existing embedded BigQuery registry;
- it writes no normal partition output;
- its resulting JSON can be compiled once and reused by later partition calls.

Warm-up dates or hours need not overlap the write range. This makes it possible
to establish a broad contract first and then materialize a smaller production
interval with stable schemas.

## BigQuery external-table schema

The BigQuery integration converts the final Arrow schema to Standard SQL types
and emits explicit `CREATE OR REPLACE EXTERNAL TABLE` DDL. Hive partition field
names are removed from the Parquet-derived column list because BigQuery exposes
them through `WITH PARTITION COLUMNS`.

Source-data ordering follows the final Arrow schema unless the caller
explicitly requests alphabetical sorting in the DDL helper. In either case,
the four ETL fields are separated from source fields and appended in canonical
order:

```text
schema_registry, schema_drifts, source_file, ingestion_timestamp
```

Arrow dictionaries are translated by their value type. Structs become
`STRUCT`, lists become `ARRAY`, map-like values become an array of key/value
structs, Arrow null becomes `STRING`, and unsupported types fail soft to
`STRING` with a warning.

## BigQuery registry sidecar

The registry embedded in the Parquet/external table remains authoritative.
Without a sidecar, finding the latest registry means querying non-null
`schema_registry` values and ordering them by ingestion timestamp, schema
generation, and the configured Hive partition columns.

The optional native BigQuery sidecar accelerates that lookup. It has exactly
two columns:

```sql
external_table_name STRING NOT NULL
last_ingested_partition STRING NOT NULL
```

`external_table_name` is `project.dataset.table`. The partition pointer is a
slash-separated Hive key in configured partition-column order. The bootstrap
sequence is:

1. Check that the sidecar exists and is a native `BASE TABLE`.
1. Read the row for the target external table.
1. Parse and validate the stored Hive key.
1. Query the embedded registry in only that partition.
1. If any step is unavailable or invalid, scan the external table normally.

The sidecar is updated only after selected outputs are written and the external
table is created or replaced. `CREATE TABLE IF NOT EXISTS` followed by an
idempotent `MERGE` inserts or updates the pointer. A random historical rerun
therefore points at the last partition completed by that run, not necessarily
the chronologically greatest date.

The sidecar is an optimization, never a second schema registry. Losing it only
causes a slower fallback lookup.

## Operation-wide native task arena

One `multi` operation owns one native task arena for CPU work. Inference,
materialization, CSV/JSONL packet encoding, and native Parquet column preparation
borrow lanes from that arena instead of constructing stage-local pools. The
physical ceiling is the effective worker count derived from CPU capacity and the
single memory budget. A stage may use fewer workers when its packet count, schema
complexity, or memory reserve cannot amortize all N.

Worker identities are stable for worker-private parser, builder, and compression
state. Upstream lanes occupy the low physical indices and output lanes the high
indices; two narrow adjacent stages can therefore overlap on complementary
workers while the operation-wide active-task peak remains at or below N. A stage
that needs the full arena uses all workers. Workers are started lazily on their
first submitted task, avoiding N-thread startup for small or serially profitable
operations. Worker busy streaks retain exact live active accounting. Each
physical worker also keeps a private monotonic high-water mark for active-count
values it has already offered to the operation-wide peak, so later streaks at
or below that value avoid a redundant shared maximum-counter load without
weakening exact peak diagnostics. Each submission keeps a preferred physical queue for cache locality,
but an idle worker may steal from the back of another queue only when the task's
lane contains the thief. This removes head-of-line stalls behind unusually slow
rows or columns without aliasing worker-private state. The scheduler wakes only
the preferred worker for an empty idle queue; compatible peers are notified when
work is placed behind an active or already queued task. Above eight workers,
a high-half worker may select the earliest local task dedicated to the high
output lane ahead of broad upstream backlog on that same queue. This preference
is local, permits at most one consecutive bypass before forcing FIFO progress,
preserves FIFO among dedicated output tasks, never bypasses the first task
reserved during lazy worker startup, and does not alter stealing or ordinal
publication. Arenas of eight workers or fewer compile the original strict local
FIFO path without the preference logic. Queue/reorder bounds and the operation
memory budget are unchanged.

The arena does not own commit order. Each stage retains its bounded ordinal
executor and reorder window, and coordinators remain the only owners of schema
reduction, diagnostics merge, Arrow publication, file byte order, and final
commit. Executor destruction waits until its last arena callback has completely
returned; the completion counter is intentionally the callback's final
synchronized action so condition variables cannot be destroyed while a worker
still notifies them.

## Parallel-inference packet heuristics

Inference keeps the existing two-pass semantics for every row: first discover
container shapes, then apply scalar and nested statistics using the shapes known
at that exact source ordinal. `multi` does not let workers mutate the inference
context. Workers instead reparse JSON in private documents and emit one compact
preorder stream of immutable evidence nodes. Each node stores its subtree end,
so the ordered reducer can traverse children without allocating one vector per
value. Field names are interned only by that reducer, in canonical row and field
order.

Parallel inference is adaptive. The first frontend batch is profiled from a
bounded prefix of already-parsed values. The pool is selected only when the
batch contains enough nested values, enough rows/estimated bytes, at least two
effective workers, and at least 96 MiB in the policy's aggregate worker pool.
Materialized flat/scalar input, small batches, and lower-memory runs use the
reference scanner without creating an inference executor. Raw-only JSONL is the
exception: parsing is intentionally deferred to worker-private documents. Its
flat scalar packets retain sixteen fields inline and may grow through a tracked
packet-local PMR overflow up to 512 fields. Stable field order uses direct
positional updates; missing, reordered, or duplicate keys fall back to a local
lookup. Wider, nested, or long-key shapes retain generic tracked evidence. Eligible
flat scalar values are classified from the compact `ValueView` tag with one
exhaustive switch. Only object and array tags perform the empty-container check;
strings retain the configured scalar parser and unsupported containers retain
the generic evidence path.

The inference stage may use up to the operation's effective workers and
redistributes, rather than increases, the policy's worker memory among them.
Plan complexity, packet volume, and the aggregate reserve can narrow the active
lane without reducing the operation's available maximum. Input packets retain at most
256 rows and use the smaller of the normal packet target and one thirty-second
of the effective worker arena, reserving for the expansion from short JSON
tokens to evidence nodes and decoded keys. A single source row above that target
is processed by the reference scanner after all earlier parallel ordinals have
been reduced. Evidence packets have their own tracked PMR arena capped at one
effective worker arena, while the operation pool remains the aggregate hard
limit.

The reducer validates every row and subtree span before use, performs the shape
pass and statistics pass in order, and preserves `flattened_fields`,
`scalar_wrappings`, inferred byte counts, schema field order, and the earliest
source-order failure. Benchmarks therefore compare the exact logical-schema
payload and diagnostic JSON, not only decoded Arrow types.

## Text-output packet heuristics

Native CSV and JSONL output share the ordinal executor used by materialization.
The coordinator validates each Arrow batch before dispatch, assigns contiguous
row ordinals, writes the CSV header once, commits fragment bytes in order,
updates statistics, selects the earliest ordinal failure, and performs the final
flush. Workers only read immutable Arrow arrays and encode private strings; they
never mutate the output stream or shared statistics.

The output stage derives its limits from the same operation policy rather than
adding public controls. It may use up to the operation's effective workers,
retains at most one unfinished fragment per active worker, and caps each packet
at 1,024 rows. The byte
target is the native packet target derived from `memory_limit_bytes`. A capped
recursive estimator accounts for worst-case JSON escaping, CSV quoting, binary
expansion, dictionaries, lists, maps, and structs. A row that reaches the target
is isolated instead of allowing later rows to enlarge the fragment. For flat
fixed-cost JSONL structs whose validated Arrow batch reports exact zero null
counts, the conservative row estimate is prepared once from the schema and then
reused in O(1) per row. It is identical to the recursive estimate for eligible
rows, so packet boundaries do not change. Null-bearing, unknown-null,
variable-width, nested, dictionary, and CSV batches retain row-aware estimation.
Released fragment buffers are overwritten when hardened cleanup is enabled.

For flat JSONL structs with at least 24 fixed-cost scalar fields, operations
with more than eight effective workers use the high half of the operation arena,
clamped to four-eight output workers. That lane is admitted once at its complete
bounded width from the first packet; workers still start lazily, so a short
batch starts only as many threads as it has packets. This avoids a full ordered
executor drain/recreation between early Arrow batches. Hosts with eight workers
or fewer, variable-width fields, and nested output retain adaptive per-batch
admission.

The effective CPU ceiling is derived internally before worker counts are
calculated. It uses the minimum available signal among hardware concurrency,
process affinity, Linux cgroup v2/v1 CPU quota, the Windows process affinity
mask, and macOS active CPUs, with a floor of one. This keeps containers and
CPU-restricted processes from oversubscribing the host without adding a public
worker-count parameter or environment-variable override.

`single` executes the identical packet callable inline and creates no native
thread. `multi` pays a fixed pool/coordination cost, so it is intended for
substantial row counts, escaping, nesting, or wider records. The benchmark
harness records the observed crossover instead of embedding an unstable public
row threshold. Native Parquet now performs that separation: workers collect and
encode independent leaf columns into private artifacts, while one coordinator
assigns physical offsets, commits column chunks and page indexes in schema order,
and writes the footer/trailer once. The adaptive route remains serial for small
nested groups, narrow scalar groups, low-memory operations, or one effective
worker.

Path-based CSV, JSONL, and Parquet outputs are written to a unique sibling staging
file. The destination is replaced only after the writer and final flush/close
succeed. An existing destination and its permissions are preserved until that
commit point; failures remove the staging file rather than truncating the valid
output. This is atomic publication on the destination filesystem, not a promise
of crash-durable `fsync` semantics.

## Parquet route and storage heuristics

Local Parquet input prefers the native Arrow C Stream reader when the file
satisfies its contract. PyArrow Dataset is the compatibility fallback;
unfiltered local reads can additionally fall back to
`ParquetFile.iter_batches`. Filtered reads fail closed if Dataset cannot apply
the filter.

The native writer bounds rows, estimated uncompressed column bytes, and staged
page bytes. Row-group rows, row-group bytes, page bytes, reader windows, and
footer retention are derived together from the operation's single
`memory_limit_bytes`. They are intentionally not independent tuning knobs: this
keeps the derived limits mutually consistent and prevents one subsystem from
consuming the full operation budget in isolation.

In `multi`, Parquet may prepare up to the minimum of the operation's effective
workers, leaf-column count, and memory-supported compression candidates. The
row-group target is narrowed only for schemas likely to use the arena, reserving
space for collected values, prepared artifacts, and compression candidates.
Nested groups below 16,384 rows, flat groups with four or fewer leaves below
65,536 rows, and operations below the 96 MiB parallel reserve stay serial.
Wider scalar schemas use progressively lower row thresholds because independent
column compression amortizes the executor sooner. These thresholds are internal
safety/performance policy, not public tuning options.

Pages choose encodings by compressed size. Profitable repeated scalars use
dictionary encoding; signed integer and temporal pages may use
`DELTA_BINARY_PACKED`; high-cardinality string/binary pages may use
`DELTA_LENGTH_BYTE_ARRAY`; non-dictionary floats use `BYTE_STREAM_SPLIT`;
otherwise plain encoding is retained. GZIP is the public default, with Snappy
and uncompressed output also supported.

Detailed native-reader contract diagnostics and certification helpers are
implementation-facing adapter concerns. Their code ownership is mapped in
[responsibilities.md](responsibilities.md#parquet-adapter-and-contracts).

## Remote staging heuristics

Remote conversion uses provider-native asynchronous clients rather than
`pyarrow.fs`. Single files stream into replayable local spools. Directory
children are listed non-recursively, ordered deterministically, and packetized
by both file count and known bytes. In `multi`, one lazy operation context owns
the event-loop host from initial listing through staged downloads and final
remote output upload. Compatible aiohttp, S3, and Azure clients are pooled on that host for the
complete operation, while directory staging shares one global transfer
semaphore. Incompatible HTTP header sets or Azure accounts receive distinct
pool entries. The paired registry route
retains a policy-bounded probe prefix and transfers ownership to materialization;
a directory that fits in that prefix is downloaded once, while later packets are
replayed by a second bounded pass. Every staged packet reserves known or
estimated bytes from the operation-owned temporary-storage pool before prefetch,
resizes that lease to its exact on-disk size, and releases it only when the
consumer closes or cancellation drains the packet. Final remote-output spools
hold an exact lease through upload. S3 switches to bounded multipart parts for
large spools, GCS uses sequential resumable ranges and reconciles the durable
provider offset after a lost response, and Azure passes a memory-bounded
concurrency window to its block-blob uploader. Small objects and generic HTTP
remain single-request paths. Multipart completion or resumable finalization is
the only publication point; failure drains active work and aborts partial remote
state while preserving the completed local spool until cleanup.

Known-size packet targets reserve only a fair share of the operation budget
across the configured lookahead. A file larger than the target is isolated as a
single packet. Unknown sizes use a deterministic fair-share estimate and remain
file-count bounded; actual transfer-size checks still apply.

### Partition source lookahead

Static partition pipelines in `multi` may prepare exactly one immutable
source for partition `N + 1` while `N` is converting or publishing. This is
a dedicated one-slot preparation executor, not another general worker pool,
and it is enabled only when the derived policy has more than one effective
worker. The child partition context shares the operation temporary-storage
permits and remote coordinator, but captures its own fixed timestamp.

The trigger is deliberately conservative. Fully prepared local inputs may
start lookahead before CPU conversion. Lazy remote-native inputs wait until
the current writer has consumed them. Remote outputs wait until their exact
spool reservation is held. The lookahead never performs schema inference,
registry mutation, callbacks, or publication for the next partition. A
failure is retained until that partition reaches its ordinal, and `N + 2`
is never submitted early. If the shared temporary-storage window is occupied,
preparation is deferred and retried synchronously at the correct ordinal.
Callable per-partition option factories and all `single` executions remain
fully sequential so evaluation order and the one-host-thread contract do not
change.

Provider routing is URI-based: GCS uses its JSON API and Google ADC, S3 uses the
normal AWS credential chain through `aiobotocore`, Azure uses asynchronous Blob
clients and `DefaultAzureCredential`, and HTTP(S) supports single files but not
portable directory listing. File outputs are staged locally and uploaded only
after successful conversion.

## Flat JSONL inference parsing

Flat JSONL inference uses a worker-private single-pass root-object visitor when
one packet remains eligible for the bounded scalar aggregate. The visitor does
not calculate key hashes or construct numeric values because inference needs
only key order, scalar category, and decoded string content. Integer tokens are
classified against exact signed 64-bit lexical bounds; floats and out-of-range
integers still use strict floating validation. Empty containers contribute no
evidence, while non-empty nested values force the canonical generic fallback.
This is an internal execution heuristic and does not change public parsing or
resource options.

## Bounded output-lane progress

The shared operation arena treats output as a bounded scheduling hint, not a
separate unbounded priority pool. Above eight workers the established high-core
policy may promote one dedicated upper-lane output task before returning to
FIFO. On four-to-five-worker arenas, the same one-bypass rule is enabled only
for explicitly classified output in shallow queues of at most four tasks.
Six-through-eight-worker arenas retain FIFO. Remote helpers inspect only the
front of the already-selected victim queue. This keeps scheduling memory fixed,
avoids global queue scans, and prevents output progress from starving upstream
materialization.

## Validation-certified positional JSONL materialization

For flat JSONL direct-scalar plans, parallel validation may certify that one
row's unescaped root keys exactly match the compiled plan in name, count, and
order. Materialization then consumes the already validated value tokens by
position and skips key decoding, hashing, `FieldRef` construction, and plan
lookup. The certificate is row-local and packet-bounded. Escaped, missing,
reordered, duplicate, nested, or variant-bearing rows use the canonical lookup
path. This optimization does not change error policy, ordering, memory limits,
or public options.

## Direct lexical positional JSONL materialization

After validation certifies that a flat JSONL row exactly matches a direct-scalar
compiled plan, materialization may convert lexical tokens directly when their
form already matches the destination type. Exact null, boolean, integer,
floating, unescaped UTF-8, and integer temporal values bypass generic value
construction. Any lexical/type mismatch, escape, coercion, nested value, or
error condition returns to the canonical parser and conversion policy. The fast
path borrows only from the packet-owned validated row for the duration of one
append and does not add cross-row state or public tuning options.

## Compact bounded arena packet metadata

The operation arena accepts at most 32 physical workers. Queue packets therefore
store their already validated lane begin/end bounds as unsigned bytes rather
than machine-sized integers. Submission plans and public arithmetic remain
`size_t`; narrowing occurs only after the plan has been clamped to the arena
ceiling. Workers widen the bytes before compatibility and relative-index
calculations. This reduces queue-packet footprint without changing lane
eligibility, output preference, stealing order, telemetry, or public options.
